"""Export a referentially complete DICOM sample from the MedObjects cluster.

Selects whole patients (every study of a center-scoped patient ID): first the patients
opened in viewer sessions, then patients that bring in structural variants the sample does
not have yet, then a round-robin by center until the study budget is reached. Exports every
instance of those studies, the complete trash events that snapshot them (moves, splits,
fusions) with the audit rows on them, a spread of standalone deletion events, and the
configuration they reference. Selection stops adding carriers once the export would exceed
the document budget.
Secrets are redacted before anything is written. The connection string is read from
MONGODB_SAMPLE_DICOM_URL (env file or environment) and never printed.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from bson import json_util
from dotenv import dotenv_values
from pymongo import MongoClient


URI_VAR = "MONGODB_SAMPLE_DICOM_URL"
CLINICAL_DB = "MedObjects"
SITE_DB = "rcloudv3-site"
STUDY_UID = "0020000D"
SERIES_UID = "0020000E"
INSTANCE_NUMBER = "00200013"
CONFIG_COLLECTIONS = [
    "cfg_anon_profiles",
    "cfg_anon_templates",
    "cfg_auth_clients",
    "cfg_auth_tenants",
    "cfg_centers",
    "cfg_hl7_mappings",
    "cfg_nodes",
    "cfg_setting_definitions",
    "cfg_values",
    "cfg_wl_aet2stations",
    "devices",
]
EXCLUDED = {
    f"{CLINICAL_DB}.anon_pseudonym_map": "re-identification ciphertext; references no exported study",
    f"{CLINICAL_DB}.anon_export_jobs": "empty in source",
    f"{CLINICAL_DB}.anon_export_checkpoints": "empty in source",
    f"{CLINICAL_DB}.cfg_visibility": "empty in source",
    f"{CLINICAL_DB}.hl7_messages": "empty in source",
    f"{CLINICAL_DB}.patient_xref": "empty in source",
    f"{CLINICAL_DB}.worklist": "empty in source",
    f"{SITE_DB}.site_erasure_requests": "references pseudonyms only, no exported study",
}
REDACTED = "REDACTED"
SECRET_NAME_MARKERS = ("secret", "password", "passwd", "token", "salt", "apikey", "privatekey")
SECRET_SETTING_SUFFIXES = ("key", "salt", "secret", "password", "token")
SECRET_VALUE_MARKERS = ("mongodb://", "mongodb+srv://", "AccountKey=", "SharedAccessSignature", "sig=")
REFERENCE_KEYS = {"hashSaltRef"}  # names a secret held elsewhere; carries no secret itself
EXAMPLES_PER_VARIANT = 100
MAX_EVENT_DOCUMENTS = 50
MAX_SELECTION_ROUNDS = 8
DOCS_PER_STUDY = 3  # a study plus ~two instances; puts patient and event costs on one scale
AGGREGATE = {"allowDiskUse": True, "maxTimeMS": 1_800_000}
LATEST_STATUS = [
    {"$sort": {"ca": -1}},
    {"$group": {"_id": {"ci": "$ci", "ct": "$ct"}, "doc": {"$first": "$$ROOT"}}},
    {"$replaceRoot": {"newRoot": "$doc"}},
]


def first_value(path: str) -> dict:
    return {"$cond": [{"$isArray": path}, {"$arrayElemAt": [path, 0]}, path]}


def key_list(expr) -> dict:
    return {"$sortArray": {"input": {"$map": {"input": {"$objectToArray": expr}, "in": "$$this.k"}}, "sortBy": 1}}


def value_set(expr) -> dict:
    return {"$sortArray": {"input": {"$setUnion": [{"$ifNull": [expr, []]}]}, "sortBy": 1}}


HEADER = {"$objectToArray": {"$ifNull": ["$header", {}]}}
# One signature per document; a variant is a distinct signature. The sample should hold every variant.
VARIANTS = {
    "instances": {
        "signature": {
            "sopClass": first_value("$header.00080016.Value"),
            "modality": first_value("$header.00080060.Value"),
            "transferSyntax": "$ContentIndex.ts",
            "encapsulated": "$ContentIndex.encapsulated",
            "partKinds": "$ContentIndex.parts.kind",
            "frames": first_value("$header.00280008.Value"),
            "photometric": first_value("$header.00280004.Value"),
            "rows": first_value("$header.00280010.Value"),
            "columns": first_value("$header.00280011.Value"),
            "documentKeys": key_list("$$ROOT"),
            "valueRepresentations": value_set({"$map": {"input": HEADER, "in": "$$this.v.vr"}}),
            "bareStringValueTags": {
                "$map": {
                    "input": {"$filter": {"input": HEADER, "cond": {"$eq": [{"$type": "$$this.v.Value"}, "string"]}}},
                    "in": "$$this.k",
                }
            },
            "valueForms": value_set(
                {
                    "$map": {
                        "input": HEADER,
                        "in": {
                            "$switch": {
                                "branches": [
                                    {"case": {"$ne": [{"$type": "$$this.v.BulkDataURI"}, "missing"]}, "then": "BulkDataURI"},
                                    {"case": {"$ne": [{"$type": "$$this.v.InlineBinary"}, "missing"]}, "then": "InlineBinary"},
                                    {"case": {"$ne": [{"$type": "$$this.v.Value"}, "missing"]}, "then": "Value"},
                                ],
                                "default": "Empty",
                            }
                        },
                    }
                }
            ),
            "headerTags": key_list({"$ifNull": ["$header", {}]}),
        },
        "example": {"study": first_value(f"$header.{STUDY_UID}.Value")},
    },
    "studies": {
        "distinct_examples": True,
        "signature": {
            "seriesCount": {"$size": {"$ifNull": ["$se", []]}},
            "modalities": value_set("$se.mo"),
            "sendingAets": value_set("$se.ae"),
            "instancesPerSeries": value_set("$se.ni"),
            "seriesCenterDiffersFromStudy": {
                "$anyElementTrue": [{"$map": {"input": {"$ifNull": ["$se", []]}, "in": {"$ne": ["$$this.ci", "$dm"]}}}]
            },
            "studyKeys": key_list("$$ROOT"),
            "seriesKeys": {
                "$sortArray": {
                    "input": {
                        "$reduce": {
                            "input": {"$ifNull": ["$se", []]},
                            "initialValue": [],
                            "in": {"$setUnion": ["$$value", {"$map": {"input": {"$objectToArray": "$$this"}, "in": "$$this.k"}}]},
                        }
                    },
                    "sortBy": 1,
                }
            },
            "patientSex": "$ps",
        },
        "example": {"center": "$dm", "patient": "$pi"},
    },
    "lopd_audit": {
        "signature": {
            "opType": "$opType",
            "action": "$action",
            "callerType": "$callerType",
            "success": "$success",
            "referencesStudy": {"$gt": [{"$strLenCP": {"$ifNull": ["$studyUID", ""]}}, 0]},
            "documentKeys": key_list("$$ROOT"),
        },
        "example": {"id": "$_id", "study": "$studyUID"},
    },
    "trash": {
        "signature": {"collection": "$co", "action": "$ac", "snapshotKeys": key_list({"$ifNull": ["$doc", {}]})},
        "example": {"event": "$ev"},
    },
    "component_status": {
        "signature": {"componentType": "$ct", "status": "$st", "metadataKeys": key_list({"$ifNull": ["$md", {}]})},
        "example": {"id": "$_id"},
    },
}


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def load_uri(env_file: Path) -> str:
    uri = dotenv_values(env_file).get(URI_VAR) if env_file.exists() else None
    uri = uri or os.environ.get(URI_VAR)
    if not uri:
        raise SystemExit(f"{URI_VAR} is not set in {env_file} or the environment")
    return uri


def tag_value(instance: dict, tag: str):
    values = instance.get("header", {}).get(tag, {}).get("Value")
    if isinstance(values, list):
        return values[0] if values else None
    return values


def patient_family(patient_id: str) -> str:
    return re.sub(r"\d+", "#", patient_id or "")


def chunked(values, size: int = 250):
    values = list(values)
    for start in range(0, len(values), size):
        yield values[start : start + size]


def fetch_in(collection, field_name: str, values, filter_extra: dict | None = None, projection: dict | None = None) -> list[dict]:
    docs = []
    for chunk in chunked(sorted((v for v in values if v not in (None, "")), key=str)):
        docs.extend(collection.find({**(filter_extra or {}), field_name: {"$in": chunk}}, projection))
    return docs


def signature_expr(spec: dict) -> dict:
    return {name: {"$ifNull": [expr, None]} for name, expr in spec["signature"].items()}


def variant_key(signature: dict) -> str:
    return json_util.dumps(signature, sort_keys=True)


def source_variants(db) -> dict[str, dict[str, dict]]:
    variants = {}
    for name, spec in VARIANTS.items():
        log(f"  {name}")
        # Few distinct carriers (e.g. patients): list them all so the cheapest one can be chosen.
        examples = {"$addToSet": spec["example"]} if spec.get("distinct_examples") else {
            "$firstN": {"n": EXAMPLES_PER_VARIANT, "input": spec["example"]}
        }
        rows = db[name].aggregate(
            [{"$group": {"_id": signature_expr(spec), "count": {"$sum": 1}, "examples": examples}}],
            **AGGREGATE,
        )
        variants[name] = {variant_key(r["_id"]): {"signature": r["_id"], "count": r["count"], "examples": r["examples"]} for r in rows}
    return variants


def row_variants(collection, spec: dict, ids) -> dict:
    variant_of = {}
    for chunk in chunked(sorted(ids, key=str)):
        pipeline = [{"$match": {"_id": {"$in": chunk}}}, {"$replaceWith": {"_id": "$_id", "signature": signature_expr(spec)}}]
        for row in collection.aggregate(pipeline):
            variant_of[row["_id"]] = variant_key(row["signature"])
    return variant_of


def describe_variant(signature: dict, common: dict) -> dict:
    """The fields where a variant differs from the most common one, with long lists summarised."""
    diff = {}
    for name, value in signature.items():
        if value == common.get(name):
            continue
        if isinstance(value, list) and len(value) > 8:
            extra = sorted(set(map(str, value)) - set(map(str, common.get(name) or [])))
            value = {"count": len(value), "notInMostCommon": extra[:12] + (["..."] if len(extra) > 12 else [])}
        diff[name] = value
    return diff


@dataclass
class Context:
    db: object
    site: object
    config: dict[str, list[dict]]
    center_ids: set[str]
    sizes: dict[tuple, int]
    max_patient_studies: int
    study_budget: int
    history_cost: dict[tuple, int] = field(default_factory=dict)

    def ineligible(self, key: tuple) -> str | None:
        """Why a patient cannot be exported whole, or None if it can."""
        if key not in self.sizes:
            return f"center {key[0]!r} is not in cfg_centers or the patient has no live studies"
        if self.sizes[key] > self.max_patient_studies:
            return f"patient has {self.sizes[key]} studies (per-patient cap {self.max_patient_studies})"
        return None

    def cost(self, key: tuple) -> int:
        """Approximate documents a whole patient adds: studies, instances, trash history and its audit rows."""
        return self.sizes[key] * DOCS_PER_STUDY + self.history_cost.get(key, 0)


@dataclass
class Selection:
    patients: dict[tuple, list[str]] = field(default_factory=dict)
    events: dict[str, str] = field(default_factory=dict)
    rows: dict[str, dict] = field(default_factory=lambda: defaultdict(dict))
    excluded_events: dict[str, str] = field(default_factory=dict)

    def add_patient(self, key: tuple, reason: str) -> None:
        reasons = self.patients.setdefault(key, [])
        if reason not in reasons:
            reasons.append(reason)


@dataclass
class Assembly:
    data: dict[str, list[dict]]
    historical: set[str]
    linked: dict[str, set[tuple]]


def studies_used(ctx: Context, selection: Selection) -> int:
    return sum(ctx.sizes[key] for key in selection.patients)


def projected_documents(ctx: Context, selection: Selection, assembly: Assembly) -> int:
    """Documents the export will hold: assembled so far, required audit rows, and the fill still to come."""
    data = assembly.data
    fixed = sum(len(docs) for name, docs in data.items() if name != "lopd_audit")
    required = len({r["_id"] for r in matching_audit(ctx.db, data["trash"])} | set(selection.rows["lopd_audit"]))
    # the fill takes the cheapest patients: plain studies of ~2 instances and no lifecycle history
    fill = (ctx.study_budget - studies_used(ctx, selection)) * DOCS_PER_STUDY
    return fixed + required + fill


def patient_sizes(db, center_ids: set[str]) -> dict[tuple, int]:
    groups = db.studies.aggregate(
        [{"$match": {"dm": {"$in": sorted(center_ids)}}}, {"$group": {"_id": {"dm": "$dm", "pi": "$pi"}, "n": {"$sum": 1}}}],
        **AGGREGATE,
    )
    return {(g["_id"]["dm"], g["_id"]["pi"]): g["n"] for g in groups}


def history_costs(db, patient_keys: set[tuple]) -> dict[tuple, int]:
    """Approximate documents that the trash history of each patient's live studies adds.

    Moves, splits and fusions leave snapshots of studies that still exist; deletions do not, so
    they are left out. The cost counts every document of those events, one audit row per
    snapshot, and the other patients the events link (fusions merge two patients' studies).
    """
    history = {}
    rows = db.trash.aggregate(
        [
            {"$match": {"co": "studies", "ac": {"$ne": "DeleteStudy"}, "doc.pi": {"$in": sorted({pi for _, pi in patient_keys})}}},
            {"$group": {"_id": {"dm": "$doc.dm", "pi": "$doc.pi"}, "events": {"$addToSet": "$ev"}}},
        ],
        **AGGREGATE,
    )
    for row in rows:
        key = (row["_id"].get("dm"), row["_id"].get("pi"))
        if key in patient_keys:
            history[key] = set(row["events"])
    events = sorted({e for evs in history.values() for e in evs})
    event_docs, event_patients = Counter(), defaultdict(set)
    for chunk in chunked(events, 2000):
        pipeline = [
            {"$match": {"ev": {"$in": chunk}}},
            {"$group": {"_id": "$ev", "n": {"$sum": 1}, "patients": {"$addToSet": {"dm": "$doc.dm", "pi": "$doc.pi"}}}},
        ]
        for row in db.trash.aggregate(pipeline, **AGGREGATE):
            event_docs[row["_id"]] = row["n"]
            event_patients[row["_id"]] = {(p.get("dm"), p.get("pi")) for p in row["patients"] if p.get("pi")}
    cost = {}
    for key, evs in history.items():
        linked = {p for e in evs for p in event_patients[e]} - {key}
        cost[key] = sum(event_docs[e] for e in evs) + sum(len(event_patients[e]) for e in evs) + len(linked) * 60
    return cost


def matching_audit(db, trash: list[dict]) -> list[dict]:
    """Audit rows recording the operation behind each study snapshot (same study and action)."""
    by_action = defaultdict(set)
    for entry in trash:
        if entry["co"] == "studies":
            by_action[entry["ac"]].add(entry["doc"]["_id"])
    return [row for action, studies in sorted(by_action.items()) for row in fetch_in(db.lopd_audit, "studyUID", studies, {"action": action})]


def assemble(ctx: Context, selection: Selection) -> Assembly:
    db = ctx.db
    by_center = defaultdict(list)
    for center, patient_id in selection.patients:
        by_center[center].append(patient_id)
    studies = [s for center, ids in sorted(by_center.items()) for s in db.studies.find({"dm": center, "pi": {"$in": sorted(ids)}})]
    study_ids = {s["_id"] for s in studies}
    instances = fetch_in(db.instances, f"header.{STUDY_UID}.Value", study_ids)
    instance_ids = {i["_id"] for i in instances}

    events = set(selection.events)
    events |= {t["ev"] for t in fetch_in(db.trash, "doc._id", study_ids, {"co": "studies"}, {"ev": 1})}
    events |= {t["ev"] for t in fetch_in(db.trash, "doc._id", instance_ids, {"co": "instances"}, {"ev": 1})}
    events -= set(selection.excluded_events)
    trash = fetch_in(db.trash, "ev", events)

    snapshot_study = {t["_id"]: t["doc"]["_id"] if t["co"] == "studies" else tag_value(t["doc"], STUDY_UID) for t in trash}
    live_outside = {
        s["_id"]: (s["dm"], s["pi"])
        for s in fetch_in(db.studies, "_id", set(snapshot_study.values()) - study_ids, projection={"dm": 1, "pi": 1})
    }
    linked = defaultdict(set)
    for entry in trash:
        owner = live_outside.get(snapshot_study[entry["_id"]])
        if owner:
            linked[entry["ev"]].add(owner)
    historical = {t["doc"]["_id"] for t in trash if t["co"] == "studies"} - study_ids - set(live_outside)

    audit = {r["_id"]: r for r in fetch_in(db.lopd_audit, "studyUID", study_ids)}
    audit.update({r["_id"]: r for r in matching_audit(db, trash)})
    audit.update({r["_id"]: r for r in fetch_in(db.lopd_audit, "_id", selection.rows["lopd_audit"])})
    status = {d["_id"]: d for d in db.component_status.aggregate(LATEST_STATUS)}
    status.update({d["_id"]: d for d in fetch_in(db.component_status, "_id", selection.rows["component_status"])})
    data = {
        **ctx.config,
        "studies": studies,
        "instances": instances,
        "trash": trash,
        "lopd_audit": list(audit.values()),
        "component_status": list(status.values()),
        "site_viewer_sessions": fetch_in(ctx.site.site_viewer_sessions, "GStrStudyUid", study_ids),
    }
    return Assembly(data, historical, dict(linked))


def settle(ctx: Context, selection: Selection) -> Assembly:
    """Assemble, pulling in patients whose live studies appear in exported events, until stable."""
    while True:
        assembly = assemble(ctx, selection)
        if not assembly.linked:
            return assembly
        for event, owners in sorted(assembly.linked.items()):
            blockers = [why for why in (ctx.ineligible(k) for k in owners) if why]
            extra = sum(ctx.sizes[k] for k in owners) if not blockers else 0
            if blockers:
                selection.excluded_events[event] = f"links a live study of another patient: {blockers[0]}"
            elif studies_used(ctx, selection) + extra > ctx.study_budget:
                selection.excluded_events[event] = "links a live study of a patient that does not fit the study budget"
            else:
                for key in sorted(owners):
                    selection.add_patient(key, f"linked by lifecycle event {event}")


class Resolver:
    """Turns variant examples into the cheapest thing to export that carries them."""

    def __init__(self, ctx: Context):
        self.ctx = ctx
        self.owner: dict[str, tuple | None] = {}
        self.snapshot_event: dict[str, str | None] = {}
        self.event: dict[str, dict] = {}

    def load_studies(self, study_ids) -> None:
        missing = sorted({s for s in study_ids if s and s not in self.owner})
        for study in self.ctx.db.studies.find({"_id": {"$in": missing}}, {"dm": 1, "pi": 1}) if missing else []:
            self.owner[study["_id"]] = (study["dm"], study["pi"])
        gone = [s for s in missing if s not in self.owner]
        self.owner.update({s: None for s in gone})
        for chunk in chunked(gone):
            for entry in self.ctx.db.trash.find({"co": "studies", "doc._id": {"$in": chunk}}, {"ev": 1, "doc._id": 1}).sort("ts", 1):
                self.snapshot_event.setdefault(entry["doc"]["_id"], entry["ev"])
        for study in gone:
            self.snapshot_event.setdefault(study, None)

    def load_events(self, events) -> None:
        missing = sorted({e for e in events if e and e not in self.event})
        studies = set()
        for chunk in chunked(missing):
            pipeline = [
                {"$match": {"ev": {"$in": chunk}}},
                {
                    "$group": {
                        "_id": "$ev",
                        "documents": {"$sum": 1},
                        "studies": {
                            "$addToSet": {
                                "$cond": [{"$eq": ["$co", "studies"]}, "$doc._id", first_value(f"$doc.header.{STUDY_UID}.Value")]
                            }
                        },
                        "centers": {"$addToSet": "$doc.dm"},
                    }
                },
            ]
            for row in self.ctx.db.trash.aggregate(pipeline):
                self.event[row["_id"]] = {
                    "documents": row["documents"],
                    "studies": {s for s in row["studies"] if s},
                    "centers": {c for c in row["centers"] if c},
                }
                studies |= self.event[row["_id"]]["studies"]
        self.load_studies(studies)

    def patient(self, key: tuple, selection: Selection):
        if key in selection.patients:
            return "patient", key, 0, None
        why = self.ctx.ineligible(key)
        return ("patient", key, self.ctx.cost(key), None) if not why else (None, None, None, why)

    def trash_event(self, event: str, selection: Selection):
        if event in selection.excluded_events:
            return None, None, None, selection.excluded_events[event]
        info = self.event.get(event)
        if not info:
            return None, None, None, "trash event not found"
        if info["documents"] > MAX_EVENT_DOCUMENTS:
            return None, None, None, f"trash event has {info['documents']} documents"
        if info["centers"] - self.ctx.center_ids:
            return None, None, None, f"trash event snapshot center {sorted(info['centers'] - self.ctx.center_ids)} not in cfg_centers"
        cost = 2 * info["documents"]  # snapshots plus roughly one audit row each
        for owner in {self.owner[s] for s in info["studies"] if self.owner.get(s)}:
            if owner not in selection.patients:
                why = self.ctx.ineligible(owner)
                if why:
                    return None, None, None, f"trash event links a live study whose {why}"
                cost += self.ctx.cost(owner)
        return "event", event, cost, None

    def candidate(self, name: str, example: dict, selection: Selection):
        if name == "studies":
            return self.patient((example.get("center"), example.get("patient")), selection)
        if name == "trash":
            return self.trash_event(example.get("event"), selection)
        if name == "component_status" or (name == "lopd_audit" and not example.get("study")):
            if example["id"] in selection.rows[name]:
                return None, None, None, "row already selected but the variant did not appear"
            return "row", (name, example["id"]), 1, None
        study = example.get("study")
        if self.owner.get(study):
            return self.patient(self.owner[study], selection)
        if name == "instances":
            return None, None, None, "the instance's study does not exist"
        if not self.snapshot_event.get(study):
            return None, None, None, "references a study that is neither live nor in trash"
        if example["id"] in selection.rows["lopd_audit"]:
            return None, None, None, "row already selected but the variant did not appear"
        kind, event, cost, why = self.trash_event(self.snapshot_event[study], selection)
        return (None, None, None, why) if why else ("event+row", (event, example["id"]), cost + 1, None)


def variants_missing_in_phase(ctx, variants, assembly, failures, phase) -> list[tuple]:
    exported = {
        name: set(row_variants(ctx.db[name], spec, [d["_id"] for d in assembly.data[name]]).values())
        for name, spec in VARIANTS.items()
        if any(in_phase(phase, name, v) for v in variants[name].values())
    }
    return sorted(
        (
            (name, key)
            for name in exported
            for key in variants[name]
            if key not in exported[name] and (name, key) not in failures and in_phase(phase, name, variants[name][key])
        ),
        key=lambda nk: (variants[nk[0]][nk[1]]["count"], nk),
    )


def cover_missing(ctx, selection, variants, assembly, resolver, failures, phase, max_documents) -> int:
    """Pick carriers for the variants of `phase` the assembled sample lacks; returns how many picks were kept.

    Phases run from the widest carriers (whole patients, which usually bring instance and audit
    variants along) to the narrowest. A pick that would push the projected export over
    `max_documents` is undone and its variants are recorded as over budget.
    """
    missing = variants_missing_in_phase(ctx, variants, assembly, failures, phase)
    if not missing:
        return 0
    examples = [(name, ex) for name, key in missing for ex in variants[name][key]["examples"]]
    resolver.load_studies(ex.get("study") for name, ex in examples if name in ("instances", "lopd_audit"))
    resolver.load_events(
        {ex.get("event") for name, ex in examples if name == "trash"}
        | {resolver.snapshot_event.get(ex.get("study")) for name, ex in examples if name == "lopd_audit"}
    )
    common = {name: max(source.values(), key=lambda v: v["count"])["signature"] for name, source in variants.items()}
    # Every carrier of each missing variant, then a greedy set cover: cheapest cost per newly covered variant first.
    carriers: dict[tuple, dict] = {}
    for name, key in missing:
        reasons = Counter()
        for example in variants[name][key]["examples"]:
            kind, ref, cost, why = resolver.candidate(name, example, selection)
            if why:
                reasons[why] += 1
                continue
            carrier = carriers.setdefault((kind, ref), {"cost": cost, "covers": set()})
            carrier["covers"].add((name, key))
        if not any((name, key) in c["covers"] for c in carriers.values()):
            failures[(name, key)] = reasons.most_common(1)[0][0] if reasons else "no examples"
    uncovered = {nk for c in carriers.values() for nk in c["covers"]}
    added = 0
    while uncovered:
        ranked = sorted(
            (
                (c["cost"] / len(c["covers"] & uncovered), -len(c["covers"] & uncovered), str(ref), kind, ref)
                for (kind, ref), c in carriers.items()
                if c["covers"] & uncovered
            ),
        )
        if not ranked:
            break
        _, _, _, kind, ref = ranked[0]
        covers = carriers.pop((kind, ref))["covers"] & uncovered
        if kind == "patient" and ref in selection.patients:
            failures.update({nk: "carrier already exported but the variant did not appear" for nk in covers})
            continue  # left uncovered so another carrier can still take it
        if kind == "patient" and studies_used(ctx, selection) + ctx.sizes[ref] > ctx.study_budget:
            failures.update({nk: "the carrying patient does not fit the study budget" for nk in covers})
            continue
        reason = "; ".join(
            f"{name} variant {json.dumps(describe_variant(variants[name][key]['signature'], common[name]), default=str)}"
            for name, key in sorted(covers)
        )
        before = copy.deepcopy(selection)
        if kind == "patient":
            selection.add_patient(ref, reason)
        elif kind == "event":
            selection.events.setdefault(ref, reason)
        elif kind == "event+row":
            selection.events.setdefault(ref[0], reason)
            selection.rows["lopd_audit"].setdefault(ref[1], reason)
        else:
            selection.rows[ref[0]].setdefault(ref[1], reason)
        if kind != "row" and projected_documents(ctx, selection, settle(ctx, selection)) > max_documents:
            restore(selection, before)
            failures.update({nk: "carrying it would exceed the document budget" for nk in covers})
            continue  # left uncovered so a cheaper carrier can still take it
        uncovered -= covers
        for nk in covers:
            failures.pop(nk, None)
        added += 1
    return added


def restore(selection: Selection, snapshot: Selection) -> None:
    selection.__dict__.update(copy.deepcopy(snapshot).__dict__)


SELECTION_PHASES = ("studies", "standalone rows", "trash", "instances and audit")


def in_phase(phase: str, name: str, variant: dict) -> bool:
    standalone = name == "component_status" or (name == "lopd_audit" and not variant["signature"].get("referencesStudy"))
    return {
        "studies": name == "studies",
        "standalone rows": standalone,
        "trash": name == "trash",
        "instances and audit": name in ("instances", "lopd_audit") and not standalone,
    }[phase]


def pick_deletion_events(ctx: Context, selection: Selection, limit: int) -> None:
    """Spread single deletion events across (center, patient-ID family), favouring patients not exported whole."""
    rows = ctx.db.trash.aggregate(
        [
            {"$match": {"co": "studies", "ac": "DeleteStudy", "doc.dm": {"$in": sorted(ctx.center_ids)}}},
            {"$sort": {"ts": 1, "_id": 1}},
            {"$group": {"_id": {"dm": "$doc.dm", "pi": "$doc.pi"}, "events": {"$firstN": {"n": 5, "input": "$ev"}}}},
        ],
        **AGGREGATE,
    )
    rows = list(rows)
    sizes = Counter()
    for chunk in chunked(sorted({e for row in rows for e in row["events"]}), 2000):
        for row in ctx.db.trash.aggregate([{"$match": {"ev": {"$in": chunk}}}, {"$group": {"_id": "$ev", "n": {"$sum": 1}}}], **AGGREGATE):
            sizes[row["_id"]] = row["n"]
    strata = defaultdict(list)
    for row in rows:
        row["event"] = min(row["events"], key=lambda e: (sizes[e], e))
        strata[(row["_id"]["dm"], patient_family(row["_id"]["pi"]))].append(row)

    def preference(row):
        key = (row["_id"]["dm"], row["_id"]["pi"])
        return (sizes[row["event"]], ctx.ineligible(key) is None, row["_id"]["pi"])

    queues = [sorted(strata[k], key=preference) for k in sorted(strata)]
    picks = []
    while len(picks) < limit and any(queues):
        for queue in queues:
            if queue and len(picks) < limit:
                picks.append(queue.pop(0))
    for row in picks:
        key = (row["_id"]["dm"], row["_id"]["pi"])
        selection.events.setdefault(
            row["event"], f"deleted study of {key[0]}/{key[1]}, a patient with {ctx.sizes.get(key, 0)} live studies"
        )


def fill_round_robin(ctx: Context, selection: Selection) -> None:
    used = studies_used(ctx, selection)
    queues = defaultdict(list)
    for key in sorted(ctx.sizes, key=lambda k: (ctx.cost(k), k)):
        if key not in selection.patients and ctx.ineligible(key) is None:
            queues[key[0]].append(key)
    progress = True
    while progress:
        progress = False
        for center in sorted(queues):
            queue = queues[center]
            while queue and used + ctx.sizes[queue[0]] > ctx.study_budget:
                queue.pop(0)
            if queue:
                key = queue.pop(0)
                selection.add_patient(key, "center round-robin fill")
                used += ctx.sizes[key]
                progress = True


def cap_audit(rows: list[dict], variant_of: dict, required: set, limit: int) -> list[dict]:
    """Keep required rows, then alternate across audit variants (uncovered and rarest first) up to `limit`."""
    kept = [r for r in rows if r["_id"] in required]
    covered = {variant_of[r["_id"]] for r in kept}
    queues = defaultdict(list)
    for row in sorted(rows, key=lambda r: (r.get("timestamp") or dt.datetime.min, str(r["_id"]))):
        if row["_id"] not in required:
            queues[variant_of[row["_id"]]].append(row)
    order = sorted(queues, key=lambda k: (k in covered, len(queues[k]), k))
    while len(kept) < limit and any(queues[k] for k in order):
        for key in order:
            if queues[key] and len(kept) < limit:
                kept.append(queues[key].pop(0))
    return kept


def flatten_settings(node, prefix=""):
    if isinstance(node, dict):
        for key, value in node.items():
            yield from flatten_settings(value, f"{prefix}{key}:" if isinstance(value, dict) else f"{prefix}{key}")
    else:
        yield prefix, node


def is_secret_setting(setting_id: str, sensitive_ids: set[str]) -> bool:
    return setting_id in sensitive_ids or setting_id.rsplit(":", 1)[-1].lower().endswith(SECRET_SETTING_SUFFIXES)


def redact(data: dict[str, list[dict]]) -> list[str]:
    redactions = []
    sensitive_ids = {d["_id"] for d in data["cfg_setting_definitions"] if d.get("sensitive")}
    for definition in data["cfg_setting_definitions"]:
        if is_secret_setting(definition["_id"], sensitive_ids) and definition.get("defaultValue"):
            definition["defaultValue"] = REDACTED
            redactions.append(f"cfg_setting_definitions[{definition['_id']}].defaultValue")
    for value in data["cfg_values"]:
        if is_secret_setting(value["_id"], sensitive_ids) and value.get("value"):
            value["value"] = REDACTED
            redactions.append(f"cfg_values[{value['_id']}].value")
    for center in data["cfg_centers"]:
        for index, token in enumerate(center.get("authTokens") or []):
            if token.get("secretHash"):
                token["secretHash"] = REDACTED
                redactions.append(f"cfg_centers[{center['_id']}].authTokens[{index}].secretHash")
        for setting_id, value in flatten_settings(center.get("settings") or {}):
            if is_secret_setting(setting_id, sensitive_ids) and value:
                node = center["settings"]
                *parents, leaf = setting_id.split(":")
                for parent in parents:
                    node = node[parent]
                node[leaf] = REDACTED
                redactions.append(f"cfg_centers[{center['_id']}].settings.{setting_id.replace(':', '.')}")
    for user in data["site_users"]:
        if user.get("GStrPassword"):
            user["GStrPassword"] = REDACTED
            redactions.append(f"site_users[{user['_id']}].GStrPassword")
    return redactions


def find_secret_leaks(data: dict[str, list[dict]]) -> list[str]:
    leaks = []

    def walk(node, path):
        if isinstance(node, dict):
            for key, value in node.items():
                child = f"{path}.{key}"
                if (
                    isinstance(value, str)
                    and value not in ("", REDACTED)
                    and key not in REFERENCE_KEYS
                    and any(marker in key.lower() for marker in SECRET_NAME_MARKERS)
                ):
                    leaks.append(child)
                walk(value, child)
        elif isinstance(node, list):
            for index, item in enumerate(node):
                walk(item, f"{path}[{index}]")
        elif isinstance(node, str) and any(marker in node for marker in SECRET_VALUE_MARKERS):
            leaks.append(path)

    for name, docs in data.items():
        for doc in docs:
            walk(doc, f"{name}[{doc.get('_id')}]")
    return leaks


def verify(data, patients, source_counts, historical, event_sizes) -> tuple[list[str], dict]:
    errors = []
    center_ids = {c["_id"] for c in data["cfg_centers"]}
    device_ids = {str(d["_id"]) for d in data["devices"]}
    study_ids = {s["_id"] for s in data["studies"]}
    known_studies = study_ids | historical
    series_owner = {}
    expected_per_series = {}

    def check_study_refs(study, label):
        if study.get("dm") not in center_ids:
            errors.append(f"{label} {study['_id']} center {study.get('dm')!r} not in cfg_centers")
        for series in study.get("se", []):
            if series.get("ci") not in center_ids:
                errors.append(f"{label} {study['_id']} series {series['si']} center {series.get('ci')!r} not in cfg_centers")
            if series.get("dv") and series["dv"] not in device_ids:
                errors.append(f"{label} {study['_id']} series {series['si']} device {series['dv']} not in devices")

    for study in data["studies"]:
        check_study_refs(study, "study")
        for series in study.get("se", []):
            series_owner[series["si"]] = study["_id"]
            expected_per_series[series["si"]] = series.get("ni", 0)

    actual_per_series = Counter()
    for instance in data["instances"]:
        study_uid, series_uid = tag_value(instance, STUDY_UID), tag_value(instance, SERIES_UID)
        if study_uid not in study_ids:
            errors.append(f"instance {instance['_id']} study {study_uid} not exported")
        if series_owner.get(series_uid) != study_uid:
            errors.append(f"instance {instance['_id']} series {series_uid} not listed in study {study_uid}")
        actual_per_series[series_uid] += 1
    for series_uid, expected in expected_per_series.items():
        if actual_per_series[series_uid] != expected:
            errors.append(f"series {series_uid}: study says {expected} instances, exported {actual_per_series[series_uid]}")

    exported_per_center = Counter(s["dm"] for s in data["studies"])
    for center, expected in source_counts.items():
        if exported_per_center[center] != expected:
            errors.append(f"center {center}: source has {expected} studies for chosen patients, exported {exported_per_center[center]}")
    if sum(p["studies"] for p in patients) != len(study_ids):
        errors.append("selected patient study totals do not match exported studies")

    for event, exported in Counter(t["ev"] for t in data["trash"]).items():
        if event_sizes.get(event) != exported:
            errors.append(f"trash event {event}: source has {event_sizes.get(event)} documents, exported {exported}")
    snapshots = {}
    for entry in data["trash"]:
        study = entry["doc"]["_id"] if entry["co"] == "studies" else tag_value(entry["doc"], STUDY_UID)
        if study not in known_studies:
            errors.append(f"trash {entry['_id']} snapshot of study {study}, which is neither exported nor historical")
        if entry["co"] == "studies":
            snapshots.setdefault(entry["doc"]["_id"], entry["doc"])
    for study_id in sorted(historical):
        if study_id not in snapshots:
            errors.append(f"historical study {study_id} has no snapshot in trash")
        else:
            check_study_refs(snapshots[study_id], "historical study")

    for row in data["lopd_audit"]:
        if row.get("studyUID") and row["studyUID"] not in known_studies:
            errors.append(f"audit {row['_id']} study {row['studyUID']} neither exported nor historical")
    for session in data["site_viewer_sessions"]:
        if session.get("GStrStudyUid") not in study_ids:
            errors.append(f"viewer session {session['_id']} study not exported")

    # Dangling references that already exist in the source configuration, reported rather than fixed.
    callers = {c["_id"] for c in data["cfg_auth_clients"]} | {n["_id"] for n in data["cfg_nodes"]}
    templates = {t["_id"] for t in data["cfg_anon_templates"]}
    users = {u["_id"] for u in data["site_users"]}
    roles = {r["_id"] for r in data["site_roles"]}
    soft = {
        "config centerId not in cfg_centers": sorted(
            {
                f"{name}:{doc.get('centerId')}"
                for name in ("cfg_auth_clients", "cfg_auth_tenants", "cfg_nodes", "cfg_wl_aet2stations", "cfg_hl7_mappings")
                for doc in data[name]
                if doc.get("centerId") not in center_ids | {"_default"}
            }
        ),
        "devices ci not in cfg_centers": sorted({f"{d['_id']}:{d.get('ci')}" for d in data["devices"] if d.get("ci") not in center_ids}),
        "component_status ci not in cfg_centers": sorted({d.get("ci") for d in data["component_status"] if d.get("ci") not in center_ids}),
        "cfg_centers parent not in cfg_centers": sorted({c["_id"] for c in data["cfg_centers"] if c.get("parent") and c["parent"] not in center_ids}),
        "cfg_anon_profiles sourceTemplate not in cfg_anon_templates": sorted(
            {str(p["_id"]) for p in data["cfg_anon_profiles"] if p.get("sourceTemplate") and p["sourceTemplate"] not in templates}
        ),
        "lopd_audit callerId not in cfg_auth_clients/cfg_nodes": sorted(
            {r["callerId"] for r in data["lopd_audit"] if r.get("callerId") and r["callerId"] not in callers}
        ),
        "lopd_audit centerId not in cfg_centers": sorted({r.get("centerId") for r in data["lopd_audit"] if r.get("centerId") not in center_ids}),
        "trash ci not in cfg_centers": sorted({t.get("ci") for t in data["trash"] if t.get("ci") not in center_ids}),
        "site_viewer_sessions user not in site_users": sorted(
            {s.get("GStrUserName") for s in data["site_viewer_sessions"] if s.get("GStrUserName") not in users}
        ),
        "site_users role not in site_roles": sorted({u.get("GStrRole") for u in data["site_users"] if u.get("GStrRole") not in roles}),
    }
    return errors, {check: refs for check, refs in soft.items() if refs}


def coverage_report(variants: dict, exported: dict, failures: dict) -> dict:
    report = {}
    for name, source in variants.items():
        seen = set(exported[name].values()) & set(source)
        common = max(source.values(), key=lambda v: v["count"])["signature"]
        missing = [
            {
                "variant": describe_variant(variant["signature"], common),
                "sourceDocuments": variant["count"],
                "why": failures.get((name, key), "not reached within the study or document budget"),
            }
            for key, variant in sorted(source.items(), key=lambda kv: -kv[1]["count"])
            if key not in seen
        ]
        report[name] = {"sourceVariants": len(source), "exportedVariants": len(seen), "missing": missing}
    return report


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    parser.add_argument("--env-file", type=Path, default=repo / ".env.local")
    parser.add_argument("--studies", type=int, default=500)
    parser.add_argument("--max-documents", type=int, default=2000)
    parser.add_argument("--max-patient-studies", type=int, default=60)
    parser.add_argument("--deleted-events", type=int, default=10)
    args = parser.parse_args()

    client = MongoClient(load_uri(args.env_file), serverSelectionTimeoutMS=20000, appname="kehrnel-dicom-sample-export")
    db, site = client[CLINICAL_DB], client[SITE_DB]
    config = {name: list(db[name].find()) for name in CONFIG_COLLECTIONS}
    config.update(site_users=list(site.site_users.find()), site_roles=list(site.site_roles.find()))
    center_ids = {c["_id"] for c in config["cfg_centers"]}

    log("profiling patients and their lifecycle history")
    ctx = Context(db, site, config, center_ids, patient_sizes(db, center_ids), args.max_patient_studies, args.studies)
    ctx.history_cost = history_costs(db, {key for key in ctx.sizes if ctx.ineligible(key) is None})
    log("profiling source variants")
    variants = source_variants(db)

    selection = Selection()
    viewed = [s["GStrStudyUid"] for s in site.site_viewer_sessions.find({}, {"GStrStudyUid": 1}) if s.get("GStrStudyUid")]
    for study in db.studies.find({"_id": {"$in": viewed}}, {"dm": 1, "pi": 1}):
        if ctx.ineligible((study["dm"], study["pi"])) is None:
            selection.add_patient((study["dm"], study["pi"]), "opened in a viewer session")
    pick_deletion_events(ctx, selection, args.deleted_events)
    resolver, failures = Resolver(ctx), {}
    for phase in SELECTION_PHASES:
        for round_number in range(1, MAX_SELECTION_ROUNDS + 1):
            added = cover_missing(ctx, selection, variants, settle(ctx, selection), resolver, failures, phase, args.max_documents)
            if not added:
                break
            projected = projected_documents(ctx, selection, settle(ctx, selection))
            log(f"{phase} round {round_number}: {added} picks, {len(selection.patients)} patients, "
                f"{studies_used(ctx, selection)} studies, ~{projected} documents projected")
    fill_round_robin(ctx, selection)
    assembly = settle(ctx, selection)
    data, historical = assembly.data, assembly.historical

    study_ids = {s["_id"] for s in data["studies"]}
    pool = data["lopd_audit"]
    audit_variant = row_variants(db.lopd_audit, VARIANTS["lopd_audit"], [r["_id"] for r in pool])
    required = {r["_id"] for r in matching_audit(db, data["trash"])} | set(selection.rows["lopd_audit"])
    audit_budget = args.max_documents - sum(len(docs) for name, docs in data.items() if name != "lopd_audit")
    if audit_budget < len(required):
        breakdown = ", ".join(f"{name}={len(docs)}" for name, docs in sorted(data.items()) if name != "lopd_audit")
        raise SystemExit(
            f"{args.max_documents - audit_budget} documents before audit plus {len(required)} required audit rows "
            f"exceed --max-documents {args.max_documents}, nothing written ({breakdown})"
        )
    data["lopd_audit"] = cap_audit(pool, audit_variant, required, audit_budget)

    patients = [
        {"center": key[0], "patientId": key[1], "studies": ctx.sizes[key], "reasons": reasons}
        for key, reasons in sorted(selection.patients.items())
    ]
    by_center = defaultdict(list)
    for center, patient_id in selection.patients:
        by_center[center].append(patient_id)
    source_counts = {c: db.studies.count_documents({"dm": c, "pi": {"$in": ids}}) for c, ids in sorted(by_center.items())}
    event_sizes = {}
    for chunk in chunked(sorted({t["ev"] for t in data["trash"]})):
        for row in db.trash.aggregate([{"$match": {"ev": {"$in": chunk}}}, {"$group": {"_id": "$ev", "n": {"$sum": 1}}}]):
            event_sizes[row["_id"]] = row["n"]

    redactions = redact(data)
    leaks = find_secret_leaks(data)
    if leaks:
        raise SystemExit("unredacted secret-like fields, nothing written:\n  " + "\n  ".join(leaks))
    errors, dangling = verify(data, patients, source_counts, historical, event_sizes)
    if errors:
        raise SystemExit(f"{len(errors)} integrity errors, nothing written:\n  " + "\n  ".join(errors[:50]))
    total = sum(len(docs) for docs in data.values())
    if total > args.max_documents:
        breakdown = ", ".join(f"{name}={len(docs)}" for name, docs in sorted(data.items()))
        raise SystemExit(f"{total} documents exceeds --max-documents {args.max_documents}, nothing written ({breakdown})")
    log("checking variant coverage of the final sample")
    coverage = coverage_report(variants, {n: row_variants(db[n], s, [d["_id"] for d in data[n]]) for n, s in VARIANTS.items()}, failures)

    chosen = set(selection.patients)
    attached = {
        t["ev"]
        for t in data["trash"]
        if (t["doc"]["_id"] if t["co"] == "studies" else tag_value(t["doc"], STUDY_UID)) in study_ids
        or (t["doc"].get("dm"), t["doc"].get("pi")) in chosen
    }
    history_only = {ev: selection.events.get(ev, "") for ev in sorted({t["ev"] for t in data["trash"]} - attached)}

    data["instances"].sort(key=lambda i: (tag_value(i, STUDY_UID), tag_value(i, SERIES_UID), int(tag_value(i, INSTANCE_NUMBER) or 0), i["_id"]))
    layout = {name: CLINICAL_DB for name in data} | {name: SITE_DB for name in ("site_viewer_sessions", "site_users", "site_roles")}
    counts = {}
    for name, docs in sorted(data.items()):
        if name != "instances":
            docs.sort(key=lambda d: str(d["_id"]))
        target = args.destination / layout[name] / f"{name}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json_util.dumps(docs, json_options=json_util.RELAXED_JSON_OPTIONS, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        counts[f"{layout[name]}.{name}"] = len(docs)
        print(f"{layout[name]}/{name}.json\t{len(docs)}")

    studies = data["studies"]
    manifest = {
        "exportedAt": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "generator": "scripts/export_dicom_sample.py",
        "format": "MongoDB Extended JSON v2 (relaxed), one JSON array per collection",
        "selection": {
            "studyBudget": args.studies,
            "maxPatientStudies": args.max_patient_studies,
            "rule": (
                "whole center-scoped patients (dm, pi): every live study, every instance of those studies and every "
                "complete trash event that snapshots them; deleted studies come only from standalone events "
                "(history.historyOnlyEvents), whose snapshots and audit rows are the only records of those studies"
            ),
            "patients": len(patients),
            "studies": len(studies),
            "series": sum(len(s.get("se", [])) for s in studies),
            "instances": len(data["instances"]),
            "studiesPerCenter": dict(sorted(Counter(s["dm"] for s in studies).items())),
            "seriesPerModality": dict(sorted(Counter(se.get("mo") for s in studies for se in s.get("se", [])).items())),
            "chosenPatients": patients,
        },
        "history": {
            "trashEvents": len({t["ev"] for t in data["trash"]}),
            "trashDocumentsByCollectionAndAction": {f"{co}/{ac}": n for (co, ac), n in sorted(Counter((t["co"], t["ac"]) for t in data["trash"]).items())},
            "historicalStudies": len(historical),
            "historicalStudiesNote": "studies that exist only as trash snapshots (deleted, or fused into another study)",
            "historyOnlyEvents": history_only,
            "historyOnlyEventsNote": (
                "standalone events on studies that are not exported live (mostly deletions); their snapshots and "
                "audit rows are the only records of those studies, and the patient may or may not be exported"
            ),
            "excludedEvents": selection.excluded_events,
        },
        "documentCounts": counts,
        "totalDocuments": total,
        "partialCollections": {
            f"{CLINICAL_DB}.lopd_audit": (
                f"{len(data['lopd_audit'])} of {len(pool)} rows on exported or historical studies, plus standalone rows "
                "chosen for their variant; every Manage row and every row on a historical study kept, the rest "
                "round-robin across variants to fit the document cap"
            ),
            f"{CLINICAL_DB}.trash": "complete events only: every trash document of each exported event",
            f"{CLINICAL_DB}.component_status": "latest status per (ci, ct) component, plus one document per status variant",
            f"{SITE_DB}.site_viewer_sessions": "only sessions on exported studies",
        },
        "variantCoverage": coverage,
        "excludedCollections": EXCLUDED,
        "redactions": {"placeholder": REDACTED, "paths": redactions},
        "integrity": {
            "passed": [
                "every instance's study and series exist in studies",
                "every series instance count (se.ni) equals exported instances",
                "every chosen patient has all of their source studies exported",
                "every study/series center is in cfg_centers and every series device is in devices, historical snapshots included",
                "every trash event is exported with all of its documents",
                "every trash snapshot, audit row and viewer session points to an exported or historical study",
            ],
            "sourceDanglingReferences": dangling,
        },
    }
    (args.destination / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    print(f"total\t{total}\tpatients:{len(patients)}\tstudies:{len(studies)}\ttrashEvents:{manifest['history']['trashEvents']}\tredactions:{len(redactions)}")


if __name__ == "__main__":
    main()
