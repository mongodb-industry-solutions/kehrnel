"""Rebuild profile-aware entities and workload materializations."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.cdisc.models import CdiscProfile
from kehrnel.engine.domains.cdisc.projection import PROJECTION_VERSION, project_record

from ..common import collections, config, replace_documents, storage_adapter


OPERATIONAL_OBJECT_SCHEMA_VERSION = "1.0.0"

SEND_EVIDENCE_DIMENSIONS = {
    "demographics": {"label": "Subject and study context", "domains": {"DM"}},
    "exposure": {"label": "Treatment and administered dose", "domains": {"TX", "EX"}},
    "bodyWeight": {"label": "Body weight and gain", "domains": {"BW", "BG"}},
    "foodConsumption": {"label": "Food consumption", "domains": {"FW"}},
    "clinicalObservations": {"label": "Clinical observations", "domains": {"CL"}},
    "laboratory": {"label": "Laboratory measurements", "domains": {"LB"}},
    "grossPathology": {"label": "Gross pathology", "domains": {"MA"}},
    "microscopicPathology": {"label": "Microscopic pathology", "domains": {"MI"}},
    "organMeasurements": {"label": "Organ measurements", "domains": {"OM"}},
    "toxicokinetics": {"label": "Concentration and exposure", "domains": {"PC", "PP"}},
    "studyContext": {"label": "Study phase and disposition", "domains": {"SE", "DS"}},
}


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _semantic_text(record: Dict[str, Any], *, excluded: set[str], maximum: int) -> str:
    data = record.get("data") or {}
    values = [str(record.get("domain") or "")]
    for key in sorted(data):
        value = data[key]
        if key in excluded or value in (None, ""):
            continue
        values.append(f"{key} {value}")
    return " | ".join(values)[:maximum]


def _entity_documents(
    *, tenant_id: str, snapshot_ref: str, records: Iterable[Dict[str, Any]], build_id: str
) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str], Dict[str, Any]] = {}
    for record in records:
        for ref in record.get("entityRefs") or []:
            key = (str(ref.get("type")), str(ref.get("id")))
            current = grouped.setdefault(
                key,
                {
                    "domains": set(),
                    "recordIds": [],
                    "studyId": record.get("studyId"),
                    "profile": record.get("profile"),
                },
            )
            current["domains"].add(record.get("domain"))
            current["recordIds"].append(record.get("_id"))
    documents = []
    for (entity_type, entity_id), value in sorted(grouped.items()):
        documents.append(
            {
                "_id": f"{snapshot_ref}:entity:{entity_type}:{entity_id}",
                "tenantId": tenant_id,
                "snapshotRef": snapshot_ref,
                "studyId": value["studyId"],
                "profile": value["profile"],
                "entityType": entity_type,
                "entityId": entity_id,
                "domains": sorted(item for item in value["domains"] if item),
                "recordIds": value["recordIds"],
                "recordCount": len(value["recordIds"]),
                "projectionBuildId": build_id,
                "projectionVersion": PROJECTION_VERSION,
            }
        )
    return documents


def _materialization_kind(profile: str) -> str:
    return {
        "send": "nonclinical-finding",
        "adam": "analysis-traceability",
        "tig": "product-evidence",
    }.get(profile, "subject-timeline")


def _first_facet(entries: List[Dict[str, Any]], key: str) -> Any:
    for entry in entries:
        value = (entry.get("facets") or {}).get(key)
        if value is not None and value != "":
            return value
    return None


def _send_operational_object(group_id: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    dimensions: Dict[str, Any] = {}
    for dimension_id, definition in SEND_EVIDENCE_DIMENSIONS.items():
        matching = [
            entry
            for entry in entries
            if str(entry.get("domain") or "").upper() in definition["domains"]
        ]
        days = {
            (entry.get("facets") or {}).get("studyDay")
            for entry in matching
            if (entry.get("facets") or {}).get("studyDay") is not None
        }
        dimensions[dimension_id] = {
            "label": definition["label"],
            "available": bool(matching),
            "domains": sorted(
                {str(entry.get("domain")) for entry in matching if entry.get("domain")}
            ),
            "recordCount": len(matching),
            "studyDays": sorted(days, key=lambda value: str(value)),
            "sourceRecordIds": [entry.get("recordId") for entry in matching if entry.get("recordId")],
        }

    return {
        "objectType": "SubjectExposureResponse",
        "objectSchemaVersion": OPERATIONAL_OBJECT_SCHEMA_VERSION,
        "subject": {
            "id": group_id,
            "species": _first_facet(entries, "species"),
            "strain": _first_facet(entries, "strain"),
            "sex": _first_facet(entries, "sex"),
            "treatmentGroup": _first_facet(entries, "treatmentGroup"),
        },
        "exposure": {
            "treatments": sorted({
                str((entry.get("facets") or {}).get("testArticle"))
                for entry in entries
                if (entry.get("facets") or {}).get("testArticle") not in (None, "")
            }),
            "doseLevels": sorted({
                str((entry.get("facets") or {}).get("doseLevel"))
                for entry in entries
                if (entry.get("facets") or {}).get("doseLevel") not in (None, "")
            }),
            "doseUnits": sorted({
                str((entry.get("facets") or {}).get("doseUnit"))
                for entry in entries
                if (entry.get("facets") or {}).get("doseUnit") not in (None, "")
            }),
            "routes": sorted({
                str((entry.get("facets") or {}).get("route"))
                for entry in entries
                if (entry.get("facets") or {}).get("route") not in (None, "")
            }),
        },
        "evidenceDimensions": dimensions,
        "sourceRecordIds": [entry.get("recordId") for entry in entries if entry.get("recordId")],
        "objectRecordCount": len(entries),
    }


def _materialization_documents(
    *, tenant_id: str, snapshot_ref: str, records: Iterable[Dict[str, Any]], build_id: str
) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    profile = "sdtm"
    study_id = None
    for record in records:
        profile = str(record.get("profile") or profile)
        study_id = record.get("studyId") or study_id
        facets = record.get("facets") or {}
        if profile == "send":
            key = str(facets.get("subjectId") or facets.get("treatmentGroup") or "study")
        elif profile == "adam":
            key = str(facets.get("parameterCode") or facets.get("subjectId") or "analysis")
        elif profile == "tig":
            key = str(facets.get("productId") or facets.get("evidenceId") or "portfolio")
        else:
            key = str(facets.get("subjectId") or "study")
        grouped[key].append(
            {
                "recordId": record.get("_id"),
                "datasetId": record.get("datasetId"),
                "domain": record.get("domain"),
                "rowOrdinal": record.get("rowOrdinal"),
                "facets": facets,
                "lineage": record.get("lineage") or {},
            }
        )
    kind = _materialization_kind(profile)
    documents = []
    send_group_evidence = {
        group_id: group_entries
        for group_id, group_entries in grouped.items()
        if profile == "send" and _first_facet(group_entries, "subjectId") is None
    }
    for group_id, entries in sorted(grouped.items()):
        entries.sort(
            key=lambda item: (
                item.get("facets", {}).get("studyDay") is None,
                item.get("facets", {}).get("studyDay") or 0,
                item.get("domain") or "",
                item.get("rowOrdinal") or 0,
            )
        )
        document = {
            "_id": f"{snapshot_ref}:view:{kind}:{group_id}",
            "tenantId": tenant_id,
            "snapshotRef": snapshot_ref,
            "studyId": study_id,
            "profile": profile,
            "kind": kind,
            "groupId": group_id,
            "entries": entries,
            "entryCount": len(entries),
            "projectionBuildId": build_id,
            "projectionVersion": PROJECTION_VERSION,
        }
        subject_id = _first_facet(entries, "subjectId") if profile == "send" else None
        if subject_id is not None:
            treatment_group = _first_facet(entries, "treatmentGroup")
            object_entries = [
                *entries,
                *send_group_evidence.get(str(treatment_group), []),
            ]
            document.update(_send_operational_object(str(subject_id), object_entries))
        documents.append(document)
    return documents


class ProjectionService:
    async def browse(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        resource = str(payload.get("resource") or "materializations").strip().lower()
        if resource not in {"entities", "materializations"}:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="resource must be entities or materializations")
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if not study_id or not snapshot_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="studyId and snapshotId are required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        coll = collections(cfg)
        snapshot_ref = f"{cfg['tenant_id']}:{study_id}:{snapshot_id}"
        snapshot = await storage.find_one(
            coll["snapshots"], {"_id": snapshot_ref, "tenantId": str(cfg["tenant_id"])}
        )
        if not snapshot:
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Snapshot was not found.")
        if bool(payload.get("publishedOnly", True)) and snapshot.get("state") != "published":
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_PUBLISHED", status=409, message="Snapshot is not published.")
        match: Dict[str, Any] = {"tenantId": str(cfg["tenant_id"]), "snapshotRef": snapshot_ref}
        for source, target in (("kind", "kind"), ("groupId", "groupId"), ("entityType", "entityType"), ("entityId", "entityId")):
            if payload.get(source) not in (None, ""):
                match[target] = str(payload[source])
        limit = min(max(int(payload.get("limit") or 100), 1), 1000)
        rows = await storage.aggregate(coll[resource], [{"$match": match}, {"$limit": limit}])
        return {"ok": True, "resource": resource, "snapshotRef": snapshot_ref, "rows": rows, "count": len(rows)}

    async def rebuild(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if not study_id or not snapshot_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="studyId and snapshotId are required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        coll = collections(cfg)
        snapshot_ref = f"{cfg['tenant_id']}:{study_id}:{snapshot_id}"
        snapshot = await storage.find_one(
            coll["snapshots"], {"_id": snapshot_ref, "tenantId": str(cfg["tenant_id"])}
        )
        if not snapshot:
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Snapshot was not found.")
        records = await storage.aggregate(
            coll["records"],
            [
                {"$match": {"tenantId": str(cfg["tenant_id"]), "snapshotRef": snapshot_ref}},
                {"$sort": {"datasetId": 1, "rowOrdinal": 1}},
            ],
        )
        maximum = int((cfg.get("projections") or {}).get("max_records_per_rebuild", 250_000))
        if len(records) > maximum:
            raise KehrnelError(
                code="CDISC_PROJECTION_LIMIT",
                status=413,
                message="Snapshot exceeds the configured synchronous projection rebuild limit.",
                details={"records": len(records), "maximum": maximum},
            )
        projected = []
        semantic_cfg = cfg.get("semantic") or {}
        semantic_enabled = bool(semantic_cfg.get("enabled", True))
        excluded = {str(item).upper() for item in semantic_cfg.get("exclude_variables") or []}
        maximum_text = int(semantic_cfg.get("max_text_chars", 4000))
        for record in records:
            try:
                profile = CdiscProfile(str(record.get("profile") or snapshot.get("profile")))
            except ValueError as exc:
                raise KehrnelError(code="CDISC_PROFILE_UNSUPPORTED", status=400, message=str(exc)) from exc
            facets, refs = project_record(profile, str(record.get("domain") or ""), record.get("data") or {})
            updated = {
                **record,
                "facets": facets,
                "entityRefs": [item.model_dump(mode="json") for item in refs],
            }
            if semantic_enabled:
                updated["semantic"] = {
                    "text": _semantic_text(updated, excluded=excluded, maximum=maximum_text),
                    "projectionVersion": PROJECTION_VERSION,
                }
            projected.append(updated)
        projection_fingerprint = [
            {
                "recordId": item.get("_id"),
                "facets": item.get("facets"),
                "entityRefs": item.get("entityRefs"),
                "semantic": item.get("semantic"),
            }
            for item in projected
        ]
        build_id = f"sha256:{_digest({'version': PROJECTION_VERSION, 'records': projection_fingerprint})}"
        entities = _entity_documents(
            tenant_id=str(cfg["tenant_id"]), snapshot_ref=snapshot_ref, records=projected, build_id=build_id
        )
        views = _materialization_documents(
            tenant_id=str(cfg["tenant_id"]), snapshot_ref=snapshot_ref, records=projected, build_id=build_id
        )
        operational_object_count = sum(1 for item in views if item.get("objectType"))
        now = datetime.now(timezone.utc).isoformat()
        manifest = {
            "_id": f"{snapshot_ref}:projection:{build_id}",
            "tenantId": str(cfg["tenant_id"]),
            "snapshotRef": snapshot_ref,
            "studyId": study_id,
            "profile": snapshot.get("profile"),
            "kind": "projection-manifest",
            "projectionBuildId": build_id,
            "projectionVersion": PROJECTION_VERSION,
            "recordCount": len(projected),
            "entityCount": len(entities),
            "viewCount": len(views),
            "operationalObjectCount": operational_object_count,
            "createdAt": now,
        }
        await replace_documents(storage, coll["records"], projected)
        await replace_documents(storage, coll["entities"], entities)
        await replace_documents(storage, coll["materializations"], [*views, manifest])
        await replace_documents(
            storage,
            coll["snapshots"],
            [{**snapshot, "projectionBuildId": build_id, "projectionVersion": PROJECTION_VERSION}],
        )
        return {
            "ok": True,
            "snapshotRef": snapshot_ref,
            "projectionBuildId": build_id,
            "projectionVersion": PROJECTION_VERSION,
            "recordCount": len(projected),
            "entityCount": len(entities),
            "materializationCount": len(views),
            "operationalObjectCount": operational_object_count,
        }
