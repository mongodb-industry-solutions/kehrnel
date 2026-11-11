"""Bounded, source-preserving series projections for the DICOM playground.

The unit of publication is an explicitly bounded batch, not the full archive.
Sequences can be inspected; correlated groups require scalar values from the
same instance. Frame/sequence-item joins are intentionally not advertised.
"""
from __future__ import annotations

import copy
import itertools
import math
import re

import jsonschema

from . import model
from .common import bounded_bson, centers, config, digest, fail

PROFILE_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["version", "id", "name", "fields", "groups"],
    "properties": {
        "version": {"const": "dicom-rule-profile/v1"},
        "id": {"type": "string", "pattern": "^[a-z][a-z0-9_-]{0,63}$"},
        "name": {"type": "string", "minLength": 1, "maxLength": 128},
        "modalities": {"type": "array", "maxItems": 32, "uniqueItems": True,
                       "items": {"type": "string", "pattern": "^[A-Z0-9]{1,16}$"}},
        "sopClasses": {"type": "array", "maxItems": 32, "uniqueItems": True,
                       "items": {"type": "string", "pattern": model.UID_RE.pattern, "maxLength": 64}},
        "fields": {"type": "array", "minItems": 1, "maxItems": 32, "items": {
            "type": "object", "additionalProperties": False,
            "required": ["name", "source", "path", "type", "aggregate", "target"],
            "properties": {
                "name": {"type": "string", "pattern": "^[a-z][A-Za-z0-9_]{0,47}$"},
                "source": {"enum": ["header", "ContentIndex"]},
                "path": {"type": "array", "minItems": 1, "maxItems": 8,
                         "items": {"type": "string", "pattern": "^(?:[A-Za-z0-9_]+|\\*)$", "maxLength": 64}},
                "type": {"enum": ["string", "number"]},
                "aggregate": {"enum": ["distinct", "common", "range"]},
                "target": {"enum": ["embedded", "research"]},
                "maxValues": {"type": "integer", "minimum": 1, "maximum": 64},
                "unit": {"type":"string","minLength":1,"maxLength":32},
            }}},
        "groups": {"type": "array", "maxItems": 8, "items": {
            "type": "object", "additionalProperties": False,
            "required": ["name", "fields", "target"],
            "properties": {
                "name": {"type": "string", "pattern": "^[a-z][A-Za-z0-9_]{0,47}$"},
                "fields": {"type": "array", "minItems": 2, "maxItems": 8, "uniqueItems": True,
                           "items": {"type": "string"}},
                "target": {"enum": ["embedded", "research"]},
                "maxGroups": {"type": "integer", "minimum": 1, "maximum": 64},
            }}},
    },
}


def validate(profile):
    try:
        jsonschema.Draft7Validator(PROFILE_SCHEMA).validate(profile)
    except jsonschema.ValidationError as exc:
        fail("DICOM_RULE_INVALID", "Invalid projection rule at " + "/".join(map(str, exc.absolute_path)))
    names = [x["name"] for x in profile["fields"]]
    group_names = [x["name"] for x in profile["groups"]]
    if len(set(names)) != len(names) or len(set(group_names)) != len(group_names):
        fail("DICOM_RULE_INVALID", "Field and group names must be unique.")
    for group in profile["groups"]:
        if not set(group["fields"]) <= set(names):
            fail("DICOM_RULE_INVALID", "A group references an undefined field.")
    for field in profile["fields"]:
        if field["aggregate"] == "range" and field["type"] != "number":
            fail("DICOM_RULE_INVALID", "Range aggregation requires a numeric field.")
        if field["source"] == "header" and any(t != "*" and not re.fullmatch("[0-9A-F]{8}", t) for t in field["path"]):
            fail("DICOM_RULE_INVALID", "Header paths use hexadecimal tags and explicit sequence wildcards.")
    for value in profile.get("sopClasses", []):
        model.uid(value)
    return "v1:" + digest(profile)


def _read(node, path, dicom):
    if not path:
        if dicom and (not isinstance(node,dict) or "vr" not in node):
            return []
        if dicom and isinstance(node, dict) and "vr" in node:
            if node["vr"] == "SQ" or "InlineBinary" in node or "BulkDataURI" in node:
                return []
            values = node.get("Value", [])
            values = values if isinstance(values,list) else [values]
            return [(value,node.get("vr")) for value in values]
        return node if isinstance(node, list) else ([] if node is None else [node])
    if dicom and isinstance(node, dict) and node.get("vr") == "SQ":
        node = node.get("Value", [])
    step, *rest = path
    if step == "*":
        if not isinstance(node, list):
            return []
        return list(itertools.chain.from_iterable(_read(child, rest, dicom) for child in node))
    return _read(node.get(step), rest, dicom) if isinstance(node, dict) else []


def extract(raw, field):
    source = _read(raw.get(field["source"], {}), field["path"], field["source"] == "header")
    result, invalid = [], 0
    for item in source:
        value, vr = item if field["source"] == "header" else (item,None)
        if value in (None, ""):
            continue
        if field["type"] == "string":
            if isinstance(value, str):
                result.append(value)
            else:
                invalid += 1
        else:
            try:
                if field["source"] == "header" and vr not in model.NUMERIC_VRS:
                    raise ValueError()
                if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                    raise ValueError()
                number = float(value)
                if not math.isfinite(number):
                    raise ValueError()
                result.append(number)
            except (ValueError, TypeError, OverflowError):
                invalid += 1
    result = list({digest(v): v for v in result}.values())
    if len(result) > field.get("maxValues", 16):
        fail("DICOM_RULE_CARDINALITY_EXCEEDED", "An instance exceeds the field value bound; narrow the rule.")
    return result, invalid


def prepare(ctx, payload):
    cfg = config(ctx)
    studies, instances = payload.get("studies", []), payload.get("instances", [])
    if len(studies) + len(instances) > cfg["ingest"]["max_documents"]:
        fail("DICOM_INGEST_BOUND_EXCEEDED", "Use a bounded batch of studies and instances.")
    bounded_bson(payload, cfg["ingest"]["max_bytes"])
    bases, parents = {}, {}
    for raw in studies:
        base, children = model.study(ctx, cfg, raw)
        if base["_id"] in bases:
            fail("DICOM_STUDY_DUPLICATE", "Duplicate study in the batch.")
        bases[base["_id"]] = base
        for child in children:
            if child["_id"] in parents:
                fail("DICOM_HIERARCHY_CONFLICT", "A series belongs to multiple studies.")
            parents[child["_id"]] = child
    accepted, replayed = {}, 0
    for raw in instances:
        if not isinstance(raw.get("header"), dict):
            fail("DICOM_HEADER_REQUIRED", "An instance source must contain a DICOM JSON header.")
        parent = parents.get(model.scalar(raw.get("header", {}), "0020000E"))
        doc = model.instance(ctx, cfg, raw, parent)
        previous = accepted.get(doc["_id"])
        if previous:
            if digest(previous["raw"]) != digest(raw):
                fail("DICOM_RECEPTION_CONFLICT", "Same SOP UID with changed source requires explicit resolution.", 409)
            replayed += 1
        else:
            accepted[doc["_id"]] = {"raw": copy.deepcopy(raw), "doc": doc}
    if not bases:
        fail("DICOM_SOURCE_REQUIRED", "Include the authoritative study catalogue for this bounded batch.")
    source_generation = digest({"studies": [bases[k] for k in sorted(bases)],
                                "instances": [accepted[k]["raw"] for k in sorted(accepted)]})
    return cfg, bases, parents, accepted, source_generation, replayed


def summarize(records, profile, expected):
    fields, attrs, extracted = {}, [], {}
    for rule in profile["fields"]:
        values, missing, invalid = [], 0, 0
        for sop, record in records.items():
            observed, failures = extract(record["raw"], rule)
            extracted[sop, rule["name"]] = observed
            values.extend(observed); missing += not bool(observed); invalid += failures
        distinct = sorted(set(values))
        max_values = rule.get("maxValues", 16)
        if len(distinct) > max_values and rule["aggregate"] != "range":
            fail("DICOM_RULE_CARDINALITY_EXCEEDED", "A series exceeds the distinct-value bound; use a separate or narrower projection.")
        coverage = {"observedSops": len(records), "withValue": len(records)-missing,
                    "missingOrEmpty": missing, "invalidValues": invalid,
                    "sourceObservedSops": expected}
        complete = expected is not None and expected == len(records) and missing == 0 and invalid == 0
        entry = {"type": rule["type"], "aggregate": rule["aggregate"], "coverage": coverage,
                 "state": "complete_against_source_observed_count" if complete else "partial_or_unknown",
                 "distinctCount": len(distinct), "source": {"container": rule["source"], "path": rule["path"]}}
        if rule.get("unit"):
            entry["unit"] = rule["unit"]
            entry["unitPolicy"] = "declared source unit; no conversion performed"
        if rule["aggregate"] == "range":
            if rule["type"] != "number":
                fail("DICOM_RULE_INVALID", "Range aggregation requires a numeric field.")
            entry.update(min=min(distinct) if distinct else None, max=max(distinct) if distinct else None,
                         queryable=False, reason="Extrema are statistics; arbitrary ranges require value-preserving projection.")
        elif rule["aggregate"] == "common":
            entry.update(values=distinct if complete and len(distinct) == 1 else [], queryable=True,
                         commonAcrossObserved=len(distinct) == 1 and missing == 0)
        else:
            entry.update(values=distinct, queryable=True)
        fields[rule["name"]] = entry
        if rule["target"] == "research":
            for value in entry.get("values", []):
                attrs.append({"p": rule["name"], "n" if rule["type"] == "number" else "s": value})
    groups = []
    for rule in profile["groups"]:
        tuples, unavailable, ambiguous = {}, 0, 0
        for sop in records:
            observed = [extracted[sop, field] for field in rule["fields"]]
            if any(len(x) > 1 for x in observed):
                ambiguous += 1; continue
            if any(not x for x in observed):
                unavailable += 1; continue
            value = {field: {"n" if fields[field]["type"] == "number" else "s": observed[i][0]}
                     for i, field in enumerate(rule["fields"])}
            key = digest(value)
            tuples.setdefault(key, {"p": rule["name"], "v": value, "sopCount": 0})["sopCount"] += 1
        if len(tuples) > rule.get("maxGroups", 16):
            fail("DICOM_RULE_CARDINALITY_EXCEEDED", "Observed groups exceed the configured group bound.")
        groups.append({"name": rule["name"], "target": rule["target"], "values": list(tuples.values()),
                       "coverage": {"observedSops": len(records), "missingSops": unavailable,
                                    "ambiguousSops": ambiguous}, "semantics": "same_instance_scalar_tuple"})
    return fields, attrs, groups


def collection_names(cfg):
    # Fixed suffixes; profiles cannot choose arbitrary databases/collections.
    result = {"studies": cfg["collections"]["studies"]+"_rule_snapshots",
            "inventory": cfg["collections"]["instances"]+"_rule_inventory",
            "research": cfg["collections"]["research"]+"_series",
            "profiles": cfg["collections"]["research"]+"_profiles",
            "generations": cfg["collections"]["research"]+"_generations",
            "inbox": cfg["collections"]["instances"]+"_inbox",
            "samples": cfg["collections"]["research"]+"_samples"}
    if len(set(result.values())) != len(result) or set(result.values()) & set(cfg["collections"].values()):
        fail("CONFIG_INVALID", "Generated playground collection names must not collide with catalogue collections.")
    return result


def preview(ctx, payload):
    profile = payload["profile"]; profile_version = validate(profile)
    cfg, bases, parents, accepted, source_gen, replayed = prepare(ctx, payload)
    generation = digest([cfg["tenant_id"], profile_version, source_gen])
    rows, indexable = [], 0
    grouped = {key: {} for key in parents}
    for sop, r in accepted.items():
        grouped[r["doc"]["_kehrnel"]["seriesUid"]][sop] = r
    selected = 0
    for series_uid, parent in parents.items():
        records = grouped[series_uid]
        modality = parent["source"].get("mo")
        if profile.get("modalities") and modality not in profile["modalities"]:
            continue
        if profile.get("sopClasses"):
            # A mixed class series is evaluated on its matching subset, with
            # original observed count retained so completeness isn't invented.
            records = {sop:r for sop,r in records.items() if model.scalar(r["raw"]["header"],"00080016") in profile["sopClasses"]}
        if not records:
            continue
        selected += len(records)
        raw_expected = parent["source"].get("ni")
        expected = raw_expected if type(raw_expected) is int and raw_expected >= 0 else None
        fields, attrs, groups = summarize(records, profile, expected)
        counts = {"receivedUniqueSops": len(records), "sourceObservedSops": expected,
                  "state": "complete_against_source_observed_count" if expected == len(records) else "partial_or_unknown"}
        embedded = {"profileVersion": profile_version, "sourceGeneration": source_gen, "counts": counts,
                    "fields": {r["name"]: fields[r["name"]] for r in profile["fields"] if r["target"] == "embedded"},
                    "groups": [g for g in groups if g["target"] == "embedded"]}
        for child in bases[parent["_kehrnel"]["studyUid"]]["se"]:
            if child["si"] == series_uid:
                child["_kehrnel"] = {**copy.deepcopy(parent["_kehrnel"]), "ruleSummary": embedded}
                bounded_bson(child["_kehrnel"], 256*1024)
        row = {"_id": generation+":"+series_uid, "seriesUid": series_uid,
               "generationId": generation, "profileId": profile["id"], "profileVersion": profile_version,
               "_kehrnel": copy.deepcopy(parent["_kehrnel"]), "counts": counts,
               "fields": {r["name"]: fields[r["name"]] for r in profile["fields"] if r["target"] == "research"},
               "attrs": attrs, "groups": list(itertools.chain.from_iterable(g["values"] for g in groups if g["target"] == "research")),
               "groupCoverage": {g["name"]: g["coverage"] for g in groups if g["target"] == "research"}}
        bounded_bson(row, 512*1024); rows.append(row); indexable += 1+len(attrs)+len(row["groups"])
    snapshots = []
    for study_uid, base in bases.items():
        base["studyUid"] = study_uid; base["_id"] = generation+":"+study_uid
        base["generationId"] = generation; snapshots.append(base)
    inventory = []
    for r in accepted.values():
        metadata = r["doc"]["_kehrnel"]
        inventory.append({"_id": r["doc"]["_id"], "st": metadata["studyUid"], "se": metadata["seriesUid"],
                          "sz": r["raw"].get("sz"), "_kehrnel": copy.deepcopy(metadata),
                          "hasContentIndex": "ContentIndex" in r["raw"],
                          "headerPlacement": cfg["headers"]["mode"]})
    result = {"ok": True, "contract": "dicom-rule-preview/v1", "persisted": False,
              "profileVersion": profile_version, "sourceGeneration": source_gen, "generationId": generation,
              "scope": "bounded_batch", "publishedCorpus": False,
              "counts": {"studies": len(snapshots), "seriesProjected": len(rows), "uniqueSops": len(accepted),
                         "selectedSops": selected, "replayedReceptions": replayed, "searchIndexObjectsEstimate": indexable},
              "collections": {"studies": snapshots, "inventory": inventory, "research": rows, "profiles": [copy.deepcopy(profile)]},
              "collectionNames": collection_names(cfg), "indexPlan": index_plan(cfg, profile),
              "limitations": ["No pixel decoding", "No full-corpus publication", "No implicit clinical conditions",
                              "Groups require same-instance scalars; multivalued sequence/frame correlation is not compiled"]}
    bounded_bson(result)
    return result


def index_plan(cfg, profile):
    names = collection_names(cfg)
    fields = {"generationId": {"type": "token"}, "_kehrnel": {"type": "document", "fields": {
        k: {"type": "token"} for k in ["tenantId","centerId","modality","day","studyUid","seriesUid"]}},
        "attrs": {"type": "embeddedDocuments", "fields": {"p": {"type": "token"}, "s": {"type": "token"}, "n": {"type": "number"}}}}
    research_groups = [g for g in profile["groups"] if g["target"] == "research"]
    if research_groups:
        definitions = {f["name"]: {"type": "document", "fields": {"n" if f["type"] == "number" else "s": {"type": "number" if f["type"] == "number" else "token"}}} for f in profile["fields"]}
        fields["groups"] = {"type": "embeddedDocuments", "fields": {"p": {"type": "token"}, "v": {"type": "document", "fields": definitions}}}
    prefix = [("_kehrnel.tenantId",1),("_kehrnel.centerId",1),("generationId",1)]
    return {"automatic": False, "collection": names["research"], "regular": [
        {"keys": prefix+[("attrs.p",1),("attrs.s",1)], "options": {"name":"idx_dicom_rule_strings"}},
        {"keys": prefix+[("attrs.p",1),("attrs.n",1)], "options": {"name":"idx_dicom_rule_numbers"}}],
        "search": {"name": "dicom_rules_"+digest({"mappings":{"dynamic":False,"fields":fields}})[:16], "definition": {"mappings": {"dynamic": False,"fields":fields}}},
        "searchExecution": "plan_only", "readiness": "Apply and verify index readiness separately; updates are eventually visible."}


def compile_query(ctx, payload):
    profile = payload["profile"]; validate(profile)
    cfg = config(ctx); rules = {r["name"]: r for r in profile["fields"]}
    requested = payload.get("where", [])
    if payload.get("text"):
        text = payload["text"].strip()
        match = re.fullmatch(r"(?:find|show)\s+(?:([A-Z0-9]+)\s+)?series\s+(?:where|with)\s+(.+)", text, re.IGNORECASE)
        if not match:
            fail("DICOM_LANGUAGE_UNSUPPORTED", "Use 'Find [modality] series where field = value', or the structured query editor.")
        if requested:
            fail("DICOM_QUERY_INVALID", "Use either natural language or structured predicates.")
        requested = []
        for clause in re.split(r"\s+and\s+", match[2], flags=re.IGNORECASE):
            part = re.fullmatch(r"([a-zA-Z][a-zA-Z0-9_]*)\s*(>=|<=|=|>|<)\s*(.+)",clause)
            if not part:
                fail("DICOM_LANGUAGE_UNSUPPORTED", "Use explicit rule field names and comparison operators.")
            field, op, literal = part.groups()
            if field not in rules:
                fail("DICOM_RULE_FIELD_UNKNOWN", "Natural language referenced a field outside the active rules.")
            value = literal.strip().strip('"').strip("'")
            if rules[field]["type"] == "number":
                try: value = float(value)
                except ValueError: fail("DICOM_QUERY_INVALID", "A numeric rule requires a numeric literal.")
            requested.append({"field":field,"op":op,"value":value})
        if match[1]:
            payload = {**payload,"modality":match[1].upper()}
    if not 1 <= len(requested) <= 16:
        fail("DICOM_QUERY_INVALID", "Use one to sixteen rule predicates.")
    scope = {"_kehrnel.tenantId":cfg["tenant_id"], "_kehrnel.centerId":{"$in":centers(ctx,cfg)},
             "generationId":payload["generationId"]}
    filters, search = [], []
    operators = {"=":"$eq",">":"$gt",">=":"$gte","<":"$lt","<=":"$lte"}
    group_name = payload.get("group")
    group = next((g for g in profile["groups"] if g["name"] == group_name and g["target"] == "research"),None)
    if group_name and not group:
        fail("DICOM_RULE_GROUP_UNKNOWN", "Query a research group declared by the active profile.")
    group_match = {"p":group_name}
    attribute_matches = {}
    for clause in requested:
        field = rules.get(clause.get("field")); op = clause.get("op"); value = clause.get("value")
        if not field or (not group and field["target"] != "research") or field["aggregate"] == "range":
            fail("DICOM_RULE_FIELD_UNKNOWN", "Query a value-preserving research field in the active profile.")
        if op not in operators or (field["type"] == "string" and (op != "=" or not isinstance(value,str))) or (field["type"] == "number" and (type(value) not in (int,float) or not math.isfinite(value))):
            fail("DICOM_QUERY_INVALID", "Predicate type/operator does not match the active rule.")
        suffix = "n" if field["type"] == "number" else "s"
        predicate = {operators[op]:value}
        if group:
            if field["name"] not in group["fields"]:
                fail("DICOM_QUERY_INVALID", "Every correlated predicate must belong to the selected group.")
            key = f"v.{field['name']}.{suffix}"
            previous = group_match.setdefault(key,{})
            if operators[op] in previous:
                fail("DICOM_QUERY_INVALID", "Duplicate comparison operator on a correlated field.")
            previous.update(predicate)
        else:
            item = attribute_matches.setdefault(field["name"],{"p":field["name"],suffix:{}})
            if operators[op] in item[suffix]:
                fail("DICOM_QUERY_INVALID", "Duplicate comparison operator on an attribute.")
            item[suffix].update(predicate)
    for name, item in attribute_matches.items():
        filters.append({"attrs":{"$elemMatch":item}})
        suffix = "n" if "n" in item else "s"
        terms = [{"equals":{"path":"attrs.p","value":name}}]
        for op,value in item[suffix].items():
            terms.append({"equals":{"path":f"attrs.{suffix}","value":value}} if op == "$eq" else {"range":{"path":f"attrs.{suffix}",{"$gt":"gt","$gte":"gte","$lt":"lt","$lte":"lte"}[op]:value}})
        search.append({"embeddedDocument":{"path":"attrs","operator":{"compound":{"filter":terms}}}})
    if group:
        filters.append({"groups":{"$elemMatch":group_match}})
        clauses = [{"equals":{"path":"groups.p","value":group_name}}]
        for key, value in group_match.items():
            if key == "p": continue
            for op, literal in value.items():
                clauses.append({"equals":{"path":"groups."+key,"value":literal}} if op == "$eq" else {"range":{"path":"groups."+key,{"$gt":"gt","$gte":"gte","$lt":"lt","$lte":"lte"}[op]:literal}})
        search.append({"embeddedDocument":{"path":"groups","operator":{"compound":{"filter":clauses}}}})
    if payload.get("modality"):
        scope["_kehrnel.modality"] = payload["modality"]
    filters.insert(0,scope)
    limit = payload.get("limit",50)
    if type(limit) is not int or not 1 <= limit <= 100:
        fail("DICOM_QUERY_INVALID", "Use a limit between 1 and 100.")
    search_scope = [{"equals":{"path":k,"value":v}} for k,v in scope.items() if isinstance(v,str)]
    search_scope.append({"in":{"path":"_kehrnel.centerId","value":centers(ctx,cfg)}})
    return {"ok":True,"contract":"dicom-rule-query/v1", "generationId":payload["generationId"],
            "predicates":requested,"modality":payload.get("modality"),
            "collection":collection_names(cfg)["research"],"pipeline":[{"$match":{"$and":filters}},{"$sort":{"_id":1}},{"$limit":limit}],
            "searchPipeline":[{"$search":{"index":index_plan(cfg,profile)["search"]["name"],"compound":{"filter":search_scope+search}}},{"$limit":limit}],
            "semantics":"same_instance_scalar_tuple" if group else "same_series_observed_values",
            "coverage":"bounded_batch; unknown and missing values are not negatives", "searchExecution":"plan_only"}
