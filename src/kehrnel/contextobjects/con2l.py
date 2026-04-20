from __future__ import annotations

from typing import Any, Dict, List

from .models import safe_list


def normalize_con2l_executable(payload: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(payload or {})
    predicates = []
    for predicate in safe_list(payload.get("predicates")):
        data = dict(predicate or {})
        predicates.append(
            {
                "field": data.get("field") or data.get("path") or "unknown",
                "op": data.get("op") or "eq",
                "value": data.get("value"),
                "label": data.get("label") or data.get("semantic"),
            }
        )
    return {
        "stage": payload.get("stage") or "executable",
        "source_definition": payload.get("source_definition")
        or payload.get("sourceDefinition")
        or payload.get("from")
        or "unknown_context_contract",
        "scope": payload.get("scope") or "subject",
        "subject_filter": dict(payload.get("subject_filter") or payload.get("subjectFilter") or {}),
        "predicates": predicates,
        "projection": dict(payload.get("projection") or {}),
        "sort": dict(payload.get("sort") or {}),
        "limit": payload.get("limit"),
        "meta": dict(payload.get("meta") or {}),
    }


def build_executable_from_resolution(draft: Dict[str, Any] | None, resolution: Dict[str, Any]) -> Dict[str, Any]:
    draft = dict(draft or {})
    request_ir = draft.get("request_ir") or draft.get("requestIr") or {}
    matched_points = resolution.get("matchedRequestedPoints") or resolution.get("requestedPoints") or []
    predicates: List[Dict[str, Any]] = []
    for point in matched_points:
        slug = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(point)).strip("_")
        predicates.append(
            {
                "field": f"semantic.{slug or 'signal'}",
                "op": "exists",
                "value": True,
                "label": point,
            }
        )

    return {
        "stage": "executable",
        "source_definition": resolution.get("contextContract") or "unknown_context_contract",
        "scope": resolution.get("scope") or request_ir.get("scope") or "subject",
        "subject_filter": {},
        "predicates": predicates,
        "projection": {"_id": 1, "source_definition": 1, "semantic": 1},
        "sort": {},
        "limit": None,
        "meta": {
            "draft": draft,
            "assertionType": resolution.get("assertionType"),
            "requestedPoints": resolution.get("requestedPoints") or matched_points,
        },
    }


def _predicate_match(predicate: Dict[str, Any]) -> Dict[str, Any]:
    field = predicate.get("field") or "unknown"
    op = predicate.get("op") or "eq"
    value = predicate.get("value")
    if op == "exists":
        return {field: {"$exists": bool(value)}}
    if op == "in":
        return {field: {"$in": safe_list(value)}}
    if op == "gt":
        return {field: {"$gt": value}}
    if op == "gte":
        return {field: {"$gte": value}}
    if op == "lt":
        return {field: {"$lt": value}}
    if op == "lte":
        return {field: {"$lte": value}}
    return {field: value}


def compile_con2l_to_query_plan(
    executable: Dict[str, Any] | None,
    *,
    default_collection: str = "contextobjects",
) -> Dict[str, Any]:
    executable = normalize_con2l_executable(executable)
    source_definition = executable["source_definition"]

    match_clauses: List[Dict[str, Any]] = []
    if source_definition and source_definition != "unknown_context_contract":
        match_clauses.append(
            {
                "$or": [
                    {"schema.id": source_definition},
                    {"schemaId": source_definition},
                    {"co.id": source_definition},
                    {"context_definition_id": source_definition},
                    {"definition_id": source_definition},
                ]
            }
        )

    if executable["subject_filter"]:
        match_clauses.append(executable["subject_filter"])
    for predicate in executable["predicates"]:
        match_clauses.append(_predicate_match(predicate))

    pipeline: List[Dict[str, Any]] = []
    if match_clauses:
        pipeline.append({"$match": {"$and": match_clauses} if len(match_clauses) > 1 else match_clauses[0]})
    if executable["sort"]:
        pipeline.append({"$sort": executable["sort"]})
    if executable["limit"]:
        pipeline.append({"$limit": int(executable["limit"])})
    if executable["projection"]:
        pipeline.append({"$project": executable["projection"]})

    return {
        "engine": "mongo",
        "collection": executable["meta"].get("collection") or default_collection,
        "pipeline": pipeline,
        "semantic": {
            "scope": executable["scope"],
            "sourceDefinition": source_definition,
            "predicateCount": len(executable["predicates"]),
        },
        "executable": executable,
    }
