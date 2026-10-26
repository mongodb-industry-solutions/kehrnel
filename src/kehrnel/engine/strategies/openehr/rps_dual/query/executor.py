"""Execute compiled plans using strategy adapters."""
from __future__ import annotations

from copy import deepcopy
from time import perf_counter
from typing import Any, Dict, List
import uuid

from bson.binary import Binary, UuidRepresentation
from bson.code import Code

from kehrnel.engine.core.types import QueryResult, QueryPlan, StrategyContext


def _prepare_pipeline(value: Any) -> Any:
    if isinstance(value, list):
        return [_prepare_pipeline(item) for item in value]
    if isinstance(value, dict):
        prepared = {key: _prepare_pipeline(item) for key, item in value.items()}
        fn_spec = prepared.get("$function")
        if isinstance(fn_spec, dict) and isinstance(fn_spec.get("body"), str):
            fn_spec["body"] = Code(fn_spec["body"])
        return prepared
    return value


def _binary_uuid_to_string(value: Binary) -> str | None:
    if value.subtype != 4 or len(value) != 16:
        return None
    try:
        return str(value.as_uuid(UuidRepresentation.STANDARD))
    except Exception:
        return str(uuid.UUID(bytes=bytes(value)))


def _looks_like_id_key(key: str | None) -> bool:
    if not key:
        return False
    normalized = key.lower()
    id_keys = {"_id", "id", "ehr_id", "ehrid", "comp_id", "composition_id", "compositionid"}
    return normalized in id_keys or normalized.endswith("_id")


def _uses_uuid_binary_ids(config: Dict[str, Any] | None) -> bool:
    ids = (config or {}).get("ids") if isinstance(config, dict) else None
    if not isinstance(ids, dict):
        return False
    uuid_policies = {"uuid", "uuidbin", "uuid_bin"}
    return any(
        str(ids.get(field) or "").strip().lower() in uuid_policies
        for field in ("ehr_id", "composition_id")
    )


def _normalize_result_value(value: Any, key: str | None = None, *, normalize_binary_uuid: bool = False) -> Any:
    if isinstance(value, Binary) and normalize_binary_uuid and _looks_like_id_key(key):
        converted = _binary_uuid_to_string(value)
        return converted if converted is not None else value
    if (
        normalize_binary_uuid
        and isinstance(value, (bytes, bytearray))
        and _looks_like_id_key(key)
        and len(value) == 16
    ):
        return str(uuid.UUID(bytes=bytes(value)))
    if isinstance(value, list):
        return [
            _normalize_result_value(item, key, normalize_binary_uuid=normalize_binary_uuid)
            for item in value
        ]
    if isinstance(value, dict):
        return {
            item_key: _normalize_result_value(
                item_value,
                str(item_key),
                normalize_binary_uuid=normalize_binary_uuid,
            )
            for item_key, item_value in value.items()
        }
    return value


async def execute(ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
    storage = (ctx.adapters or {}).get("storage")
    rows: List[Dict[str, Any]] = []
    prepared_pipeline = _prepare_pipeline(plan.plan.get("pipeline", []))
    explain = deepcopy(plan.plan.get("explain") or {})
    db_duration_ms = None
    if plan.plan.get("pipeline") is not None:
        explain["pipeline"] = plan.plan.get("pipeline")
    if plan.plan.get("collection") is not None and "collection" not in explain:
        explain["collection"] = plan.plan.get("collection")
    if plan.plan.get("scope") is not None and "scope" not in explain:
        explain["scope"] = plan.plan.get("scope")
    try:
        if plan.engine == "mongo_pipeline":
            if storage and plan.plan.get("collection"):
                db_started = perf_counter()
                rows = await storage.aggregate(plan.plan["collection"], prepared_pipeline)
                db_duration_ms = round((perf_counter() - db_started) * 1000, 2)
        elif plan.engine.startswith("atlas_search") or plan.engine == "text_search_dual":
            # same aggregate; pipeline starts with $search
            if storage and plan.plan.get("collection"):
                db_started = perf_counter()
                rows = await storage.aggregate(plan.plan["collection"], prepared_pipeline)
                db_duration_ms = round((perf_counter() - db_started) * 1000, 2)
    except Exception as exc:  # surface pipeline even on failure
        explain["error"] = str(exc)
    if rows:
        normalize_binary_uuid = _uses_uuid_binary_ids(ctx.config)
        rows = [
            _normalize_result_value(row, normalize_binary_uuid=normalize_binary_uuid)
            for row in rows
        ]
        sequence_tokens = [
            row.get("__searchSequenceToken")
            for row in rows
            if isinstance(row, dict) and row.get("__searchSequenceToken")
        ]
        if sequence_tokens:
            pagination = dict(explain.get("pagination") or plan.plan.get("pagination") or {})
            pagination["previousPageToken"] = sequence_tokens[0]
            pagination["nextPageToken"] = sequence_tokens[-1]
            explain["pagination"] = pagination
            for row in rows:
                if isinstance(row, dict):
                    row.pop("__searchSequenceToken", None)
    if db_duration_ms is not None:
        explain_timings = dict(explain.get("timings") or {})
        explain_timings["kehrnel_db_ms"] = db_duration_ms
        explain["timings"] = explain_timings
    return QueryResult(engine_used=plan.engine, rows=rows, explain=explain)
