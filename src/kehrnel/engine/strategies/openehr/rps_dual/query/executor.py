"""Execute compiled plans using strategy adapters."""
from __future__ import annotations

from copy import deepcopy
from time import perf_counter
from typing import Any, Dict, List
import uuid

from bson import ObjectId
from bson.binary import Binary, UuidRepresentation
from bson.code import Code

from kehrnel.engine.core.errors import KehrnelError
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
    # ObjectId is the default identifier representation for RPS Dual.  It is
    # not JSON serializable, so an otherwise successful query would fail at
    # the HTTP response boundary unless it is rendered in its canonical text
    # form.  This conversion is independent of the optional UUID id policy.
    if isinstance(value, ObjectId):
        return str(value)
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

    supported_engine = (
        plan.engine == "mongo_pipeline"
        or plan.engine.startswith("atlas_search")
        or plan.engine == "text_search_dual"
    )
    if not supported_engine:
        raise KehrnelError(
            code="OPENEHR_QUERY_ENGINE_UNSUPPORTED",
            status=500,
            message="The compiled openEHR query selected an unsupported execution engine.",
            details={"engine": plan.engine},
        )
    collection = plan.plan.get("collection")
    if not isinstance(collection, str) or not collection.strip():
        raise KehrnelError(
            code="OPENEHR_QUERY_PLAN_INVALID",
            status=500,
            message="The compiled openEHR query has no target collection.",
            details={"engine": plan.engine},
        )
    if storage is None:
        raise KehrnelError(
            code="OPENEHR_QUERY_STORAGE_UNAVAILABLE",
            status=503,
            message="The active openEHR strategy has no query storage adapter.",
            details={"engine": plan.engine, "collection": collection},
        )
    try:
        if plan.engine == "mongo_pipeline":
            db_started = perf_counter()
            rows = await storage.aggregate(collection, prepared_pipeline)
            db_duration_ms = round((perf_counter() - db_started) * 1000, 2)
        elif plan.engine.startswith("atlas_search") or plan.engine == "text_search_dual":
            # same aggregate; pipeline starts with $search
            db_started = perf_counter()
            rows = await storage.aggregate(collection, prepared_pipeline)
            db_duration_ms = round((perf_counter() - db_started) * 1000, 2)
    except KehrnelError:
        raise
    except Exception as exc:
        # An execution failure is not an empty cohort.  Return a stable,
        # non-sensitive error so callers cannot accidentally treat an Atlas
        # Search index/configuration problem as a valid zero-result query.
        raise KehrnelError(
            code="OPENEHR_QUERY_EXECUTION_FAILED",
            status=502,
            message="The openEHR query could not be executed.",
            details={
                "engine": plan.engine,
                "collection": collection,
                "errorType": type(exc).__name__,
            },
        ) from exc
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise KehrnelError(
            code="OPENEHR_QUERY_RESULT_INVALID",
            status=502,
            message="The openEHR storage adapter returned an invalid query result.",
            details={"engine": plan.engine, "collection": collection},
        )
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
