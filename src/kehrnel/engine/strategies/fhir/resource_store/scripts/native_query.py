"""Bounded, read-only MongoDB find surface for FHIR implementation work."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.resource_store.scripts import bridge
from kehrnel.engine.strategies.fhir.resource_store.scripts.capabilities import (
    resolve_resource_capabilities,
)
from kehrnel.engine.strategies.fhir.resource_store.scripts.serialization import (
    canonical_resources,
)

MAX_FILTER_BYTES = 16 * 1024
MAX_DEPTH = 8
MAX_LIMIT = 200
MAX_SKIP = 10_000
MAX_TIME_MS = 5_000

_ALLOWED_OPERATORS = frozenset(
    {
        "$and",
        "$or",
        "$nor",
        "$eq",
        "$ne",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$in",
        "$nin",
        "$exists",
        "$type",
        "$all",
        "$size",
        "$elemMatch",
        "$not",
    }
)


def _invalid(message: str, **details: Any) -> KehrnelError:
    return KehrnelError(
        code="FHIR_NATIVE_QUERY_INVALID", status=400, message=message, details=details
    )


def _validate_expression(value: Any, *, depth: int = 0) -> None:
    if depth > MAX_DEPTH:
        raise _invalid(
            "MongoDB query exceeds the maximum nesting depth", maximum_depth=MAX_DEPTH
        )
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise _invalid("MongoDB query keys must be strings")
            if key.startswith("$") and key not in _ALLOWED_OPERATORS:
                raise _invalid("MongoDB operator is not allowed", operator=key)
            _validate_expression(child, depth=depth + 1)
    elif isinstance(value, list):
        for child in value:
            _validate_expression(child, depth=depth + 1)


def validate_native_query_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(payload or {})
    resource_type = payload.get("resource_type")
    if not isinstance(resource_type, str) or not resource_type.strip():
        raise _invalid("resource_type is required")
    resource_type = resource_type.strip()

    query_filter = payload.get("filter") or {}
    if not isinstance(query_filter, dict):
        raise _invalid("filter must be a MongoDB query object")
    if len(json.dumps(query_filter, default=str).encode("utf-8")) > MAX_FILTER_BYTES:
        raise _invalid(
            "filter exceeds the bounded query size", maximum_bytes=MAX_FILTER_BYTES
        )
    _validate_expression(query_filter)

    projection = payload.get("projection")
    if projection is not None:
        if not isinstance(projection, dict) or len(projection) > 50:
            raise _invalid("projection must be an object with at most 50 fields")
        if any(value not in {0, 1} for value in projection.values()):
            raise _invalid("projection values must be 0 or 1")

    sort = payload.get("sort") or {}
    if not isinstance(sort, dict) or len(sort) > 10:
        raise _invalid("sort must be an object with at most 10 fields")
    if any(value not in {-1, 1} for value in sort.values()):
        raise _invalid("sort directions must be 1 or -1")

    limit = payload.get("limit", 20)
    skip = payload.get("skip", 0)
    if (
        not isinstance(limit, int)
        or isinstance(limit, bool)
        or not 1 <= limit <= MAX_LIMIT
    ):
        raise _invalid("limit must be between 1 and 200")
    if not isinstance(skip, int) or isinstance(skip, bool) or not 0 <= skip <= MAX_SKIP:
        raise _invalid("skip must be between 0 and 10000")

    return {
        "resource_type": resource_type,
        "filter": query_filter,
        "projection": projection,
        "sort": sort,
        "limit": limit,
        "skip": skip,
        "include_operational": bool(payload.get("include_operational", False)),
    }


async def fhir_native_query(
    ctx: StrategyContext, payload: dict[str, Any] | None
) -> dict[str, Any]:
    """Execute a constrained MongoDB ``find`` against one FHIR resource collection."""
    request = validate_native_query_payload(payload)
    cfg = bridge.resolve_strategy_config(ctx)
    uri, database, prefix = bridge.resolve_mongo(ctx)
    search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    mql_ctx = bridge.build_mql_context(
        uri,
        database,
        prefix,
        search_cfg.get("config_dir"),
        search_cfg.get("compartment_definitions_dir"),
    )
    try:
        supported = resolve_resource_capabilities(cfg, mql_ctx.config_loader).storable
        if request["resource_type"] not in supported:
            raise KehrnelError(
                code="FHIR_RESOURCE_TYPE_UNSUPPORTED",
                status=400,
                message=f"{request['resource_type']} is not writable/searchable in the active contract",
            )
        collection_name = bridge.collection_name(prefix, request["resource_type"])
        collection = mql_ctx.db[collection_name]

        def _execute() -> tuple[list[dict[str, Any]], int]:
            cursor = collection.find(request["filter"], request["projection"])
            if request["sort"]:
                cursor = cursor.sort(list(request["sort"].items()))
            cursor = (
                cursor.skip(request["skip"])
                .limit(request["limit"])
                .max_time_ms(MAX_TIME_MS)
            )
            rows = list(cursor)
            total = collection.count_documents(request["filter"], maxTimeMS=MAX_TIME_MS)
            return rows, total

        rows, total = await asyncio.to_thread(_execute)
    finally:
        bridge.close_mql_context(mql_ctx)

    if not request["include_operational"]:
        rows = canonical_resources(rows)
    return {
        "ok": True,
        "contract_version": "1.0",
        "mode": "read-only-find",
        "database": database,
        "collection": collection_name,
        "filter": request["filter"],
        "projection": request["projection"],
        "sort": request["sort"],
        "limit": request["limit"],
        "skip": request["skip"],
        "total": total,
        "returned": len(rows),
        "include_operational": request["include_operational"],
        "rows": rows,
        "safety": {
            "write_operations": False,
            "aggregation_pipeline": False,
            "maximum_time_ms": MAX_TIME_MS,
            "maximum_rows": MAX_LIMIT,
        },
    }
