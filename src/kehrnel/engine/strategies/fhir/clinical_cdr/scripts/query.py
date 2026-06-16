"""FHIR search → MQL compile path for fhir.clinical_cdr."""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import parse_qsl

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.explain import enrich_explain
from kehrnel.engine.core.types import QueryPlan, QueryResult, StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge

# FHIRSearchConverter loads 80+ YAML configs on init (~3–5s). Cache per search path set.
_converter_cache: dict[tuple[str, ...], Any] = {}


def _converter_cache_key(config_dir: Any, compartment_dir: Any) -> tuple[str, ...]:
    if isinstance(config_dir, (list, tuple)):
        dirs = tuple(str(d) for d in config_dir)
    elif config_dir:
        dirs = (str(config_dir),)
    else:
        dirs = ("__bundled__",)
    comp = str(compartment_dir) if compartment_dir else "__bundled_compartment__"
    return dirs + (comp,)


def _require_converter():
    try:
        from fhir_search_to_mql import FHIRSearchConverter
        from fhir_search_to_mql.core.exceptions import (
            ConversionError,
            MissingConfigurationError,
            ParsingError,
        )
        from fhir_search_to_mql.parser.search_request_parser import (
            criteria_dict_to_query_string,
            criteria_tuples_to_query_string,
            parse_fhir_search,
        )
    except ImportError as exc:
        raise KehrnelError(
            code="FHIR_LIBS_NOT_INSTALLED",
            status=500,
            message="fhir-search-to-mql is not installed. Install kehrnel with the [fhir] extra.",
            details={"import_error": str(exc)},
        ) from exc
    return (
        FHIRSearchConverter,
        MissingConfigurationError,
        ConversionError,
        ParsingError,
        parse_fhir_search,
        criteria_dict_to_query_string,
        criteria_tuples_to_query_string,
    )


def _search_paths_from_config(cfg: dict[str, Any]) -> tuple[Any, Any]:
    search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    return search_cfg.get("config_dir"), search_cfg.get("compartment_definitions_dir")


def build_search_converter(ctx: StrategyContext):
    """FHIRSearchConverter using activation search paths or bundled defaults."""
    FHIRSearchConverter, *_ = _require_converter()
    cfg = bridge.resolve_strategy_config(ctx)
    config_dir, compartment_dir = _search_paths_from_config(cfg)
    cache_key = _converter_cache_key(config_dir, compartment_dir)
    cached = _converter_cache.get(cache_key)
    if cached is not None:
        return cached
    kwargs: dict[str, Any] = {}
    if config_dir:
        kwargs["config_dir"] = config_dir
    if compartment_dir:
        kwargs["compartment_definitions_dir"] = compartment_dir
    converter = FHIRSearchConverter(**kwargs)
    _converter_cache[cache_key] = converter
    return converter


def normalize_compile_input(query: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize structured, URL, or tuple criteria into a single compile payload."""
    payload = dict(query or {})
    if not payload:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Query payload is required (resource_type and criteria, or fhir_search)",
        )

    (
        _,
        _,
        _,
        ParsingError,
        parse_fhir_search,
        criteria_dict_to_query_string,
        criteria_tuples_to_query_string,
    ) = _require_converter()

    fhir_search = payload.get("fhir_search") or payload.get("fhir_search_url")
    if isinstance(fhir_search, str) and fhir_search.strip():
        try:
            parsed_search = parse_fhir_search(fhir_search)
        except ParsingError as exc:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=str(exc),
                details={"fhir_search": fhir_search},
            ) from exc
        payload["resource_type"] = parsed_search["resource_type"]
        compartment = parsed_search.get("compartment")
        if isinstance(compartment, dict) and compartment and not payload.get("compartment"):
            payload["compartment"] = compartment
        qs = (parsed_search.get("query_string") or "").strip()
        if qs and not payload.get("criteria"):
            payload["criteria"] = dict(parse_qsl(qs, keep_blank_values=True))
        elif qs:
            payload["_parsed_query_string"] = qs

    resource_type = payload.get("resource_type")
    if not resource_type or not isinstance(resource_type, str):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="resource_type is required",
        )

    criteria = payload.get("criteria")
    query_string = payload.get("_parsed_query_string") or payload.get("query_string")
    if not query_string and isinstance(criteria, dict) and criteria:
        query_string = criteria_dict_to_query_string(criteria)
    elif not query_string and isinstance(criteria, list):
        query_string = criteria_tuples_to_query_string(criteria)

    payload["resource_type"] = resource_type.strip()
    payload["query_string"] = (query_string or "").strip() or None
    return payload


def _compile_mql(
    converter: Any,
    *,
    resource_type: str,
    query_string: str | None,
    compartment: dict[str, Any] | None,
    MissingConfigurationError: type,
    ConversionError: type,
) -> dict[str, Any]:
    try:
        if compartment:
            comp_type = compartment.get("type")
            comp_id = compartment.get("id")
            if not comp_type or not comp_id:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="compartment requires type and id",
                )
            return converter.convert_with_compartment(
                str(comp_type),
                str(comp_id),
                resource_type,
                query_string=query_string,
            )
        return converter.convert(resource_type, query_string=query_string)
    except MissingConfigurationError as exc:
        raise KehrnelError(
            code="FHIR_SEARCH_NOT_CONFIGURED",
            status=400,
            message=str(exc),
            details={"resource_type": resource_type},
        ) from exc
    except ConversionError as exc:
        raise KehrnelError(
            code="FHIR_SEARCH_COMPILE_FAILED",
            status=400,
            message=str(exc),
            details={"resource_type": resource_type, "query_string": query_string},
        ) from exc


async def compile_fhir_query(
    ctx: StrategyContext,
    domain: str,
    query: dict[str, Any] | None,
) -> QueryPlan:
    """Compile FHIR search parameters to a MongoDB filter plan (no execution)."""
    normalized = normalize_compile_input(query)
    resource_type = normalized["resource_type"]
    query_string = normalized.get("query_string")
    compartment = normalized.get("compartment") if isinstance(normalized.get("compartment"), dict) else None

    _, MissingConfigurationError, ConversionError, *_ = _require_converter()
    converter = build_search_converter(ctx)

    # Fail fast when the resource has no fhir-mql YAML config.
    try:
        converter.config_loader.get_config(resource_type)
    except MissingConfigurationError as exc:
        raise KehrnelError(
            code="FHIR_SEARCH_NOT_CONFIGURED",
            status=400,
            message=str(exc),
            details={"resource_type": resource_type},
        ) from exc

    mql_filter = _compile_mql(
        converter,
        resource_type=resource_type,
        query_string=query_string,
        compartment=compartment,
        MissingConfigurationError=MissingConfigurationError,
        ConversionError=ConversionError,
    )

    cfg = bridge.resolve_strategy_config(ctx)
    database = cfg.get("database")
    prefix = str(cfg.get("collection_prefix") or "")
    collection = bridge.collection_name(prefix, resource_type)

    debug = bool(normalized.get("debug"))
    explain_domain = (getattr(ctx.manifest, "domain", None) or domain or "fhir").lower()

    plan_body: dict[str, Any] = {
        "filter": mql_filter,
        "collection": collection,
        "resource_type": resource_type,
        "database": database,
        "query_input": {
            k: normalized[k]
            for k in ("resource_type", "criteria", "fhir_search", "compartment", "_count", "_sort", "_offset")
            if k in normalized
        },
    }
    for key in ("_count", "_sort", "_offset", "limit", "offset"):
        if key in normalized:
            plan_body["query_input"][key] = normalized[key]

    explain: dict[str, Any] = {
        "builder": {
            "chosen": "fhir_mql",
            "resource_type": resource_type,
            "query_string": query_string,
            "has_compartment": bool(compartment),
        },
        "filter": mql_filter if debug else None,
    }
    explain = enrich_explain(
        explain,
        ctx,
        domain=explain_domain,
        engine="fhir_mql",
        scope=resource_type,
    )
    plan_body["explain"] = explain

    return QueryPlan(engine="fhir_mql", plan=plan_body, explain=explain)


def _coerce_query_plan(plan: QueryPlan | dict[str, Any] | None) -> QueryPlan:
    """Accept QueryPlan or runtime/API JSON shapes from compile_query."""
    if plan is None:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Query plan is required for execute_query",
        )
    if isinstance(plan, QueryPlan):
        return plan
    if not isinstance(plan, dict):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Query plan must be an object",
        )

    if "plan" in plan and isinstance(plan["plan"], dict):
        inner = plan["plan"]
        if "filter" in inner or "collection" in inner:
            return QueryPlan(
                engine=str(plan.get("engine") or inner.get("engine") or "fhir_mql"),
                plan=inner,
                explain=plan.get("explain") or inner.get("explain"),
            )

    if "filter" in plan or "collection" in plan:
        return QueryPlan(
            engine=str(plan.get("engine") or "fhir_mql"),
            plan=plan,
            explain=plan.get("explain"),
        )

    raise KehrnelError(
        code="INVALID_INPUT",
        status=400,
        message="Query plan is missing filter/collection (run compile_query first)",
        details={"plan_keys": sorted(plan.keys())},
    )


def pagination_from_plan(plan_body: dict[str, Any]) -> tuple[int | None, int | None]:
    """Read FHIR _count / _offset (or limit / offset) from compiled plan metadata."""
    query_input = plan_body.get("query_input") if isinstance(plan_body.get("query_input"), dict) else {}
    limit_raw = query_input.get("_count", query_input.get("limit"))
    offset_raw = query_input.get("_offset", query_input.get("offset"))
    limit = int(limit_raw) if limit_raw is not None else None
    offset = int(offset_raw) if offset_raw is not None else None
    if limit is not None and limit < 0:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="_count must be non-negative")
    if offset is not None and offset < 0:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="_offset must be non-negative")
    return limit, offset


def resolve_multi_step_mql(
    db: Any,
    default_collection: str,
    mql: dict[str, Any],
) -> dict[str, Any]:
    """
    Expand fhir-mql ``_multi_step`` envelopes into a single collection filter.

    Matches ``scripts/spike_generate_and_search.py`` execution semantics.
    """
    if not isinstance(mql, dict) or "_multi_step" not in mql:
        return mql

    composed = dict(mql.get("_query") or {})
    and_clauses: list[dict[str, Any]] = []
    for step in mql["_multi_step"]:
        step_coll_name = step.get("collection") or default_collection
        step_coll = db[step_coll_name]
        field = step.get("project_field", "_id")
        ids = list(
            step_coll.find(
                step.get("query") or {},
                {field: 1, "_id": 0},
            )
        )
        id_values = [doc.get(field) for doc in ids if doc.get(field) is not None]
        target_field = step.get("target_field") or field
        if id_values:
            and_clauses.append({target_field: {"$in": id_values}})
        else:
            and_clauses.append({"_id": {"$in": []}})

    if and_clauses:
        return {"$and": [composed] + and_clauses} if composed else {"$and": and_clauses}
    return composed


def _execute_find(
    collection: Any,
    mql: dict[str, Any],
    *,
    limit: int | None,
    skip: int | None,
) -> list[dict[str, Any]]:
    resolved = resolve_multi_step_mql(collection.database, collection.name, mql)
    cursor = collection.find(resolved)
    if skip:
        cursor = cursor.skip(skip)
    if limit is not None:
        cursor = cursor.limit(limit)
    return list(cursor)


async def execute_fhir_query(
    ctx: StrategyContext,
    plan: QueryPlan | dict[str, Any] | None,
) -> QueryResult:
    """Run a compiled FHIR MQL plan against MongoDB and return canonical resource rows."""
    qp = _coerce_query_plan(plan)
    if qp.engine != "fhir_mql":
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message=f"Unsupported query engine '{qp.engine}' (expected fhir_mql)",
        )

    plan_body = qp.plan if isinstance(qp.plan, dict) else {}
    mql_filter = plan_body.get("filter")
    if mql_filter is None:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Query plan is missing filter",
        )
    if not isinstance(mql_filter, dict):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Query plan filter must be an object",
        )

    collection_name = plan_body.get("collection")
    if not collection_name or not isinstance(collection_name, str):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Query plan is missing collection",
        )

    limit, skip = pagination_from_plan(plan_body)
    explain = dict(qp.explain or plan_body.get("explain") or {})

    uri, database, prefix = bridge.resolve_mongo(ctx)
    cfg = bridge.resolve_strategy_config(ctx)
    config_dir, compartment_dir = _search_paths_from_config(cfg)
    mql_ctx = bridge.build_mql_context(
        uri,
        database,
        prefix,
        config_dir=config_dir,
        compartment_dir=compartment_dir,
    )

    try:
        collection = mql_ctx.db[collection_name]

        def _run_query() -> tuple[list[dict[str, Any]], int]:
            row_list = _execute_find(collection, mql_filter, limit=limit, skip=skip)
            resolved_filter = resolve_multi_step_mql(collection.database, collection.name, mql_filter)
            match_total = collection.count_documents(resolved_filter)
            return row_list, match_total

        rows, total = await asyncio.to_thread(_run_query)
    finally:
        bridge.close_mql_context(mql_ctx)

    explain = dict(explain)
    explain["total"] = total
    explain["returned"] = len(rows)
    explain.setdefault("execution", {})
    if isinstance(explain["execution"], dict):
        explain["execution"].update(
            {
                "collection": collection_name,
                "limit": limit,
                "skip": skip or 0,
            }
        )

    return QueryResult(engine_used="fhir_mql", rows=rows, explain=explain)


def _search_payload_to_compile_query(payload: dict[str, Any]) -> dict[str, Any]:
    """Strip op-only keys and map API ``limit`` to FHIR ``_count``."""
    query = dict(payload or {})
    query.pop("explain_only", None)
    if query.get("limit") is not None and "_count" not in query:
        query["_count"] = query.pop("limit")
    return query


async def fhir_search(ctx: StrategyContext, payload: dict[str, Any] | None) -> dict[str, Any]:
    """
    Compile and execute FHIR search (or compile-only when ``explain_only`` is true).
    """
    payload = dict(payload or {})
    explain_only = bool(payload.pop("explain_only", False))
    compile_input = _search_payload_to_compile_query(payload)

    plan = await compile_fhir_query(ctx, "fhir", compile_input)
    if explain_only:
        return {
            "ok": True,
            "explain_only": True,
            "engine": plan.engine,
            "plan": plan.plan,
            "explain": plan.explain,
        }

    result = await execute_fhir_query(ctx, plan)
    explain = dict(result.explain or {})
    return {
        "ok": True,
        "engine_used": result.engine_used,
        "rows": result.rows,
        "total": explain.get("total"),
        "returned": explain.get("returned", len(result.rows)),
        "explain": explain,
        "plan": plan.plan,
    }


def fhir_list_search_params(ctx: StrategyContext, payload: dict[str, Any] | None) -> dict[str, Any]:
    """List fhir-mql search parameter definitions for a resource type."""
    payload = payload or {}
    resource_type = payload.get("resource_type")
    if not resource_type or not isinstance(resource_type, str):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="resource_type is required",
        )

    _, MissingConfigurationError, _, *_ = _require_converter()
    converter = build_search_converter(ctx)
    try:
        raw_params = converter.config_loader.get_search_parameters(resource_type.strip())
    except MissingConfigurationError as exc:
        raise KehrnelError(
            code="FHIR_SEARCH_NOT_CONFIGURED",
            status=400,
            message=str(exc),
            details={"resource_type": resource_type},
        ) from exc

    parameters: list[dict[str, Any]] = []
    for name, spec in sorted(raw_params.items()):
        if not isinstance(spec, dict):
            continue
        modifiers: list[str] = []
        fields = spec.get("fields")
        if isinstance(fields, dict):
            modifiers = sorted(k for k in fields if k and k != "default")
        entry: dict[str, Any] = {
            "name": name,
            "type": spec.get("type"),
            "description": spec.get("description"),
            "modifiers": modifiers,
        }
        if spec.get("composite"):
            entry["composite"] = True
        parameters.append(entry)

    return {
        "ok": True,
        "resource_type": resource_type.strip(),
        "parameter_count": len(parameters),
        "parameters": parameters,
    }
