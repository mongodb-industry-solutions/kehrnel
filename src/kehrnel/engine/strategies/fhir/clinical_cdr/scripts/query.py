"""FHIR search → MQL compile path for fhir.clinical_cdr."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.explain import enrich_explain
from kehrnel.engine.core.types import QueryPlan, QueryResult, StrategyContext
from kehrnel.engine.domains.fhir import implementation_guides
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.capabilities import (
    resolve_resource_capabilities,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.document_contract import (
    STORED_DOCUMENT_SCHEMA_VERSION,
    build_projection_versions,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.profile_validation import (
    describe_profile_validation,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.release_support import (
    allowed_search_parameters,
    release_evidence,
    validate_search_scope,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.semantic import (
    describe_semantic_config,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import (
    SEARCH_CONTRACT_VERSION,
    build_compile_response,
    build_search_response,
    canonical_resources,
)

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
            UnsupportedParameterError,
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
        UnsupportedParameterError,
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


# FHIR result-control parameters are NOT filter params — they must never reach
# the fhir-mql converter (which would now reject them, fail-closed). We support
# _count / _offset / _sort; other control params are rejected explicitly.
# One server-side maximum result count enforced across every input form (URL,
# structured, internal op). Overridable via strategy config `query.max_count`.
DEFAULT_MAX_RESULT_COUNT = 1000

_SUPPORTED_RESULT_CONTROLS = {"_count", "_offset", "_sort"}

# FHIR comparator prefixes advertised per search-parameter type (T6). Only types
# that support prefixes get a non-empty list.
_COMPARATORS_BY_TYPE = {
    "date": ["eq", "ne", "gt", "lt", "ge", "le", "sa", "eb", "ap"],
    "datetime": ["eq", "ne", "gt", "lt", "ge", "le", "sa", "eb", "ap"],
    "number": ["eq", "ne", "gt", "lt", "ge", "le", "ap"],
    "quantity": ["eq", "ne", "gt", "lt", "ge", "le", "ap"],
}
_KNOWN_UNSUPPORTED_RESULT_CONTROLS = {
    "_total",
    "_summary",
    "_elements",
    "_contained",
    "_include",
    "_revinclude",
}


def _split_result_controls(
    query_string: str | None,
) -> tuple[str | None, dict[str, Any]]:
    """Extract result controls from a filter query string.

    Returns (filter_query_string_without_controls, controls). Raises KehrnelError
    for known-but-unsupported control params so behavior is truthful (T2).
    """
    if not query_string:
        return query_string, {}

    filter_pairs: list[tuple[str, str]] = []
    controls: dict[str, Any] = {}
    for name, value in parse_qsl(query_string, keep_blank_values=True):
        if name in _KNOWN_UNSUPPORTED_RESULT_CONTROLS:
            raise KehrnelError(
                code="FHIR_SEARCH_UNSUPPORTED_PARAM",
                status=400,
                message=f"Result parameter '{name}' is not supported",
                details={"parameter": name},
            )
        if name == "_count":
            controls["_count"] = value
        elif name == "_offset":
            controls["_offset"] = value
        elif name == "_sort":
            controls["_sort"] = value
        else:
            filter_pairs.append((name, value))

    filtered = urlencode(filter_pairs) if filter_pairs else None
    return filtered, controls


def _resolve_sort_field(spec: dict[str, Any] | None) -> str | None:
    """Resolve a search-parameter spec to a single scalar Mongo sort field.

    fhir-mql specs carry ``fields`` as either a list of ``{field, ...}`` (token/date)
    or a dict of modifier→list (string). Sort uses the primary/default field.
    Returns None when no single sortable field can be determined.
    """
    if not isinstance(spec, dict):
        return None
    fields = spec.get("fields")
    entries = None
    if isinstance(fields, list):
        entries = fields
    elif isinstance(fields, dict):
        entries = fields.get("default")
    if isinstance(entries, list) and entries and isinstance(entries[0], dict):
        field = entries[0].get("field")
        return field if isinstance(field, str) and field else None
    return None


def resolve_sort(
    sort_value: str, search_params: dict[str, Any]
) -> list[tuple[str, int]]:
    """Compile a FHIR ``_sort`` value to a Mongo sort spec.

    Comma-separated keys; a leading ``-`` means descending. Each key must map to a
    configured, single-field sortable search parameter — otherwise raise (fail
    closed / truthful rather than silently ignoring).
    """
    order: list[tuple[str, int]] = []
    for raw in str(sort_value or "").split(","):
        key = raw.strip()
        if not key:
            continue
        direction = 1
        if key.startswith("-"):
            direction = -1
            key = key[1:]
        field = _resolve_sort_field(search_params.get(key))
        if not field:
            raise KehrnelError(
                code="FHIR_SEARCH_UNSUPPORTED_PARAM",
                status=400,
                message=f"_sort key '{key}' is not a sortable search parameter",
                details={"_sort": sort_value, "key": key},
            )
        order.append((field, direction))
    return order


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
        *_,
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
        if (
            isinstance(compartment, dict)
            and compartment
            and not payload.get("compartment")
        ):
            payload["compartment"] = compartment
        qs = (parsed_search.get("query_string") or "").strip()
        if qs:
            # Keep the raw query string (NOT dict(parse_qsl(...)), which collapses
            # repeated params like name=Smith&name=Jones). The converter parses
            # repeats natively; result-control extraction also preserves them.
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

    # Extract result controls (_count/_offset/_sort) from the filter string so
    # they never reach the (now fail-closed) fhir-mql converter. URL-form controls
    # thereby become equivalent to top-level ones; explicit top-level values win.
    filter_query_string, url_controls = _split_result_controls(query_string)
    for key in _SUPPORTED_RESULT_CONTROLS:
        if key in url_controls and payload.get(key) is None:
            payload[key] = url_controls[key]

    # Apply request-level fallbacks ONLY when neither the URL nor an explicit
    # top-level value set the control (so URL _count/_offset always win).
    if payload.get("_count") is None and payload.get("default_count") is not None:
        payload["_count"] = payload["default_count"]
    if payload.get("_offset") is None and payload.get("default_offset") is not None:
        payload["_offset"] = payload["default_offset"]
    payload.pop("default_count", None)
    payload.pop("default_offset", None)

    # Validate types + ranges: _count/_offset non-negative integers.
    for key in ("_count", "_offset"):
        if payload.get(key) is not None:
            try:
                payload[key] = int(payload[key])
            except (TypeError, ValueError) as exc:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message=f"{key} must be an integer",
                    details={key: payload.get(key)},
                ) from exc
            if payload[key] < 0:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message=f"{key} must be non-negative",
                    details={key: payload[key]},
                )
    if payload.get("_sort") is not None:
        payload["_sort"] = str(payload["_sort"]).strip() or None

    payload["resource_type"] = resource_type.strip()
    payload["query_string"] = (filter_query_string or "").strip() or None
    handling = str(payload.get("handling") or "strict").lower()
    payload["handling"] = "lenient" if handling == "lenient" else "strict"
    return payload


def _compile_mql(
    converter: Any,
    *,
    resource_type: str,
    query_string: str | None,
    compartment: dict[str, Any] | None,
    MissingConfigurationError: type,
    ConversionError: type,
    UnsupportedParameterError: type,
    handling: str = "strict",
    ignored_out: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    # Legitimate unfiltered search: result controls (_count/_sort/_offset) were
    # already stripped upstream, so an empty query_string here means the request
    # had no filter params at all → match-all {}. (Requests whose filters were
    # unsupported keep those params in query_string and still fail closed below.)
    if not compartment and not (query_string and query_string.strip()):
        return {}
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
                handling=handling,
                ignored_out=ignored_out,
            )
        return converter.convert(
            resource_type,
            query_string=query_string,
            handling=handling,
            ignored_out=ignored_out,
        )
    except MissingConfigurationError as exc:
        raise KehrnelError(
            code="FHIR_SEARCH_NOT_CONFIGURED",
            status=400,
            message=str(exc),
            details={"resource_type": resource_type},
        ) from exc
    except UnsupportedParameterError as exc:
        # Fail closed: an unsupported parameter in strict mode is a client error,
        # not a silently-broadened query. (query.py never reaches Mongo here.)
        raise KehrnelError(
            code="FHIR_SEARCH_UNSUPPORTED_PARAM",
            status=400,
            message=str(exc),
            details={"resource_type": resource_type, "query_string": query_string},
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
    compartment = (
        normalized.get("compartment")
        if isinstance(normalized.get("compartment"), dict)
        else None
    )
    handling = normalized.get("handling", "strict")

    (
        _,
        MissingConfigurationError,
        ConversionError,
        _ParsingError,
        _parse_fhir_search,
        _criteria_dict_to_query_string,
        _criteria_tuples_to_query_string,
        UnsupportedParameterError,
    ) = _require_converter()
    converter = build_search_converter(ctx)
    ignored_params: list[dict[str, Any]] = []

    # Enforce one server maximum result count across ALL input forms (URL _count,
    # structured _count, internal op limit). The request model caps body limit at
    # 1000, but URL _count bypasses that — clamp here so no path can request an
    # unbounded page. Config `query.max_count` overrides the default.
    cfg_for_limit = bridge.resolve_strategy_config(ctx)
    release = str(cfg_for_limit.get("schema_version") or "R5").strip().upper()
    query_cfg = (
        cfg_for_limit.get("query")
        if isinstance(cfg_for_limit.get("query"), dict)
        else {}
    )
    max_count = query_cfg.get("max_count") or DEFAULT_MAX_RESULT_COUNT
    default_count = query_cfg.get("default_count") or max_count
    # Apply a default page size to EVERY path with no _count (incl. internal ops),
    # so no query is ever unbounded.
    if normalized.get("_count") is None:
        normalized["_count"] = default_count
    if normalized.get("_count") is not None and normalized["_count"] > max_count:
        normalized["_count"] = max_count

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

    validate_search_scope(
        release,
        resource_type,
        query_string=query_string,
        compartment=compartment,
        sort_value=normalized.get("_sort"),
    )

    mql_filter = _compile_mql(
        converter,
        resource_type=resource_type,
        query_string=query_string,
        compartment=compartment,
        MissingConfigurationError=MissingConfigurationError,
        ConversionError=ConversionError,
        UnsupportedParameterError=UnsupportedParameterError,
        handling=handling,
        ignored_out=ignored_params,
    )

    # Resolve _sort to a Mongo sort spec at compile time (converter available here).
    # Unsortable keys fail closed (see resolve_sort).
    resolved_sort: list[tuple[str, int]] = []
    if normalized.get("_sort"):
        try:
            search_params = converter.config_loader.get_search_parameters(resource_type)
        except Exception:
            search_params = {}
        resolved_sort = resolve_sort(normalized["_sort"], search_params)

    cfg = bridge.resolve_strategy_config(ctx)
    database = cfg.get("database")
    prefix = str(cfg.get("collection_prefix") or "")
    mql_filter = _bind_multi_step_collections(mql_filter, prefix)
    collection = bridge.collection_name(prefix, resource_type)

    debug = bool(normalized.get("debug"))
    explain_domain = (getattr(ctx.manifest, "domain", None) or domain or "fhir").lower()

    plan_body: dict[str, Any] = {
        "filter": mql_filter,
        "collection": collection,
        "resource_type": resource_type,
        "database": database,
        "handling": handling,
        "ignored_parameters": ignored_params,
        # Compiled Mongo sort spec ([] = default id-ordering only). Stored as lists
        # for JSON/QueryPlan round-trips.
        "sort": [[field, direction] for field, direction in resolved_sort],
        "query_input": {
            k: normalized[k]
            for k in (
                "resource_type",
                "criteria",
                "fhir_search",
                "compartment",
                "_count",
                "_sort",
                "_offset",
            )
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
    query_input = (
        plan_body.get("query_input")
        if isinstance(plan_body.get("query_input"), dict)
        else {}
    )
    limit_raw = query_input.get("_count", query_input.get("limit"))
    offset_raw = query_input.get("_offset", query_input.get("offset"))
    limit = int(limit_raw) if limit_raw is not None else None
    offset = int(offset_raw) if offset_raw is not None else None
    if limit is not None and limit < 0:
        raise KehrnelError(
            code="INVALID_INPUT", status=400, message="_count must be non-negative"
        )
    if offset is not None and offset < 0:
        raise KehrnelError(
            code="INVALID_INPUT", status=400, message="_offset must be non-negative"
        )
    return limit, offset


def resolve_multi_step_mql(
    db: Any,
    default_collection: str,
    mql: dict[str, Any],
    session: Any = None,
) -> dict[str, Any]:
    """
    Expand fhir-mql ``_multi_step`` envelopes into a single collection filter.

    When a snapshot ``session`` is provided, the sub-queries run inside it so the
    resolution, the main find, and the count are all read from ONE snapshot
    (true cross-stage consistency). Matches ``spike_generate_and_search.py`` semantics.
    """
    if not isinstance(mql, dict) or "_multi_step" not in mql:
        return mql

    find_kwargs = {"session": session} if session is not None else {}
    composed = dict(mql.get("_query") or {})
    and_clauses: list[dict[str, Any]] = []
    for plan in mql["_multi_step"]:
        if not isinstance(plan, dict):
            continue
        steps = plan.get("steps")
        if not isinstance(steps, list):
            # Backward-compatible support for the old flat stage envelope.
            steps = [plan]
        if len(steps) != 1:
            raise KehrnelError(
                code="FHIR_SEARCH_CHAIN_DEPTH_UNSUPPORTED",
                status=400,
                message="Only one-hop FHIR chaining is currently supported",
            )
        step = steps[0]
        step_coll_name = (
            step.get("collection")
            or step.get("resource_type")
            or default_collection
        )
        step_coll = db[step_coll_name]
        field = step.get("extract_field") or step.get("project_field") or "id"
        docs = list(
            step_coll.find(
                step.get("query") or {},
                {field: 1, "_id": 0},
                **find_kwargs,
            ).limit(10001)
        )
        if len(docs) > 10000:
            raise KehrnelError(
                code="FHIR_SEARCH_CHAIN_LIMIT_EXCEEDED",
                status=422,
                message="FHIR chained search produced more than 10000 intermediate ids",
            )
        id_values: list[Any] = []
        for doc in docs:
            id_values.extend(_extract_dotted_values(doc, str(field)))
        if step.get("extract_transform") == "reference_id":
            id_values = [_reference_id(value) for value in id_values]
        id_values = list(dict.fromkeys(value for value in id_values if value is not None))
        target_field = plan.get("target_field") or step.get("target_field") or "id"
        if id_values:
            and_clauses.append({target_field: {"$in": id_values}})
            constraints = plan.get("target_constraints")
            if isinstance(constraints, dict) and constraints:
                and_clauses.append(constraints)
        else:
            and_clauses.append({"id": {"$in": []}})

    if and_clauses:
        return {"$and": [composed] + and_clauses} if composed else {"$and": and_clauses}
    return composed


def _extract_dotted_values(document: Any, dotted_path: str) -> list[Any]:
    """Resolve a projected dotted field through embedded objects and arrays."""

    parts = [part for part in dotted_path.split(".") if part]

    def walk(value: Any, index: int) -> list[Any]:
        if index == len(parts):
            return value if isinstance(value, list) else [value]
        if isinstance(value, list):
            result: list[Any] = []
            for item in value:
                result.extend(walk(item, index))
            return result
        if not isinstance(value, dict) or parts[index] not in value:
            return []
        return walk(value[parts[index]], index + 1)

    return walk(document, 0)


def _reference_id(value: Any) -> Any:
    """Normalize relative or absolute FHIR references to their logical id."""

    if not isinstance(value, str):
        return value
    reference = value.rstrip("/")
    if not reference or reference.startswith("#"):
        return None
    return reference.rsplit("/", 1)[-1]


def _bind_multi_step_collections(
    mql: dict[str, Any], collection_prefix: str
) -> dict[str, Any]:
    """Bind compiler resource names to tenant-prefixed Mongo collections."""

    if not isinstance(mql, dict) or "_multi_step" not in mql:
        return mql
    for plan in mql.get("_multi_step") or []:
        if not isinstance(plan, dict):
            continue
        steps = plan.get("steps") if isinstance(plan.get("steps"), list) else [plan]
        for step in steps:
            if not isinstance(step, dict) or step.get("collection"):
                continue
            resource_type = step.get("resource_type")
            if resource_type:
                step["collection"] = bridge.collection_name(
                    collection_prefix, str(resource_type)
                )
    return mql


def _execute_find(
    collection: Any,
    resolved_mql: dict[str, Any],
    *,
    limit: int | None,
    skip: int | None,
    sort: list[tuple[str, int]] | None = None,
) -> list[dict[str, Any]]:
    """Execute an already-resolved MQL filter with deterministic ordering.

    Ordering always ends with a logical-id tie-breaker so paging (skip/limit)
    cannot duplicate or drop resources across pages (T2). Callers pass a
    pre-resolved filter so multi-step resolution happens exactly once (T4).
    """
    # FHIR _count=0 means "return no resources" (count-only). Mongo .limit(0)
    # is UNLIMITED, so handle it explicitly rather than fetching everything.
    if limit == 0:
        return []
    order = list(sort or [])
    if not any(field == "id" for field, _ in order):
        order.append(("id", 1))  # stable logical-id tie-breaker
    cursor = collection.find(resolved_mql).sort(order)
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

    # Compiled sort spec (resolved at compile time). Explicit sorts receive an
    # id tie-breaker for deterministic ordering. An unsorted FHIR search remains
    # unsorted so MongoDB can select the most selective filter index; forcing an
    # id-only order makes range searches scan ``id_unique`` and filter every row.
    sort_spec: list[tuple[str, int]] = [
        (f, int(d)) for f, d in (plan_body.get("sort") or []) if isinstance(f, str)
    ]

    explain = dict(qp.explain or plan_body.get("explain") or {})
    privileged = bool((getattr(ctx, "meta", None) or {}).get("privileged", False))

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

        def _run_query() -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
            # T4: indexed find (cursor-streamed — no 16 MB $facet result limit, and
            # the sort can use the collection index) + count over the SAME resolved
            # filter. When the deployment supports snapshot sessions we run both in
            # one snapshot so rows/total are consistent; otherwise totals are
            # best-effort under concurrent writes (reported truthfully below).
            multi_step_resolved = (
                isinstance(mql_filter, dict) and "_multi_step" in mql_filter
            )
            client = getattr(getattr(collection, "database", None), "client", None)
            session = None
            snapshot_mode = "best_effort"
            # Snapshot read concern requires a replica set (standalone Mongo — incl.
            # single-node CI containers — cannot use it, and the failure only surfaces
            # when the session is USED, not created). Gate on real replica-set support.
            if client is not None:
                try:
                    hello = collection.database.client.admin.command("hello")
                    is_replica_set = bool(hello.get("setName"))
                except Exception:
                    is_replica_set = False
                if is_replica_set:
                    try:
                        session = client.start_session(snapshot=True)
                        session.__enter__()
                        snapshot_mode = "session"
                    except Exception:
                        session = None
                        snapshot_mode = "best_effort"
            try:
                kwargs = {"session": session} if session is not None else {}
                # Resolve multi-step INSIDE the snapshot session (when present) so
                # resolution + find + count share one consistent read.
                resolved_filter = resolve_multi_step_mql(
                    collection.database, collection.name, mql_filter, session=session
                )
                # Explicit sort, then a logical-id tie-breaker for stable paging.
                # FHIR does not guarantee result order when `_sort` is absent.
                order = list(sort_spec)
                if order and not any(f == "id" for f, _ in order):
                    order.append(("id", 1))
                if limit == 0:
                    row_list: list[dict[str, Any]] = []  # FHIR _count=0 → count only
                else:
                    cursor = collection.find(resolved_filter, **kwargs)
                    if order:
                        cursor = cursor.sort(order)
                    if skip:
                        cursor = cursor.skip(skip)
                    if limit is not None:
                        cursor = cursor.limit(limit)
                    row_list = list(cursor)
                match_total = collection.count_documents(resolved_filter, **kwargs)

                # Privileged: gather REAL Mongo executionStats (best-effort).
                exec_stats = None
                if privileged and limit != 0:
                    try:
                        find_cmd: dict[str, Any] = {
                            "find": collection.name,
                            "filter": resolved_filter,
                        }
                        if order:
                            find_cmd["sort"] = {f: d for f, d in order}
                        if skip:
                            find_cmd["skip"] = skip
                        if limit is not None:
                            find_cmd["limit"] = limit
                        stats = collection.database.command(
                            {"explain": find_cmd, "verbosity": "executionStats"},
                            **kwargs,
                        )
                        exec_stats = stats.get("executionStats")
                    except Exception:
                        exec_stats = None
            finally:
                if session is not None:
                    session.__exit__(None, None, None)
            return (
                row_list,
                match_total,
                {
                    "multi_step_resolved": multi_step_resolved,
                    "snapshot": snapshot_mode,
                    "resolved_filter": resolved_filter,
                    "mongo_execution_stats": exec_stats,
                },
            )

        rows, total, executed = await asyncio.to_thread(_run_query)
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
    # Bounded executed summary (no large resolved-id arrays by default). `snapshot`
    # is truthful: "session" = rows/total consistent; "best_effort" = totals best-effort.
    explain["executed"] = {
        "collection": collection_name,
        "snapshot": executed["snapshot"],
        "multi_step_resolved": executed["multi_step_resolved"],
    }
    # Surface lenient-mode ignored params so the FHIR boundary can report them.
    explain["ignored_parameters"] = plan_body.get("ignored_parameters", [])
    # Real Mongo executionStats — privileged only, and only when actually gathered.
    if privileged and executed.get("mongo_execution_stats") is not None:
        explain["mongo_execution_stats"] = executed["mongo_execution_stats"]
    # Full resolved filter (may contain large $in arrays) — privileged only.
    if privileged:
        explain["_executed_pipeline"] = {"filter": executed["resolved_filter"]}

    return QueryResult(engine_used="fhir_mql", rows=rows, explain=explain)


def _search_payload_to_compile_query(payload: dict[str, Any]) -> dict[str, Any]:
    """Strip op-only keys and map API ``limit`` to FHIR ``_count``."""
    query = dict(payload or {})
    query.pop("explain_only", None)
    if query.get("limit") is not None and "_count" not in query:
        query["_count"] = query.pop("limit")
    return query


async def fhir_search(
    ctx: StrategyContext, payload: dict[str, Any] | None
) -> dict[str, Any]:
    """
    Compile and execute FHIR search (or compile-only when ``explain_only`` is true).
    """
    payload = dict(payload or {})
    explain_only = bool(payload.pop("explain_only", False))
    # SECURITY: privileged output (raw explain / Mongo stats) is NOT authorized by
    # a client-supplied flag. It comes only from server-set runtime context
    # (ctx.meta.privileged), populated by the trusted HTTP boundary from the
    # authenticated request + policy. Any payload debug/privileged flag is ignored.
    payload.pop("debug", None)
    payload.pop("privileged", None)
    meta = getattr(ctx, "meta", None) or {}
    include_privileged = bool(meta.get("privileged", False))
    compile_input = _search_payload_to_compile_query(payload)

    plan = await compile_fhir_query(ctx, "fhir", compile_input)
    if explain_only:
        response = build_compile_response(plan_body=plan.plan, engine=plan.engine)
        # Backward-compatible keys (pre-contract consumers).
        response["explain_only"] = True
        response["plan"] = plan.plan
        response["explain"] = plan.explain
        return response

    result = await execute_fhir_query(ctx, plan)
    explain = dict(result.explain or {})
    response = build_search_response(
        plan_body=plan.plan,
        engine_used=result.engine_used,
        rows=result.rows,
        explain=explain,
        include_privileged=include_privileged,
    )
    # Backward-compatible keys: canonical rows (operational fields stripped) + plan.
    response["rows"] = canonical_resources(result.rows)
    response["plan"] = plan.plan
    response["explain"] = explain
    return response


def fhir_list_search_params(
    ctx: StrategyContext, payload: dict[str, Any] | None
) -> dict[str, Any]:
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
    cfg = bridge.resolve_strategy_config(ctx)
    release = str(cfg.get("schema_version") or "R5").strip().upper()
    try:
        raw_params = converter.config_loader.get_search_parameters(
            resource_type.strip()
        )
    except MissingConfigurationError as exc:
        raise KehrnelError(
            code="FHIR_SEARCH_NOT_CONFIGURED",
            status=400,
            message=str(exc),
            details={"resource_type": resource_type},
        ) from exc

    parameters: list[dict[str, Any]] = []
    allowed = allowed_search_parameters(release, resource_type.strip())
    for name, spec in sorted(raw_params.items()):
        if not isinstance(spec, dict):
            continue
        if allowed is not None and name not in allowed:
            continue
        modifiers: list[str] = []
        fields = spec.get("fields")
        if isinstance(fields, dict):
            modifiers = sorted(k for k in fields if k and k != "default")
        param_type = spec.get("type")
        entry: dict[str, Any] = {
            "name": name,
            "type": param_type,
            "description": spec.get("description"),
            "modifiers": modifiers,
            "comparators": _COMPARATORS_BY_TYPE.get(str(param_type), []),
            "repeats": True,  # repeated params are preserved and AND-combined
            # "configured" = declared in the fhir-mql YAML (compiles). It does NOT
            # assert a passing execution test — that evidence comes from the T7 suite.
            "support_level": "configured",
        }
        if str(param_type) == "reference":
            # FHIR reference target resource types are not declared in the fhir-mql
            # config; do not fabricate them (advertise as unknown).
            entry["reference_targets"] = None
        if spec.get("composite"):
            entry["composite"] = True
        parameters.append(entry)

    return {
        "ok": True,
        "resource_type": resource_type.strip(),
        "parameter_count": len(parameters),
        "parameters": parameters,
    }


def fhir_capabilities(
    ctx: StrategyContext, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Authoritative capability catalog (T6).

    Reports the DISTINCT sets the strategy actually supports so the Explorer can
    build truthful UI instead of inferring from parameter type or a static list:
      - schema_supported_resource_types: concrete resources in the active release
      - searchable_resource_types:      have active fhir-mql search configs
      - generatable_resource_types:     can be synthesized for preview/inspection
      - storable_resource_types:        satisfy the mandatory projection contract
      - synthetic_writable_resource_types: can be synthesized and persisted
      - generation_only_resource_types: can be previewed but not persisted
      - recipe_resource_types:          occur in one or more example data recipes

    Discovery failures are surfaced via ``degraded`` + ``discovery_errors`` rather
    than silently returning empty arrays.
    """
    payload = payload or {}
    converter = build_search_converter(ctx)
    discovery_errors: list[dict[str, str]] = []
    try:
        cfg = bridge.resolve_strategy_config(ctx)
        fhir_version = cfg.get("schema_version") or "R5"
    except Exception as exc:
        cfg = {}
        fhir_version = "R5"
        discovery_errors.append({"source": "strategy_config", "error": str(exc)})

    try:
        capability_sets = resolve_resource_capabilities(cfg, converter.config_loader)
        schema_supported = sorted(capability_sets.schema_supported)
        searchable = sorted(capability_sets.searchable)
        generatable = sorted(capability_sets.generatable)
        storable = sorted(capability_sets.storable)
        synthetic_writable = sorted(capability_sets.synthetic_writable)
        generation_only = sorted(
            capability_sets.generatable - capability_sets.synthetic_writable
        )
        recipe_resources = sorted(capability_sets.recipe_resources)
    except Exception as exc:
        schema_supported = []
        searchable = []
        generatable = []
        storable = []
        synthetic_writable = []
        generation_only = []
        recipe_resources = []
        discovery_errors.append({"source": "resource_capabilities", "error": str(exc)})

    from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.cohort_blueprints import (
        BLUEPRINT_CONTRACT_VERSION,
        fhir_cohort_catalog,
    )
    from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.validation import (
        available_validation_levels,
    )

    try:
        validation_levels = list(available_validation_levels(str(fhir_version)))
    except ValueError as exc:
        validation_levels = []
        discovery_errors.append({"source": "fhir_release", "error": str(exc)})

    ig_packages: list[dict[str, Any]] = []
    available_profiles: list[dict[str, Any]] = []
    active_profiles: list[dict[str, Any]] = []
    profile_validation_status: dict[str, Any] = {
        "contract_version": "fhir-profile-validation.v1",
        "mode": "disabled",
        "adapter_available": False,
        "enforced": False,
    }
    try:
        inspected_packages = (
            implementation_guides.inspect_configured_implementation_guides(cfg)
        )
        for compiled in inspected_packages:
            ig_packages.append(compiled["package"])
            available_profiles.extend(compiled["inventory"]["profiles"])
        active_profiles = implementation_guides.resolve_active_profiles(
            cfg, inspected_packages
        )
        profile_validation_status = describe_profile_validation(cfg, ctx.adapters)
    except Exception as exc:
        discovery_errors.append({"source": "implementation_guides", "error": str(exc)})

    catalog: dict[str, Any] = {
        "ok": True,
        "degraded": bool(discovery_errors),
        "discovery_errors": discovery_errors,
        "contract_version": SEARCH_CONTRACT_VERSION,
        "fhir_version": fhir_version,
        "release_support": release_evidence(fhir_version),
        "schema_supported_resource_types": schema_supported,
        "searchable_resource_types": searchable,
        "generatable_resource_types": generatable,
        "storable_resource_types": storable,
        "synthetic_writable_resource_types": synthetic_writable,
        "generation_only_resource_types": generation_only,
        "recipe_resource_types": recipe_resources,
        "capability_counts": {
            "schema_supported": len(schema_supported),
            "searchable": len(searchable),
            "storable": len(storable),
            "generatable_preview": len(generatable),
            "synthetic_writable": len(synthetic_writable),
            "generation_only": len(generation_only),
            "recipe_resources": len(recipe_resources),
        },
        "capability_semantics": {
            "generatable_resource_types": (
                "Resources fhir-gen can synthesize for preview and inspection; "
                "this does not imply persistence support."
            ),
            "synthetic_writable_resource_types": (
                "Resources that can be synthesized, denormalized, indexed, and persisted."
            ),
            "generation_only_resource_types": (
                "Generatable resources excluded from persistence because the active "
                "strategy has no mandatory search/compartment projection."
            ),
        },
        "synthetic_cohorts": {
            "supported": str(fhir_version).upper() in {"R5", "R6"},
            "contract_version": BLUEPRINT_CONTRACT_VERSION,
            "catalog_operation": "fhir_cohort_catalog",
            "plan_operation": "fhir_cohort_plan",
            "execute_operation": "synthetic_generate_batch",
            "execution_mode": "asynchronous-job-recommended",
            "assets": [
                {
                    "id": item["id"],
                    "version": item["version"],
                    "title": item["title"],
                    "maturity": item["maturity"],
                }
                for item in (
                    fhir_cohort_catalog(
                        ctx,
                        {
                            "schema_version": fhir_version,
                            "include_blueprints": False,
                        },
                    ).get("assets")
                    if str(fhir_version).upper() in {"R5", "R6"}
                    else []
                )
            ],
        },
        "ingest_supported": True,
        "write_supported": bool(storable),
        "import_formats": ["bundle", "ndjson", "resource"],
        "migration": {
            "run_history": True,
            "chunked_execution": True,
            "idempotent_checkpoints": True,
            "cooperative_cancellation": True,
            "retry_from_checkpoint": True,
            "source_payload_retained": False,
            "reference_integrity": "informational",
        },
        "validation_levels": validation_levels,
        "profile_conformance": profile_validation_status["enforced"],
        "profile_validation": profile_validation_status,
        "active_profiles": active_profiles,
        "conformance_mode": "implementation-guide-overlay"
        if ig_packages
        else "fhir-core",
        "implementation_guide_packages": ig_packages,
        "available_profiles": available_profiles,
        "persistence_invariants": {
            "search_projection": "mandatory",
            "compartment_projection": "mandatory",
            "configured_indexes": "mandatory",
            "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
        },
        # Controls that actually EXECUTE. _sort is compiled to a Mongo sort over the
        # configured field with an id tie-breaker; unsortable keys fail closed.
        "supported_result_controls": ["_count", "_offset", "_sort"],
        "planned_result_controls": [],
        "unsupported_result_controls": sorted(_KNOWN_UNSUPPORTED_RESULT_CONTROLS),
        "handling_modes": ["strict", "lenient"],
        # Truthful feature flags — advertise only what executes.
        "chaining_supported": True,
        "reverse_chaining_supported": True,
        "chaining_limits": {
            "maximum_hops": 1,
            "typed_forward_chain_required": True,
            "combined_with_compartment_search": False,
            "maximum_intermediate_ids": 10000,
        },
        "include_supported": False,
        "semantic": describe_semantic_config(cfg, ctx.adapters),
    }

    if not discovery_errors:
        search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
        compartment_dir = (
            search_cfg.get("compartment_definitions_dir")
            or bridge._bundled_compartment_definitions_dir()
        )
        versions = build_projection_versions(
            converter.config_loader,
            fhir_release=str(fhir_version),
            compartment_definitions_dir=compartment_dir,
            resource_types=storable,
        )
        catalog["persistence_invariants"]["projection_contract_version"] = (
            versions.projection_contract_version
        )

    # Optionally include per-parameter detail for a specific resource type.
    resource_type = payload.get("resource_type")
    if isinstance(resource_type, str) and resource_type.strip():
        try:
            catalog["parameters"] = fhir_list_search_params(
                ctx, {"resource_type": resource_type}
            )["parameters"]
            catalog["resource_type"] = resource_type.strip()
        except KehrnelError as exc:
            catalog["parameters_error"] = {"code": exc.code, "message": exc.message}

    return catalog


def fhir_support_matrix(
    ctx: StrategyContext, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Generate portable JSON and Markdown support evidence from capabilities.

    This is deliberately derived from ``fhir_capabilities`` rather than a second
    maintained list, so portal, CLI, and published pilot evidence cannot drift.
    """
    payload = payload or {}
    capabilities = fhir_capabilities(ctx, {})
    schema = set(capabilities.get("schema_supported_resource_types") or [])
    searchable = set(capabilities.get("searchable_resource_types") or [])
    storable = set(capabilities.get("storable_resource_types") or [])
    generatable = set(capabilities.get("generatable_resource_types") or [])
    synthetic_writable = set(
        capabilities.get("synthetic_writable_resource_types") or []
    )
    recipe = set(capabilities.get("recipe_resource_types") or [])
    rows: list[dict[str, Any]] = []
    include_parameters = bool(payload.get("include_parameters", False))
    for resource_type in sorted(schema | searchable | storable | generatable | recipe):
        row: dict[str, Any] = {
            "resource_type": resource_type,
            "schema": resource_type in schema,
            "search": resource_type in searchable,
            "write": resource_type in storable,
            # `generate` is retained for contract compatibility and means
            # preview/inspection. Consumers should prefer the explicit fields.
            "generate": resource_type in generatable,
            "generate_preview": resource_type in generatable,
            "generate_and_store": resource_type in synthetic_writable,
            "example_recipe": resource_type in recipe,
        }
        if include_parameters and resource_type in searchable:
            try:
                parameters = fhir_list_search_params(
                    ctx, {"resource_type": resource_type}
                )
                row["search_parameter_count"] = parameters.get("parameter_count", 0)
                row["search_parameters"] = [
                    item.get("name") for item in parameters.get("parameters") or []
                ]
            except KehrnelError as exc:
                row["search_parameters_error"] = {
                    "code": exc.code,
                    "message": exc.message,
                }
        rows.append(row)

    def mark(value: bool) -> str:
        return "yes" if value else "—"

    markdown_lines = [
        f"# FHIR {capabilities.get('fhir_version')} accelerator support matrix",
        "",
        (
            "Generated from the activated `fhir_capabilities` contract. "
            "A yes indicates implemented accelerator behavior, not certification or full FHIR conformance."
        ),
        "",
        "| Resource | Schema | Search | Write | Generate preview | Generate + store | Example recipe |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    markdown_lines.extend(
        "| {resource_type} | {schema} | {search} | {write} | {preview} | {persist} | {recipe} |".format(
            resource_type=row["resource_type"],
            schema=mark(row["schema"]),
            search=mark(row["search"]),
            write=mark(row["write"]),
            preview=mark(row["generate_preview"]),
            persist=mark(row["generate_and_store"]),
            recipe=mark(row["example_recipe"]),
        )
        for row in rows
    )
    return {
        "ok": not capabilities.get("degraded", False),
        "contract_version": capabilities.get("contract_version"),
        "fhir_version": capabilities.get("fhir_version"),
        "release_support": capabilities.get("release_support"),
        "generated_from": "fhir_capabilities",
        "generated_at": datetime.now(UTC).isoformat(),
        "profile_conformance": capabilities.get("profile_conformance", False),
        "capability_semantics": capabilities.get("capability_semantics", {}),
        "rows": rows,
        "markdown": "\n".join(markdown_lines) + "\n",
    }
