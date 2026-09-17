"""FHIR domain search routes (strategy-backed)."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from kehrnel.api.bridge.app.core.database import (
    _default_env_id,
    _extract_env_id,
    _get_activation,
    _is_env_access_allowed,
)
from kehrnel.api.core.admin.routes import _json_safe
from kehrnel.api.domains.fhir.models import FhirSearchRequest
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.domains.fhir import implementation_guides
from kehrnel.engine.terminology import TerminologyService
from kehrnel.engine.strategies.fhir.resource_store.scripts.serialization import (
    operation_outcome,
    operation_outcome_from_error,
)
from kehrnel.engine.strategies.fhir.resource_store.scripts.storage_adapter import (
    MongoFHIRStorageAdapter,
)

# Serialization seam used by the FHIR REST boundary. db is not needed for the
# serialize path (canonical mapping is pure); read/persist use a db-bound adapter.
_SERIALIZER = MongoFHIRStorageAdapter(db=None)


FHIR_JSON_MEDIA_TYPE = "application/fhir+json"
FHIR_IMPORT_MAX_BYTES = int(
    os.getenv("KEHRNEL_FHIR_IMPORT_MAX_BYTES", str(25 * 1024 * 1024))
)
FHIR_IG_UPLOAD_MAX_BYTES = int(
    os.getenv("KEHRNEL_FHIR_IG_UPLOAD_MAX_BYTES", str(32 * 1024 * 1024))
)
FHIR_IG_STAGING_MAX_BYTES = int(
    os.getenv("KEHRNEL_FHIR_IG_STAGING_MAX_BYTES", str(512 * 1024 * 1024))
)


def _fhir_json(content: dict, status_code: int = 200) -> JSONResponse:
    """Return FHIR-typed JSON (application/fhir+json)."""
    return JSONResponse(
        status_code=status_code, content=content, media_type=FHIR_JSON_MEDIA_TYPE
    )


def _operation_outcome_response(exc: Exception) -> JSONResponse:
    """FHIR HTTP error boundary: map an error to an OperationOutcome (T5).

    Internal ops keep raising KehrnelError; only the FHIR REST edge emits FHIR
    OperationOutcome with the correct HTTP status and media type. For unexpected
    (non-Kehrnel/HTTP) errors, a generic diagnostic is returned so internal
    details are not leaked to clients.
    """
    if isinstance(exc, HTTPException):
        return _fhir_json(
            operation_outcome(code="processing", message=str(exc.detail)),
            status_code=exc.status_code,
        )
    if isinstance(exc, KehrnelError):
        return _fhir_json(
            operation_outcome_from_error(exc),
            status_code=int(getattr(exc, "status", 500) or 500),
        )
    # Unexpected error — do not leak internals.
    return _fhir_json(
        operation_outcome(
            code="PROCESSING_ERROR", message="Internal error processing FHIR request"
        ),
        status_code=500,
    )


router = APIRouter(prefix="/api/domains/fhir", tags=["FHIR"])

FHIR_DOMAIN = "fhir"
DEFAULT_STRATEGY_ID = os.getenv("KEHRNEL_FHIR_STRATEGY_ID", "fhir.resource_store")


def _auth_enabled() -> bool:
    # Match the application-wide secure default. Development/test environments
    # that intentionally run without auth must opt out explicitly.
    return os.getenv("KEHRNEL_AUTH_ENABLED", "true").lower() in ("1", "true", "yes")


def resolve_active_env_id(request: Request) -> str:
    """Resolve environment from x-active-env (or configured default)."""
    env_id = _extract_env_id(request) or _default_env_id()
    if not env_id:
        raise HTTPException(
            status_code=400,
            detail="Missing active environment. Provide x-active-env (or env_id query param).",
        )
    if _auth_enabled() and not _is_env_access_allowed(request, env_id):
        raise HTTPException(
            status_code=403,
            detail=f"Access to env_id={env_id} is not permitted for this API key.",
        )
    return env_id


def _require_fhir_activation(request: Request, env_id: str):
    runtime, activation = _get_activation(request, env_id, FHIR_DOMAIN)
    strategy_id = (getattr(activation, "strategy_id", None) or "").strip()
    if strategy_id != DEFAULT_STRATEGY_ID:
        raise HTTPException(
            status_code=409,
            detail=(
                f"FHIR domain requires strategy {DEFAULT_STRATEGY_ID!r}; "
                f"active activation is {strategy_id!r}."
            ),
        )
    return runtime, activation


def to_strategy_query(payload: FhirSearchRequest) -> dict[str, Any]:
    """Map API request to fhir.resource_store compile_query input.

    For a ``fhir_search`` URL, the URL's own ``_count``/``_offset`` are authoritative;
    the request-level ``limit``/``offset`` are passed only as *defaults* (applied when
    the URL omits them) so they can never override an explicit URL ``_count``.
    """
    query: dict[str, Any] = {
        "resource_type": payload.resource_type,
        "criteria": payload.criteria,
    }
    if payload.fhir_search:
        query["fhir_search"] = payload.fhir_search
        query["default_count"] = payload.limit
        if payload.offset:
            query["default_offset"] = payload.offset
    else:
        # Structured criteria form: limit/offset are the explicit selection.
        query["_count"] = payload.limit
        if payload.offset:
            query["_offset"] = payload.offset
    return query


def _prefer_handling(request: Request) -> str | None:
    """Parse FHIR `Prefer: handling=strict|lenient` (takes precedence over body)."""
    prefer = request.headers.get("prefer") or ""
    for part in prefer.split(","):
        token = part.strip().lower()
        if token.startswith("handling="):
            value = token.split("=", 1)[1].strip()
            if value in ("strict", "lenient"):
                return value
    return None


def _with_offset(url: str, offset: int) -> str:
    """Return ``url`` with ``_offset`` replaced (or removed when zero)."""
    parts = urlsplit(url)
    query = [
        (name, value)
        for name, value in parse_qsl(parts.query, keep_blank_values=True)
        if name != "_offset"
    ]
    if offset > 0:
        query.append(("_offset", str(offset)))
    return urlunsplit(parts._replace(query=urlencode(query, doseq=True)))


def _build_bundle_links(
    *,
    self_url: str | None,
    returned: int,
    total: int | None,
    explain: dict[str, Any],
) -> list[dict[str, str]]:
    """Conformant Bundle.link entries for a searchset.

    Paging is expressed through the server-specific ``_offset`` parameter — this
    store has no opaque continuation tokens — but the ``self``/``next``/
    ``previous`` relations themselves are standard, so ordinary FHIR clients can
    page without knowing that detail.
    """
    if not self_url:
        return []
    links = [{"relation": "self", "url": self_url}]

    execution = explain.get("execution")
    if not isinstance(execution, dict):
        return links
    limit = execution.get("limit")
    skip = execution.get("skip") or 0
    if not isinstance(limit, int) or limit <= 0 or not isinstance(skip, int):
        return links

    if skip > 0:
        links.append(
            {
                "relation": "previous",
                "url": _with_offset(self_url, max(0, skip - limit)),
            }
        )
    # With a known total, offer `next` only while results remain. Without one,
    # a full page is the only available signal that more may exist.
    has_more = (
        (skip + returned) < total if isinstance(total, int) else returned >= limit
    )
    if has_more:
        links.append({"relation": "next", "url": _with_offset(self_url, skip + limit)})
    return links


def build_search_bundle(
    *,
    rows: list[dict[str, Any]],
    total: int | None,
    env_id: str,
    explain: dict[str, Any] | None,
    self_url: str | None = None,
) -> dict[str, Any]:
    """FHIR R5 searchset Bundle with Kehrnel execution metadata."""
    explain = explain or {}
    bundle_total = total if total is not None else len(rows)
    # T5: canonical serialization at the FHIR boundary — strip operational storage
    # fields (_id/_search/_compartments/_stored_at/_fhir_resource_type) while keeping
    # primitive extensions (_birthDate, …).
    canonical_rows = _SERIALIZER.serialize_many(rows)
    # Conformant searchset Bundle only. Execution metadata is NOT a valid FHIR
    # `Meta` property, so it is not attached here — the ops-level envelope
    # (fhir_search) carries execution_summary/compiled_plan separately.
    entries = [{"resource": row, "search": {"mode": "match"}} for row in canonical_rows]

    # Lenient mode: report any ignored parameters as an OperationOutcome warning
    # entry (search.mode=outcome) so clients can see what was omitted.
    ignored = (explain or {}).get("ignored_parameters") or []
    if ignored:
        names = ", ".join(str(i.get("name") or i.get("reason") or "?") for i in ignored)
        entries.append(
            {
                "search": {"mode": "outcome"},
                "resource": operation_outcome(
                    severity="warning",
                    code="not-supported",
                    message=f"Ignored unsupported search parameter(s): {names}",
                ),
            }
        )
    bundle: dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": bundle_total,
    }
    links = _build_bundle_links(
        self_url=self_url,
        returned=len(canonical_rows),
        total=total,
        explain=explain,
    )
    if links:
        bundle["link"] = links
    bundle["entry"] = entries
    return bundle


def _fhir_version_number(value: Any) -> str:
    """Map the configured release label to CapabilityStatement.fhirVersion."""
    normalized = str(value or "R5").strip().upper()
    return {
        "R4": "4.0.1",
        "R5": "5.0.0",
        "R6": "6.0.0",
    }.get(normalized, str(value or "5.0.0"))


def _terminology_operation_declarations(service: TerminologyService) -> list[dict[str, str]]:
    operations = {
        str(operation)
        for provider in service.config.get("providers", [])
        if provider.get("enabled", True)
        for operation in provider.get("operations", [])
    }
    definitions = {
        "lookup": "http://hl7.org/fhir/OperationDefinition/CodeSystem-lookup",
        "validate-code": "http://hl7.org/fhir/OperationDefinition/CodeSystem-validate-code",
        "subsumes": "http://hl7.org/fhir/OperationDefinition/CodeSystem-subsumes",
        "expand": "http://hl7.org/fhir/OperationDefinition/ValueSet-expand",
        "translate": "http://hl7.org/fhir/OperationDefinition/ConceptMap-translate",
    }
    return [
        {"name": name, "definition": definition}
        for name, definition in definitions.items()
        if name in operations
    ]


@router.get("/metadata")
async def fhir_capability_statement(request: Request):
    """Describe the limited, activated FHIR REST surface truthfully."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, activation = _require_fhir_activation(request, env_id)
        capabilities = await runtime.dispatch(
            env_id,
            "op",
            {"domain": FHIR_DOMAIN, "op": "fhir_capabilities", "payload": {}},
        )
        if not isinstance(capabilities, dict):
            raise KehrnelError(
                code="FHIR_CAPABILITIES_FAILED",
                status=500,
                message="Unexpected capability result shape from strategy runtime",
            )

        searchable = {
            str(value)
            for value in (capabilities.get("searchable_resource_types") or [])
            if isinstance(value, str) and value
        }
        storable = {
            str(value)
            for value in (capabilities.get("storable_resource_types") or [])
            if isinstance(value, str) and value
        }
        # Compatibility with older/custom strategy implementations that exposed
        # only the former global write flag. Current Kehrnel always returns the
        # per-resource storable set.
        if "storable_resource_types" not in capabilities and capabilities.get(
            "write_supported"
        ):
            storable = set(searchable)
        resources = []
        for resource_type in sorted(searchable | storable):
            interactions = [{"code": "read"}]
            if resource_type in searchable:
                interactions.append({"code": "search-type"})
            if resource_type in storable:
                interactions.extend([{"code": "create"}, {"code": "update"}])
            resources.append(
                {
                    "type": resource_type,
                    "interaction": interactions,
                    "versioning": "no-version",
                    "conditionalRead": "not-supported",
                    "conditionalCreate": False,
                    "conditionalUpdate": False,
                    "conditionalDelete": "not-supported",
                }
            )
        rest = {"mode": "server", "resource": resources}
        terminology_operations = _terminology_operation_declarations(
            TerminologyService(runtime, env_id)
        )
        if terminology_operations:
            rest["operation"] = terminology_operations
        statement = {
            "resourceType": "CapabilityStatement",
            "status": "active",
            "experimental": True,
            "date": datetime.now(UTC).isoformat(),
            "publisher": "Kehrnel",
            "kind": "instance",
            "software": {
                "name": "Kehrnel FHIR Resource Store Accelerator",
                "version": str(getattr(activation, "version", None) or "preview"),
            },
            "implementation": {
                "description": (
                    "FHIR resource-store accelerator. Supports create, update, read, and type-level "
                    "search for the resources listed here; it is not a complete FHIR server."
                ),
                "url": str(request.base_url).rstrip("/") + "/api/domains/fhir",
            },
            "fhirVersion": _fhir_version_number(capabilities.get("fhir_version")),
            "format": [FHIR_JSON_MEDIA_TYPE, "json"],
            "rest": [rest],
        }
        return _fhir_json(_json_safe(statement))
    except Exception as exc:
        return _operation_outcome_response(exc)


def _fhir_parameter_payload(resource: dict[str, Any] | None) -> dict[str, Any]:
    if not resource:
        return {}
    if resource.get("resourceType") != "Parameters":
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="FHIR terminology operations require a Parameters resource body.",
        )
    payload: dict[str, Any] = {}
    for parameter in resource.get("parameter") or []:
        if not isinstance(parameter, dict) or not parameter.get("name"):
            continue
        name = str(parameter["name"])
        value = next(
            (item for key, item in parameter.items() if key.startswith("value")),
            None,
        )
        if value is None and isinstance(parameter.get("resource"), dict):
            value = parameter["resource"]
        if value is not None:
            payload[name] = value
    return payload


def _fhir_terminology_request_payload(
    request: Request,
    operation: str,
    body: dict[str, Any] | None,
    *,
    resource_type: str,
) -> dict[str, Any]:
    payload = _fhir_parameter_payload(body)
    for name, value in request.query_params.multi_items():
        if name not in {"env_id", "environment"}:
            payload[name] = value
    for name in ("includeDesignations", "activeOnly", "abstract", "reverse"):
        if name in payload and isinstance(payload[name], str):
            payload[name] = payload[name].strip().lower() in {"1", "true", "yes", "on"}
    for name in ("offset", "count"):
        if name in payload and isinstance(payload[name], str):
            try:
                payload[name] = int(payload[name])
            except ValueError as exc:
                raise KehrnelError(code="INVALID_INPUT", status=400, message=f"{name} must be an integer") from exc
    if resource_type == "CodeSystem" and payload.get("url") and not payload.get("system"):
        payload["system"] = payload["url"]
        payload.pop("url", None)
    aliases = {
        "codeA": "code_a",
        "codeB": "code_b",
        "displayLanguage": "language",
        "valueSetVersion": "value_set_version",
        "systemVersion": "version",
        "includeDesignations": "include_designations",
        "activeOnly": "active_only",
        "conceptMapVersion": "concept_map_version",
        "targetsystem": "target_system",
    }
    for source, target in aliases.items():
        if source in payload and target not in payload:
            payload[target] = payload[source]
    return payload


def _terminology_parameters(operation: str, result: dict[str, Any]) -> dict[str, Any]:
    parameters: list[dict[str, Any]] = []

    def add(name: str, value: Any, value_type: str = "String") -> None:
        if value is not None:
            parameters.append({"name": name, f"value{value_type}": value})

    if operation == "validate-code":
        add("result", bool(result.get("valid")), "Boolean")
        add("message", result.get("message"))
        add("display", result.get("display"))
    elif operation == "subsumes":
        add("outcome", result.get("outcome"), "Code")
    elif operation == "lookup":
        concept = result.get("concept") if isinstance(result.get("concept"), dict) else {}
        displays = [
            item.get("term")
            for item in concept.get("descriptions") or []
            if isinstance(item, dict) and item.get("active", True) and item.get("term")
        ]
        add("name", "SNOMED CT")
        add("version", result.get("version") or concept.get("releaseId"))
        add("display", displays[0] if displays else None)
        add("inactive", not bool(concept.get("active", True)), "Boolean")
        for description in concept.get("descriptions") or []:
            if not isinstance(description, dict) or not description.get("active", True):
                continue
            parts = [
                {"name": "language", "valueCode": description.get("languageCode")},
                {"name": "value", "valueString": description.get("term")},
            ]
            parameters.append(
                {
                    "name": "designation",
                    "part": [part for part in parts if list(part.values())[-1] is not None],
                }
            )
    elif operation == "translate":
        add("result", bool(result.get("matched")), "Boolean")
        add("message", result.get("message"))
    return {"resourceType": "Parameters", "parameter": parameters}


def _terminology_response(content: dict[str, Any], result: dict[str, Any]) -> JSONResponse:
    """Return standard FHIR content with non-clinical execution provenance."""
    response = _fhir_json(_json_safe(content))
    evidence = result.get("evidence") if isinstance(result.get("evidence"), dict) else {}
    provider = result.get("provider") if isinstance(result.get("provider"), dict) else {}
    header_values = {
        "X-Kehrnel-Terminology-Provider": provider.get("id"),
        "X-Kehrnel-Terminology-Correlation": evidence.get("correlationId"),
        "X-Kehrnel-Terminology-Release": result.get("releaseId"),
    }
    for name, value in header_values.items():
        if value:
            response.headers[name] = str(value)
    return response


async def _execute_fhir_terminology(
    request: Request,
    operation: str,
    body: dict[str, Any] | None = None,
    *,
    resource_type: str = "CodeSystem",
) -> JSONResponse:
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        payload = _fhir_terminology_request_payload(
            request, operation, body, resource_type=resource_type
        )
        result = await TerminologyService(runtime, env_id).execute(operation, payload)
        if operation == "expand":
            value_set = result.get("valueSet")
            if not isinstance(value_set, dict):
                concepts = result.get("concepts") or result.get("matches") or []
                contains = []
                for concept in concepts:
                    if not isinstance(concept, dict):
                        continue
                    descriptions = concept.get("descriptions") or []
                    display = next(
                        (
                            item.get("term")
                            for item in descriptions
                            if isinstance(item, dict) and item.get("active", True) and item.get("term")
                        ),
                        None,
                    )
                    contains.append(
                        {
                            "system": result.get("system") or payload.get("system"),
                            "code": concept.get("conceptId") or concept.get("code"),
                            "display": display,
                            "inactive": not bool(concept.get("active", True)),
                        }
                    )
                value_set = {
                    "resourceType": "ValueSet",
                    "url": payload.get("url"),
                    "version": payload.get("value_set_version"),
                    "status": "active",
                    "expansion": {
                        "timestamp": datetime.now(UTC).isoformat(),
                        "total": len(contains),
                        "contains": contains,
                    },
                }
            return _terminology_response(value_set, result)
        provider_response = result.get("response")
        if isinstance(provider_response, dict) and provider_response.get("resourceType") == "Parameters":
            return _terminology_response(provider_response, result)
        return _terminology_response(_terminology_parameters(operation, result), result)
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/CodeSystem/$lookup")
async def fhir_terminology_lookup_get(request: Request):
    return await _execute_fhir_terminology(request, "lookup", resource_type="CodeSystem")


@router.post("/CodeSystem/$lookup")
async def fhir_terminology_lookup_post(
    request: Request, body: dict[str, Any] = Body(...)
):
    return await _execute_fhir_terminology(request, "lookup", body, resource_type="CodeSystem")


@router.get("/CodeSystem/$validate-code")
async def fhir_terminology_validate_get(request: Request):
    return await _execute_fhir_terminology(request, "validate-code", resource_type="CodeSystem")


@router.post("/CodeSystem/$validate-code")
async def fhir_terminology_validate_post(
    request: Request, body: dict[str, Any] = Body(...)
):
    return await _execute_fhir_terminology(request, "validate-code", body, resource_type="CodeSystem")


@router.get("/CodeSystem/$subsumes")
async def fhir_terminology_subsumes_get(request: Request):
    return await _execute_fhir_terminology(request, "subsumes", resource_type="CodeSystem")


@router.post("/CodeSystem/$subsumes")
async def fhir_terminology_subsumes_post(
    request: Request, body: dict[str, Any] = Body(...)
):
    return await _execute_fhir_terminology(request, "subsumes", body, resource_type="CodeSystem")


@router.get("/ValueSet/$validate-code")
async def fhir_terminology_value_set_validate_get(request: Request):
    return await _execute_fhir_terminology(request, "validate-code", resource_type="ValueSet")


@router.post("/ValueSet/$validate-code")
async def fhir_terminology_value_set_validate_post(
    request: Request, body: dict[str, Any] = Body(...)
):
    return await _execute_fhir_terminology(
        request, "validate-code", body, resource_type="ValueSet"
    )


@router.get("/ValueSet/$expand")
async def fhir_terminology_expand_get(request: Request):
    return await _execute_fhir_terminology(request, "expand", resource_type="ValueSet")


@router.post("/ValueSet/$expand")
async def fhir_terminology_expand_post(
    request: Request, body: dict[str, Any] = Body(...)
):
    return await _execute_fhir_terminology(request, "expand", body, resource_type="ValueSet")


@router.get("/ConceptMap/$translate")
async def fhir_terminology_translate_get(request: Request):
    return await _execute_fhir_terminology(request, "translate", resource_type="ConceptMap")


@router.post("/ConceptMap/$translate")
async def fhir_terminology_translate_post(
    request: Request, body: dict[str, Any] = Body(...)
):
    return await _execute_fhir_terminology(
        request, "translate", body, resource_type="ConceptMap"
    )


@router.post("/search")
async def search_fhir(request: Request, payload: FhirSearchRequest = Body(...)):
    """
    Execute FHIR search via the active environment's fhir.resource_store strategy.

    Compiles FHIR search parameters to MQL and runs against MongoDB; returns a
    searchset Bundle.
    """
    try:
        env_id = resolve_active_env_id(request)
        runtime, activation = _require_fhir_activation(request, env_id)

        strategy_query = to_strategy_query(payload)
        prefer = _prefer_handling(request)  # Prefer header wins over body field
        if prefer:
            strategy_query["handling"] = prefer

        result = await runtime.dispatch(
            env_id,
            "query",
            {
                "domain": FHIR_DOMAIN,
                "query": strategy_query,
            },
        )

        if not isinstance(result, dict):
            raise KehrnelError(
                code="FHIR_SEARCH_FAILED",
                status=500,
                message="Unexpected query result shape from strategy runtime",
            )

        rows = result.get("rows") or []
        if not isinstance(rows, list):
            rows = []

        explain = (
            result.get("explain") if isinstance(result.get("explain"), dict) else {}
        )
        explain.setdefault("strategy_id", getattr(activation, "strategy_id", None))
        explain.setdefault("activation_id", getattr(activation, "activation_id", None))

        total_raw = explain.get("total")
        total = int(total_raw) if total_raw is not None else None

        bundle = build_search_bundle(
            rows=rows,
            total=total,
            env_id=env_id,
            explain=explain,
            self_url=getattr(request.state, "fhir_self_url", None),
        )
        resp = _fhir_json(_json_safe(bundle))
        if prefer:
            resp.headers["Preference-Applied"] = f"handling={prefer}"
        return resp
    except Exception as exc:
        # All FHIR-boundary errors (incl. HTTPException) become OperationOutcome
        # with application/fhir+json and the correct status.
        return _operation_outcome_response(exc)


@router.post("/explain")
async def explain_fhir_search(request: Request, payload: dict[str, Any] = Body(...)):
    """Compile FHIR Search to MQL without executing it against MongoDB."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        compile_payload = dict(payload or {})
        compile_payload["explain_only"] = True
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_search",
                "payload": compile_payload,
            },
        )
        if not isinstance(result, dict):
            raise KehrnelError(
                code="FHIR_EXPLAIN_FAILED",
                status=500,
                message="Unexpected explain result shape from strategy runtime",
            )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/native-query")
async def execute_native_fhir_query(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Execute a bounded, read-only MongoDB find for implementation work.

    This is an accelerator operation, not part of the FHIR REST specification.
    It deliberately excludes writes, aggregation pipelines, server-side
    JavaScript, unbounded result sets and cross-collection access.
    """
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_native_query",
                "payload": dict(payload or {}),
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/explain/execute")
async def explain_execute_fhir_search(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """
    Execute a FHIR Search and return MongoDB execution evidence, without resources.

    This is an operational diagnostics endpoint, not part of the FHIR REST
    surface: it reports the compiled MQL plan alongside the real MongoDB
    ``executionStats`` (winning plan, index used, docs examined) so an adopting
    team can prove which index served a query.

    SECURITY: privileged diagnostics are authorized here, at the trusted HTTP
    boundary, never by a client-supplied flag. Any ``__privileged``/``privileged``
    key in the request body is discarded before dispatch. Matched resources are
    deliberately omitted from the response so evidence collection never becomes
    a way to read clinical content that ``GET /{type}`` would not already allow.
    """
    try:
        env_id = resolve_active_env_id(request)
        runtime, activation = _require_fhir_activation(request, env_id)

        search_payload = dict(payload or {})
        # Never honour caller-supplied privilege markers.
        for reserved in ("__privileged", "privileged", "debug", "explain_only"):
            search_payload.pop(reserved, None)

        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_search",
                "payload": search_payload,
                # Server-set marker: enables real Mongo executionStats collection.
                "__privileged": True,
            },
        )
        if not isinstance(result, dict):
            raise KehrnelError(
                code="FHIR_EXPLAIN_EXECUTE_FAILED",
                status=500,
                message="Unexpected result shape from strategy runtime",
            )

        explain = (
            result.get("explain") if isinstance(result.get("explain"), dict) else {}
        )
        evidence = {
            "ok": True,
            "contract_version": result.get("contract_version"),
            "engine": result.get("engine_used") or result.get("engine"),
            "strategy_id": getattr(activation, "strategy_id", None),
            "activation_id": getattr(activation, "activation_id", None),
            "compiled_plan": result.get("plan"),
            "execution": explain.get("execution"),
            "executed": explain.get("executed"),
            "ignored_parameters": explain.get("ignored_parameters") or [],
            "total": explain.get("total"),
            "returned": explain.get("returned"),
            # Privileged evidence — present because this route set __privileged.
            "mongo_execution_stats": explain.get("mongo_execution_stats"),
            "resolved_filter": (explain.get("_executed_pipeline") or {}).get("filter"),
            # Matched resources are intentionally not returned; use GET /{type}.
            "resources_omitted": True,
        }
        return JSONResponse(content=_json_safe(evidence))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/index-manifest")
async def get_fhir_index_manifest(request: Request):
    """
    Return the accelerator-managed index manifest for the active environment.

    The manifest already exists as activation evidence (digest, per-collection
    index definitions, managed-index budget and any violations); this exposes it
    as a first-class contract so the experience layer can render index evidence
    and an adopting team can export it, instead of reaching through the generic
    admin ops endpoint.
    """
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_index_manifest",
                "payload": {},
            },
        )
        if not isinstance(result, dict):
            raise KehrnelError(
                code="FHIR_INDEX_MANIFEST_FAILED",
                status=500,
                message="Unexpected index manifest shape from strategy runtime",
            )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/capabilities")
async def get_fhir_runtime_capabilities(
    request: Request, resource_type: str | None = None
):
    """Machine-readable accelerator capabilities used by operational clients."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_capabilities",
                "payload": {"resource_type": resource_type} if resource_type else {},
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/support-matrix")
async def get_fhir_support_matrix(request: Request):
    """Generate support evidence from the same runtime contract used by HDL."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        include_parameters = _query_bool(
            request.query_params.get("include_parameters"), False
        )
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_support_matrix",
                "payload": {"include_parameters": include_parameters},
            },
        )
        requested_format = str(request.query_params.get("format") or "json").lower()
        if requested_format in {"md", "markdown"}:
            return PlainTextResponse(
                content=str(result.get("markdown") or ""),
                media_type="text/markdown; charset=utf-8",
            )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/stats")
async def get_fhir_store_stats(request: Request):
    """Return bounded storage, index, and provenance diagnostics for the active store."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        summary_only = str(
            request.query_params.get("summary") or ""
        ).strip().lower() in {"1", "true", "yes", "on"}
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_stats",
                "payload": {"summary_only": True} if summary_only else {},
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/search-parameters/{resource_type}")
async def get_fhir_search_parameters(request: Request, resource_type: str):
    """Return the active runtime's parameter contract for one resource type."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_list_search_params",
                "payload": {"resource_type": resource_type},
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/resource-catalog")
async def get_fhir_resource_catalog(request: Request):
    """List resource models owned by the active Resource Store package."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {"domain": FHIR_DOMAIN, "op": "fhir_resource_catalog", "payload": {}},
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/resource-catalog/{resource_type}")
async def get_fhir_resource_definition(request: Request, resource_type: str):
    """Return one package-backed resource structure, search, and index model."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_resource_catalog",
                "payload": {"resource_type": resource_type},
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/synthetic/cohorts")
async def get_fhir_cohort_catalog(request: Request):
    """List patient-centred cohort blueprints from the active strategy."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        include_blueprints = _query_bool(
            request.query_params.get("include_blueprints"), True
        )
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_cohort_catalog",
                "payload": {"include_blueprints": include_blueprints},
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/synthetic/cohorts/plan")
async def plan_fhir_cohort(request: Request, payload: dict[str, Any] = Body(...)):
    """Compile cohort intent into deterministic counts without generating data."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_cohort_plan",
                "payload": payload,
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/synthetic/cohorts/preview")
async def preview_fhir_cohort(request: Request, payload: dict[str, Any] = Body(...)):
    """Generate a bounded, non-persistent cohort preview with quality evidence."""
    try:
        cohort = payload.get("cohort") if isinstance(payload, dict) else None
        request_cohort = dict(cohort) if isinstance(cohort, dict) else dict(payload)
        patients = request_cohort.get("patients", 1)
        if (
            not isinstance(patients, int)
            or isinstance(patients, bool)
            or not 1 <= patients <= 10
        ):
            raise KehrnelError(
                code="FHIR_COHORT_PREVIEW_TOO_LARGE",
                status=400,
                message="Synchronous cohort previews support between 1 and 10 patients",
            )
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        sample_limit = payload.get("sample_limit", 25)
        if (
            not isinstance(sample_limit, int)
            or isinstance(sample_limit, bool)
            or not 1 <= sample_limit <= 100
        ):
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="sample_limit must be between 1 and 100",
            )
        request_cohort["patients"] = patients
        preview_payload = {
            **payload,
            "dry_run": True,
            "store_canonical": False,
            "include_sample": True,
            "sample_limit": sample_limit,
        }
        if isinstance(cohort, dict):
            preview_payload["cohort"] = request_cohort
        else:
            preview_payload.update(request_cohort)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "synthetic_generate_batch",
                "payload": preview_payload,
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/implementation-guides/stage")
async def stage_fhir_implementation_guide(
    request: Request,
    fhir_release: str | None = Query(
        default=None,
        description=(
            "FHIR release of the activation draft (R4, R5, or R6). Required only "
            "when no FHIR Resource Store activation exists."
        ),
    ),
):
    """Stage and inspect one bounded FHIR NPM archive for later activation.

    Staging is intentionally available before the FHIR strategy is active so an
    implementation guide can be reviewed as part of the activation draft. An
    existing FHIR activation remains authoritative for the release; otherwise
    callers must provide ``fhir_release=R4|R5|R6``.
    """
    try:
        env_id = resolve_active_env_id(request)
        runtime = getattr(request.app.state, "strategy_runtime", None)
        if runtime is None:
            raise HTTPException(status_code=503, detail="Strategy runtime is not available.")
        activation = runtime.registry.get_activation(env_id, FHIR_DOMAIN)
        configured_release = str(
            ((getattr(activation, "config", None) or {}).get("schema_version"))
            or ""
        ).strip().upper()
        requested_release = str(fhir_release or "").strip().upper()
        if activation is not None:
            strategy_id = (getattr(activation, "strategy_id", None) or "").strip()
            if strategy_id != DEFAULT_STRATEGY_ID:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"FHIR domain requires strategy {DEFAULT_STRATEGY_ID!r}; "
                        f"active activation is {strategy_id!r}."
                    ),
                )
            if requested_release and requested_release != configured_release:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Requested FHIR release {requested_release} does not match "
                        f"the active release {configured_release}."
                    ),
                )
        expected_release = configured_release or requested_release
        if expected_release not in {"R4", "R5", "R6"}:
            raise KehrnelError(
                code="FHIR_RELEASE_REQUIRED",
                status=400,
                message=(
                    "No FHIR strategy is active. Provide fhir_release=R4, R5, or R6 "
                    "to stage a package for an activation draft."
                ),
            )
        staging_root = str(os.getenv("KEHRNEL_FHIR_IG_STAGING_ROOT") or "").strip()
        if not staging_root:
            raise KehrnelError(
                code="FHIR_IG_STAGING_NOT_CONFIGURED",
                status=503,
                message="Set KEHRNEL_FHIR_IG_STAGING_ROOT to enable IG uploads",
            )
        declared = request.headers.get("content-length")
        if declared and int(declared) > FHIR_IG_UPLOAD_MAX_BYTES:
            raise KehrnelError(
                code="FHIR_IG_UPLOAD_TOO_LARGE",
                status=413,
                message=f"FHIR IG upload exceeds {FHIR_IG_UPLOAD_MAX_BYTES} bytes",
            )
        data = await request.body()
        if len(data) > FHIR_IG_UPLOAD_MAX_BYTES:
            raise KehrnelError(
                code="FHIR_IG_UPLOAD_TOO_LARGE",
                status=413,
                message=f"FHIR IG upload exceeds {FHIR_IG_UPLOAD_MAX_BYTES} bytes",
            )
        filename = (
            request.headers.get("x-fhir-package-filename")
            or request.query_params.get("filename")
            or "package.tgz"
        )
        result = await asyncio.to_thread(
            implementation_guides.stage_implementation_guide,
            data,
            filename=filename,
            environment_id=env_id,
            staging_root=staging_root,
            expected_release=expected_release,
            max_upload_bytes=FHIR_IG_UPLOAD_MAX_BYTES,
            max_environment_bytes=FHIR_IG_STAGING_MAX_BYTES,
        )
        return JSONResponse(status_code=201, content=_json_safe({"ok": True, **result}))
    except implementation_guides.ImplementationGuideError as exc:
        return _operation_outcome_response(
            KehrnelError(code="FHIR_IG_INVALID", status=400, message=str(exc))
        )
    except (TypeError, ValueError) as exc:
        return _operation_outcome_response(
            KehrnelError(code="INVALID_INPUT", status=400, message=str(exc))
        )
    except Exception as exc:
        return _operation_outcome_response(exc)


def _query_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


async def _read_bounded_body(request: Request) -> bytes:
    declared = request.headers.get("content-length")
    if declared:
        try:
            if int(declared) > FHIR_IMPORT_MAX_BYTES:
                raise KehrnelError(
                    code="FHIR_IMPORT_TOO_LARGE",
                    status=413,
                    message=f"FHIR request exceeds the {FHIR_IMPORT_MAX_BYTES}-byte limit",
                )
        except ValueError:
            pass
    body = await request.body()
    if len(body) > FHIR_IMPORT_MAX_BYTES:
        raise KehrnelError(
            code="FHIR_IMPORT_TOO_LARGE",
            status=413,
            message=f"FHIR request exceeds the {FHIR_IMPORT_MAX_BYTES}-byte limit",
        )
    return body


async def _decode_import_request(request: Request) -> dict[str, Any]:
    content_type = (
        (request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    )
    body = await _read_bounded_body(request)
    if content_type in {
        "application/fhir+ndjson",
        "application/ndjson",
        "application/x-ndjson",
    }:
        payload: dict[str, Any] = {"ndjson": body.decode("utf-8")}
    else:
        try:
            decoded = json.loads(body or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=f"Invalid JSON import body: {exc}",
            ) from exc
        if not isinstance(decoded, dict):
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="FHIR import body must be a JSON object",
            )
        if decoded.get("resourceType") == "Bundle":
            payload = {"bundle": decoded}
        elif decoded.get("resourceType"):
            payload = {"resource": decoded}
        else:
            payload = decoded

    # Query options make raw Bundle/NDJSON uploads useful without wrapping them.
    for name in ("validation_level", "mode"):
        if request.query_params.get(name) is not None:
            payload[name] = request.query_params[name]
    for name, default in (("dry_run", False), ("fail_on_error", True)):
        if request.query_params.get(name) is not None:
            payload[name] = _query_bool(request.query_params.get(name), default)
    return payload


@router.post("/import")
async def import_fhir_resources(request: Request):
    """Bounded import endpoint for raw FHIR JSON, Bundle, NDJSON, or an envelope."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        payload = await _decode_import_request(request)
        result = await runtime.dispatch(
            env_id,
            "op",
            {"domain": FHIR_DOMAIN, "op": "fhir_import_resources", "payload": payload},
        )
        status = 200 if result.get("ok") or result.get("committed") else 422
        return JSONResponse(status_code=status, content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/migration/runs")
async def create_fhir_migration_run(
    request: Request, payload: dict[str, Any] = Body(default_factory=dict)
):
    """Create a resumable tenant-scoped migration run without storing source data."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {"domain": FHIR_DOMAIN, "op": "fhir_migration_start", "payload": payload},
        )
        return JSONResponse(status_code=201, content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/migration/runs")
async def list_fhir_migration_runs(request: Request):
    """List recent migration checkpoints and bounded reports for this environment."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        payload: dict[str, Any] = {}
        if request.query_params.get("limit"):
            try:
                payload["limit"] = int(request.query_params["limit"])
            except ValueError as exc:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="limit must be an integer",
                ) from exc
        if request.query_params.get("status"):
            payload["status"] = request.query_params["status"]
        result = await runtime.dispatch(
            env_id,
            "op",
            {"domain": FHIR_DOMAIN, "op": "fhir_migration_list", "payload": payload},
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.get("/migration/runs/{run_id}")
async def get_fhir_migration_run(request: Request, run_id: str):
    """Read one migration run and its ordered chunk reports."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_migration_get",
                "payload": {"run_id": run_id, "include_chunks": True},
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/migration/runs/{run_id}/chunks/{chunk_index}")
async def import_fhir_migration_chunk(request: Request, run_id: str, chunk_index: int):
    """Import one bounded chunk; exact retries return the stored report."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        payload = await _decode_import_request(request)
        payload.update(
            {
                "run_id": run_id,
                "chunk_index": chunk_index,
                "final": _query_bool(request.query_params.get("final"), False),
            }
        )
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_migration_import_chunk",
                "payload": payload,
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/migration/runs/{run_id}/cancel")
async def cancel_fhir_migration_run(request: Request, run_id: str):
    """Request cooperative cancellation of a migration run."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_migration_cancel",
                "payload": {"run_id": run_id},
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/migration/runs/{run_id}/reference-integrity")
async def inspect_fhir_migration_references(
    request: Request, run_id: str, payload: dict[str, Any] = Body(default_factory=dict)
):
    """Run an informational relative-reference integrity report for one import."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_reference_integrity",
                "payload": {**payload, "run_id": run_id},
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/semantic/preview")
async def preview_fhir_semantic_projection(request: Request):
    """Preview configured semantic text/chunks without embedding or persistence."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        try:
            payload = json.loads(await _read_bounded_body(request) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=f"Invalid semantic preview JSON: {exc}",
            ) from exc
        if not isinstance(payload, dict):
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Semantic preview payload must be a JSON object",
            )
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_semantic_preview",
                "payload": payload,
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/semantic/materialize")
async def materialize_fhir_semantic_projection(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Explicitly embed selected FHIR resources into the configured sidecar."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_semantic_materialize",
                "payload": payload,
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/semantic/search")
async def search_fhir_semantic_projection(
    request: Request, payload: dict[str, Any] = Body(...)
):
    """Run an explicit Atlas Vector Search over semantic sidecars."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        result = await runtime.dispatch(
            env_id,
            "op",
            {
                "domain": FHIR_DOMAIN,
                "op": "fhir_semantic_search",
                "payload": payload,
            },
        )
        return JSONResponse(content=_json_safe(result))
    except Exception as exc:
        return _operation_outcome_response(exc)


def _write_validation_failure(report: dict[str, Any]) -> JSONResponse:
    findings = (report.get("validation") or {}).get("findings") or []
    first = next((item for item in findings if item.get("severity") == "error"), None)
    message = (
        (first or {}).get("message")
        or report.get("message")
        or "FHIR resource validation failed"
    )
    return _fhir_json(
        operation_outcome(code="invalid", message=str(message)), status_code=422
    )


async def _write_one(
    request: Request,
    *,
    resource_type: str,
    resource_id: str | None,
    create: bool,
) -> JSONResponse:
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        try:
            resource = json.loads(await _read_bounded_body(request))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=f"Invalid FHIR JSON body: {exc}",
            ) from exc
        if not isinstance(resource, dict):
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="FHIR resource must be a JSON object",
            )
        body_type = resource.get("resourceType")
        if body_type != resource_type:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=f"Body resourceType {body_type!r} does not match URL type {resource_type!r}",
            )
        if create:
            resource.setdefault("id", str(uuid.uuid4()))
        elif resource.get("id") not in {None, resource_id}:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Body id must match the URL id",
            )
        if resource_id:
            resource["id"] = resource_id
        payload = {
            "resource": resource,
            "mode": "create" if create else "upsert",
            "fail_on_error": True,
        }
        result = await runtime.dispatch(
            env_id,
            "op",
            {"domain": FHIR_DOMAIN, "op": "fhir_import_resources", "payload": payload},
        )
        if not result.get("committed"):
            return _write_validation_failure(result)
        status = 201 if create or (result.get("write") or {}).get("inserted") else 200
        response = _fhir_json(_json_safe(resource), status_code=status)
        response.headers["Location"] = (
            str(request.base_url).rstrip("/")
            + f"/api/domains/fhir/{resource_type}/{resource['id']}"
        )
        return response
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/{resource_type}")
async def create_fhir_resource(request: Request, resource_type: str):
    """FHIR create interaction (server-assigned id when absent)."""
    return await _write_one(
        request, resource_type=resource_type, resource_id=None, create=True
    )


@router.put("/{resource_type}/{resource_id}")
async def update_fhir_resource(request: Request, resource_type: str, resource_id: str):
    """FHIR update/upsert interaction. Version history and If-Match are not yet supported."""
    return await _write_one(
        request, resource_type=resource_type, resource_id=resource_id, create=False
    )


@router.get("/{resource_type}")
async def search_fhir_get(request: Request, resource_type: str):
    """FHIR type-level search, for example ``GET /Patient?gender=female``."""
    query_items = [
        (name, value)
        for name, value in request.query_params.multi_items()
        if name not in {"env_id", "environment"}
    ]
    query_string = urlencode(query_items, doseq=True)
    expression = resource_type + (f"?{query_string}" if query_string else "")
    # Type-level search is addressable, so its Bundle can carry paging links.
    # POST /search is RPC-style and deliberately gets none.
    request.state.fhir_self_url = str(request.url)
    return await search_fhir(
        request,
        FhirSearchRequest(resource_type=resource_type, fhir_search=expression),
    )


@router.get("/{compartment_type}/{compartment_id}/{resource_type}")
async def search_fhir_compartment_get(
    request: Request,
    compartment_type: str,
    compartment_id: str,
    resource_type: str,
):
    """FHIR compartment search, e.g. ``GET /Patient/p-1/Observation``."""

    query_items = [
        (name, value)
        for name, value in request.query_params.multi_items()
        if name not in {"env_id", "environment"}
    ]
    query_string = urlencode(query_items, doseq=True)
    expression = "/".join(
        quote(value, safe="")
        for value in (compartment_type, compartment_id, resource_type)
    )
    if query_string:
        expression += f"?{query_string}"
    request.state.fhir_self_url = str(request.url)
    return await search_fhir(
        request,
        FhirSearchRequest(resource_type=resource_type, fhir_search=expression),
    )


@router.get("/{resource_type}/{resource_id}")
async def read_fhir_resource(request: Request, resource_type: str, resource_id: str):
    """FHIR instance read backed by the strategy's fail-closed ``_id`` search."""
    response = await search_fhir(
        request,
        FhirSearchRequest(
            resource_type=resource_type,
            fhir_search=f"{resource_type}?_id={quote(resource_id, safe='')}&_count=1",
            limit=1,
        ),
    )
    if response.status_code != 200:
        return response

    bundle = json.loads(response.body)
    entries = bundle.get("entry") or []
    match = next(
        (
            entry.get("resource")
            for entry in entries
            if isinstance(entry, dict)
            and (entry.get("search") or {}).get("mode") == "match"
            and isinstance(entry.get("resource"), dict)
        ),
        None,
    )
    if match is None:
        return _fhir_json(
            operation_outcome(
                code="not-found",
                message=f"{resource_type}/{resource_id} was not found",
            ),
            status_code=404,
        )
    return _fhir_json(_json_safe(match))
