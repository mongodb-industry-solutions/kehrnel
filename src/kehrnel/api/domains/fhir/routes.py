"""FHIR domain search routes (strategy-backed)."""

from __future__ import annotations

import os
import json
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import JSONResponse

from kehrnel.api.bridge.app.core.database import (
    _default_env_id,
    _extract_env_id,
    _get_activation,
    _is_env_access_allowed,
)
from kehrnel.api.core.admin.routes import _json_safe
from kehrnel.api.domains.fhir.models import FhirSearchRequest
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import (
    operation_outcome,
    operation_outcome_from_error,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.storage_adapter import (
    MongoFHIRStorageAdapter,
)

# Serialization seam used by the FHIR REST boundary. db is not needed for the
# serialize path (canonical mapping is pure); read/persist use a db-bound adapter.
_SERIALIZER = MongoFHIRStorageAdapter(db=None)


FHIR_JSON_MEDIA_TYPE = "application/fhir+json"
FHIR_IMPORT_MAX_BYTES = int(os.getenv("KEHRNEL_FHIR_IMPORT_MAX_BYTES", str(25 * 1024 * 1024)))


def _fhir_json(content: dict, status_code: int = 200) -> JSONResponse:
    """Return FHIR-typed JSON (application/fhir+json)."""
    return JSONResponse(status_code=status_code, content=content, media_type=FHIR_JSON_MEDIA_TYPE)


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
        return _fhir_json(operation_outcome_from_error(exc), status_code=int(getattr(exc, "status", 500) or 500))
    # Unexpected error — do not leak internals.
    return _fhir_json(
        operation_outcome(code="PROCESSING_ERROR", message="Internal error processing FHIR request"),
        status_code=500,
    )

router = APIRouter(prefix="/api/domains/fhir", tags=["FHIR"])

FHIR_DOMAIN = "fhir"
DEFAULT_STRATEGY_ID = os.getenv("KEHRNEL_FHIR_STRATEGY_ID", "fhir.clinical_cdr")


def _auth_enabled() -> bool:
    return os.getenv("KEHRNEL_AUTH_ENABLED", "false").lower() in ("1", "true", "yes")


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
    """Map API request to fhir.clinical_cdr compile_query input.

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


def build_search_bundle(
    *,
    rows: list[dict[str, Any]],
    total: int | None,
    env_id: str,
    explain: dict[str, Any] | None,
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
        entries.append({
            "search": {"mode": "outcome"},
            "resource": operation_outcome(
                severity="warning",
                code="not-supported",
                message=f"Ignored unsupported search parameter(s): {names}",
            ),
        })
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": bundle_total,
        "entry": entries,
    }


def _fhir_version_number(value: Any) -> str:
    """Map the configured release label to CapabilityStatement.fhirVersion."""
    normalized = str(value or "R5").strip().upper()
    return {
        "R5": "5.0.0",
        "R6": "6.0.0",
    }.get(normalized, str(value or "5.0.0"))


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

        resource_types = capabilities.get("searchable_resource_types") or []
        interactions = [{"code": "read"}, {"code": "search-type"}]
        if capabilities.get("write_supported"):
            interactions.extend([{"code": "create"}, {"code": "update"}])
        resources = [
            {
                "type": resource_type,
                "interaction": interactions,
                "versioning": "no-version",
                "conditionalRead": "not-supported",
                "conditionalCreate": False,
                "conditionalUpdate": False,
                "conditionalDelete": "not-supported",
            }
            for resource_type in resource_types
            if isinstance(resource_type, str) and resource_type
        ]
        statement = {
            "resourceType": "CapabilityStatement",
            "status": "active",
            "experimental": True,
            "date": datetime.now(timezone.utc).isoformat(),
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
            "rest": [{"mode": "server", "resource": resources}],
        }
        return _fhir_json(_json_safe(statement))
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/search")
async def search_fhir(request: Request, payload: FhirSearchRequest = Body(...)):
    """
    Execute FHIR search via the active environment's fhir.clinical_cdr strategy.

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

        explain = result.get("explain") if isinstance(result.get("explain"), dict) else {}
        explain.setdefault("strategy_id", getattr(activation, "strategy_id", None))
        explain.setdefault("activation_id", getattr(activation, "activation_id", None))

        total_raw = explain.get("total")
        total = int(total_raw) if total_raw is not None else None

        bundle = build_search_bundle(rows=rows, total=total, env_id=env_id, explain=explain)
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


@router.get("/capabilities")
async def get_fhir_runtime_capabilities(request: Request, resource_type: str | None = None):
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


@router.get("/stats")
async def get_fhir_store_stats(request: Request):
    """Return bounded storage, index, and provenance diagnostics for the active store."""
    try:
        env_id = resolve_active_env_id(request)
        runtime, _activation = _require_fhir_activation(request, env_id)
        summary_only = str(request.query_params.get("summary") or "").strip().lower() in {
            "1", "true", "yes", "on"
        }
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
    """List resource models owned by the active Clinical CDR package."""
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
    content_type = (request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    body = await _read_bounded_body(request)
    if content_type in {"application/fhir+ndjson", "application/ndjson", "application/x-ndjson"}:
        payload: dict[str, Any] = {"ndjson": body.decode("utf-8")}
    else:
        try:
            decoded = json.loads(body or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise KehrnelError(code="INVALID_INPUT", status=400, message=f"Invalid JSON import body: {exc}") from exc
        if not isinstance(decoded, dict):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="FHIR import body must be a JSON object")
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
    """Migration endpoint for raw Bundle JSON, NDJSON, or a JSON import envelope."""
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


def _write_validation_failure(report: dict[str, Any]) -> JSONResponse:
    findings = ((report.get("validation") or {}).get("findings") or [])
    first = next((item for item in findings if item.get("severity") == "error"), None)
    message = (first or {}).get("message") or report.get("message") or "FHIR resource validation failed"
    return _fhir_json(operation_outcome(code="invalid", message=str(message)), status_code=422)


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
            raise KehrnelError(code="INVALID_INPUT", status=400, message=f"Invalid FHIR JSON body: {exc}") from exc
        if not isinstance(resource, dict):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="FHIR resource must be a JSON object")
        body_type = resource.get("resourceType")
        if body_type != resource_type:
            raise KehrnelError(code="INVALID_INPUT", status=400, message=f"Body resourceType {body_type!r} does not match URL type {resource_type!r}")
        if create:
            resource.setdefault("id", str(uuid.uuid4()))
        elif resource.get("id") not in {None, resource_id}:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="Body id must match the URL id")
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
        response.headers["Location"] = str(request.base_url).rstrip("/") + f"/api/domains/fhir/{resource_type}/{resource['id']}"
        return response
    except Exception as exc:
        return _operation_outcome_response(exc)


@router.post("/{resource_type}")
async def create_fhir_resource(request: Request, resource_type: str):
    """FHIR create interaction (server-assigned id when absent)."""
    return await _write_one(request, resource_type=resource_type, resource_id=None, create=True)


@router.put("/{resource_type}/{resource_id}")
async def update_fhir_resource(request: Request, resource_type: str, resource_id: str):
    """FHIR update/upsert interaction. Version history and If-Match are not yet supported."""
    return await _write_one(request, resource_type=resource_type, resource_id=resource_id, create=False)


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
