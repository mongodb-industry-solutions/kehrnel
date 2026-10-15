"""Tenant-facing terminology gateway and configuration routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request

from kehrnel.api.bridge.app.core.database import _default_env_id, _extract_env_id, _is_env_access_allowed
from kehrnel.api.core.admin.routes import _error_response, _json_safe, _require_admin_access
from kehrnel.api.terminology.models import (
    TerminologyBatchValidateRequest,
    TerminologyExpandRequest,
    TerminologyLookupRequest,
    TerminologySearchRequest,
    TerminologySubsumesRequest,
    TerminologyTranslateRequest,
    TerminologyValidateCodeRequest,
)
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.terminology import TerminologyService, normalize_terminology_config, redact_terminology_config

router = APIRouter(prefix="/api/terminology", tags=["Terminology"])


def _context(request: Request) -> tuple[Any, str]:
    env_id = _extract_env_id(request) or _default_env_id()
    if not env_id:
        raise HTTPException(status_code=400, detail="Missing active environment. Provide x-active-env (or env_id query param).")
    if not _is_env_access_allowed(request, env_id):
        raise HTTPException(status_code=403, detail=f"Access to env_id={env_id} is not permitted for this API key.")
    runtime = getattr(request.app.state, "strategy_runtime", None)
    if runtime is None:
        raise KehrnelError(code="RUNTIME_UNAVAILABLE", status=503, message="Strategy runtime is unavailable.")
    return runtime, env_id


async def _execute(request: Request, operation: str, payload: dict[str, Any]):
    runtime, env_id = _context(request)
    return _json_safe(await TerminologyService(runtime, env_id).execute(operation, payload))


@router.get("/capabilities")
async def terminology_capabilities(request: Request, include_readiness: bool = False):
    try:
        runtime, env_id = _context(request)
        return _json_safe(await TerminologyService(runtime, env_id).capabilities(include_readiness=include_readiness))
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.get("/configuration")
async def get_terminology_configuration(request: Request):
    try:
        runtime, env_id = _context(request)
        service = TerminologyService(runtime, env_id)
        return {"ok": True, "environmentId": env_id, "configuration": redact_terminology_config(service.config)}
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.put("/configuration")
async def put_terminology_configuration(request: Request, body: dict[str, Any] = Body(...)):
    try:
        _require_admin_access(request)
        runtime, env_id = _context(request)
        environment = runtime.get_environment(env_id)
        if not environment:
            raise KehrnelError(code="ENVIRONMENT_NOT_FOUND", status=404, message=f"Environment {env_id} not found")
        raw_config = body.get("configuration") if isinstance(body.get("configuration"), dict) else body
        config = normalize_terminology_config(raw_config)
        metadata = dict(environment.metadata or {})
        metadata["terminology"] = config
        runtime.upsert_environment(
            env_id,
            name=environment.name,
            description=environment.description,
            metadata=metadata,
            bindings_ref=environment.bindings_ref,
        )
        return {"ok": True, "environmentId": env_id, "configuration": redact_terminology_config(config)}
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/lookup")
async def terminology_lookup(request: Request, payload: TerminologyLookupRequest = Body(...)):
    try:
        return await _execute(request, "lookup", payload.model_dump(exclude_none=True))
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/validate-code")
async def terminology_validate_code(request: Request, payload: TerminologyValidateCodeRequest = Body(...)):
    try:
        return await _execute(request, "validate-code", payload.model_dump(exclude_none=True))
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/providers/{provider_id}/test")
async def test_terminology_provider(
    request: Request,
    provider_id: str,
    payload: TerminologyValidateCodeRequest = Body(...),
):
    """Execute a real validation against one explicit provider and return evidence."""
    try:
        result = await _execute(
            request,
            "validate-code",
            {**payload.model_dump(exclude_none=True), "provider_id": provider_id},
        )
        return {"ok": True, "healthy": True, "providerId": provider_id, "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.get("/providers/{provider_id}/capabilities")
async def inspect_terminology_provider_capabilities(request: Request, provider_id: str):
    """Read the native capability contract or an external FHIR metadata endpoint."""
    try:
        runtime, env_id = _context(request)
        return _json_safe(
            await TerminologyService(runtime, env_id).provider_capabilities(provider_id)
        )
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/validate-codes")
async def terminology_validate_codes(request: Request, payload: TerminologyBatchValidateRequest = Body(...)):
    """Validate a bounded set of codings through their tenant-routed providers."""
    try:
        runtime, env_id = _context(request)
        service = TerminologyService(runtime, env_id)
        results: list[dict[str, Any]] = []
        for index, coding in enumerate(payload.codings):
            item = coding.model_dump(exclude_none=True)
            path = item.pop("path", None)
            try:
                result = _json_safe(await service.execute("validate-code", item))
                results.append({"index": index, "path": path, **result})
            except KehrnelError as exc:
                results.append(
                    {
                        "index": index,
                        "path": path,
                        "ok": False,
                        "valid": False,
                        "system": item.get("system"),
                        "version": item.get("version"),
                        "code": item.get("code"),
                        "error": {
                            "code": exc.code,
                            "message": str(exc),
                            "details": exc.details,
                        },
                    }
                )
        valid = sum(1 for item in results if item.get("valid") is True)
        return {
            "ok": all(item.get("ok", True) and item.get("valid") is True for item in results),
            "environmentId": env_id,
            "summary": {"total": len(results), "valid": valid, "invalid": len(results) - valid},
            "results": results,
        }
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/subsumes")
async def terminology_subsumes(request: Request, payload: TerminologySubsumesRequest = Body(...)):
    try:
        return await _execute(request, "subsumes", payload.model_dump(exclude_none=True))
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/expand")
async def terminology_expand(request: Request, payload: TerminologyExpandRequest = Body(...)):
    try:
        return await _execute(request, "expand", payload.model_dump(exclude_none=True))
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/translate")
async def terminology_translate(request: Request, payload: TerminologyTranslateRequest = Body(...)):
    try:
        return await _execute(request, "translate", payload.model_dump(exclude_none=True))
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)


@router.post("/search")
async def terminology_search(request: Request, payload: TerminologySearchRequest = Body(...)):
    try:
        return await _execute(request, "search", payload.model_dump(exclude_none=True))
    except HTTPException:
        raise
    except Exception as exc:
        return _error_response(exc)
