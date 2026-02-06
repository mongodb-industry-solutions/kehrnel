"""Portal-facing API surface for strategy operations."""
from __future__ import annotations

from typing import Any, Dict
from fastapi import APIRouter, Body, Request
from kehrnel.core.errors import KehrnelError

router = APIRouter(tags=["portal"])


def _runtime(request: Request):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise ValueError("Strategy runtime not initialized")
    return rt


def _wrap(exc: Exception):
    code = "INTERNAL_ERROR"
    status = 500
    message = str(exc)
    details: Dict[str, Any] = {}
    if isinstance(exc, KehrnelError):
        code = exc.code
        status = exc.status
        message = str(exc)
        details = getattr(exc, "details", {}) or {}
    elif isinstance(exc, ValueError):
        code = "INVALID_INPUT"
        status = 400
    elif isinstance(exc, KeyError):
        code = "NOT_FOUND"
        status = 404
    return {"ok": False, "error": {"code": code, "message": message, "details": details}}, status


@router.post("/v1/portal/environments/{env_id}/transform", summary="Transform a single payload")
async def portal_transform(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = _runtime(request)
        res = await rt.dispatch(env_id, "transform", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:  # pragma: no cover - passthrough to portal
        body, status = _wrap(exc)
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=status, content=body)


@router.post("/v1/portal/environments/{env_id}/ingest", summary="Transform and persist a single payload")
async def portal_ingest(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = _runtime(request)
        res = await rt.dispatch(env_id, "ingest", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:  # pragma: no cover
        body, status = _wrap(exc)
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=status, content=body)


@router.post("/v1/portal/environments/{env_id}/compile_query", summary="Compile a query (e.g., AQL) into engine pipeline")
async def portal_compile_query(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = _runtime(request)
        if not payload.get("domain"):
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")
        res = await rt.dispatch(env_id, "compile_query", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:  # pragma: no cover
        body, status = _wrap(exc)
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=status, content=body)


@router.post("/v1/portal/environments/{env_id}/query", summary="Compile and execute a query")
async def portal_query(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = _runtime(request)
        if not payload.get("domain"):
            raise KehrnelError(code="DOMAIN_REQUIRED", status=400, message="domain is required")
        res = await rt.dispatch(env_id, "query", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:  # pragma: no cover
        body, status = _wrap(exc)
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=status, content=body)


@router.post("/v1/portal/environments/{env_id}/reverse_transform", summary="Reverse a flattened base document to composition")
async def portal_reverse_transform(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = _runtime(request)
        res = await rt.dispatch(env_id, "reverse_transform", payload or {})
        return {"ok": True, "result": res}
    except Exception as exc:  # pragma: no cover
        body, status = _wrap(exc)
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=status, content=body)
