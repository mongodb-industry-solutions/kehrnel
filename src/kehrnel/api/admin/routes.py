"""
Admin/runtime API for strategy discovery and environment operations.
"""
from fastapi import APIRouter, HTTPException, Request, Body
from typing import Any, Dict, List

from kehrnel.core.manifest import StrategyManifest

router = APIRouter()


@router.get("/v1/strategies", response_model=Dict[str, List[StrategyManifest]])
async def list_strategies(request: Request):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        return {"strategies": []}
    manifests = [m for m in rt.list_strategies()]
    return {"strategies": manifests}


@router.get("/v1/strategies/{strategy_id}", response_model=StrategyManifest)
async def get_strategy(strategy_id: str, request: Request):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    manifest = rt.registry.get_manifest(strategy_id)
    if not manifest:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return manifest


@router.post("/v1/environments/{env_id}/extensions/{strategy_id}/{op}", include_in_schema=False)
async def run_extension(env_id: str, strategy_id: str, op: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    activation = rt.registry.get_activation(env_id)
    if not activation:
        raise HTTPException(status_code=404, detail=f"No activation for env {env_id}")
    if activation.strategy_id != strategy_id:
        raise HTTPException(status_code=409, detail=f"Environment {env_id} active with {activation.strategy_id}, not {strategy_id}")
    result = await rt.dispatch(env_id, "op", {"op": op, "payload": payload or {}})
    return {"ok": True, "result": result}


@router.post("/v1/environments/{env_id}/activate", include_in_schema=False)
async def activate_env(env_id: str, request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    strategy_id = body.get("strategy_id")
    version = body.get("version") or "latest"
    config = body.get("config") or {}
    bindings = body.get("bindings") or {}
    allow_plain = body.get("allow_plaintext_bindings", False)
    if not strategy_id:
        raise HTTPException(status_code=400, detail="strategy_id is required")
    try:
        from strategy_sdk import StrategyBindings
        activation = await rt.activate(env_id, strategy_id, version, config, StrategyBindings(**bindings), allow_plaintext_bindings=allow_plain)
        return {"ok": True, "activation": {"env_id": activation.env_id, "strategy_id": activation.strategy_id, "version": activation.version, "config": activation.config, "bindings_meta": activation.bindings_meta}}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/v1/environments/{env_id}", include_in_schema=False)
async def get_env(env_id: str, request: Request):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    activation = rt.registry.get_activation(env_id)
    if not activation:
        raise HTTPException(status_code=404, detail="No activation for env")
    return {"env_id": activation.env_id, "strategy_id": activation.strategy_id, "version": activation.version, "config": activation.config, "bindings_meta": activation.bindings_meta}


@router.post("/v1/environments/{env_id}/plan", include_in_schema=False)
async def plan_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    res = await rt.dispatch(env_id, "plan", payload or {})
    return {"ok": True, "result": res}


@router.post("/v1/environments/{env_id}/apply", include_in_schema=False)
async def apply_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    res = await rt.dispatch(env_id, "apply", payload or {})
    return {"ok": True, "result": res}


@router.post("/v1/environments/{env_id}/transform", include_in_schema=False)
async def transform_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    res = await rt.dispatch(env_id, "transform", payload or {})
    return {"ok": True, "result": res}


@router.post("/v1/environments/{env_id}/ingest", include_in_schema=False)
async def ingest_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    res = await rt.dispatch(env_id, "ingest", payload or {})
    return {"ok": True, "result": res}


@router.post("/v1/environments/{env_id}/query", include_in_schema=False)
async def query_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    res = await rt.dispatch(env_id, "query", payload or {})
    return {"ok": True, "result": res}


@router.post("/v1/environments/{env_id}/compile_query", include_in_schema=False)
async def compile_query_env(env_id: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict), debug: bool = False):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    if debug and isinstance(payload, dict):
        payload["debug"] = True
    res = await rt.dispatch(env_id, "compile_query", payload or {})
    return {"ok": True, "result": res}
