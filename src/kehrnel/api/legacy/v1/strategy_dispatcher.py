from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Body
from pydantic import BaseModel
from typing import Any, Dict

from kehrnel.strategy_sdk import StrategyBindings

router = APIRouter()


class IngestRequest(BaseModel):
    payload: Dict[str, Any]


@router.post("/strategies/{strategy_id}/ingest", summary="Ingest via strategy capability")
async def ingest_strategy(strategy_id: str, request: Request, body: IngestRequest):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")

    strategy = rt.registry.get_manifest(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")

    bindings = StrategyBindings(
        storage=getattr(rt, "default_bindings", None).storage if getattr(rt, "default_bindings", None) else None,
        extras={"flattener": getattr(request.app.state, "flattener", None)},
    )
    ctx = rt.strategy_runtime.context() if hasattr(rt, "strategy_runtime") else None
    try:
        handle = rt.registry.get_active(rt.environment, rt.tenant).get(strategy_id)
        if not handle:
            # Auto-activate with default config if not active
            handle = rt.registry.activate(
                strategy_id=strategy_id,
                config=strategy.default_config if hasattr(strategy, "default_config") else {},
                bindings=bindings,
                environment=rt.environment,
                tenant=rt.tenant,
            )
            handle = rt.registry.get_active(rt.environment, rt.tenant).get(strategy_id)
        if not handle:
            raise HTTPException(status_code=500, detail="Strategy activation failed")
        return handle.handle.dispatch("ingest", body.payload, bindings=bindings, context=ctx)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/strategies/{strategy_id}/transform", summary="Transform via strategy capability")
async def transform_strategy(strategy_id: str, request: Request, body: IngestRequest):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")

    strategy = rt.registry.get_manifest(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")

    bindings = StrategyBindings(
        storage=getattr(rt, "default_bindings", None).storage if getattr(rt, "default_bindings", None) else None,
        extras={"flattener": getattr(request.app.state, "flattener", None)},
    )
    ctx = rt.strategy_runtime.context() if hasattr(rt, "strategy_runtime") else None
    try:
        active = rt.registry.get_active(rt.environment, rt.tenant).get(strategy_id)
        if not active:
            active = rt.registry.activate(
                strategy_id=strategy_id,
                config=strategy.default_config if hasattr(strategy, "default_config") else {},
                bindings=bindings,
                environment=rt.environment,
                tenant=rt.tenant,
            )
            active = rt.registry.get_active(rt.environment, rt.tenant).get(strategy_id)
        if not active:
            raise HTTPException(status_code=500, detail="Strategy activation failed")
        return active.handle.dispatch("transform", body.payload, bindings=bindings, context=ctx)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/strategies/{strategy_id}/search", summary="Search via strategy capability")
async def search_strategy(strategy_id: str, request: Request, body: Dict[str, Any]):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")

    strategy = rt.registry.get_manifest(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")

    bindings = StrategyBindings(
        storage=getattr(rt, "default_bindings", None).storage if getattr(rt, "default_bindings", None) else None,
        extras={"flattener": getattr(request.app.state, "flattener", None)},
        search=getattr(rt, "default_bindings", None).search if getattr(rt, "default_bindings", None) else None,
    )
    ctx = rt.strategy_runtime.context() if hasattr(rt, "strategy_runtime") else None
    try:
        active = rt.registry.get_active(rt.environment, rt.tenant).get(strategy_id)
        if not active:
            active = rt.registry.activate(
                strategy_id=strategy_id,
                config=strategy.default_config if hasattr(strategy, "default_config") else {},
                bindings=bindings,
                environment=rt.environment,
                tenant=rt.tenant,
            )
            active = rt.registry.get_active(rt.environment, rt.tenant).get(strategy_id)
        if not active:
            raise HTTPException(status_code=500, detail="Strategy activation failed")
        return active.handle.dispatch("search", body, bindings=bindings, context=ctx)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/strategies/{strategy_id}/config", summary="Get strategy config schema/defaults")
def get_strategy_config(strategy_id: str, request: Request):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    manifest = rt.registry.get_manifest(strategy_id)
    if not manifest:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")
    active = rt.registry.get_active(rt.environment, rt.tenant).get(strategy_id)
    return {
        "strategy_id": strategy_id,
        "schema": manifest.config_schema if hasattr(manifest, "config_schema") else None,
        "default_config": manifest.default_config if hasattr(manifest, "default_config") else {},
        "active_config": active.activation.config if active else None,
    }


@router.post("/strategies/{strategy_id}/config", summary="Validate/apply strategy config")
async def set_strategy_config(strategy_id: str, request: Request, body: Dict[str, Any]):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")
    manifest = rt.registry.get_manifest(strategy_id)
    if not manifest:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")

    config = body or {}
    bindings = getattr(rt, "default_bindings", None)
    try:
        record = rt.registry.activate(
            strategy_id=strategy_id,
            config=config,
            bindings=bindings,
            environment=rt.environment,
            tenant=rt.tenant,
        )
        return {"activation_id": record.activation_id, "config": config}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
