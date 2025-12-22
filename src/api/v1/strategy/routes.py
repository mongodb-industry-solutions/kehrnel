from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Body
from pydantic import BaseModel
from typing import Any, Dict, Optional

from strategy_runtime.bindings import build_bindings

router = APIRouter()


class ActivateRequest(BaseModel):
    config: Dict[str, Any] = {}
    strategy_id: Optional[str] = None
    tenant: Optional[str] = None
    environment: Optional[str] = None


@router.get("/strategies", summary="List registered strategy manifests and active states")
def list_strategies(request: Request):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        return {"manifests": [], "active": {}}
    manifests = [m.model_dump() for m in rt.registry.list_manifests()]
    active = {}
    for sid, active_s in rt.registry.get_active(rt.environment, rt.tenant).items():
        active[sid] = {
            "activation_id": active_s.activation.activation_id,
            "version": active_s.activation.version,
            "config": active_s.activation.config,
        }
    return {"manifests": manifests, "active": active}


@router.post("/strategies/activate", summary="Activate a strategy for this environment (in-memory/file-backed)")
def activate_strategy(req: ActivateRequest, request: Request):
    rt = getattr(request.app.state, "strategy_runtime", None)
    if not rt:
        raise HTTPException(status_code=503, detail="Strategy runtime not initialized")

    strategy_id = req.strategy_id or None
    if not strategy_id:
        raise HTTPException(status_code=400, detail="strategy_id is required")

    env = req.environment or rt.environment
    tenant = req.tenant or rt.tenant

    flattener = getattr(request.app.state, "flattener", None)
    default_bindings = getattr(rt, "default_bindings", None)
    if default_bindings is None:
        from strategy_sdk import StrategyBindings
        default_bindings = StrategyBindings(
            storage=getattr(rt, "default_storage", None),
            extras={"flattener": flattener} if flattener else {},
        )

    bindings = build_bindings(default_bindings, req.config or {})
    try:
        record = rt.registry.activate(
            strategy_id=strategy_id,
            config=req.config or {},
            bindings=bindings,
            environment=env,
            tenant=tenant,
        )
        # Persist default bindings on runtime for reuse
        rt.default_bindings = bindings
        return {"activation_id": record.activation_id, "environment": env, "tenant": tenant}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
