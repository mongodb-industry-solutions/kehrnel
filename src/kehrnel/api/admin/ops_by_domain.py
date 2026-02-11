from fastapi import APIRouter, Body, Request
from typing import Any, Dict

from kehrnel.api.admin.routes import _error_response

router = APIRouter()


@router.post("/environments/{env_id}/activations/{domain}/ops/{op}", include_in_schema=False)
async def run_op_by_domain(env_id: str, domain: str, op: str, request: Request, payload: Dict[str, Any] = Body(default_factory=dict)):
    try:
        rt = getattr(request.app.state, "strategy_runtime", None)
        if not rt:
            raise ValueError("Strategy runtime not initialized")
        activation = rt.registry.get_activation(env_id, domain.lower())
        if not activation:
            raise KeyError(f"No activation for env {env_id} domain {domain}")
        result = await rt.dispatch(env_id, "op", {"op": op, "payload": payload or {}, "domain": domain})
        return {"ok": True, "result": result}
    except Exception as exc:
        return _error_response(exc)
