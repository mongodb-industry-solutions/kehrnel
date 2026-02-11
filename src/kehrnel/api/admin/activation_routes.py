from typing import Any, Dict

from fastapi import APIRouter, Body, Request

from kehrnel.api.admin.routes import _error_response
from kehrnel.core.errors import KehrnelError

router = APIRouter()


@router.post("/environments/{env_id}/activations", include_in_schema=False)
async def create_activation(env_id: str, request: Request, body: Dict[str, Any] = Body(default_factory=dict)):
    try:
        # Reuse existing activation handler
        from kehrnel.api.admin.routes import activate_env
        # normalize camelCase keys
        if body.get("strategyId") and not body.get("strategy_id"):
            body["strategy_id"] = body["strategyId"]
        if body.get("allowPlaintextBindings") and not body.get("allow_plaintext_bindings"):
            body["allow_plaintext_bindings"] = body["allowPlaintextBindings"]
        return await activate_env(env_id, request, body)
    except Exception as exc:
        return _error_response(exc)
