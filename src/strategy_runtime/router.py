from __future__ import annotations

from typing import Any, Dict, Optional

from .models import ActiveStrategy
from .selector import pick_by_capability_and_protocol
from strategy_sdk import StrategyRuntimeError


class CapabilityRouter:
    """
    Simple capability-based router over active strategies for an environment/tenant.
    Strategy selection policy is basic (first matching); can be extended to protocol-aware routing.
    """

    def __init__(self, active: Dict[str, ActiveStrategy]):
        self.active = active

    def route(
        self,
        capability: str,
        strategy_id: Optional[str] = None,
        protocol: Optional[str] = None,
    ) -> ActiveStrategy:
        if strategy_id:
            if strategy_id not in self.active:
                raise StrategyRuntimeError(f"Strategy '{strategy_id}' not active")
            return self.active[strategy_id]
        # fallback: first active strategy declaring capability
        selected = pick_by_capability_and_protocol(self.active.values(), capability, protocol)
        if selected:
            return selected
        raise StrategyRuntimeError(f"No active strategy implements capability '{capability}'")

    def dispatch(
        self,
        capability: str,
        payload: Any,
        *,
        strategy_id: Optional[str] = None,
        protocol: Optional[str] = None,
        bindings=None,
        context=None,
        **kwargs,
    ) -> Any:
        target = self.route(capability, strategy_id, protocol)
        handle = target.handle
        return handle.dispatch(capability, payload, bindings=bindings, context=context, **kwargs)
