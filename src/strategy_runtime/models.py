from __future__ import annotations

from typing import Any, Dict, Optional
from dataclasses import dataclass, field

from strategy_sdk import StrategyManifest


@dataclass(frozen=True)
class EnvironmentKey:
    environment: str
    tenant: Optional[str] = None


@dataclass
class ActivationRecord:
    """Represents a strategy activation for an environment/tenant."""

    activation_id: str
    strategy_id: str
    version: str
    environment: str
    tenant: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    bindings_meta: Dict[str, Any] = field(default_factory=dict)
    notes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ActiveStrategy:
    manifest: StrategyManifest
    activation: ActivationRecord
    handle: Any
