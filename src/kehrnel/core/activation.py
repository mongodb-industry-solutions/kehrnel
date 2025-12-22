"""EnvironmentActivation scaffold."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class EnvironmentActivation:
    """
    Represents a strategy activation for an environment (no tenancy at this layer).
    """
    env_id: str
    strategy_id: str
    version: str
    config: Dict[str, Any] = field(default_factory=dict)
    bindings_meta: Dict[str, Any] = field(default_factory=dict)
    activation_id: str | None = None
    bindings: Optional[Dict[str, Any]] = None
    bindings_ref: Optional[str] = None
