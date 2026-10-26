"""Public SDK imports for the types used by the Kehrnel runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence

from kehrnel.engine.core.types import (
    ApplyPlan,
    ApplyResult,
    QueryPlan,
    QueryResult,
    StrategyContext,
    TransformResult,
)


@dataclass
class StrategyBindings:
    """Adapters and connection details resolved by Kehrnel at activation time."""

    storage: Optional[Any] = None
    artifact: Optional[Any] = None
    search: Optional[Any] = None
    vector: Optional[Any] = None
    queue: Optional[Any] = None
    validation: Optional[Any] = None
    embedding: Optional[Any] = None
    db: Optional[Any] = None
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyInitResult:
    config_applied: Dict[str, Any]
    warnings: Sequence[str] = field(default_factory=list)
    notes: Sequence[str] = field(default_factory=list)
__all__ = [
    "ApplyPlan",
    "ApplyResult",
    "QueryPlan",
    "QueryResult",
    "StrategyBindings",
    "StrategyContext",
    "StrategyInitResult",
    "TransformResult",
]
