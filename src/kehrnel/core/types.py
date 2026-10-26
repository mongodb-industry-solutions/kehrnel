"""Backward-compatible wrapper for ``kehrnel.engine.core.types``."""

from kehrnel.engine.core.types import (
    ApplyPlan,
    ApplyResult,
    QueryPlan,
    QueryResult,
    StrategyContext,
    TransformResult,
)

__all__ = [
    "StrategyContext",
    "QueryPlan",
    "QueryResult",
    "ApplyPlan",
    "ApplyResult",
    "TransformResult",
]

