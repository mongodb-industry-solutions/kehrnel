"""
Lightweight runtime utilities for loading, activating, and routing strategy plugins.

This is intentionally minimal and in-memory for the first refactor slice; persistence
for activations/registry will be added later alongside the portal integration.
"""

from .models import ActivationRecord, ActiveStrategy, EnvironmentKey
from .registry import StrategyRegistry
from .router import CapabilityRouter

__all__ = [
    "ActivationRecord",
    "ActiveStrategy",
    "EnvironmentKey",
    "StrategyRegistry",
    "CapabilityRouter",
]
