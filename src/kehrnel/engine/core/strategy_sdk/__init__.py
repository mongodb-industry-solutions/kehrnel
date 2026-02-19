"""
Strategy SDK scaffolding for the multi-strategy kehrnel runtime.

This package introduces:
- Capability enums and core types
- Strategy manifest model (JSON-schema friendly)
- Plugin base class with lifecycle hooks
- Loader utilities to import strategies from entrypoints or dotted paths
"""

from .capabilities import StrategyCapability
from .manifest import (
    AdapterRequirements,
    StrategyManifest,
    StrategyUI,
    StrategyCompatibility,
)
from .plugin import (
    StrategyPlugin,
    StrategyContext,
    StrategyBindings,
    StrategyInitResult,
)
from .loader import load_strategy_from_entrypoint, load_strategy_from_path, discover_entrypoint_manifests
from .runtime import StrategyHandle, StrategyRuntimeError

__all__ = [
    "StrategyCapability",
    "AdapterRequirements",
    "StrategyManifest",
    "StrategyUI",
    "StrategyCompatibility",
    "StrategyPlugin",
    "StrategyContext",
    "StrategyBindings",
    "StrategyInitResult",
    "StrategyHandle",
    "StrategyRuntimeError",
    "load_strategy_from_entrypoint",
    "load_strategy_from_path",
    "discover_entrypoint_manifests",
]
