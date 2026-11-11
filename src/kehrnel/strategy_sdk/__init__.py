"""Public Python authoring surface for executable Kehrnel strategy packs."""

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
from .types import ApplyPlan, ApplyResult, QueryPlan, QueryResult, TransformResult
from .loader import load_strategy_from_entrypoint, load_strategy_from_path, discover_entrypoint_manifests
from .runtime import StrategyHandle, StrategyRuntimeError
from .conformance import (
    StrategyConformanceDiagnostic,
    StrategyConformanceError,
    StrategyConformanceReport,
    require_strategy_conformance,
    validate_strategy_entrypoint,
    validate_strategy_plugin,
)

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
    "ApplyPlan",
    "ApplyResult",
    "QueryPlan",
    "QueryResult",
    "TransformResult",
    "StrategyConformanceDiagnostic",
    "StrategyConformanceError",
    "StrategyConformanceReport",
    "require_strategy_conformance",
    "validate_strategy_entrypoint",
    "validate_strategy_plugin",
    "load_strategy_from_entrypoint",
    "load_strategy_from_path",
    "discover_entrypoint_manifests",
]
