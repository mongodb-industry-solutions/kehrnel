"""
Backward-compatible import surface.

Historically, runtime components lived under ``kehrnel.core``.
They now live under ``kehrnel.engine.core``.
"""

from kehrnel.engine.core.bundle_store import BundleStore
from kehrnel.engine.core.bundles import compute_bundle_digest, validate_bundle
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.pack_validator import StrategyPackValidator
from kehrnel.engine.core.registry import ActivationRegistry, FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.engine.core.types import (
    ApplyPlan,
    ApplyResult,
    QueryPlan,
    QueryResult,
    StrategyContext,
    TransformResult,
)

__all__ = [
    "ActivationRegistry",
    "FileActivationRegistry",
    "StrategyRuntime",
    "StrategyManifest",
    "StrategyPackValidator",
    "KehrnelError",
    "StrategyContext",
    "QueryPlan",
    "QueryResult",
    "ApplyPlan",
    "ApplyResult",
    "TransformResult",
    "BundleStore",
    "validate_bundle",
    "compute_bundle_digest",
    "load_strategy",
]

