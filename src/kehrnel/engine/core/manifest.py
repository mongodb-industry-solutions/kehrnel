"""Compatibility imports for the canonical public manifest contract.

New code should import from :mod:`kehrnel.contracts`. This module remains so
existing runtime and strategy-pack imports keep working unchanged.
"""

from kehrnel.contracts.manifest import (
    AdapterRequirements,
    InteractionContract,
    InteractionMode,
    StrategyCompatibility,
    StrategyManifest,
    StrategyManifestSource,
    StrategyOp,
    StrategyStatus,
    StrategyUI,
    read_strategy_manifest,
)

__all__ = [
    "AdapterRequirements",
    "InteractionContract",
    "InteractionMode",
    "StrategyCompatibility",
    "StrategyManifest",
    "StrategyManifestSource",
    "StrategyOp",
    "StrategyStatus",
    "StrategyUI",
    "read_strategy_manifest",
]
