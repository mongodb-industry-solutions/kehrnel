from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence
from abc import ABC, abstractmethod

from .manifest import StrategyManifest


@dataclass
class StrategyBindings:
    """Adapter instances provided by the kernel at activation time."""

    storage: Optional[Any] = None
    search: Optional[Any] = None
    vector: Optional[Any] = None
    queue: Optional[Any] = None
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyContext:
    """Execution context (multi-tenant/environment aware)."""

    environment: str
    tenant: Optional[str] = None
    activation_id: Optional[str] = None
    strategy_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyInitResult:
    config_applied: Dict[str, Any]
    warnings: Sequence[str] = field(default_factory=list)
    notes: Sequence[str] = field(default_factory=list)


class StrategyPlugin(ABC):
    """
    Base class for all strategy plugins. Implement only the capabilities you advertise.
    Each hook receives bindings + context so the plugin does not need global state.
    """

    manifest: StrategyManifest

    def __init__(self, manifest: StrategyManifest):
        self.manifest = manifest

    # ─── Lifecycle ──────────────────────────────────────────────────────────
    def validate_config(self, config: Dict[str, Any]) -> None:
        """Optional: raise ValueError with details if config is invalid."""
        return None

    def initialize(
        self,
        config: Dict[str, Any],
        bindings: StrategyBindings,
        context: StrategyContext,
    ) -> StrategyInitResult:
        """
        Optional hook invoked when the strategy is activated.
        Return details about applied config for auditing.
        """
        return StrategyInitResult(config_applied=config)

    def shutdown(self) -> None:
        """Optional cleanup hook."""
        return None

    # ─── Capabilities (override as needed) ──────────────────────────────────
    def ingest(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        raise NotImplementedError("ingest not implemented for this strategy")

    def transform(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        raise NotImplementedError("transform not implemented for this strategy")

    def validate(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        raise NotImplementedError("validate not implemented for this strategy")

    def query(self, query_ast: Any, bindings: StrategyBindings, context: StrategyContext) -> Any:
        raise NotImplementedError("query not implemented for this strategy")

    def search(self, query: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        raise NotImplementedError("search not implemented for this strategy")

    def enrich(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        raise NotImplementedError("enrich not implemented for this strategy")

    def embed(self, payload: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        raise NotImplementedError("embed not implemented for this strategy")

    def generate(self, params: Dict[str, Any], bindings: StrategyBindings, context: StrategyContext) -> Any:
        raise NotImplementedError("generate not implemented for this strategy")
