from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import os

from strategy_runtime import StrategyRegistry, CapabilityRouter
from strategy_sdk import StrategyBindings, StrategyContext
from adapters.mongo_storage import MongoStorageAdapter
from src.app.core.config import settings
from strategy_runtime.bindings import build_bindings

from strategies.openehr.rps_dual import MANIFEST as OPENEHR_MANIFEST
from strategies.fhir.resource_first import MANIFEST as FHIR_MANIFEST
from strategies.fhir.simulated import MANIFEST as FHIR_SIM_MANIFEST


@dataclass
class StrategyRuntimeState:
    registry: StrategyRegistry
    environment: str
    tenant: Optional[str]

    def router(self) -> CapabilityRouter:
        active = self.registry.get_active(self.environment, self.tenant)
        return CapabilityRouter(active)

    def context(self) -> StrategyContext:
        return StrategyContext(environment=self.environment, tenant=self.tenant)


def _build_mongo_binding(app):
    """
    Build a Mongo storage adapter from the app's config/state when possible.
    Returns None if mandatory pieces are missing.
    """
    cfg = getattr(app.state, "config", None) or {}
    target = cfg.get("target", {}) if isinstance(cfg, dict) else {}
    conn = target.get("connection_string") or getattr(settings, "MONGODB_URI", None)
    db_name = target.get("database_name") or getattr(settings, "MONGODB_DB", None)
    comp_coll = (
        target.get("compositions_collection")
        or getattr(settings, "FLAT_COMPOSITIONS_COLL_NAME", None)
        or getattr(settings, "COMPOSITIONS_COLL_NAME", None)
    )
    search_coll = target.get("search_collection") or getattr(settings, "SEARCH_COMPOSITIONS_COLL_NAME", None)

    if not conn or not db_name or not comp_coll:
        return None

    storage_cfg = {
        "connection_string": conn,
        "database_name": db_name,
        "compositions_collection": comp_coll,
        "search_collection": search_coll or "compositions_search",
    }
    try:
        return MongoStorageAdapter.from_config(storage_cfg)
    except Exception as exc:
        print(f"Warning: could not build MongoStorageAdapter ({exc})")
        return None


def init_strategy_runtime(app, environment: str = "dev", tenant: Optional[str] = None):
    """
    Initialize an in-memory strategy registry and activate the built-in openEHR strategy.

    This is non-intrusive: if activation fails, the app continues without strategy routing.
    """
    registry_path = os.getenv("KEHRNEL_REGISTRY_PATH")
    registry = StrategyRegistry.from_file(registry_path) if registry_path else StrategyRegistry()
    # Register built-in manifests
    registry.register_manifest(OPENEHR_MANIFEST)
    registry.register_manifest(FHIR_MANIFEST)
    registry.register_manifest(FHIR_SIM_MANIFEST)

    # Build bindings: reuse the existing flattener if present; storage is optional
    flattener = getattr(app.state, "flattener", None)
    storage = _build_mongo_binding(app)
    bindings = StrategyBindings(
        extras={"flattener": flattener} if flattener else {},
        storage=storage,
    )
    try:
        # Activate openEHR by default if no persisted activations
        if registry.get_active(environment, tenant) == {}:
            registry.activate(
                strategy_id=OPENEHR_MANIFEST.id,
                config=getattr(app.state, "strategy_raw", {}) or OPENEHR_MANIFEST.default_config,
                bindings=bindings,
                environment=environment,
                tenant=tenant,
            )

        def _binding_factory(act, manifest):
            return build_bindings(bindings, act.config)

        registry.restore_activations(environment=environment, tenant=tenant, bindings_factory=_binding_factory)

        app.state.strategy_runtime = StrategyRuntimeState(
            registry=registry,
            environment=environment,
            tenant=tenant,
        )
        app.state.strategy_runtime.default_bindings = bindings
        print(f"Strategy runtime initialized for env={environment}")
    except Exception as exc:
        print(f"Warning: strategy runtime init skipped ({exc})")
        app.state.strategy_runtime = None
