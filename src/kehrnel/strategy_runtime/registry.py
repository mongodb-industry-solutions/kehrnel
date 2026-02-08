from __future__ import annotations

import uuid
from pathlib import Path
from typing import Dict, Optional, Tuple

from kehrnel.strategy_sdk import (
    StrategyHandle,
    StrategyManifest,
    load_strategy_from_path,
    StrategyBindings,
    StrategyContext,
)
from .models import ActivationRecord, ActiveStrategy, EnvironmentKey
from .store import FileRegistryStore


class StrategyRegistry:
    """
    In-memory registry/activation manager.
    Future iterations should back this with a persistent store (DB/git) and expose APIs.
    """

    def __init__(self):
        self._manifests: Dict[str, StrategyManifest] = {}
        self._active: Dict[Tuple[str, Optional[str]], Dict[str, ActiveStrategy]] = {}
        self._store: FileRegistryStore | None = None

    @classmethod
    def from_file(cls, path) -> "StrategyRegistry":
        reg = cls()
        reg._store = FileRegistryStore(Path(path))
        # preload manifests
        for _, manifest in reg._store.list_manifests().items():
            reg.register_manifest(manifest)
        return reg

    # ─── Manifest Management ────────────────────────────────────────────────
    def register_manifest(self, manifest: StrategyManifest) -> None:
        self._manifests[manifest.id] = manifest
        if self._store:
            self._store.save_manifest(manifest)

    def load_and_register(self, entrypoint: str) -> StrategyManifest:
        manifest, _ = load_strategy_from_path(entrypoint)
        self.register_manifest(manifest)
        return manifest

    def list_manifests(self):
        return list(self._manifests.values())

    def get_manifest(self, strategy_id: str) -> Optional[StrategyManifest]:
        return self._manifests.get(strategy_id)

    # ─── Activation ─────────────────────────────────────────────────────────
    def activate(
        self,
        strategy_id: str,
        config: dict,
        bindings: StrategyBindings,
        environment: str,
        tenant: Optional[str] = None,
    ) -> ActivationRecord:
        manifest = self.get_manifest(strategy_id)
        if not manifest:
            raise ValueError(f"Strategy '{strategy_id}' not registered")
        activation_id = str(uuid.uuid4())
        ctx = StrategyContext(environment=environment, tenant=tenant, activation_id=activation_id, strategy_id=strategy_id)
        _, plugin = load_strategy_from_path(manifest.entrypoint, manifest_override=manifest)
        handle = StrategyHandle(manifest, plugin)
        handle.activate(config=config, bindings=bindings, context=ctx)
        record = ActivationRecord(
            activation_id=activation_id,
            strategy_id=strategy_id,
            version=manifest.version,
            environment=environment,
            tenant=tenant,
            config=config,
            bindings_meta={k: type(v).__name__ for k, v in (bindings.__dict__).items() if v},
        )
        env_key = (environment, tenant)
        self._active.setdefault(env_key, {})
        self._active[env_key][strategy_id] = ActiveStrategy(
            manifest=manifest,
            activation=record,
            handle=handle,
        )
        if self._store:
            self._store.save_activation(record)
        return record

    def deactivate(self, strategy_id: str, environment: str, tenant: Optional[str] = None) -> None:
        env_key = (environment, tenant)
        env_map = self._active.get(env_key, {})
        active = env_map.pop(strategy_id, None)
        if active and getattr(active, "handle", None):
            active.handle.shutdown()

    def get_active(self, environment: str, tenant: Optional[str] = None) -> Dict[str, ActiveStrategy]:
        return self._active.get((environment, tenant), {})

    # ─── Restore persisted activations ─────────────────────────────────────
    def restore_activations(
        self,
        environment: str,
        tenant: Optional[str],
        bindings_factory,
    ) -> None:
        """
        Re-activate strategies stored in the backing store for a given env/tenant.
        bindings_factory: callable taking (ActivationRecord, StrategyManifest) -> StrategyBindings
        """
        if not self._store:
            return
        for act in self._store.list_activations_for(environment, tenant):
            manifest = self.get_manifest(act.strategy_id)
            if not manifest:
                continue
            bindings = bindings_factory(act, manifest)
            try:
                self.activate(
                    strategy_id=act.strategy_id,
                    config=act.config,
                    bindings=bindings,
                    environment=environment,
                    tenant=tenant,
                )
            except Exception:
                continue
