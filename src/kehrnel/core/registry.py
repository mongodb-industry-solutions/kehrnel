"""ActivationRegistry scaffold."""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Dict, Optional

from .manifest import StrategyManifest
from .activation import EnvironmentActivation


class ActivationRegistry:
    def register_manifest(self, manifest: StrategyManifest) -> None:
        raise NotImplementedError

    def list_manifests(self):
        raise NotImplementedError

    def get_manifest(self, strategy_id: str) -> Optional[StrategyManifest]:
        raise NotImplementedError

    def activate(self, activation: EnvironmentActivation) -> EnvironmentActivation:
        raise NotImplementedError

    def get_activation(self, env_id: str) -> Optional[EnvironmentActivation]:
        raise NotImplementedError


class FileActivationRegistry(ActivationRegistry):
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.manifests: Dict[str, StrategyManifest] = {}
        self.activations: Dict[str, EnvironmentActivation] = {}
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            for mid, m in (data.get("manifests") or {}).items():
                self.manifests[mid] = StrategyManifest(**m)
            for aid, a in (data.get("activations") or {}).items():
                self.activations[aid] = EnvironmentActivation(**a)
        except Exception:
            return

    def _save(self):
        data = {
            "manifests": {mid: m.model_dump() for mid, m in self.manifests.items()},
            "activations": {aid: a.__dict__ for aid, a in self.activations.items()},
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def register_manifest(self, manifest: StrategyManifest) -> None:
        self.manifests[manifest.id] = manifest
        self._save()

    def list_manifests(self):
        return list(self.manifests.values())

    def get_manifest(self, strategy_id: str) -> Optional[StrategyManifest]:
        return self.manifests.get(strategy_id)

    def activate(self, activation: EnvironmentActivation) -> EnvironmentActivation:
        if not activation.activation_id:
            activation.activation_id = str(uuid.uuid4())
        self.activations[activation.env_id] = activation
        self._save()
        return activation

    def get_activation(self, env_id: str) -> Optional[EnvironmentActivation]:
        return self.activations.get(env_id)
