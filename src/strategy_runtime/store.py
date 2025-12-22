from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, Optional, Iterable

from strategy_sdk import StrategyManifest
from .models import ActivationRecord


class FileRegistryStore:
    """
    Minimal file-backed store for manifests and activations.
    Not concurrent-safe; good enough for a single-node dev/runtime preview.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict[str, dict]:
        if not self.path.exists():
            return {"manifests": {}, "activations": []}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {"manifests": {}, "activations": []}

    def save(self, data: Dict[str, dict]):
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        os.replace(tmp_path, self.path)

    def save_manifest(self, manifest: StrategyManifest):
        data = self.load()
        data.setdefault("manifests", {})
        data["manifests"][manifest.id] = manifest.model_dump()
        self.save(data)

    def list_manifests(self) -> Dict[str, StrategyManifest]:
        data = self.load()
        res = {}
        for k, v in data.get("manifests", {}).items():
            try:
                res[k] = StrategyManifest(**v)
            except Exception:
                continue
        return res

    def save_activation(self, activation: ActivationRecord):
        data = self.load()
        acts = data.setdefault("activations", [])
        acts = [a for a in acts if a.get("activation_id") != activation.activation_id]
        acts.append(activation.__dict__)
        data["activations"] = acts
        self.save(data)

    def list_activations(self) -> Dict[str, ActivationRecord]:
        data = self.load()
        res = {}
        for item in data.get("activations", []):
            try:
                act = ActivationRecord(**item)
                res[act.activation_id] = act
            except Exception:
                continue
        return res

    def list_activations_for(self, environment: str, tenant: Optional[str] = None) -> Iterable[ActivationRecord]:
        return [
            act for act in self.list_activations().values()
            if act.environment == environment and act.tenant == tenant
        ]
