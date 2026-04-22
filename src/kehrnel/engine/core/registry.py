"""ActivationRegistry scaffold."""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, List, Any

from .manifest import StrategyManifest
from .activation import EnvironmentActivation
from .environment import EnvironmentRecord


class ActivationRegistry:
    def clear_manifests(self) -> None:
        """Clear all cached manifests. Called on startup before registering fresh manifests from disk."""
        raise NotImplementedError

    def register_manifest(self, manifest: StrategyManifest) -> None:
        raise NotImplementedError

    def list_manifests(self):
        raise NotImplementedError

    def get_manifest(self, strategy_id: str) -> Optional[StrategyManifest]:
        raise NotImplementedError

    def activate(self, activation: EnvironmentActivation, reason: str = "activate") -> EnvironmentActivation:
        raise NotImplementedError

    def get_activation(self, env_id: str, domain: str | None = None) -> Optional[EnvironmentActivation]:
        raise NotImplementedError

    def get_activation_by_strategy(self, env_id: str, strategy_id: str) -> Optional[EnvironmentActivation]:
        raise NotImplementedError

    def list_activations(self, env_id: str) -> Dict[str, EnvironmentActivation]:
        raise NotImplementedError

    def deactivate(self, env_id: str, domain: str, reason: str = "deactivate") -> Optional[EnvironmentActivation]:
        raise NotImplementedError

    def pop_history(self, env_id: str, domain: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def list_history(self, env_id: str, domain: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def upsert_environment(self, environment: EnvironmentRecord) -> EnvironmentRecord:
        raise NotImplementedError

    def get_environment(self, env_id: str) -> Optional[EnvironmentRecord]:
        raise NotImplementedError

    def list_environments(self) -> Dict[str, EnvironmentRecord]:
        raise NotImplementedError

    def delete_environment(self, env_id: str, remove_activations: bool = False) -> Optional[EnvironmentRecord]:
        raise NotImplementedError


class FileActivationRegistry(ActivationRegistry):
    HISTORY_LIMIT = 25

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.manifests: Dict[str, StrategyManifest] = {}
        self.activations: Dict[str, Dict[str, EnvironmentActivation]] = {}
        self.history: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        self.environments: Dict[str, EnvironmentRecord] = {}
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            for mid, m in (data.get("manifests") or {}).items():
                self.manifests[mid] = StrategyManifest(**m)
            for env_id, env_data in (data.get("environments") or {}).items():
                if isinstance(env_data, dict):
                    self.environments[env_id] = EnvironmentRecord(**env_data)
            for env_id, env_acts in (data.get("activations") or {}).items():
                if isinstance(env_acts, dict) and all(isinstance(v, dict) for v in env_acts.values()):
                    self.activations[env_id] = {domain: EnvironmentActivation(**act) for domain, act in env_acts.items()}
                else:
                    # compatibility format: single activation per env
                    self.activations[env_id] = {"default": EnvironmentActivation(**env_acts)}
            history = data.get("history") or {}
            if isinstance(history, dict):
                self.history = {env: {dom: list(entries or []) for dom, entries in hist.items()} for env, hist in history.items()}
        except Exception:
            return

    def _save(self):
        data = {
            "manifests": {mid: m.model_dump() for mid, m in self.manifests.items()},
            "environments": {env_id: env.snapshot() for env_id, env in self.environments.items()},
            "activations": {env_id: {domain: act.__dict__ for domain, act in acts.items()} for env_id, acts in self.activations.items()},
            "history": self.history,
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def _activation_timestamps(self, env_id: str) -> tuple[str | None, str | None]:
        env_acts = self.activations.get(env_id) or {}
        created_candidates = [
            getattr(act, "activated_at", None)
            for act in env_acts.values()
            if getattr(act, "activated_at", None)
        ]
        updated_candidates = [
            getattr(act, "updated_at", None) or getattr(act, "activated_at", None)
            for act in env_acts.values()
            if getattr(act, "updated_at", None) or getattr(act, "activated_at", None)
        ]
        created_at = min(created_candidates) if created_candidates else None
        updated_at = max(updated_candidates) if updated_candidates else created_at
        return created_at, updated_at

    def _synthetic_environment(self, env_id: str) -> Optional[EnvironmentRecord]:
        if env_id not in self.activations:
            return None
        created_at, updated_at = self._activation_timestamps(env_id)
        return EnvironmentRecord(
            env_id=env_id,
            name=env_id,
            description="",
            metadata={"synthetic": True},
            bindings_ref=None,
            created_at=created_at,
            updated_at=updated_at,
        )

    def _ensure_environment_record(self, env_id: str) -> EnvironmentRecord:
        existing = self.environments.get(env_id)
        if existing:
            return existing
        synthesized = self._synthetic_environment(env_id)
        if synthesized:
            self.environments[env_id] = synthesized
            return synthesized
        now = datetime.utcnow().isoformat()
        record = EnvironmentRecord(
            env_id=env_id,
            name=env_id,
            description="",
            metadata={},
            bindings_ref=None,
            created_at=now,
            updated_at=now,
        )
        self.environments[env_id] = record
        return record

    def clear_manifests(self) -> None:
        """Clear all cached manifests to prepare for fresh registration from disk."""
        self.manifests.clear()
        # Note: we don't save here; caller will register fresh manifests and save

    def register_manifest(self, manifest: StrategyManifest) -> None:
        self.manifests[manifest.id] = manifest
        self._save()

    def list_manifests(self):
        return list(self.manifests.values())

    def get_manifest(self, strategy_id: str) -> Optional[StrategyManifest]:
        return self.manifests.get(strategy_id)

    def activate(self, activation: EnvironmentActivation, reason: str = "activate") -> EnvironmentActivation:
        if not activation.activation_id:
            activation.activation_id = str(uuid.uuid4())
        env_map = self.activations.setdefault(activation.env_id, {})
        domain_key = str(activation.domain).lower()
        activation.domain = domain_key
        if env_map.get(domain_key):
            self._push_history(activation.env_id, domain_key, env_map[domain_key], reason or "upgrade")
        env_map[domain_key] = activation
        env_record = self._ensure_environment_record(activation.env_id)
        env_record.updated_at = getattr(activation, "updated_at", None) or getattr(activation, "activated_at", None) or datetime.utcnow().isoformat()
        self._save()
        return activation

    def get_activation(self, env_id: str, domain: str | None = None) -> Optional[EnvironmentActivation]:
        env_map = self.activations.get(env_id) or {}
        if domain:
            return env_map.get(str(domain).lower())
        # fallback: if only one activation, return it
        if len(env_map) == 1:
            return list(env_map.values())[0]
        return None

    def get_activation_by_strategy(self, env_id: str, strategy_id: str) -> Optional[EnvironmentActivation]:
        env_map = self.activations.get(env_id) or {}
        for act in env_map.values():
            if act.strategy_id == strategy_id:
                return act
        return None

    def list_activations(self, env_id: str) -> Dict[str, EnvironmentActivation]:
        return self.activations.get(env_id) or {}

    def deactivate(self, env_id: str, domain: str, reason: str = "deactivate") -> Optional[EnvironmentActivation]:
        env_map = self.activations.get(env_id) or {}
        domain_key = str(domain).lower()
        existing = env_map.pop(domain_key, None)
        if existing:
            self._push_history(env_id, domain_key, existing, reason or "deactivate")
            if not env_map:
                self.activations.pop(env_id, None)
            env_record = self.environments.get(env_id)
            if env_record:
                env_record.updated_at = datetime.utcnow().isoformat()
            self._save()
        return existing

    def _push_history(self, env_id: str, domain: str, activation: EnvironmentActivation, reason: str) -> None:
        if not activation:
            return
        snapshot = activation.snapshot() if hasattr(activation, "snapshot") else activation.__dict__
        entry = {"activation": snapshot, "timestamp": datetime.utcnow().isoformat(), "reason": reason}
        dom_hist = self.history.setdefault(env_id, {}).setdefault(domain, [])
        dom_hist.append(entry)
        if len(dom_hist) > self.HISTORY_LIMIT:
            self.history[env_id][domain] = dom_hist[-self.HISTORY_LIMIT :]

    def pop_history(self, env_id: str, domain: str) -> Optional[Dict[str, Any]]:
        env_hist = self.history.get(env_id) or {}
        dom_hist = env_hist.get(str(domain).lower()) or []
        if not dom_hist:
            return None
        entry = dom_hist.pop()
        if not dom_hist:
            env_hist.pop(str(domain).lower(), None)
        if not env_hist:
            self.history.pop(env_id, None)
        self._save()
        return entry

    def list_history(self, env_id: str, domain: str) -> List[Dict[str, Any]]:
        env_hist = self.history.get(env_id) or {}
        return list(env_hist.get(str(domain).lower()) or [])

    def upsert_environment(self, environment: EnvironmentRecord) -> EnvironmentRecord:
        env_id = str(environment.env_id).strip()
        if not env_id:
            raise ValueError("env_id is required")
        existing = self.environments.get(env_id) or self._synthetic_environment(env_id)
        now = datetime.utcnow().isoformat()
        record = EnvironmentRecord(
            env_id=env_id,
            name=environment.name if environment.name is not None else getattr(existing, "name", None) or env_id,
            description=environment.description if environment.description is not None else getattr(existing, "description", None) or "",
            metadata=dict(environment.metadata or {}) if environment.metadata is not None else dict(getattr(existing, "metadata", {}) or {}),
            bindings_ref=environment.bindings_ref if environment.bindings_ref is not None else getattr(existing, "bindings_ref", None),
            created_at=environment.created_at or getattr(existing, "created_at", None) or now,
            updated_at=environment.updated_at or now,
        )
        self.environments[env_id] = record
        self._save()
        return record

    def get_environment(self, env_id: str) -> Optional[EnvironmentRecord]:
        return self.environments.get(env_id) or self._synthetic_environment(env_id)

    def list_environments(self) -> Dict[str, EnvironmentRecord]:
        combined = {env_id: env for env_id, env in self.environments.items()}
        for env_id in self.activations.keys():
            if env_id not in combined:
                synthesized = self._synthetic_environment(env_id)
                if synthesized:
                    combined[env_id] = synthesized
        return combined

    def delete_environment(self, env_id: str, remove_activations: bool = False) -> Optional[EnvironmentRecord]:
        existing = self.environments.get(env_id) or self._synthetic_environment(env_id)
        if not existing:
            return None
        if env_id in self.activations and not remove_activations:
            raise ValueError(f"Environment {env_id} still has active strategy activations")
        self.environments.pop(env_id, None)
        if remove_activations:
            self.activations.pop(env_id, None)
            self.history.pop(env_id, None)
        self._save()
        return existing
