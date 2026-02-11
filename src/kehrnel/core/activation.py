"""EnvironmentActivation scaffold."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class EnvironmentActivation:
    """
    Represents a strategy activation for an environment (no tenancy at this layer).
    """
    env_id: str
    domain: str
    strategy_id: str
    version: str
    manifest_digest: str | None = None
    config: Dict[str, Any] = field(default_factory=dict)
    bindings_meta: Dict[str, Any] = field(default_factory=dict)
    activation_id: str | None = None
    activated_at: str | None = None
    updated_at: str | None = None
    config_hash: str | None = None
    bindings: Optional[Dict[str, Any]] = None
    bindings_ref: Optional[str] = None
    replaced: bool = False
    previous_activation_id: Optional[str] = None
    bundle_refs: Dict[str, str] = field(default_factory=dict)
    store_profiles: Dict[str, Any] = field(default_factory=dict)
    already_active: bool = False
    replaced_from: Optional[Dict[str, Any]] = None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "env_id": self.env_id,
            "domain": self.domain,
            "strategy_id": self.strategy_id,
            "version": self.version,
            "manifest_digest": self.manifest_digest,
            "config": self.config,
            "bindings_meta": self.bindings_meta,
            "activation_id": self.activation_id,
            "activated_at": self.activated_at,
            "updated_at": self.updated_at,
            "config_hash": self.config_hash,
            "bindings": self.bindings,
            "bindings_ref": self.bindings_ref,
            "replaced": self.replaced,
            "previous_activation_id": self.previous_activation_id,
            "bundle_refs": self.bundle_refs,
            "store_profiles": self.store_profiles,
            "already_active": self.already_active,
            "replaced_from": self.replaced_from,
        }
