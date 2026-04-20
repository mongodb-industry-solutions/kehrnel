"""Minimal environment record for Kehrnel-owned runtime state."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class EnvironmentRecord:
    env_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    bindings_ref: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def snapshot(self) -> Dict[str, Any]:
        return {
            "env_id": self.env_id,
            "name": self.name,
            "description": self.description,
            "metadata": self.metadata,
            "bindings_ref": self.bindings_ref,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
