"""Stable manifest digest helpers.

The digest intentionally excludes runtime-hydrated fields (default_config,
config_schema, pack_spec) so activations stay valid across code reloads and
pack default merges that do not change manifest.json identity.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from .manifest import StrategyManifest
from .activation import EnvironmentActivation


def stable_manifest_payload(manifest: StrategyManifest) -> Dict[str, Any]:
    ops = manifest.ops or []
    payload: Dict[str, Any] = {
        "id": manifest.id,
        "version": manifest.version,
        "entrypoint": manifest.entrypoint,
        "domain": manifest.domain,
        "capabilities": sorted(manifest.capabilities or []),
        "ops": sorted(
            [{"name": op.name, "kind": op.kind} for op in ops],
            key=lambda item: item["name"],
        ),
    }
    pack_format = getattr(manifest, "pack_format", None)
    if pack_format:
        payload["pack_format"] = pack_format
    return payload


def compute_manifest_digest(manifest: StrategyManifest | None) -> str:
    if manifest is None:
        return ""
    try:
        blob = json.dumps(stable_manifest_payload(manifest), sort_keys=True).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()
    except Exception:
        return ""


def activation_version_compatible(activation: EnvironmentActivation, manifest: StrategyManifest) -> bool:
    if activation.strategy_id != manifest.id:
        return False
    act_ver = (activation.version or "").strip()
    if not act_ver or act_ver.lower() in ("latest", "current"):
        return True
    man_ver = (manifest.version or "").strip()
    return act_ver == man_ver
