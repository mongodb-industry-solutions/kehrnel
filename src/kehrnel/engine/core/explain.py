"""Shared helpers for enriching explain metadata."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.core.manifest import StrategyManifest


def _compute_config_hash(cfg: Dict[str, Any]) -> str:
    try:
        blob = json.dumps(cfg or {}, sort_keys=True).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()
    except Exception:
        return ""


def _compute_manifest_digest(manifest: StrategyManifest | None) -> str | None:
    if manifest is None:
        return None
    try:
        payload = manifest.model_dump()
        blob = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(blob).hexdigest()
    except Exception:
        return None


def enrich_explain(
    explain: Dict[str, Any],
    ctx: StrategyContext,
    domain: str,
    engine: str,
    scope: str | None = None,
) -> Dict[str, Any]:
    """Ensure explain carries consistent metadata across strategies."""
    ctx_meta = ctx.meta or {}
    cfg_hash = ctx_meta.get("config_hash") or _compute_config_hash(ctx.config)
    manifest = getattr(ctx, "manifest", None) or getattr(ctx, "manifest", None)
    manifest_digest = ctx_meta.get("manifest_digest") or _compute_manifest_digest(manifest)
    explain = dict(explain or {})
    explain.setdefault("engine", engine)
    explain.setdefault("domain", (getattr(manifest, "domain", None) or domain or "").lower() or None)
    explain.setdefault("strategy_id", getattr(manifest, "id", None))
    explain.setdefault("strategy_version", getattr(manifest, "version", None))
    explain.setdefault("activation_id", ctx_meta.get("activation_id"))
    explain.setdefault("config_hash", cfg_hash)
    explain.setdefault("manifest_digest", manifest_digest)
    if scope:
        explain.setdefault("scope", scope)
    return explain
