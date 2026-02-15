"""Persistent CLI auth/context state for the unified `kehrnel` command."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

DEFAULT_CONFIG_PATH = Path.home() / ".kehrnel" / "config.json"


def _default_state() -> Dict[str, Any]:
    return {
        "auth": {
            "api_key": None,
            "runtime_url": None,
        },
        "context": {
            "environment": None,
            "domain": None,
            "strategy": None,
            "runtime_url": None,
        },
    }


def load_cli_state(path: Path | None = None) -> Dict[str, Any]:
    config_path = path or DEFAULT_CONFIG_PATH
    if not config_path.exists():
        return _default_state()
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return _default_state()

    base = _default_state()
    auth = raw.get("auth") if isinstance(raw, dict) else None
    ctx = raw.get("context") if isinstance(raw, dict) else None

    if isinstance(auth, dict):
        base["auth"].update(auth)
    if isinstance(ctx, dict):
        base["context"].update(ctx)

    return base


def save_cli_state(state: Dict[str, Any], path: Path | None = None) -> Path:
    config_path = path or DEFAULT_CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    return config_path


def mask_api_key(value: str | None) -> str:
    if not value:
        return "(not set)"
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"
