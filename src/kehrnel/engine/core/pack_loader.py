"""Strategy pack loader/validator helper for Strategy Pack v1."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.pack_validator import StrategyPackValidator


def load_strategy(strategy_id: Optional[str], pack_path: str | Path) -> StrategyManifest:
    """
    Load and validate a strategy pack from disk.

    - Validates manifest and optional spec.json (when pack_format=strategy-pack/v1)
    - Hydrates defaults/schema into the manifest payload
    - Optionally checks the manifest.id matches the expected strategy_id
    """
    base = Path(pack_path)
    manifest_path = base / "manifest.json"
    if not manifest_path.exists():
        raise KehrnelError(code="MANIFEST_NOT_FOUND", status=404, message=f"manifest.json not found in {base}")
    try:
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise KehrnelError(code="MANIFEST_INVALID", status=400, message=f"manifest.json invalid JSON: {exc}") from exc

    errors = StrategyPackValidator(manifest_data, base).validate()
    if errors:
        raise KehrnelError(code="PACK_INVALID", status=400, message="; ".join(errors), details={"errors": errors})

    defaults_path = base / "defaults.json"
    if defaults_path.exists() and not manifest_data.get("default_config"):
        manifest_data["default_config"] = json.loads(defaults_path.read_text(encoding="utf-8"))

    schema_path = base / "schema.json"
    if schema_path.exists() and not manifest_data.get("config_schema"):
        manifest_data["config_schema"] = json.loads(schema_path.read_text(encoding="utf-8"))

    if manifest_data.get("pack_format") == "strategy-pack/v1":
        spec_field = manifest_data.get("spec")
        if isinstance(spec_field, dict):
            spec_path = spec_field.get("path") or "spec.json"
        elif isinstance(spec_field, str):
            spec_path = spec_field
        else:
            spec_path = "spec.json"
        spec_file = base / spec_path
        if spec_file.exists():
            try:
                manifest_data["pack_spec"] = json.loads(spec_file.read_text(encoding="utf-8"))
            except Exception:
                pass

    manifest = StrategyManifest(**manifest_data)
    if strategy_id and manifest.id != strategy_id:
        raise KehrnelError(
            code="STRATEGY_ID_MISMATCH",
            status=400,
            message=f"Strategy id mismatch: expected {strategy_id}, found {manifest.id}",
            details={"expected": strategy_id, "found": manifest.id},
        )
    return manifest
