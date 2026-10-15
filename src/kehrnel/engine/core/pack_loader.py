"""Single read/validate/hydrate/identify path for strategy manifests."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional

from pydantic import ValidationError

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import StrategyManifest, read_strategy_manifest
from kehrnel.engine.core.manifest_digest import PackageLayoutError, compute_package_digest
from kehrnel.engine.core.pack_validator import StrategyPackValidator
from kehrnel.contracts.schema_registry import published_contract_digest


NON_DISCOVERABLE_PATH_PARTS = frozenset({"samples", "strategy-pack-template"})


def strategy_pack_root(manifest_path: str | Path) -> Path:
    base = Path(manifest_path).parent
    return base.parent if base.name == "specification" else base


def is_discoverable_manifest(manifest_path: str | Path, strategy_root: str | Path) -> bool:
    """Return whether a manifest is an executable pack, not a sample/template."""

    path = Path(manifest_path)
    try:
        relative = path.resolve().relative_to(Path(strategy_root).resolve())
    except ValueError:
        return False
    if path.name != "manifest.json":
        return False
    return not any(
        part in NON_DISCOVERABLE_PATH_PARTS or part.startswith("_disabled")
        for part in relative.parts
    )


def iter_strategy_manifest_paths(strategy_root: str | Path) -> Iterable[Path]:
    root = Path(strategy_root)
    if not root.exists():
        return
    for path in sorted(root.glob("**/manifest.json")):
        if is_discoverable_manifest(path, root):
            yield path


def load_strategy_manifest(
    manifest_path: str | Path,
    *,
    strategy_id: Optional[str] = None,
) -> StrategyManifest:
    """Load one manifest through the canonical validation and identity path."""

    manifest_path = Path(manifest_path)
    base = manifest_path.parent
    pack_root = strategy_pack_root(manifest_path)
    if manifest_path.is_symlink():
        raise KehrnelError(
            code="PACK_LAYOUT_INVALID",
            status=400,
            message="manifest.json must not be a symlink",
            details={"path": str(manifest_path)},
        )
    if not manifest_path.exists():
        raise KehrnelError(
            code="MANIFEST_NOT_FOUND",
            status=404,
            message=f"manifest.json not found in {base}",
        )
    try:
        # This is deliberately first: the digest builder performs a complete
        # no-follow layout preflight before opening any package file.
        package_digest = compute_package_digest(pack_root)
    except PackageLayoutError as exc:
        raise KehrnelError(
            code="PACK_LAYOUT_INVALID",
            status=400,
            message=str(exc),
            details={"path": str(pack_root)},
        ) from exc
    try:
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise KehrnelError(code="MANIFEST_INVALID", status=400, message=f"manifest.json invalid JSON: {exc}") from exc
    if not isinstance(manifest_data, dict):
        raise KehrnelError(
            code="MANIFEST_INVALID",
            status=400,
            message="manifest.json must contain a JSON object",
            details={
                "diagnostics": [
                    {
                        "code": "MANIFEST_SCHEMA_INVALID",
                        "message": "manifest.json must contain a JSON object",
                        "path": "/",
                        "severity": "error",
                    }
                ]
            },
        )

    validator = StrategyPackValidator(manifest_data, base)
    diagnostics = validator.validate_diagnostics()
    errors = [item.message for item in diagnostics]
    if errors:
        raise KehrnelError(
            code="PACK_INVALID",
            status=400,
            message="; ".join(errors),
            details={
                "errors": errors,
                "diagnostics": [item.as_dict() for item in diagnostics],
                "manifest": {
                    "id": manifest_data.get("id"),
                    "domain": manifest_data.get("domain"),
                    "version": manifest_data.get("version"),
                    "entrypoint": manifest_data.get("entrypoint"),
                },
            },
        )

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

    manifest_data["packageDigest"] = package_digest
    manifest_data["contractDigest"] = published_contract_digest()

    try:
        manifest = read_strategy_manifest(manifest_data)
    except ValidationError as exc:
        diagnostics = []
        for item in exc.errors(include_url=False):
            location = "/".join(str(part) for part in item.get("loc") or ())
            diagnostics.append(
                {
                    "code": "MANIFEST_SCHEMA_INVALID",
                    "message": item.get("msg", "invalid value"),
                    "path": f"/{location}" if location else "/",
                    "severity": "error",
                }
            )
        raise KehrnelError(
            code="MANIFEST_INVALID",
            status=400,
            message="hydrated manifest does not satisfy the runtime contract",
            details={"diagnostics": diagnostics},
        ) from exc
    if strategy_id and manifest.id != strategy_id:
        raise KehrnelError(
            code="STRATEGY_ID_MISMATCH",
            status=400,
            message=f"Strategy id mismatch: expected {strategy_id}, found {manifest.id}",
            details={"expected": strategy_id, "found": manifest.id},
        )
    return manifest


def load_strategy(strategy_id: Optional[str], pack_path: str | Path) -> StrategyManifest:
    """Load a strategy from a pack directory (legacy public entry point)."""

    base = Path(pack_path)
    manifest_path = base / "manifest.json"
    if not manifest_path.exists() and (base / "specification" / "manifest.json").exists():
        manifest_path = base / "specification" / "manifest.json"
    return load_strategy_manifest(manifest_path, strategy_id=strategy_id)
