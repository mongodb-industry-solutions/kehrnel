"""FastAPI assembly for the new kehrnel runtime."""
from __future__ import annotations

import json
import os
import logging
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from kehrnel.api.admin.routes import router as admin_router
from kehrnel.api.admin.ops_by_domain import router as ops_domain_router
from kehrnel.api.admin.activation_routes import router as activation_router
from kehrnel.api.portal.routes import router as portal_router
from kehrnel.core.runtime import StrategyRuntime
from kehrnel.core.manifest import StrategyManifest
from kehrnel.core.registry import FileActivationRegistry
from kehrnel.core.errors import KehrnelError
from kehrnel.core.bundle_store import BundleStore
from kehrnel.core.pack_validator import StrategyPackValidator


def _strategy_paths() -> list[Path]:
    paths = [Path(__file__).resolve().parents[1] / "strategies"]
    extra = os.getenv("KEHRNEL_STRATEGY_PATHS")
    if extra:
        for sep in (":", ","):
            if sep in extra:
                extra_paths = [p for p in extra.split(sep) if p]
                break
        else:
            extra_paths = [extra] if extra else []
        for part in extra_paths:
            if part:
                paths.append(Path(part))
    return paths


def validate_strategy_pack(manifest_data: dict, base_path: Path) -> list[str]:
    """Validate that a strategy pack has required assets and fields."""
    return StrategyPackValidator(manifest_data, base_path).validate()


def _seed_default_bundles(store: BundleStore):
    try:
        bundles_dir = Path(__file__).resolve().parents[1] / "strategies" / "openehr" / "rps_dual" / "bundles"
        if bundles_dir.exists():
            for path in bundles_dir.glob("*.json"):
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    store.save_bundle(data, mode="upsert")
                except Exception:
                    continue
    except Exception:
        return


def _load_manifests() -> list[StrategyManifest]:
    manifests: list[StrategyManifest] = []
    diagnostics: list[dict[str, object]] = []
    log = logging.getLogger("kehrnel.strategy.discovery")
    manifest_records: list[dict[str, object]] = []
    for base in _strategy_paths():
        if not base.exists():
            continue
        for manifest_path in base.glob("**/manifest.json"):
            try:
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                base_dir = manifest_path.parent
                defaults_path = base_dir / "defaults.json"
                schema_path = base_dir / "schema.json"
                spec_field = data.get("spec")
                if isinstance(spec_field, dict):
                    spec_path = base_dir / (spec_field.get("path") or "spec.json")
                elif isinstance(spec_field, str):
                    spec_path = base_dir / spec_field
                else:
                    spec_path = base_dir / "spec.json"
                if defaults_path.exists():
                    data["default_config"] = json.loads(defaults_path.read_text(encoding="utf-8"))
                if schema_path.exists():
                    data["config_schema"] = json.loads(schema_path.read_text(encoding="utf-8"))
                if data.get("pack_format") == "strategy-pack/v1" and spec_path.exists():
                    try:
                        data["pack_spec"] = json.loads(spec_path.read_text(encoding="utf-8"))
                    except Exception:
                        pass
                manifest_records.append({"data": data, "manifest_path": manifest_path})
            except Exception as exc:
                log.error("Failed to load manifest %s: %s", manifest_path, exc)
                diagnostics.append(
                    {
                        "id": None,
                        "domain": None,
                        "version": None,
                        "entrypoint": None,
                        "is_valid": False,
                        "validation_errors": [f"{manifest_path}: {exc}"],
                        "paths": {"manifest_path": str(manifest_path), "base_dir": str(manifest_path.parent)},
                    }
                )
    # validate and collect diagnostics
    for rec in manifest_records:
        data = rec["data"]
        manifest_path: Path = rec["manifest_path"]
        errors = validate_strategy_pack(data, manifest_path.parent)
        rec["errors"] = errors
    # enforce unique IDs
    seen = set()
    unique: list[StrategyManifest] = []
    for rec in manifest_records:
        data = rec["data"]
        manifest_path: Path = rec["manifest_path"]
        errors = list(rec.get("errors") or [])
        mid = data.get("id")
        if not errors and mid in seen:
            errors.append(f"Duplicate strategy id detected during discovery: {mid}")
        if not errors:
            seen.add(mid)
            unique.append(StrategyManifest(**data))
        diagnostics.append(
            {
                "id": data.get("id"),
                "domain": data.get("domain"),
                "version": data.get("version"),
                "entrypoint": data.get("entrypoint"),
                "is_valid": len(errors) == 0,
                "validation_errors": errors,
                "paths": {"manifest_path": str(manifest_path), "base_dir": str(manifest_path.parent)},
            }
        )
    return unique, diagnostics


def create_app(registry_path: str | None = None, bundle_path: str | None = None) -> FastAPI:
    app = FastAPI(title="Kehrnel Runtime", version="0.0.0")
    reg_path = registry_path or os.getenv("KEHRNEL_REGISTRY_PATH") or str(Path(".kehrnel_registry.json").resolve())
    bundle_dir = Path(bundle_path) if bundle_path else Path(".kehrnel/bundles")
    bundle_store = BundleStore(bundle_dir)
    _seed_default_bundles(bundle_store)
    runtime = StrategyRuntime(FileActivationRegistry(Path(reg_path)), bundle_store=bundle_store)
    manifests, diagnostics = _load_manifests()
    for manifest in manifests:
        runtime.register_manifest(manifest)
    app.state.strategy_runtime = runtime
    app.state.strategy_diagnostics = diagnostics
    app.state.bundle_store = bundle_store
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        code = "INTERNAL_ERROR"
        status = 500
        message = str(exc)
        details = {}
        if isinstance(exc, KehrnelError):
            code = exc.code
            status = exc.status
            message = str(exc)
            details = getattr(exc, "details", {}) or {}
        elif isinstance(exc, ValueError):
            code = "INVALID_INPUT"
            status = 400
        elif isinstance(exc, KeyError):
            code = "NOT_FOUND"
            status = 404
        return JSONResponse(status_code=status, content={"error": {"code": code, "message": message, "details": details}})
    app.include_router(admin_router)
    app.include_router(ops_domain_router)
    app.include_router(activation_router)
    app.include_router(portal_router)
    return app


app = create_app()
