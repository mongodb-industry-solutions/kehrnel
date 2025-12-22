"""FastAPI assembly for the new kehrnel runtime."""
from __future__ import annotations

import json
import os
import logging
from pathlib import Path
from fastapi import FastAPI

from kehrnel.api.admin.routes import router as admin_router
from kehrnel.core.runtime import StrategyRuntime
from kehrnel.core.manifest import StrategyManifest
from kehrnel.core.registry import FileActivationRegistry


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


def _load_manifests() -> list[StrategyManifest]:
    manifests: list[StrategyManifest] = []
    log = logging.getLogger("kehrnel.strategy.discovery")
    for base in _strategy_paths():
        if not base.exists():
            continue
        for manifest_path in base.glob("**/manifest.json"):
            try:
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifests.append(StrategyManifest(**data))
            except Exception as exc:
                log.error("Failed to load manifest %s: %s", manifest_path, exc)
                raise
    # enforce unique IDs
    seen = set()
    unique: list[StrategyManifest] = []
    for m in manifests:
        if m.id in seen:
            raise ValueError(f"Duplicate strategy id detected during discovery: {m.id}")
        seen.add(m.id)
        unique.append(m)
    return unique


def create_app(registry_path: str | None = None) -> FastAPI:
    app = FastAPI(title="Kehrnel Runtime", version="0.0.0")
    reg_path = registry_path or os.getenv("KEHRNEL_REGISTRY_PATH") or str(Path(".kehrnel_registry.json").resolve())
    runtime = StrategyRuntime(FileActivationRegistry(Path(reg_path)))
    for manifest in _load_manifests():
        runtime.register_manifest(manifest)
    app.state.strategy_runtime = runtime
    app.include_router(admin_router)
    return app


app = create_app()
