"""Shared configuration and persistence helpers for the CDISC SDR pack."""

from __future__ import annotations

import json
import inspect
from pathlib import Path
from typing import Any, Dict, Iterable

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext


PACK_ROOT = Path(__file__).resolve().parent
MANIFEST_PATH = PACK_ROOT / "manifest.json"
DEFAULTS_PATH = PACK_ROOT / "defaults.json"
SCHEMA_PATH = PACK_ROOT / "schema.json"

# Version of the persisted CDISC SDR document contract.  This is deliberately
# independent from the strategy release and the external CDISC standard
# versions: change it only when a stored document shape requires migration.
MODEL_SCHEMA_VERSION = "1.0.0"


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base or {})
    for key, value in (overlay or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def config(ctx: StrategyContext) -> Dict[str, Any]:
    return deep_merge(load_json(DEFAULTS_PATH), ctx.config or {})


def collections(cfg: Dict[str, Any]) -> Dict[str, str]:
    configured = cfg.get("collections") if isinstance(cfg.get("collections"), dict) else {}
    required = {
        "studies",
        "snapshots",
        "datasets",
        "records",
        "entities",
        "materializations",
        "standards",
        "artifacts",
        "validation_runs",
        "validation_findings",
        "validation_waivers",
        "transformations",
    }
    missing = sorted(name for name in required if not str(configured.get(name) or "").strip())
    if missing:
        raise KehrnelError(
            code="CONFIG_INVALID",
            status=400,
            message="CDISC collection configuration is incomplete.",
            details={"missing": missing},
        )
    return {key: str(value) for key, value in configured.items()}


def model_doc(model: Any) -> Dict[str, Any]:
    return model.model_dump(by_alias=True, mode="json", exclude_none=True)


def stamp_model_schema(document: Dict[str, Any]) -> Dict[str, Any]:
    """Return a stored-model document carrying its explicit evolution marker."""

    document.setdefault("modelSchemaVersion", MODEL_SCHEMA_VERSION)
    return document


def storage_adapter(ctx: StrategyContext) -> Any:
    storage = (ctx.adapters or {}).get("storage")
    if storage is None:
        raise KehrnelError(code="STORAGE_UNAVAILABLE", status=500, message="MongoDB storage adapter is required")
    return storage


def ensure_not_cancelled(ctx: StrategyContext) -> None:
    callback = (ctx.meta or {}).get("should_cancel")
    if callable(callback) and callback():
        raise KehrnelError(code="JOB_CANCELED", status=409, message="CDISC operation was canceled")


async def report_progress(
    ctx: StrategyContext,
    *,
    progress: int,
    phase: str,
    stats: Dict[str, Any] | None = None,
) -> None:
    callback = (ctx.meta or {}).get("progress_cb")
    if not callable(callback):
        return
    result = callback(progress=progress, phase=phase, stats=stats or {})
    if inspect.isawaitable(result):
        await result


async def replace_documents(
    storage: Any,
    collection: str,
    docs: Iterable[Dict[str, Any]],
    *,
    batch_size: int | None = None,
) -> int:
    materialized = [stamp_model_schema(doc) for doc in docs]
    if not materialized:
        return 0
    replace_many = getattr(storage, "replace_many", None)
    if callable(replace_many):
        size = max(1, int(batch_size or len(materialized)))
        for start in range(0, len(materialized), size):
            await replace_many(collection, materialized[start:start + size])
        return len(materialized)
    replace_one = getattr(storage, "replace_one", None)
    if callable(replace_one):
        for doc in materialized:
            await replace_one(collection, {"_id": doc["_id"]}, doc, upsert=True)
        return len(materialized)
    db = getattr(storage, "db", None)
    if db is not None:
        for doc in materialized:
            await db[collection].replace_one({"_id": doc["_id"]}, doc, upsert=True)
        return len(materialized)
    raise KehrnelError(
        code="IDEMPOTENT_STORAGE_REQUIRED",
        status=500,
        message="CDISC operations require a storage adapter with replace/upsert support.",
    )
