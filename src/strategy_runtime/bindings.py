from __future__ import annotations

from typing import Any, Dict, Optional

from strategy_sdk import StrategyBindings
from adapters.mongo_storage import MongoStorageAdapter


def build_storage_from_config(config: Dict[str, Any]) -> Optional[MongoStorageAdapter]:
    target = config.get("target", {}) if isinstance(config, dict) else {}
    conn = target.get("connection_string")
    db_name = target.get("database_name")
    comp_coll = target.get("compositions_collection")
    search_coll = target.get("search_collection") or target.get("search", {}).get("collection")
    if not conn or not db_name or not comp_coll:
        return None
    cfg = {
        "connection_string": conn,
        "database_name": db_name,
        "compositions_collection": comp_coll,
        "search_collection": search_coll or "compositions_search",
    }
    try:
        return MongoStorageAdapter.from_config(cfg)
    except Exception:
        return None


def build_search_adapter_from_config(config: Dict[str, Any]) -> Optional[Any]:
    search_cfg = config.get("search", {}) if isinstance(config, dict) else {}
    if not search_cfg:
        return None
    backend = search_cfg.get("backend")
    if backend == "opensearch":
        try:
            from adapters.opensearch_search import OpenSearchAdapter  # lazy import
            return OpenSearchAdapter.from_config(search_cfg)
        except Exception:
            return None
    # extend for other backends (atlas_search, etc.)
    return None


def build_bindings(default_bindings: StrategyBindings, config: Optional[Dict[str, Any]]) -> StrategyBindings:
    """
    Merge default bindings with optional config-derived storage.
    """
    cfg = config or {}
    storage = build_storage_from_config(cfg) or default_bindings.storage
    search = build_search_adapter_from_config(cfg) or getattr(default_bindings, "search", None)
    extras = dict(default_bindings.extras or {})
    return StrategyBindings(storage=storage, search=search, extras=extras)
