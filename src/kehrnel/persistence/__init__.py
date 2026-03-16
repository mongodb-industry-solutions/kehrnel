import json
import os
from pathlib import Path
from typing import Any, Dict
import yaml

from .strategy import (
    PersistenceStrategy,
    load_strategy_from_file,
    load_strategy_from_json,
    get_default_strategy,
)
from .mongo import MongoStore, MongoSource
from .mongodb.storage import MongoStorageAdapter
from .mongodb.connection import get_database

class MemoryPersister:
    """
    Minimal in-memory persistence adapter.
    """

    def __init__(self):
        self.base: list[dict] = []
        self.search: list[dict] = []
        self.stats = type("S", (), {"inserted": 0})()

    def connect(self):
        # Keep the same interface as MongoStore; no-op for in-memory.
        return None

    def insert_one(self, doc: dict, *, search: bool = False):
        (self.search if search else self.base).append(doc)
        self.stats.inserted += 1

    def insert_many(self, docs, workers: int = 4):
        for d in docs:
            self.insert_one(d, search=bool(d.get("sn") is not None))


def _load_config(cfg: Any) -> Dict[str, Any]:
    def _expand_env(node: Any) -> Any:
        """Expand ${VARS} in string values (useful for keeping secrets out of files)."""
        if isinstance(node, str):
            return os.path.expandvars(node)
        if isinstance(node, list):
            return [_expand_env(x) for x in node]
        if isinstance(node, dict):
            return {k: _expand_env(v) for k, v in node.items()}
        return node

    if isinstance(cfg, (str, Path)):
        text = Path(cfg).read_text(encoding="utf-8")
        try:
            return _expand_env(json.loads(text))
        except json.JSONDecodeError:
            return _expand_env(yaml.safe_load(text))
    if isinstance(cfg, dict):
        return _expand_env(cfg)
    raise ValueError("Unsupported persistence config type")


def get_driver(cfg: Any):
    data = _load_config(cfg)
    kind = (data.get("driver") or data.get("provider") or data.get("type") or "mongo").lower()
    if kind in ("memory", "mem", "inmemory", "in-memory"):
        return MemoryPersister()
    if kind in ("mongo", "mongodb"):
        return MongoStore(data)
    raise ValueError(f"Unsupported driver type: {kind}")


__all__ = [
    "PersistenceStrategy",
    "load_strategy_from_file",
    "load_strategy_from_json",
    "get_default_strategy",
    "MemoryPersister",
    "MongoStore",
    "MongoSource",
    "MongoStorageAdapter",
    "get_database",
    "get_driver",
]
