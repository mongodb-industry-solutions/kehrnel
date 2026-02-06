import json
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


def _load_config(cfg: Any) -> Dict[str, Any]:
    if isinstance(cfg, (str, Path)):
        text = Path(cfg).read_text(encoding="utf-8")
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return yaml.safe_load(text)
    if isinstance(cfg, dict):
        return cfg
    raise ValueError("Unsupported persistence config type")


def get_driver(cfg: Any):
    data = _load_config(cfg)
    kind = (data.get("driver") or data.get("provider") or data.get("type") or "mongo").lower()
    if kind in ("mongo", "mongodb"):
        return MongoStore(data)
    raise ValueError(f"Unsupported driver type: {kind}")


__all__ = [
    "PersistenceStrategy",
    "load_strategy_from_file",
    "load_strategy_from_json",
    "get_default_strategy",
    "MongoStore",
    "MongoSource",
    "MongoStorageAdapter",
    "get_database",
    "get_driver",
]
