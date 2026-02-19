import json
import os
from pathlib import Path
from typing import Any, Callable, Dict
import yaml

from .layout import (
    PersistenceLayout,
    load_layout_from_file,
    load_layout_from_json,
    get_default_layout,
)
from .mongo import MongoStore, MongoSource
from .fs import FileStore, read_jsonl, write_jsonl
from .mongodb.storage import MongoStorageAdapter
from .mongodb.connection import get_database

DriverFactory = Callable[[Dict[str, Any]], Any]
_DRIVER_FACTORIES: Dict[str, DriverFactory] = {}
_PRIMARY_DRIVERS: set[str] = set()


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


def register_driver(name: str, factory: DriverFactory, *aliases: str) -> None:
    """Register a new persistence driver factory.

    This keeps the architecture open for future adapters (e.g. CosmosDB)
    while shipping only MongoDB and filesystem by default.
    """
    keys = [name, *aliases]
    canonical = (name or "").strip().lower()
    if canonical:
        _PRIMARY_DRIVERS.add(canonical)
    for key in keys:
        normalized = (key or "").strip().lower()
        if normalized:
            _DRIVER_FACTORIES[normalized] = factory


def list_drivers(include_aliases: bool = False) -> list[str]:
    if include_aliases:
        return sorted(_DRIVER_FACTORIES)
    return sorted(_PRIMARY_DRIVERS)


def get_driver(cfg: Any):
    data = _load_config(cfg)
    kind = (data.get("driver") or data.get("provider") or data.get("type") or "mongodb").lower()
    factory = _DRIVER_FACTORIES.get(kind)
    if not factory:
        supported = ", ".join(sorted(_DRIVER_FACTORIES))
        raise ValueError(f"Unsupported driver type: {kind}. Supported: {supported}")
    return factory(data)


# Built-ins: only MongoDB + filesystem.
register_driver("mongodb", MongoStore, "mongo")
register_driver("filesystem", FileStore, "fs", "file")


__all__ = [
    "PersistenceLayout",
    "load_layout_from_file",
    "load_layout_from_json",
    "get_default_layout",
    "MongoStore",
    "MongoSource",
    "FileStore",
    "read_jsonl",
    "write_jsonl",
    "MongoStorageAdapter",
    "get_database",
    "register_driver",
    "list_drivers",
    "get_driver",
]
