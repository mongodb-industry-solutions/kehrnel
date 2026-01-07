"""Compatibility shim to satisfy legacy transformer imports, fed by normalized config."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict

settings = None  # will be configured per compile


class SearchConfig(SimpleNamespace):
    def __init__(self, search_index_name: str = "search_nodes_index", flatten_collection: str = "compositions_search"):
        super().__init__(search_index_name=search_index_name, flatten_collection=flatten_collection)


class Settings(SimpleNamespace):
    def __init__(self, search_config: SearchConfig):
        super().__init__(search_config=search_config)


def configure(cfg: Dict[str, Any]):
    global settings
    coll = (cfg.get("collections") or {}).get("search", {}) if isinstance(cfg, dict) else {}
    idx = coll.get("atlas_index_name", "search_nodes_index")
    # flatten collection should be the canonical compositions collection
    flatten = (cfg.get("collections") or {}).get("compositions", {}).get("name", "compositions")
    settings = Settings(SearchConfig(search_index_name=idx, flatten_collection=flatten))
