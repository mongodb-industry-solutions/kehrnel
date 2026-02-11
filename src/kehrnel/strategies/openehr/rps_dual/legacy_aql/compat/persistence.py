"""Compatibility shim for legacy transformers to get strategy config."""
from __future__ import annotations

from typing import Any, Dict

from kehrnel.persistence import PersistenceStrategy

DEFAULT_STRATEGY: PersistenceStrategy | None = None


def configure(cfg: Dict[str, Any]):
    global DEFAULT_STRATEGY
    try:
        # map to expected shape
        collections = cfg.get("collections", {})
        fields = cfg.get("fields", {})
        strategy_dict = {
            "collections": {
                "compositions": collections.get("compositions", {}),
                "search": collections.get("search", {}),
            },
            "fields": {
                "composition": fields.get("composition", {}),
                "search": fields.get("search", {}),
            },
        }
        DEFAULT_STRATEGY = PersistenceStrategy(**strategy_dict)
    except Exception:
        DEFAULT_STRATEGY = PersistenceStrategy()


def get_default_strategy() -> PersistenceStrategy:
    return DEFAULT_STRATEGY or PersistenceStrategy()
