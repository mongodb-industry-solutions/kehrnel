"""Shortcut mapping service for slim search docs."""
from __future__ import annotations

from functools import lru_cache
from typing import Dict, List, Any

DEFAULT_DOC_ID = "shortcuts"
DEFAULT_COLLECTION = "_shortcuts"

# Minimal default shortcut mapping commonly used in slim docs
DEFAULT_SHORTCUTS = {
    "value": "v",
    "defining_code": "df",
    "code_string": "cs",
    "terminology_id": "ti",
    "value.value": "vv",
    "defining_code.code_string": "df.cs",
}


@lru_cache(maxsize=1)
def load_shortcuts(cfg_key: str | None = None) -> Dict[str, str]:
    # TODO: load from configured shortcuts dictionary if available
    return DEFAULT_SHORTCUTS


def canonical_to_slim(path: str, shortcuts: Dict[str, str] | None = None) -> str:
    """
    Convert a canonical path like data.value.defining_code.code_string to
    data.v.df.cs using shortcut map.
    """
    sc = shortcuts or DEFAULT_SHORTCUTS
    parts: List[str] = []
    for segment in path.split("."):
        parts.append(sc.get(segment, segment))
    slim = ".".join(parts)
    # second-pass replacements for combined keys
    for long, short in sc.items():
        slim = slim.replace(long, short)
    return slim


async def load_shortcuts_from_storage(storage, cache: Dict | None = None) -> Dict[str, str]:
    if cache and cache.get("shortcuts"):
        return cache["shortcuts"]
    if not storage:
        data = DEFAULT_SHORTCUTS
    else:
        doc = await storage.find_one(DEFAULT_COLLECTION, {"_id": DEFAULT_DOC_ID}) or {}
        items = doc.get("items") or {}
        # Support compatibility shapes with keys/values
        keys = doc.get("keys") or {}
        values = doc.get("values") or {}
        merged: Dict[str, str] = {}
        merged.update(items)
        merged.update(keys)
        merged.update(values)
        data = merged if merged else DEFAULT_SHORTCUTS
    if cache is not None:
        cache["shortcuts"] = data
    return data


async def get_shortcuts(ctx) -> Dict[str, Any]:
    cache = (ctx.meta or {}).get("dict_cache") if ctx else {}
    cached = cache.get("shortcuts") if cache else None
    if cached is not None:
        return {"items": cached, "source": "cache", "missing": False}
    storage = (ctx.adapters or {}).get("storage") if ctx else None
    missing = False
    items: Dict[str, str] = {}
    if storage:
        doc = await storage.find_one(DEFAULT_COLLECTION, {"_id": DEFAULT_DOC_ID}) or {}
        items = doc.get("items") or {}
        items.update(doc.get("keys") or {})
        items.update(doc.get("values") or {})
        if not items:
            missing = True
    else:
        items = DEFAULT_SHORTCUTS
        missing = True
    if cache is not None:
        cache["shortcuts"] = items
    return {"items": items, "source": "storage" if not missing else "defaults", "missing": missing}
