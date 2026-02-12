"""Codes service for RPS-Dual with storage-backed read-through cache."""
from __future__ import annotations

from functools import lru_cache
from typing import Dict, Optional, Any

DEFAULT_COLLECTION = "_codes"
DEFAULT_DOC_ID = "codes"


def _default_tokenize(code: str) -> int:
    """
    Deterministic, stable-ish tokenization: convert to a negative int to mirror
    existing atcode strategy (negative ints). Do NOT use Python hash (not stable).
    """
    total = 0
    for ch in code:
        total = (total * 33 + ord(ch)) % 100000
    return -total or -1


@lru_cache(maxsize=256)
def atcode_to_token(code: str) -> int:
    if not code:
        return -1
    return _default_tokenize(code)


@lru_cache(maxsize=256)
def archetype_to_token(code: str) -> int:
    if not code:
        return -1
    return _default_tokenize(code)


def composition_archetype_token(code: str) -> int:
    return archetype_to_token(code)


async def load_codes_from_storage(storage, cache: Dict | None = None) -> Dict[str, int]:
    if cache and cache.get("codes"):
        return cache["codes"]
    if not storage:
        data = {}
    else:
        doc = await storage.find_one(DEFAULT_COLLECTION, {"_id": DEFAULT_DOC_ID}) or {}
        data = doc.get("items") or doc.get("codes") or {}
    if cache is not None:
        cache["codes"] = data
    return data


async def get_codes(ctx) -> Dict[str, Any]:
    cache = (ctx.meta or {}).get("dict_cache") if ctx else {}
    cached = cache.get("codes") if cache else None
    if cached is not None:
        return {"items": cached, "source": "cache", "missing": False}
    storage = (ctx.adapters or {}).get("storage") if ctx else None
    missing = False
    items: Dict[str, int] = {}
    if storage:
        doc = await storage.find_one(DEFAULT_COLLECTION, {"_id": DEFAULT_DOC_ID}) or {}
        items = doc.get("items") or {}
        if not items:
            missing = True
    else:
        missing = True
    if not items:
        items = {}
    if cache is not None:
        cache["codes"] = items
    return {"items": items, "source": "storage" if not missing else "defaults", "missing": missing}


async def load_codes_fixture(path) -> Dict[str, Any]:
    import json
    return json.loads(path.read_text(encoding="utf-8"))
