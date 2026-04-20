"""Codes service for RPS-Dual with storage-backed read-through cache."""
from __future__ import annotations

from functools import lru_cache
from typing import Dict, Optional, Any

DEFAULT_COLLECTION = "_codes"
DEFAULT_DOC_ID = "ar_code"


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
        data = _extract_code_items(doc)
    if cache is not None:
        cache["codes"] = data
    return data


def _resolve_codes_source(ctx) -> tuple[str, str]:
    cfg = getattr(ctx, "config", None) if ctx else None
    collections = cfg.get("collections") if isinstance(cfg, dict) else {}
    collections = collections if isinstance(collections, dict) else {}
    coding = cfg.get("coding") if isinstance(cfg, dict) else {}
    coding = coding if isinstance(coding, dict) else {}
    dictionaries = cfg.get("dictionaries") if isinstance(cfg, dict) else {}
    dictionaries = dictionaries if isinstance(dictionaries, dict) else {}

    codes_collection = (
        ((collections.get("codes") or {}).get("name") if isinstance(collections.get("codes"), dict) else None)
        or DEFAULT_COLLECTION
    )
    codes_doc_id = (
        ((coding.get("archetype_ids") or {}).get("dictionary") if isinstance(coding.get("archetype_ids"), dict) else None)
        or ((dictionaries.get("arcodes") or {}).get("doc_id") if isinstance(dictionaries.get("arcodes"), dict) else None)
        or DEFAULT_DOC_ID
    )
    return codes_collection, codes_doc_id


def _extract_code_items(doc: Dict[str, Any] | None) -> Dict[str, int]:
    if not isinstance(doc, dict):
        return {}

    explicit = doc.get("items") or doc.get("codes")
    if isinstance(explicit, dict) and explicit:
        return explicit

    ar_book: Dict[str, int] = {}
    for rm, subtree in doc.items():
        if rm in ("_id", "_max", "_min", "unknown", "at"):
            continue
        if not isinstance(subtree, dict):
            continue
        for name, versions in subtree.items():
            if not isinstance(versions, dict):
                continue
            for version, code in versions.items():
                ar_book[f"{rm}.{name}.{version}"] = code

    at_book = doc.get("at") if isinstance(doc.get("at"), dict) else {}
    return {**ar_book, **at_book}


async def get_codes(ctx) -> Dict[str, Any]:
    cache = (ctx.meta or {}).get("dict_cache") if ctx else {}
    cached = cache.get("codes") if cache else None
    if cached is not None:
        return {"items": cached, "source": "cache", "missing": False}
    storage = (ctx.adapters or {}).get("storage") if ctx else None
    missing = False
    items: Dict[str, int] = {}
    if storage:
        collection, doc_id = _resolve_codes_source(ctx)
        doc = await storage.find_one(collection, {"_id": doc_id}) or {}
        items = _extract_code_items(doc)
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
