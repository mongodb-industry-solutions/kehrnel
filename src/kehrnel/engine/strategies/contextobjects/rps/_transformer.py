"""
_transformer.py — ContextObjects RPS storage transformer
=========================================================

Converts an in-memory ContextObject instance into the two MongoDB documents
used by the RPS dual-collection layout:

  primary doc  → context_objects        (full object + flattened content nodes cn[])
  search doc   → context_objects_search  (denormalised path→value pairs sn[])

Field names come from the strategy config (defaults: ot, rid, ver, cn, sn, meta).
"""

from __future__ import annotations

import datetime
import hashlib
import json
from typing import Any


def _cfg_field(config: dict, *keys: str, default: str) -> str:
    """Walk config dict with a dotted path, return default if missing."""
    cur: Any = config or {}
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
    return str(cur) if cur else default


def resolve_field_names(config: dict) -> dict:
    """Extract field name shorthands from strategy config (with defaults)."""
    f = config.get("fields") or {}
    nf = config.get("node_fields") or {}
    return {
        "OT":   f.get("object_type", "ot"),
        "RID":  f.get("record_id", "rid"),
        "VER":  f.get("version", "ver"),
        "CN":   f.get("content_nodes", "cn"),
        "SN":   f.get("search_nodes", "sn"),
        "META": f.get("meta", "meta"),
        "P":    nf.get("path", "p"),
        "NID":  nf.get("node_id", "nid"),
        "ANC":  nf.get("ancestors", "anc"),
        "V":    nf.get("value", "v"),
        "DT":   nf.get("data_type", "dt"),
    }


def resolve_collection_names(config: dict) -> tuple[str, str]:
    """Return (objects_collection, search_collection) from config."""
    colls = config.get("collections") or {}
    objects_coll = (colls.get("objects") or {}).get("name") or "context_objects"
    search_coll  = (colls.get("search")  or {}).get("name") or "context_objects_search"
    return objects_coll, search_coll


def transform(instance: dict, config: dict | None = None) -> tuple[dict, dict]:
    """Return (primary_doc, search_doc) ready for MongoDB upsert.

    Args:
        instance: Raw ContextObject dict (objectType, recordId, version, data, meta).
        config:   Strategy config dict (controls field name shorthands).

    Returns:
        (primary_doc, search_doc) — both are plain dicts.

    Raises:
        ValueError: if objectType or recordId is missing.
    """
    cfg    = config or {}
    fn     = resolve_field_names(cfg)

    object_type = str(instance.get("objectType") or "").strip()
    record_id   = str(instance.get("recordId")   or "").strip()

    if not object_type:
        raise ValueError("instance.objectType is required")
    if not record_id:
        raise ValueError("instance.recordId is required")

    version = str(instance.get("version") or "1.0")
    data    = instance.get("data") or {}
    meta    = dict(instance.get("meta") or {})
    if "created" not in meta:
        meta["created"] = datetime.datetime.utcnow().isoformat() + "Z"

    nodes = extract_nodes(data, fn)

    primary_doc = {
        fn["OT"]:   object_type,
        fn["RID"]:  record_id,
        fn["VER"]:  version,
        fn["CN"]:   nodes,
        fn["META"]: meta,
    }

    search_doc = {
        fn["RID"]: record_id,
        fn["OT"]:  object_type,
        fn["SN"]: [
            {
                fn["P"]:   n[fn["P"]],
                fn["NID"]: n[fn["NID"]],
                fn["V"]:   n[fn["V"]],
                fn["DT"]:  n[fn["DT"]],
            }
            for n in nodes
            if n.get(fn["V"]) is not None
        ],
    }

    return primary_doc, search_doc


def extract_nodes(data: dict, fn: dict | None = None) -> list[dict]:
    """Recursively walk data and produce a flat list of content-node dicts."""
    if fn is None:
        fn = resolve_field_names({})
    nodes: list[dict] = []
    _walk(data, prefix="/data", ancestors=[], out=nodes, fn=fn)
    return nodes


def _walk(node: dict, prefix: str, ancestors: list[str], out: list[dict], fn: dict) -> None:
    for node_id, child in node.items():
        if not isinstance(child, dict):
            continue

        path  = f"{prefix}[{node_id}]"
        value, dtype = _extract_leaf(child)
        has_sub      = any(k for k in child if not k.startswith("_"))

        out.append({
            fn["P"]:   path,
            fn["NID"]: node_id,
            fn["ANC"]: list(ancestors),
            fn["V"]:   value,
            fn["DT"]:  dtype,
        })

        if has_sub:
            _walk(
                {k: v for k, v in child.items() if not k.startswith("_")},
                prefix=path,
                ancestors=ancestors + [node_id],
                out=out,
                fn=fn,
            )


def _extract_leaf(child: dict) -> tuple[Any, str]:
    raw_val  = child.get("_value")
    raw_type = str(child.get("_type") or "").lower().strip()

    if raw_val is None:
        return None, "object"

    dtype = raw_type or _infer_type(raw_val)
    value = _coerce_value(raw_val, dtype, child)
    return value, dtype


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        if _looks_like_datetime(value):
            return "datetime"
        return "string"
    return "object"


def _coerce_value(raw: Any, dtype: str, node: dict) -> Any:
    if dtype == "coded_text":
        return {"text": str(raw), "code": node.get("_code"), "system": node.get("_system")}
    if dtype == "quantity":
        return {"value": raw, "unit": node.get("_unit")}
    if dtype == "integer":
        try:
            return int(raw)
        except (TypeError, ValueError):
            return raw
    if dtype == "number":
        try:
            return float(raw)
        except (TypeError, ValueError):
            return raw
    return raw


def _looks_like_datetime(s: str) -> bool:
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            datetime.datetime.strptime(s[:19], fmt[:len(fmt)])
            return True
        except ValueError:
            continue
    return False


def content_hash(instance: dict) -> str:
    """SHA-256 of canonical JSON of instance.data (for deduplication)."""
    payload = json.dumps(instance.get("data") or {}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]
