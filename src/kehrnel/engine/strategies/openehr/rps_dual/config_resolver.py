"""Resolve strategy config URIs (file://, collection://) into concrete payloads.

This module is intentionally small and dependency-free. It exists primarily to:
- load pack assets referenced from config (e.g. dictionaries, shortcuts, mappings)
- optionally load seed documents from MongoDB collections
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union
from urllib.parse import parse_qsl

import yaml


def _strip_jsonc(text: str) -> str:
    """Best-effort JSONC -> JSON conversion.

    We remove // line comments and /* */ blocks outside of strings.
    """

    out: list[str] = []
    i = 0
    n = len(text)
    in_str = False
    esc = False
    while i < n:
        ch = text[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            i += 1
            continue

        if ch == '"':
            in_str = True
            out.append(ch)
            i += 1
            continue

        # line comment
        if ch == "/" and i + 1 < n and text[i + 1] == "/":
            i += 2
            while i < n and text[i] not in ("\n", "\r"):
                i += 1
            continue

        # block comment
        if ch == "/" and i + 1 < n and text[i + 1] == "*":
            i += 2
            while i + 1 < n and not (text[i] == "*" and text[i + 1] == "/"):
                i += 1
            i += 2 if i + 1 < n else 0
            continue

        out.append(ch)
        i += 1
    return "".join(out)


def _load_text_payload(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        return yaml.safe_load(raw)
    if suffix in {".json", ".jsonc"}:
        try:
            return json.loads(raw)
        except Exception:
            return json.loads(_strip_jsonc(raw))
    # fallback: try yaml, then json
    try:
        return yaml.safe_load(raw)
    except Exception:
        return json.loads(raw)


def _resolve_file_uri(uri: str, base_dir: Path) -> Any:
    rel = uri[len("file://") :]
    p = (base_dir / rel).resolve()
    # prevent accidental escape
    base = base_dir.resolve()
    if base not in p.parents and p != base:
        raise ValueError("file:// path escapes strategy root")
    if not p.exists():
        raise FileNotFoundError(str(p))
    return _load_text_payload(p)


def _parse_collection_uri(uri: str) -> tuple[str, Dict[str, Any]]:
    # collection://name?key=value&k2=v2
    rest = uri[len("collection://") :]
    if not rest:
        raise ValueError("collection:// URI missing collection name")
    if "?" in rest:
        name, query = rest.split("?", 1)
    else:
        name, query = rest, ""
    name = name.strip()
    if not name:
        raise ValueError("collection:// URI missing collection name")

    filt: Dict[str, Any] = {}
    for k, v in parse_qsl(query, keep_blank_values=False):
        if not k:
            continue
        # best-effort numeric conversion
        if re.fullmatch(r"-?[0-9]+", v or ""):
            filt[k] = int(v)
        else:
            filt[k] = v
    return name, filt


def resolve_uri(
    ref: Union[str, Dict[str, Any], list, None],
    db: Any = None,
    base_dir: Optional[Path] = None,
) -> Any:
    if ref is None:
        return None
    if isinstance(ref, (dict, list)):
        return ref
    if not isinstance(ref, str):
        raise TypeError("URI reference must be a string, object, list, or null")
    if not base_dir:
        base_dir = Path.cwd()

    if ref.startswith("file://"):
        return _resolve_file_uri(ref, base_dir)
    if ref.startswith("collection://"):
        if db is None:
            raise ValueError("collection:// requires a MongoDB database handle")
        coll, filt = _parse_collection_uri(ref)
        if not filt:
            raise ValueError("collection:// URI requires a filter (e.g. collection://name?_id=docId)")
        return db[coll].find_one(filt)
    raise ValueError("Unsupported reference; expected file:// or collection://")


async def resolve_uri_async(
    ref: Union[str, Dict[str, Any], list, None],
    db: Any = None,
    base_dir: Optional[Path] = None,
) -> Any:
    if ref is None:
        return None
    if isinstance(ref, (dict, list)):
        return ref
    if not isinstance(ref, str):
        raise TypeError("URI reference must be a string, object, list, or null")
    if not base_dir:
        base_dir = Path.cwd()

    if ref.startswith("file://"):
        return _resolve_file_uri(ref, base_dir)
    if ref.startswith("collection://"):
        if db is None:
            raise ValueError("collection:// requires a MongoDB database handle")
        coll, filt = _parse_collection_uri(ref)
        if not filt:
            raise ValueError("collection:// URI requires a filter (e.g. collection://name?_id=docId)")
        return await db[coll].find_one(filt)
    raise ValueError("Unsupported reference; expected file:// or collection://")


__all__ = ["resolve_uri", "resolve_uri_async"]
