"""Memory-safe streaming reader for official SNOMED CT JSON arrays."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator


def iter_json_objects_from_top_array(path: str | Path, *, chunk_size: int = 1024 * 1024) -> Iterator[dict[str, Any]]:
    """
    Yield objects from a huge top-level JSON array without loading the full file.

    The Spain SNOMED JSON currently arrives as one top-level array with one
    concept object per item. This iterator tracks strings/escapes/braces and
    parses each object individually.
    """
    p = Path(path)
    in_obj = False
    depth = 0
    in_str = False
    esc = False
    buf: list[str] = []

    with p.open("r", encoding="utf-8") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            for ch in chunk:
                if not in_obj:
                    if ch == "{":
                        in_obj = True
                        depth = 1
                        in_str = False
                        esc = False
                        buf = ["{"]
                    continue

                buf.append(ch)
                if in_str:
                    if esc:
                        esc = False
                    elif ch == "\\":
                        esc = True
                    elif ch == '"':
                        in_str = False
                    continue

                if ch == '"':
                    in_str = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        yield json.loads("".join(buf))
                        in_obj = False
                        buf = []


def iter_concepts_from_json(path: str | Path, *, limit: int | None = None) -> Iterator[dict[str, Any]]:
    """Yield SNOMED concept objects from an official JSON file."""
    count = 0
    for concept in iter_json_objects_from_top_array(path):
        yield concept
        count += 1
        if limit is not None and count >= limit:
            break
