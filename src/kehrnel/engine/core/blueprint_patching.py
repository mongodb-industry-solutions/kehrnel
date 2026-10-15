"""Deterministic RFC 6901/6902 subset used for Solution Blueprint edits.

Only :class:`~kehrnel.contracts.source_architecture.BlueprintOverride` values
reach this module, so the editable roots and supported operations have already
been constrained by the public contract.  The functions here deliberately
operate on an explicitly supplied document and have no persistence or runtime
side effects.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable

from kehrnel.contracts.source_architecture import (
    BlueprintOverride,
    BlueprintOverrideOperation,
)


def pointer_tokens(pointer: str) -> list[str]:
    """Decode an already validated RFC 6901 JSON Pointer."""

    return [
        token.replace("~1", "/").replace("~0", "~")
        for token in pointer[1:].split("/")
    ]


def list_index(token: str, size: int, *, allow_end: bool) -> int:
    """Resolve one canonical JSON Patch list index.

    Leading-zero indexes are rejected so the same pointer cannot have several
    textual representations.  ``-`` is accepted only for an ``add`` leaf.
    """

    if token == "-" and allow_end:
        return size
    if not token.isdigit() or (len(token) > 1 and token.startswith("0")):
        raise ValueError("invalid JSON Pointer array index")
    index = int(token)
    upper_bound = size if allow_end else size - 1
    if index < 0 or index > upper_bound:
        raise IndexError("JSON Pointer array index is out of range")
    return index


def pointer_exists(document: Any, pointer: str) -> bool:
    """Return whether ``pointer`` resolves against ``document``."""

    if pointer == "":
        return True
    current = document
    try:
        for token in pointer_tokens(pointer):
            if isinstance(current, dict):
                if token not in current:
                    return False
                current = current[token]
            elif isinstance(current, list):
                current = current[list_index(token, len(current), allow_end=False)]
            else:
                return False
    except (IndexError, ValueError):
        return False
    return True


def pointers_overlap(left: str, right: str) -> bool:
    """Return whether either pointer addresses an ancestor of the other."""

    left_parts = pointer_tokens(left) if left else []
    right_parts = pointer_tokens(right) if right else []
    common_length = min(len(left_parts), len(right_parts))
    return left_parts[:common_length] == right_parts[:common_length]


def apply_blueprint_override(
    document: dict[str, Any],
    override: BlueprintOverride,
) -> None:
    """Apply one validated Blueprint override to ``document`` in place."""

    tokens = pointer_tokens(override.path)
    parent: Any = document
    for token in tokens[:-1]:
        if isinstance(parent, dict):
            if token not in parent:
                raise KeyError(token)
            parent = parent[token]
        elif isinstance(parent, list):
            parent = parent[list_index(token, len(parent), allow_end=False)]
        else:
            raise TypeError("JSON Pointer traverses a scalar")

    leaf = tokens[-1]
    value = deepcopy(override.value)
    if isinstance(parent, dict):
        exists = leaf in parent
        if override.operation is BlueprintOverrideOperation.ADD:
            parent[leaf] = value
        elif override.operation is BlueprintOverrideOperation.REPLACE:
            if not exists:
                raise KeyError(leaf)
            parent[leaf] = value
        else:
            if not exists:
                raise KeyError(leaf)
            del parent[leaf]
        return

    if isinstance(parent, list):
        if override.operation is BlueprintOverrideOperation.ADD:
            index = list_index(leaf, len(parent), allow_end=True)
            parent.insert(index, value)
        else:
            index = list_index(leaf, len(parent), allow_end=False)
            if override.operation is BlueprintOverrideOperation.REPLACE:
                parent[index] = value
            else:
                del parent[index]
        return
    raise TypeError("JSON Pointer parent is a scalar")


def apply_blueprint_overrides(
    document: dict[str, Any],
    overrides: Iterable[BlueprintOverride],
) -> dict[str, Any]:
    """Return a deep-copied document with ``overrides`` applied in order."""

    candidate = deepcopy(document)
    for override in overrides:
        apply_blueprint_override(candidate, override)
    return candidate
