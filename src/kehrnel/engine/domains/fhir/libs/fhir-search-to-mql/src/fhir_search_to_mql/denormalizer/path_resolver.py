"""
Lightweight FHIRPath-style resolver used by data-type extractors.

We do NOT implement full FHIRPath. The grammar supported here is the small
subset that denormalization rules realistically need:

  - Dot notation:                ``code.coding.system``
  - Array iteration:             ``component[*].code``
  - Implicit single → list:      ``identifier.value`` works whether
                                 ``identifier`` is a single object or a list.
  - Path union:                  ``code | component[*].code``

The resolver always returns a *flat* list of leaf values (or an empty list)
so callers can iterate without special-casing scalar vs. array sources.

This module is intentionally framework-agnostic — it does not import any
extractor or denormalizer types. Extractors compose it on top of their
existing data-type-specific logic so the per-extractor contract stays
``(value, field_mappings) -> dict``.
"""

from __future__ import annotations

from typing import Any, Iterable, List


def expand_legacy_fhir_paths(expression: str) -> str:
    """
    Add union branches so denormalization tolerates R4 / legacy payloads
  alongside FHIR R5 ``CodeableReference`` shapes (and nested References).

    * ``…[*].concept.coding…`` also tries ``…[*].coding…`` (R4 flat
      ``CodeableConcept`` on the same element, e.g. ``serviceType``).
    * ``….reference.reference`` also tries ``….reference`` (R5
      ``CodeableReference.reference`` is a ``Reference`` object).
    """
    parts = [p.strip() for p in expression.split("|") if p.strip()]
    if not parts:
        return expression

    expanded: list[str] = []
    seen: set[str] = set()
    for part in parts:
        for branch in _legacy_branches_for_path(part):
            if branch not in seen:
                seen.add(branch)
                expanded.append(branch)
    return " | ".join(expanded) if expanded else expression


def _legacy_branches_for_path(path: str) -> list[str]:
    branches = [path]
    if ".concept." in path:
        legacy = path.replace(".concept.", ".")
        if legacy != path:
            branches.append(legacy)
    if path.endswith(".reference.reference"):
        shorter = path[: -len(".reference")]
        if shorter not in branches:
            branches.append(shorter)
    return branches


def resolve_path(root: Any, expression: str, *, _expand_legacy: bool = True) -> List[Any]:
    """
    Resolve a path expression against ``root``.

    Args:
        root: Any nested combination of dicts and lists. For denormalization
            this is typically the full FHIR resource.
        expression: A path expression in the supported subset (see module
            docstring). Whitespace around segments and around ``|`` is
            tolerated. R4 legacy branches are added automatically for common
            ``CodeableReference`` patterns (see ``expand_legacy_fhir_paths``).

    Returns:
        Flat list of leaf values matching the expression. Missing fields,
        ``None`` values, and out-of-range indices return ``[]`` for that
        branch — they never raise.
    """
    if root is None or not expression:
        return []

    expression = expression.strip()
    if _expand_legacy:
        expression = expand_legacy_fhir_paths(expression)
    if not expression:
        return []

    # Path union — evaluate each branch independently and concatenate.
    if "|" in expression:
        out: List[Any] = []
        for branch in expression.split("|"):
            out.extend(resolve_path(root, branch.strip(), _expand_legacy=False))
        return out

    # Single branch: walk dot/array segments left to right.
    current: List[Any] = [root]
    for segment in _split_segments(expression):
        current = list(_step(current, segment))
        if not current:
            return []
    return [v for v in current if v is not None]


def _split_segments(expression: str) -> List[str]:
    """Split ``a.b[*].c`` into ``["a", "b[*]", "c"]`` (no empty parts)."""
    return [seg for seg in expression.split(".") if seg]


def _step(values: Iterable[Any], segment: str) -> Iterable[Any]:
    """
    Apply one segment of the path to every value in ``values``.

    Segments may end in ``[*]`` to flatten an array. Otherwise the segment
    must be a key on a dict; dicts that don't contain that key contribute
    nothing.
    """
    iterate = segment.endswith("[*]")
    key = segment[:-3] if iterate else segment

    for v in values:
        if v is None:
            continue
        if isinstance(v, dict):
            if not key:
                # Bare ``[*]`` at the start of an expression — flatten the
                # current value (if it's a list) without changing keys.
                child = v
            else:
                if key not in v:
                    continue
                child = v[key]
        elif isinstance(v, list) and not key:
            # Bare ``[*]`` applied to a list: flatten one level.
            yield from v
            continue
        else:
            continue

        if iterate:
            if isinstance(child, list):
                for item in child:
                    if item is not None:
                        yield item
            elif child is not None:
                yield child
        else:
            if isinstance(child, list):
                # Implicit auto-flatten for single→list FHIR cardinality.
                for item in child:
                    if item is not None:
                        yield item
            elif child is not None:
                yield child


def looks_like_resource(value: Any) -> bool:
    """
    Heuristic: a value is a "full FHIR resource" if it's a dict with the
    canonical ``resourceType`` discriminator. Extractors use this to decide
    whether their incoming value should be treated as a typed payload
    (existing behavior) or walked via the path resolver (new behavior for
    ``source: $resource`` rules).
    """
    return isinstance(value, dict) and isinstance(value.get("resourceType"), str)


def is_path_expression(source_path: str | None) -> bool:
    """
    True if ``source_path`` looks like a navigable expression rather than a
    descriptive label. The denormalization rules in this codebase
    historically used ``source_path`` as a metadata hint (e.g.
    ``"name[*].family"``); the resolver only kicks in when the rule's
    ``source`` is ``$resource``, so this helper is mostly a defensive guard.
    """
    if not source_path:
        return False
    return any(token in source_path for token in (".", "[*]", "|"))
