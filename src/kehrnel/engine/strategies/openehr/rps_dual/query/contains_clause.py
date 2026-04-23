from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def _is_supported_contains_predicate(predicate: Any) -> bool:
    if predicate is None:
        return True
    if not isinstance(predicate, dict):
        return False
    return (
        predicate.get("path") == "archetype_node_id"
        and predicate.get("operator") == "="
        and bool(predicate.get("value"))
    )


def collect_archetype_contains_chain(contains_clause: Dict[str, Any] | None) -> Optional[List[Dict[str, str]]]:
    """
    Collect a linear CONTAINS chain made of archetype predicates.

    We support chains such as:
    VERSION -> COMPOSITION[...] -> SECTION[...] -> EVALUATION[...] -> CLUSTER[...]

    The returned list only includes nodes that carry an archetype predicate, in
    root-to-leaf order. If the shape is not a simple linear chain with
    archetype_node_id predicates, returns None.
    """
    if not isinstance(contains_clause, dict) or not contains_clause:
        return None

    chain: List[Dict[str, str]] = []
    current = contains_clause
    seen_composition = False

    while isinstance(current, dict) and current:
        rm_type = str(current.get("rmType") or "").upper()
        predicate = current.get("predicate")
        if not _is_supported_contains_predicate(predicate):
            return None

        if rm_type == "COMPOSITION":
            seen_composition = True

        if isinstance(predicate, dict) and predicate.get("value"):
            chain.append(
                {
                    "alias": str(current.get("alias") or ""),
                    "rmType": rm_type,
                    "archetype_id": str(predicate["value"]),
                }
            )

        child = current.get("contains")
        if child is None:
            break
        if not isinstance(child, dict):
            return None
        current = child

    if not seen_composition:
        return None
    return chain


def is_match_friendly_contains_clause(contains_clause: Dict[str, Any] | None) -> bool:
    return collect_archetype_contains_chain(contains_clause) is not None


def has_nested_contains_clause(contains_clause: Dict[str, Any] | None) -> bool:
    chain = collect_archetype_contains_chain(contains_clause)
    return bool(chain and len(chain) > 1)


def _iter_referenced_paths(ast: Dict[str, Any] | None) -> List[str]:
    if not isinstance(ast, dict):
        return []

    paths: List[str] = []

    columns = ((ast.get("select") or {}).get("columns") if isinstance(ast.get("select"), dict) else None) or {}
    if isinstance(columns, dict):
        for col in columns.values():
            if not isinstance(col, dict):
                continue
            value = col.get("value")
            if isinstance(value, dict):
                path = value.get("path")
                if isinstance(path, str) and path.strip():
                    paths.append(path.strip())

    order_columns = ((ast.get("orderBy") or {}).get("columns") if isinstance(ast.get("orderBy"), dict) else None) or {}
    if isinstance(order_columns, dict):
        for col in order_columns.values():
            if not isinstance(col, dict):
                continue
            path = col.get("path")
            if isinstance(path, str) and path.strip():
                paths.append(path.strip())

    return paths


def _extract_path_alias(path: str) -> Optional[str]:
    if not isinstance(path, str) or "/" not in path:
        return None
    alias = path.split("/", 1)[0].strip()
    return alias or None


def find_deepest_referenced_alias(
    ast: Dict[str, Any] | None,
    contains_clause: Dict[str, Any] | None,
    *,
    composition_alias: str,
) -> Optional[str]:
    chain = collect_archetype_contains_chain(contains_clause)
    if not chain or len(chain) < 2:
        return None

    referenced_aliases = {
        alias
        for alias in (_extract_path_alias(path) for path in _iter_referenced_paths(ast))
        if alias and alias != composition_alias
    }
    if not referenced_aliases:
        return None

    for entry in reversed(chain[1:]):
        alias = entry.get("alias")
        if alias and alias in referenced_aliases:
            return alias
    return None


def build_shortened_ancestry_regex(codes: List[Any], separator: str) -> Optional[str]:
    if not codes:
        return None

    normalized = [str(code) for code in codes]
    if len(normalized) == 1:
        return f"^{re.escape(normalized[0])}$"

    escaped_sep = re.escape(separator)
    not_sep = rf"[^{re.escape(separator)}]+"
    reversed_codes = list(reversed(normalized))

    pattern = "^" + re.escape(reversed_codes[0])
    for code in reversed_codes[1:]:
        pattern += rf"(?:{escaped_sep}{not_sep})*{escaped_sep}{re.escape(code)}"
    pattern += "$"
    return pattern


async def build_shortened_contains_condition(
    contains_clause: Dict[str, Any] | None,
    archetype_resolver: Any,
    *,
    path_field: str,
    data_field: str,
    separator: str,
    nested_only: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Build a shortened-model $elemMatch for a linear CONTAINS chain.

    The shortened IBM/openEHR path stores nodes in child-to-root order, possibly
    with AT-code intermediates between archetype nodes. We therefore match the
    deepest archetype and require its ancestors to appear later in the path in
    the same order, allowing extra segments in between.
    """
    chain = collect_archetype_contains_chain(contains_clause)
    if not chain or archetype_resolver is None:
        return None
    if nested_only and len(chain) < 2:
        return None

    codes: List[Any] = []
    for entry in chain:
        code = await archetype_resolver.get_archetype_code(entry["archetype_id"])
        if code is None:
            return None
        codes.append(code)

    deepest_code = codes[-1]
    if len(codes) == 1:
        return {
            "$elemMatch": {
                path_field: str(deepest_code),
                f"{data_field}.ani": deepest_code,
            }
        }

    regex = build_shortened_ancestry_regex(codes, separator)
    if not regex:
        return None

    return {
        "$elemMatch": {
            path_field: {"$regex": regex},
            f"{data_field}.ani": deepest_code,
        }
    }


async def build_shortened_row_fanout_spec(
    ast: Dict[str, Any] | None,
    contains_clause: Dict[str, Any] | None,
    *,
    composition_alias: str,
    archetype_resolver: Any,
    separator: str,
) -> Optional[Dict[str, Any]]:
    if archetype_resolver is None:
        return None

    chain = collect_archetype_contains_chain(contains_clause)
    if not chain or len(chain) < 2:
        return None

    target_alias = find_deepest_referenced_alias(
        ast,
        contains_clause,
        composition_alias=composition_alias,
    )
    if not target_alias:
        return None

    target_idx = next((idx for idx, entry in enumerate(chain) if entry.get("alias") == target_alias), -1)
    if target_idx <= 0:
        return None

    enriched_chain: List[Dict[str, str]] = []
    codes: List[str] = []
    for entry in chain[: target_idx + 1]:
        code = await archetype_resolver.get_archetype_code(entry["archetype_id"])
        if code is None:
            return None
        code_str = str(code)
        enriched_chain.append({**entry, "code": code_str})
        codes.append(code_str)

    target_regex = build_shortened_ancestry_regex(codes, separator)
    if not target_regex:
        return None

    return {
        "target_alias": target_alias,
        "target_regex": target_regex,
        "aliases": [entry["alias"] for entry in enriched_chain if entry.get("alias")],
        "alias_codes": {
            entry["alias"]: entry["code"]
            for entry in enriched_chain
            if entry.get("alias")
        },
        "chain": enriched_chain,
    }
