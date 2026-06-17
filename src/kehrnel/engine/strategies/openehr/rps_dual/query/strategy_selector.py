from __future__ import annotations

from typing import Any, Dict, Iterable, List

from .contains_clause import is_match_friendly_contains_clause

_MATCH_FRIENDLY_OPERATORS = {"=", "!=", ">", "<", ">=", "<="}


def _iter_condition_nodes(node: Dict[str, Any] | None) -> Iterable[Dict[str, Any]]:
    if not isinstance(node, dict) or not node:
        return

    operator = str(node.get("operator") or "").upper()
    if operator in {"AND", "OR"}:
        conditions = node.get("conditions")
        if isinstance(conditions, dict):
            for child in conditions.values():
                yield from _iter_condition_nodes(child)
        children = node.get("children")
        if isinstance(children, list):
            for child in children:
                yield from _iter_condition_nodes(child)
        return

    if node.get("path") and node.get("operator"):
        yield node


def _iter_order_paths(order_by: Dict[str, Any] | None) -> Iterable[str]:
    if not isinstance(order_by, dict):
        return
    columns = order_by.get("columns")
    if not isinstance(columns, dict):
        return
    for col in columns.values():
        if isinstance(col, dict):
            path = col.get("path")
            if isinstance(path, str) and path.strip():
                yield path.strip()

def should_prefer_match_for_cross_patient_ast(
    ast: Dict[str, Any],
    *,
    ehr_alias: str = "e",
    composition_alias: str = "c",
    version_alias: str = "v",
) -> bool:
    """
    Prefer the flattened collection's $match path for cross-patient queries when
    the query shape is limited to predicates we expect to be backed by B-tree
    indexes on the composition store.
    """
    if not isinstance(ast, dict) or not ast:
        return False

    if ast.get("let"):
        return False

    allowed_where_paths = {
        f"{ehr_alias}/ehr_id/value",
        f"{composition_alias}/uid/value",
        f"{composition_alias}/archetype_details/template_id/value",
        f"{composition_alias}/archetype_node_id",
        f"{version_alias}/commit_audit/time_committed/value",
    }
    allowed_order_paths = {
        f"{ehr_alias}/ehr_id/value",
        f"{composition_alias}/uid/value",
        f"{version_alias}/commit_audit/time_committed/value",
    }

    where_conditions: List[Dict[str, Any]] = list(_iter_condition_nodes(ast.get("where")))
    for condition in where_conditions:
        operator = str(condition.get("operator") or "").upper()
        path = str(condition.get("path") or "").strip()
        if operator not in _MATCH_FRIENDLY_OPERATORS:
            return False
        if path not in allowed_where_paths:
            return False

    for path in _iter_order_paths(ast.get("orderBy")):
        if path not in allowed_order_paths:
            return False

    contains_clause = ast.get("contains")
    if contains_clause and not is_match_friendly_contains_clause(contains_clause):
        return False

    return bool(where_conditions) or is_match_friendly_contains_clause(contains_clause)
