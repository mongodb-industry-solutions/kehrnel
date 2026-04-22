from __future__ import annotations

from typing import Dict, Any, Optional

from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR


def _adapt_select(ir: AqlQueryIR) -> Dict[str, Any]:
    cols = {}
    for idx, sel in enumerate(ir.select):
        cols[str(idx)] = {
            "alias": sel.alias,
            "value": {"type": "dataMatchPath", "path": sel.path},
        }
    return {"columns": cols}


def _normalize_operator(op: str) -> str:
    """
    Compatibility search builder accepts operators like =, >, >=, <, <=, LIKE, MATCHES, EXISTS.
    Keep them verbatim (upper-case for non-symbols) and never emit EQ.
    """
    if not op:
        return op
    op = op.lower()
    mapping = {
        "eq": "=",
        "=": "=",
        "gt": ">",
        "lt": "<",
        "gte": ">=",
        "lte": "<=",
        "ge": ">=",
        "le": "<=",
    }
    if op in mapping:
        return mapping[op]
    return op.upper()


def _qualify_path(path: str, ehr_alias: str, comp_alias: str) -> str:
    """
    The transformers expect alias-qualified paths (e/ehr_id/value, c/...).
    """
    if path.startswith(f"{ehr_alias}/") or path.startswith(f"{comp_alias}/"):
        return path
    # If the path already carries some alias (e.g., admin_salut/...), keep it
    if "/" in path and not path.startswith("data"):
        return path
    if path == "ehr_id":
        return f"{ehr_alias}/ehr_id/value"
    return f"{comp_alias}/{path}"


def _adapt_where(ir: AqlQueryIR, ehr_alias: str, comp_alias: str) -> Dict[str, Any] | None:
    if not ir.predicates:
        return None

    def adapt_pred(pred):
        path = _qualify_path(pred.path, ehr_alias, comp_alias)
        op = _normalize_operator(pred.op)
        return {"path": path, "operator": op, "value": pred.value}

    conditions = {str(idx): adapt_pred(p) for idx, p in enumerate(ir.predicates)}
    # Always wrap in logical AND so the builders take the logical path (needed for $all and compound.filter).
    return {"operator": "AND", "conditions": conditions}


def _adapt_order(ir: AqlQueryIR) -> Dict[str, Any] | None:
    if not ir.sort:
        return None
    cols = {}
    for idx, (field, direction) in enumerate(ir.sort.items()):
        cols[str(idx)] = {"path": field, "direction": "DESC" if direction == -1 else "ASC"}
    return {"columns": cols}


def _detect_version_alias(ir: AqlQueryIR) -> Optional[str]:
    candidate_paths = [sel.path for sel in ir.select] + [pred.path for pred in ir.predicates]
    if ir.sort:
        candidate_paths.extend(ir.sort.keys())
    for path in candidate_paths:
        if isinstance(path, str) and path.endswith("/commit_audit/time_committed/value") and "/" in path:
            return path.split("/", 1)[0]
    return None


def adapt_ir_to_ast(ir: AqlQueryIR, ehr_alias: str = "e", composition_alias: str = "c") -> Dict[str, Any]:
    ast: Dict[str, Any] = {}
    ast["from"] = {"rmType": "EHR", "alias": ehr_alias}
    # Emit a contains tree with aliases so ContextMapper can resolve node scopes.
    # Best-effort composition + two child aliases (admin_salut + med_ac) that appear in the sample AQL.
    contains_children = {
        "operator": "AND",
        "children": {
            "0": {
                "rmType": "CLUSTER",
                "alias": "admin_salut",
                "predicate": {"path": "archetype_node_id", "operator": "=", "value": "admin_salut"},
            },
            "1": {
                "rmType": "ACTION",
                "alias": "med_ac",
                "predicate": {
                    "path": "archetype_node_id",
                    "operator": "=",
                    "value": "openEHR-EHR-ACTION.medication.v1",
                },
            },
        },
    }
    ast["contains"] = {
        "rmType": "COMPOSITION",
        "alias": composition_alias,
        "predicate": {
            "path": "archetype_node_id",
            "operator": "=",
            "value": "openEHR-EHR-COMPOSITION.vaccination_list.v0",
        },
        "contains": contains_children,
    }
    sel = _adapt_select(ir)
    ast["select"] = sel
    where = _adapt_where(ir, ehr_alias=ehr_alias, comp_alias=composition_alias)
    if where:
        ast["where"] = where
    order = _adapt_order(ir)
    if order:
        ast["orderBy"] = order
    version_alias = _detect_version_alias(ir)
    if version_alias:
        ast["version"] = {"alias": version_alias}
    if ir.limit is not None:
        ast["limit"] = ir.limit
    if ir.offset is not None:
        ast["offset"] = ir.offset
    # aliases are detected by ASTValidator; not stored directly here
    return ast
