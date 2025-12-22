"""
Projection compiler for RPS-Dual.
Translates AQL select expressions into Mongo-friendly projections that
work both for patient pipelines (cn nodes array) and cross-patient
pipelines after $lookup.
"""
from __future__ import annotations

from typing import Any, Dict, List

from kehrnel.protocols.openehr.aql.ir import SelectExpr, AqlQueryIR
from .path_resolver import PathResolver


def compile_projection(ir: AqlQueryIR, cfg: Dict[str, Any], scope: str, after_lookup: bool = False, shortcuts: Dict[str, str] | None = None) -> Dict[str, Any]:
    """
    Returns a $project dict. If no select is defined, returns {} (caller can skip stage).
    For patient scope: use cn array directly.
    For cross-patient after lookup: assumes full composition nodes are in field 'cn' under looked-up document.
    """
    if not ir.select:
        return {}
    resolver = PathResolver(cfg, shortcuts=shortcuts)
    project: Dict[str, Any] = {}
    for sel in ir.select:
        rp = resolver.resolve_full(sel.path, scope=scope)
        use_search = scope == "cross_patient" and not after_lookup
        path = rp.sn_data_path if use_search else rp.cn_data_path
        if after_lookup:
            nodes_field = f"comp.{resolver.comp_nodes}"
            path_expr = f"$$node.{path.replace('data.', 'data.',1)}"
        else:
            nodes_field = resolver.search_nodes if use_search else resolver.comp_nodes
            path_expr = f"$$node.{path}"
        # Build $first of filtered nodes where path matches regex
        project[sel.alias] = {
            "$first": {
                "$map": {
                    "input": {
                        "$filter": {
                            "input": f"${nodes_field}",
                            "as": "node",
                            "cond": {
                                "$regexMatch": {
                            "input": "$$node.p",
                            "regex": rp.cn_regex,
                        }
                    },
                }
            },
                    "as": "node",
                    "in": path_expr,
                }
            }
        }
    return {"$project": project}
