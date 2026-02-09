"""Compile AQL IR to Atlas Search-first pipeline for cross-patient scope."""
from __future__ import annotations

from typing import Any, Dict, List

from collections import defaultdict

from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from .path_resolver import PathResolver


def compile_atlas_search(ir: AqlQueryIR, cfg: Dict[str, Any], shortcuts: Dict[str, str] | None = None) -> Dict[str, Any]:
    search_cfg = cfg.get("collections", {}).get("search", {}) if isinstance(cfg, dict) else {}
    resolver = PathResolver(cfg, shortcuts=shortcuts)
    groups = defaultdict(list)
    resolved_paths: Dict[str, Any] = {}
    notes: List[str] = []
    post_match: Dict[str, Any] = {}
    pushed = []
    post = []
    for pred in ir.predicates:
        rp = resolver.resolve_full(pred.path, scope="cross_patient")
        resolved_paths[pred.path] = {
            "cn_regex": rp.cn_regex,
            "sn_wildcard": rp.sn_wildcard,
            "cn_data_path": rp.cn_data_path,
            "sn_data_path": rp.sn_data_path,
        }
        groups[rp.grouping_key].append((pred, rp))
    embedded_blocks: List[Dict[str, Any]] = []
    for _, preds in groups.items():
        # preds: list of (pred, rp)
        group_filters: List[Dict[str, Any]] = []
        if preds:
            rp = preds[0][1]
            group_filters.append({"wildcard": {"path": f"{resolver.search_nodes}.p", "query": rp.sn_wildcard}})
        for pred, rp in preds:
            if pred.op in ("eq", "="):
                group_filters.append({"equals": {"path": f"{resolver.search_nodes}.{rp.sn_data_path}", "value": pred.value}})
                pushed.append(pred.path)
            elif pred.op in ("lt", "gt"):
                group_filters.append({"range": {"path": f"{resolver.search_nodes}.{rp.sn_data_path}", pred.op: pred.value}})
                pushed.append(pred.path)
            else:
                post_match[f"{resolver.search_nodes}.{rp.sn_data_path}"] = pred.value
                notes.append(f"Predicate {pred.op} compiled as post-filter $match")
                post.append(pred.path)
        if group_filters:
            embedded_blocks.append(
                {
                    "embeddedDocument": {
                        "path": resolver.search_nodes,
                        "operator": {"compound": {"filter": group_filters}},
                    }
                }
            )
    search_body: Dict[str, Any] = {
        "index": search_cfg.get("atlas_index_name", "default"),
    }
    compound_filters: List[Dict[str, Any]] = embedded_blocks
    if compound_filters:
        search_body["compound"] = {"filter": compound_filters}
    else:
        search_body["text"] = {"query": "*", "path": resolver.search_path}
    if ir.sort:
        search_body["sort"] = [{"path": k, "order": "desc" if v == -1 else "asc"} for k, v in ir.sort.items()]
    search_stage = {
        "$search": {
            **search_body
        }
    }
    pipeline: List[Dict[str, Any]] = [search_stage]
    if post_match:
        pipeline.append({"$match": post_match})
        notes.append("Post-filter $match appended after $search")
    if pipeline and "$search" not in pipeline[0]:
        raise ValueError("$search must be the first stage for Atlas Search")
    if cfg.get("query_engine", {}).get("lookup_full_composition"):
        comp_coll = cfg.get("collections", {}).get("compositions", {}).get("name")
        if comp_coll:
            pipeline.append(
                {
                    "$lookup": {
                        "from": comp_coll,
                        "localField": "cid",
                        "foreignField": "cid",
                        "as": "comp",
                    }
                }
            )
            pipeline.append({"$unwind": {"path": "$comp", "preserveNullAndEmptyArrays": True}})
    # post $lookup projection + limit/skip
    if ir.offset:
        pipeline.append({"$skip": ir.offset})
    if ir.limit:
        pipeline.append({"$limit": ir.limit})
    if ir.sort and "sort" not in search_body:
        pipeline.append({"$sort": ir.sort})
    explain = {
        "scope": "cross_patient",
        "why_engine": "cross-patient query prefers Atlas Search first stage",
        "resolved_paths": resolved_paths,
        "notes": notes,
        "pushed_into_search": pushed,
        "post_filter": post,
    }
    return {
        "engine": "atlas_search_dual",
        "collection": search_cfg.get("name"),
        "pipeline": pipeline,
        "lookup_full_composition": cfg.get("query_engine", {}).get("lookup_full_composition", False),
        "compositions_collection": cfg.get("collections", {}).get("compositions", {}).get("name"),
        "explain": explain,
        "warnings": notes,
    }
