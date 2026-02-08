"""Compile AQL IR to Mongo aggregation ($match) for patient scope."""
from __future__ import annotations

from typing import Any, Dict, List

from kehrnel.protocols.openehr.aql.ir import AqlQueryIR
from .path_resolver import PathResolver


def compile_match(ir: AqlQueryIR, cfg: Dict[str, Any], shortcuts: Dict[str, str] | None = None) -> Dict[str, Any]:
    resolver = PathResolver(cfg, shortcuts=shortcuts)
    match: Dict[str, Any] = {}
    resolved_paths = {}
    notes = []
    for pred in ir.predicates:
        field = resolver.resolve(pred.path, scope="patient")
        resolved_paths[pred.path] = field
        if pred.op.lower() in ("eq", "="):
            match[field] = pred.value
        else:
            notes.append(f"Unsupported op {pred.op} for patient scope")
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    # sort/skip/limit ordering
    if ir.sort:
        pipeline.append({"$sort": ir.sort})
    if ir.offset:
        pipeline.append({"$skip": ir.offset})
    if ir.limit:
        pipeline.append({"$limit": ir.limit})
    explain = {
        "scope": "patient",
        "why_engine": "patient scope with equality predicates",
        "resolved_paths": resolved_paths,
        "notes": notes,
    }
    return {
        "engine": "mongo_pipeline",
        "collection": cfg.get("collections", {}).get("compositions", {}).get("name"),
        "pipeline": pipeline,
        "lookup_full_composition": False,
        "explain": explain,
        "warnings": notes,
    }
