"""Shared helpers to compile AQL AST/IR payloads with the vendored query transformer stack."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.engine.strategies.openehr.rps_dual.config import RPSDualConfig, build_schema_config
from kehrnel.engine.strategies.openehr.rps_dual.query.ast_adapter import adapt_ir_to_ast
from kehrnel.engine.strategies.openehr.rps_dual.query.strategy_selector import (
    should_prefer_match_for_cross_patient_ast,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.aql_transformer import AQLtoMQLTransformer
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.ast_validator import ASTValidator
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.context_mapper import ContextMapper
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.format_resolver import FormatResolver
from kehrnel.persistence import get_default_strategy, load_strategy_from_json


def extract_ehr_id(ir: AqlQueryIR) -> str | None:
    for pred in ir.predicates:
        if pred.path == "ehr_id" and pred.op in ("eq", "="):
            return pred.value
    return None


def build_runtime_strategy(cfg_model: RPSDualConfig):
    return load_strategy_from_json(
        {
            "name": "openehr.rps_dual.runtime",
            "collections": {
                "compositions": {"name": cfg_model.collections.compositions.name},
                "search": {
                    "name": cfg_model.collections.search.name,
                    "enabled": cfg_model.collections.search.enabled,
                    "atlas_index_name": cfg_model.collections.search.atlasIndex.name if cfg_model.collections.search.atlasIndex else None,
                },
            },
            "fields": {
                "composition": {
                    "nodes": cfg_model.fields.document.cn,
                    "path": cfg_model.fields.node.p,
                    "data": cfg_model.fields.node.data,
                    "archetype_path": "ap",
                    "ehr_id": cfg_model.fields.document.ehr_id,
                    "comp_id": cfg_model.fields.document.comp_id,
                    "template_id": cfg_model.fields.document.tid,
                    "version": cfg_model.fields.document.v,
                    "time_committed": cfg_model.fields.document.time_committed,
                },
                "search": {
                    "nodes": cfg_model.fields.document.sn,
                    "path": cfg_model.fields.node.p,
                    "data": cfg_model.fields.node.data,
                    "archetype_path": "ap",
                    "ehr_id": cfg_model.fields.document.ehr_id,
                    "comp_id": cfg_model.fields.document.comp_id,
                    "template_id": cfg_model.fields.document.tid,
                    "sort_time": cfg_model.fields.document.sort_time,
                },
            },
        }
    )


def extract_ehr_id_from_ast(ast_doc: Dict[str, Any]) -> str | None:
    ehr_alias = ((ast_doc.get("from") or {}).get("alias") or "e").strip() if isinstance(ast_doc, dict) else "e"
    targets = {"ehr_id", f"{ehr_alias}/ehr_id/value"}

    def visit(node: Any) -> str | None:
        if isinstance(node, dict):
            path = node.get("path")
            operator = node.get("operator")
            if path in targets and operator in ("=", "eq", "EQ"):
                return node.get("value")
            conditions = node.get("conditions")
            if isinstance(conditions, dict):
                for child in conditions.values():
                    found = visit(child)
                    if found is not None:
                        return found
        elif isinstance(node, list):
            for child in node:
                found = visit(child)
                if found is not None:
                    return found
        return None

    return visit((ast_doc or {}).get("where"))


async def build_query_pipeline_from_ast(
    ast_doc: Dict[str, Any],
    cfg_model: RPSDualConfig,
    *,
    db: Any = None,
    shortcut_map: Optional[Dict[str, str]] = None,
    strategy: Any = None,
    ehr_id: Optional[str] = None,
) -> Tuple[str, List[Dict[str, Any]], str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    cfg = cfg_model.model_dump()
    ASTValidator.validate_ast(ast_doc)
    ehr_alias, comp_alias = ASTValidator.detect_key_aliases(ast_doc)
    ctx_map = ContextMapper().build_context_map(ast_doc)
    schema_cfgs = build_schema_config(cfg_model)
    # Initialize resolver to keep behavior aligned with strategy compile path.
    FormatResolver(ctx_map, ehr_alias, comp_alias, schema_cfgs["composition"])
    resolved_ehr_id = ehr_id if ehr_id is not None else extract_ehr_id_from_ast(ast_doc)
    scope = "patient" if resolved_ehr_id is not None else "cross_patient"
    prefer_match = (
        scope == "cross_patient"
        and should_prefer_match_for_cross_patient_ast(
            ast_doc,
            ehr_alias=ehr_alias,
            composition_alias=comp_alias,
            version_alias=ASTValidator.detect_version_alias(ast_doc) or "v",
        )
    )
    runtime_strategy = strategy or build_runtime_strategy(cfg_model)
    if scope == "patient":
        builder_reason = "scope_patient"
    elif prefer_match:
        builder_reason = "scope_cross_patient_match_friendly"
    else:
        builder_reason = "scope_cross_patient"
    transformer = AQLtoMQLTransformer(
        ast=ast_doc,
        ehr_id=resolved_ehr_id,
        schema_config=schema_cfgs["composition"],
        search_schema_config=schema_cfgs["search"],
        db=db,
        search_index_name=schema_cfgs["search"].get("index_name"),
        strategy=runtime_strategy or get_default_strategy(),
        shortcut_map=shortcut_map or {},
    )
    if scope == "cross_patient" and cfg.get("collections", {}).get("search", {}).get("enabled") and not prefer_match:
        pipeline = await transformer.build_search_pipeline()
        engine = "search_pipeline_builder"
    else:
        pipeline = await transformer.build_pipeline()
        engine = "pipeline_builder"
    if not pipeline:
        raise ValueError("Query pipeline is empty")
    stage0 = list(pipeline[0].keys())[0]
    if scope == "patient" and stage0 != "$match":
        raise ValueError(f"Patient scope must start with $match, got {stage0}")
    if scope == "cross_patient" and prefer_match and stage0 != "$match":
        raise ValueError(f"Match-friendly cross-patient scope must start with $match, got {stage0}")
    if scope == "cross_patient" and not prefer_match and stage0 != "$search":
        raise ValueError(f"Cross-patient scope must start with $search, got {stage0}")
    builder_info = {
        "chosen": engine,
        "scope": scope,
        "reason": builder_reason,
        "has_ehr_id_pred": resolved_ehr_id is not None,
        "prefer_match": prefer_match,
        "search_enabled": cfg.get("collections", {}).get("search", {}).get("enabled"),
    }
    return engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info


async def build_query_pipeline(
    ir: AqlQueryIR,
    cfg_model: RPSDualConfig,
    *,
    db: Any = None,
    shortcut_map: Optional[Dict[str, str]] = None,
    strategy: Any = None,
) -> Tuple[str, List[Dict[str, Any]], str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    ast_doc = adapt_ir_to_ast(ir, ehr_alias="e", composition_alias="c")
    return await build_query_pipeline_from_ast(
        ast_doc,
        cfg_model,
        db=db,
        shortcut_map=shortcut_map,
        strategy=strategy,
        ehr_id=extract_ehr_id(ir),
    )
