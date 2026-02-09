"""Shared helpers to compile AQL IRs with the vendored query transformer stack."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.strategies.openehr.rps_dual.config import RPSDualConfig, build_schema_config
from kehrnel.strategies.openehr.rps_dual.query.ast_adapter import adapt_ir_to_ast
from kehrnel.strategies.openehr.rps_dual.query.transformers.aql_transformer import AQLtoMQLTransformer
from kehrnel.strategies.openehr.rps_dual.query.transformers.ast_validator import ASTValidator
from kehrnel.strategies.openehr.rps_dual.query.transformers.context_mapper import ContextMapper
from kehrnel.strategies.openehr.rps_dual.query.transformers.format_resolver import FormatResolver
from kehrnel.persistence import get_default_strategy


def extract_ehr_id(ir: AqlQueryIR) -> str | None:
    for pred in ir.predicates:
        if pred.path == "ehr_id" and pred.op in ("eq", "="):
            return pred.value
    return None


async def build_query_pipeline(ir: AqlQueryIR, cfg_model: RPSDualConfig) -> Tuple[str, List[Dict[str, Any]], str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    cfg = cfg_model.model_dump()
    ast_doc = adapt_ir_to_ast(ir, ehr_alias="e", composition_alias="c")
    ASTValidator.validate_ast(ast_doc)
    ehr_alias, comp_alias = ASTValidator.detect_key_aliases(ast_doc)
    ctx_map = ContextMapper().build_context_map(ast_doc)
    schema_cfgs = build_schema_config(cfg_model)
    # Initialize resolver to keep behavior aligned with strategy compile path.
    FormatResolver(ctx_map, ehr_alias, comp_alias, schema_cfgs["composition"])
    builder_reason = "scope_patient" if ir.scope == "patient" else "scope_cross_patient"
    transformer = AQLtoMQLTransformer(
        ast=ast_doc,
        ehr_id=extract_ehr_id(ir),
        schema_config=schema_cfgs["composition"],
        search_index_name=schema_cfgs["search"].get("index_name"),
        strategy=get_default_strategy(),
    )
    if ir.scope == "cross_patient" and cfg.get("collections", {}).get("search", {}).get("enabled"):
        pipeline = await transformer.build_search_pipeline()
        engine = "search_pipeline_builder"
    else:
        pipeline = await transformer.build_pipeline()
        engine = "pipeline_builder"
    if not pipeline:
        raise ValueError("Query pipeline is empty")
    stage0 = list(pipeline[0].keys())[0]
    if ir.scope == "patient" and stage0 != "$match":
        raise ValueError(f"Patient scope must start with $match, got {stage0}")
    if ir.scope == "cross_patient" and stage0 != "$search":
        raise ValueError(f"Cross-patient scope must start with $search, got {stage0}")
    builder_info = {
        "chosen": engine,
        "scope": ir.scope,
        "reason": builder_reason,
        "has_ehr_id_pred": any(p.path == "ehr_id" for p in ir.predicates),
        "search_enabled": cfg.get("collections", {}).get("search", {}).get("enabled"),
    }
    return engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info
