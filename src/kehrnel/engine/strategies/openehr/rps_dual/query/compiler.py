"""Shared helpers to compile AQL IRs with the vendored query transformer stack."""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from kehrnel.core.errors import KehrnelError
from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.legacy.aql_parser.aql_to_ast import ParseError, parse_aql_to_ast, validate_ast_structure
from kehrnel.strategies.openehr.rps_dual.config import RPSDualConfig, build_schema_config
from kehrnel.strategies.openehr.rps_dual.query.ast_adapter import adapt_ir_to_ast
from kehrnel.strategies.openehr.rps_dual.query.transformers.aql_transformer import AQLtoMQLTransformer
from kehrnel.strategies.openehr.rps_dual.query.transformers.ast_validator import ASTValidator
from kehrnel.strategies.openehr.rps_dual.query.transformers.context_mapper import ContextMapper
from kehrnel.strategies.openehr.rps_dual.query.transformers.format_resolver import FormatResolver


def extract_ehr_id(ir: AqlQueryIR) -> str | None:
    for pred in ir.predicates:
        if pred.path == "ehr_id" and pred.op in ("eq", "="):
            return pred.value
    return None


def parse_aql_strict(aql_text: str) -> Dict[str, Any]:
    raw = (aql_text or "").strip()
    if not raw:
        raise KehrnelError(code="AQL_EMPTY", status=400, message="AQL query is empty")
    try:
        ast_doc = parse_aql_to_ast(raw)
    except ParseError as exc:
        raise KehrnelError(code="AQL_PARSE_ERROR", status=400, message=exc.message) from exc
    except Exception as exc:
        raise KehrnelError(code="AQL_PARSE_ERROR", status=400, message=str(exc)) from exc
    if not validate_ast_structure(ast_doc):
        raise KehrnelError(code="AQL_AST_INVALID", status=400, message="Parsed AQL AST is invalid")
    return ast_doc


def _iter_ast_conditions(node: Any):
    if not isinstance(node, dict):
        return
    # direct condition format
    if "path" in node and "operator" in node:
        yield node
    conds = node.get("conditions")
    if isinstance(conds, dict):
        for child in conds.values():
            yield from _iter_ast_conditions(child)
    elif isinstance(conds, list):
        for child in conds:
            yield from _iter_ast_conditions(child)


def extract_ehr_id_from_ast(ast_doc: Dict[str, Any]) -> str | None:
    where = ast_doc.get("where") or {}
    for cond in _iter_ast_conditions(where):
        path = str(cond.get("path") or "").lower()
        op = str(cond.get("operator") or "").strip().lower()
        if op == "eq":
            op = "="
        if op != "=":
            continue
        if "ehr_id" not in path:
            continue
        return cond.get("value")
    return None


def infer_scope_from_ast(ast_doc: Dict[str, Any]) -> str:
    return "patient" if extract_ehr_id_from_ast(ast_doc) is not None else "cross_patient"


async def build_query_pipeline_from_ast(
    ast_doc: Dict[str, Any],
    cfg_model: RPSDualConfig,
    scope_hint: str | None = None,
) -> Tuple[str, List[Dict[str, Any]], str, Dict[str, Any], Dict[str, Any], Dict[str, Any], str]:
    try:
        ASTValidator.validate_ast(ast_doc)
    except Exception as exc:
        raise KehrnelError(code="AQL_NOT_SUPPORTED", status=400, message=str(exc)) from exc

    ehr_alias, comp_alias = ASTValidator.detect_key_aliases(ast_doc)
    ctx_map = ContextMapper().build_context_map(ast_doc)
    cfg = cfg_model.model_dump()
    schema_cfgs = build_schema_config(cfg_model)
    # Initialize resolver to keep behavior aligned with strategy compile path.
    FormatResolver(ctx_map, ehr_alias, comp_alias, schema_cfgs["composition"])

    inferred_scope = infer_scope_from_ast(ast_doc)
    requested_scope = (scope_hint or "").strip().lower()
    if requested_scope in ("patient", "cross_patient"):
        effective_scope = requested_scope
    else:
        effective_scope = inferred_scope

    ehr_id = extract_ehr_id_from_ast(ast_doc)
    transformer = AQLtoMQLTransformer(
        ast=ast_doc,
        ehr_id=ehr_id,
        schema_config=schema_cfgs["composition"],
        search_schema_config=schema_cfgs["search"],
        search_index_name=schema_cfgs["search"].get("index_name"),
    )

    search_enabled = bool(cfg.get("collections", {}).get("search", {}).get("enabled"))
    if effective_scope == "cross_patient" and search_enabled:
        transformer.search_pipeline_builder.schema_config = schema_cfgs["search"]
        pipeline = await transformer.build_search_pipeline()
        engine = "search_pipeline_builder"
    else:
        pipeline = await transformer.build_pipeline()
        engine = "pipeline_builder"
    if not pipeline:
        raise KehrnelError(code="QUERY_PIPELINE_EMPTY", status=400, message="Query pipeline is empty")

    stage0 = list(pipeline[0].keys())[0]
    if effective_scope == "patient" and stage0 != "$match":
        raise KehrnelError(
            code="AQL_SCOPE_CONFLICT",
            status=400,
            message=f"Patient scope must compile to $match pipeline, got {stage0}",
        )
    if effective_scope == "cross_patient" and stage0 != "$search":
        raise KehrnelError(
            code="AQL_SCOPE_CONFLICT",
            status=400,
            message=f"Cross-patient scope must compile to $search pipeline, got {stage0}",
        )

    builder_info = {
        "chosen": engine,
        "scope": effective_scope,
        "scope_inferred": inferred_scope,
        "scope_hint": scope_hint,
        "reason": "aql_ast",
        "has_ehr_id_pred": ehr_id is not None,
        "search_enabled": search_enabled,
    }
    return engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info, effective_scope


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
        search_schema_config=schema_cfgs["search"],
        search_index_name=schema_cfgs["search"].get("index_name"),
    )
    if ir.scope == "cross_patient" and cfg.get("collections", {}).get("search", {}).get("enabled"):
        transformer.search_pipeline_builder.schema_config = schema_cfgs["search"]
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
