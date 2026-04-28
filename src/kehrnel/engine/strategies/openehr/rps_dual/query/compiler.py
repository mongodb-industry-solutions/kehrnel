"""Shared helpers to compile AQL AST/IR payloads with the vendored query transformer stack."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.engine.strategies.openehr.rps_dual.config import RPSDualConfig, build_schema_config
from kehrnel.engine.strategies.openehr.rps_dual.query.ast_adapter import adapt_ir_to_ast
from kehrnel.engine.strategies.openehr.rps_dual.query.strategy_selector import (
    should_prefer_match_for_cross_patient_ast,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.aql_transformer import AQLtoMQLTransformer
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.archetype_resolver import ArchetypeResolver
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.ast_validator import ASTValidator
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.context_mapper import ContextMapper
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.format_resolver import FormatResolver
from kehrnel.engine.strategies.openehr.rps_dual.services.codes_service import (
    DEFAULT_COLLECTION as DEFAULT_CODES_COLLECTION,
    DEFAULT_DOC_ID as DEFAULT_CODES_DOC_ID,
)
from kehrnel.engine.strategies.openehr.rps_dual.services.shortcuts_service import (
    DEFAULT_COLLECTION as DEFAULT_SHORTCUTS_COLLECTION,
    DEFAULT_DOC_ID as DEFAULT_SHORTCUTS_DOC_ID,
)
from kehrnel.persistence import get_default_strategy, load_strategy_from_json


_SHARED_RESOLVER_CACHE_LIMIT = 8


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
            "coding": {
                "atcodes": {
                    "strategy": cfg_model.transform.coding.atcodes.strategy,
                    "store_original": cfg_model.transform.coding.atcodes.store_original,
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


def _dictionary_sources(raw_cfg: Dict[str, Any] | None) -> Dict[str, str]:
    if not isinstance(raw_cfg, dict):
        return {
            "codes_collection": DEFAULT_CODES_COLLECTION,
            "codes_doc_id": DEFAULT_CODES_DOC_ID,
            "shortcuts_collection": DEFAULT_SHORTCUTS_COLLECTION,
            "shortcuts_doc_id": DEFAULT_SHORTCUTS_DOC_ID,
        }

    collections = raw_cfg.get("collections") if isinstance(raw_cfg.get("collections"), dict) else {}
    dictionaries = raw_cfg.get("dictionaries") if isinstance(raw_cfg.get("dictionaries"), dict) else {}
    coding = raw_cfg.get("coding") if isinstance(raw_cfg.get("coding"), dict) else {}

    codes_collection = (
        ((collections.get("codes") or {}).get("name") if isinstance(collections.get("codes"), dict) else None)
        or DEFAULT_CODES_COLLECTION
    )
    codes_doc_id = (
        ((coding.get("archetype_ids") or {}).get("dictionary") if isinstance(coding.get("archetype_ids"), dict) else None)
        or ((dictionaries.get("arcodes") or {}).get("doc_id") if isinstance(dictionaries.get("arcodes"), dict) else None)
        or DEFAULT_CODES_DOC_ID
    )
    shortcuts_collection = (
        ((collections.get("shortcuts") or {}).get("name") if isinstance(collections.get("shortcuts"), dict) else None)
        or DEFAULT_SHORTCUTS_COLLECTION
    )
    shortcuts_doc_id = (
        ((dictionaries.get("shortcuts") or {}).get("doc_id") if isinstance(dictionaries.get("shortcuts"), dict) else None)
        or DEFAULT_SHORTCUTS_DOC_ID
    )
    return {
        "codes_collection": codes_collection,
        "codes_doc_id": codes_doc_id,
        "shortcuts_collection": shortcuts_collection,
        "shortcuts_doc_id": shortcuts_doc_id,
    }


def _overlay_dictionary_sources(
    schema_cfgs: Dict[str, Dict[str, Any]],
    raw_cfg: Dict[str, Any] | None,
) -> Dict[str, Dict[str, Any]]:
    resolved = _dictionary_sources(raw_cfg)
    merged = deepcopy(schema_cfgs)
    for key in ("composition", "search"):
        if key not in merged or not isinstance(merged[key], dict):
            continue
        merged[key].update(resolved)
    return merged


def _compile_cache_bucket(
    compile_cache: Optional[Dict[str, Any]],
    name: str,
) -> Optional[Dict[str, Any]]:
    if not isinstance(compile_cache, dict):
        return None
    bucket = compile_cache.get(name)
    if isinstance(bucket, dict):
        return bucket
    bucket = {}
    compile_cache[name] = bucket
    return bucket


def _remember_cached_value(
    cache: Optional[Dict[str, Any]],
    key: str,
    value: Any,
    *,
    limit: int,
) -> None:
    if not isinstance(cache, dict):
        return
    if key in cache:
        cache.pop(key, None)
    elif len(cache) >= limit:
        oldest_key = next(iter(cache), None)
        if oldest_key is not None:
            cache.pop(oldest_key, None)
    cache[key] = value


def _shared_resolver_cache_key(schema_cfgs: Dict[str, Dict[str, Any]]) -> str:
    composition = schema_cfgs.get("composition") or {}
    search = schema_cfgs.get("search") or {}
    return "|".join(
        str(value or "")
        for value in (
            composition.get("codes_collection"),
            composition.get("codes_doc_id"),
            composition.get("collection"),
            search.get("collection"),
            composition.get("separator"),
            composition.get("atcode_strategy"),
        )
    )


def _get_or_create_shared_archetype_resolver(
    *,
    db: Any,
    schema_cfgs: Dict[str, Dict[str, Any]],
    compile_cache: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[ArchetypeResolver], str]:
    if db is None:
        return None, "disabled"

    cache_key = _shared_resolver_cache_key(schema_cfgs)
    bucket = _compile_cache_bucket(compile_cache, "archetype_resolver")
    cached = bucket.get(cache_key) if isinstance(bucket, dict) else None
    if isinstance(cached, ArchetypeResolver):
        _remember_cached_value(bucket, cache_key, cached, limit=_SHARED_RESOLVER_CACHE_LIMIT)
        return cached, "hit"

    resolver = ArchetypeResolver(
        db,
        codes_collection=(schema_cfgs.get("composition") or {}).get("codes_collection"),
        codes_doc_id=(schema_cfgs.get("composition") or {}).get("codes_doc_id"),
        search_collection=(schema_cfgs.get("search") or {}).get("collection"),
        composition_collection=(schema_cfgs.get("composition") or {}).get("collection"),
        separator=(schema_cfgs.get("composition") or {}).get("separator"),
        atcode_strategy=(schema_cfgs.get("composition") or {}).get("atcode_strategy"),
    )
    _remember_cached_value(bucket, cache_key, resolver, limit=_SHARED_RESOLVER_CACHE_LIMIT)
    return resolver, "miss"


async def build_query_pipeline_from_ast(
    ast_doc: Dict[str, Any],
    cfg_model: RPSDualConfig,
    *,
    db: Any = None,
    shortcut_map: Optional[Dict[str, str]] = None,
    strategy: Any = None,
    ehr_id: Optional[str] = None,
    raw_cfg: Optional[Dict[str, Any]] = None,
    compile_cache: Optional[Dict[str, Any]] = None,
) -> Tuple[str, List[Dict[str, Any]], str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    cfg = cfg_model.model_dump()
    ASTValidator.validate_ast(ast_doc)
    ehr_alias, comp_alias = ASTValidator.detect_key_aliases(ast_doc)
    ctx_map = ContextMapper().build_context_map(ast_doc)
    schema_cfgs = _overlay_dictionary_sources(build_schema_config(cfg_model), raw_cfg)
    # Initialize resolver to keep behavior aligned with strategy compile path.
    FormatResolver(ctx_map, ehr_alias, comp_alias, schema_cfgs["composition"])
    archetype_resolver, resolver_cache_status = _get_or_create_shared_archetype_resolver(
        db=db,
        schema_cfgs=schema_cfgs,
        compile_cache=compile_cache,
    )
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
        archetype_resolver=archetype_resolver,
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
        "cache": {
            "archetype_resolver": resolver_cache_status,
        },
    }
    return engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info


async def build_query_pipeline(
    ir: AqlQueryIR,
    cfg_model: RPSDualConfig,
    *,
    db: Any = None,
    shortcut_map: Optional[Dict[str, str]] = None,
    strategy: Any = None,
    raw_cfg: Optional[Dict[str, Any]] = None,
    compile_cache: Optional[Dict[str, Any]] = None,
) -> Tuple[str, List[Dict[str, Any]], str, Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    ast_doc = adapt_ir_to_ast(ir, ehr_alias="e", composition_alias="c")
    return await build_query_pipeline_from_ast(
        ast_doc,
        cfg_model,
        db=db,
        shortcut_map=shortcut_map,
        strategy=strategy,
        ehr_id=extract_ehr_id(ir),
        raw_cfg=raw_cfg,
        compile_cache=compile_cache,
    )
