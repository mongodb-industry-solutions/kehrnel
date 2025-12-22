from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from kehrnel.core.plugin import StrategyPlugin
from kehrnel.core.types import ApplyPlan, ApplyResult, TransformResult, QueryPlan, QueryResult, StrategyContext
from kehrnel.core.manifest import StrategyManifest
from kehrnel.protocols.openehr.aql.ir import AqlQueryIR
from kehrnel.strategies.openehr.rps_dual.query.compiler_match import compile_match
from kehrnel.strategies.openehr.rps_dual.query.compiler_atlas_search import compile_atlas_search
from kehrnel.strategies.openehr.rps_dual.query.executor import execute as execute_query_plan
from kehrnel.strategies.openehr.rps_dual.query.projection_compiler import compile_projection
from kehrnel.strategies.openehr.rps_dual.services.codes_service import atcode_to_token
from kehrnel.strategies.openehr.rps_dual.services.codes_service import get_codes
from kehrnel.strategies.openehr.rps_dual.services.shortcuts_service import get_shortcuts
from kehrnel.strategies.openehr.rps_dual.config import normalize_config, RPSDualConfig, build_schema_config
from kehrnel.strategies.openehr.rps_dual.legacy_aql import (
    compile_patient,
    compile_cross_patient,
)
from kehrnel.strategies.openehr.rps_dual.legacy_aql.compat import settings as legacy_settings
from kehrnel.strategies.openehr.rps_dual.legacy_aql.compat import persistence as legacy_persistence
from kehrnel.strategies.openehr.rps_dual.legacy_aql.ast_adapter import adapt_ir_to_legacy_ast
from kehrnel.strategies.openehr.rps_dual.legacy_aql.transformers.aql_transformer import AQLtoMQLTransformer
from kehrnel.strategies.openehr.rps_dual.legacy_aql.transformers.format_resolver import FormatResolver
from kehrnel.strategies.openehr.rps_dual.legacy_aql.transformers.context_mapper import ContextMapper
from kehrnel.strategies.openehr.rps_dual.legacy_aql.transformers.ast_validator import ASTValidator
from kehrnel.strategies.openehr.rps_dual.legacy_aql.transformers import search_pipeline_builder as search_builder_module


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST_PATH = Path(__file__).parent / "manifest.json"
SCHEMA_PATH = Path(__file__).parent / "schema.json"
DEFAULTS_PATH = Path(__file__).parent / "defaults.json"


MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))


class RPSDualStrategy(StrategyPlugin):
    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        self.schema = load_json(SCHEMA_PATH)
        self.defaults = load_json(DEFAULTS_PATH)
        # hydrate manifest schemas/defaults
        self.manifest.config_schema = self.schema
        self.manifest.default_config = self.defaults
        self.normalized_config: RPSDualConfig | None = None

    async def validate_config(self, ctx: StrategyContext) -> None:
        return None

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = ctx.config
        artifacts = {"collections": [], "indexes": [], "search_indexes": []}
        comp = cfg.get("collections", {}).get("compositions", {})
        search = cfg.get("collections", {}).get("search", {})
        if comp.get("name"):
            artifacts["collections"].append(comp["name"])
        if search.get("enabled") and search.get("name"):
            artifacts["collections"].append(search["name"])
            if search.get("atlas_index_name"):
                artifacts["search_indexes"].append({"collection": search["name"], "name": search["atlas_index_name"], "definition": search.get("slim_projection") or {}})
        # basic index specs
        artifacts["indexes"].append({"collection": comp.get("name"), "keys": [("ehr_id", 1)]})
        return ApplyPlan(artifacts=artifacts)

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        storage = (ctx.adapters or {}).get("storage")
        index_admin = (ctx.adapters or {}).get("index_admin")
        atlas_search = (ctx.adapters or {}).get("atlas_search") or (ctx.adapters or {}).get("text_search")
        created = []
        warnings = []
        for coll in plan.artifacts.get("collections", []):
            if index_admin:
                await index_admin.ensure_collection(coll)
                created.append(coll)
        for idx in plan.artifacts.get("indexes", []):
            if index_admin:
                res = await index_admin.ensure_indexes(idx.get("collection"), [{"keys": idx.get("keys", []), "options": {}}])
                warnings.extend(res.get("warnings", []))
        for si in plan.artifacts.get("search_indexes", []):
            if atlas_search:
                res = await atlas_search.ensure_search_index(si["collection"], si["name"], si.get("definition", {}))
                warnings.extend(res.get("warnings", []))
        return ApplyResult(created=created, warnings=warnings)

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        # Minimal stub: assume payload already has canonicalJSON
        cfg = ctx.config
        comp_coll = cfg.get("collections", {}).get("compositions", {})
        search_cfg = cfg.get("collections", {}).get("search", {})
        fields = cfg.get("fields", {})
        base = payload.copy()
        search_doc = None
        if search_cfg.get("enabled"):
            search_doc = {
                search_cfg.get("nodes_field", "sn"): payload.get("search_nodes") or [],
                fields.get("search", {}).get("ehr_id", "ehr_id"): payload.get("ehr_id"),
            }
        return TransformResult(base=base, search=search_doc, meta={})

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        tf = await self.transform(ctx, payload)
        storage = (ctx.adapters or {}).get("storage")
        cfg = ctx.config
        comp_name = cfg.get("collections", {}).get("compositions", {}).get("name")
        search_name = cfg.get("collections", {}).get("search", {}).get("name")
        inserted = {}
        if storage and comp_name and tf.base:
            await storage.insert_one(comp_name, tf.base)
            inserted["base"] = comp_name
        if storage and search_name and tf.search:
            await storage.insert_one(search_name, tf.search)
            inserted["search"] = search_name
        return {"inserted": inserted}

    async def compile_query(self, ctx: StrategyContext, protocol: str, query: Dict[str, Any]) -> QueryPlan:
        ir = AqlQueryIR(**query) if not isinstance(query, AqlQueryIR) else query
        cfg_model = normalize_config(ctx.config)
        cfg = cfg_model.model_dump()
        shortcuts_res = await get_shortcuts(ctx)
        codes_res = await get_codes(ctx)
        warnings = []
        if shortcuts_res.get("missing"):
            warnings.append("dictionary_missing:_shortcuts")
        if codes_res.get("missing"):
            warnings.append("dictionary_missing:_codes")
        # configure legacy compat so vendored transformers can read config
        legacy_settings.configure(cfg)
        legacy_persistence.configure(cfg)
        legacy_ast = adapt_ir_to_legacy_ast(ir, ehr_alias="e", composition_alias="c")
        ASTValidator.validate_ast(legacy_ast)
        ehr_alias, comp_alias = ASTValidator.detect_key_aliases(legacy_ast)
        ctx_map = ContextMapper().build_context_map(legacy_ast)
        schema_cfgs = build_schema_config(cfg_model)
        fmt_resolver = FormatResolver(ctx_map, ehr_alias, comp_alias, schema_cfgs["composition"])
        # sync settings into vendored search builder
        search_builder_module.settings = legacy_settings.settings
        # Parity note: legacy AQL transformers are wired with partial parity. See docs/status-aql-parity.md.
        # decide builder based on explicit scope (patient -> pipeline, cross_patient -> search)
        builder_reason = "scope_patient" if ir.scope == "patient" else "scope_cross_patient"
        transformer = AQLtoMQLTransformer(
            ast=legacy_ast,
            ehr_id=_extract_ehr_id(ir),
            schema_config=schema_cfgs["composition"],
            strategy=legacy_persistence.get_default_strategy(),
            search_index_name=schema_cfgs["search"].get("index_name"),
        )
        if ir.scope == "cross_patient" and cfg.get("collections", {}).get("search", {}).get("enabled"):
            transformer.search_pipeline_builder.schema_config = schema_cfgs["search"]
            pipeline = await transformer.build_search_pipeline()
            engine = "legacy_search_pipeline_builder"
        else:
            pipeline = await transformer.build_pipeline()
            engine = "legacy_pipeline_builder"
        builder_info = {
            "chosen": engine,
            "scope": ir.scope,
            "reason": builder_reason,
            "has_ehr_id_pred": any(p.path == "ehr_id" for p in ir.predicates),
            "search_enabled": cfg.get("collections", {}).get("search", {}).get("enabled"),
        }
        plan_dict = {
            "engine": engine,
            "pipeline": pipeline,
        }
        plan_dict["dicts"] = {
            "codes": {"source": codes_res.get("source"), "missing": codes_res.get("missing", False)},
            "shortcuts": {"source": shortcuts_res.get("source"), "missing": shortcuts_res.get("missing", False)},
        }
        plan_dict.setdefault("warnings", [])
        plan_dict["warnings"].extend(warnings)
        explain = {
            "dicts": plan_dict["dicts"],
            "warnings": plan_dict["warnings"],
            "stage0": list(pipeline[0].keys())[0] if pipeline else None,
            "schema": schema_cfgs,
            "legacy_ast": legacy_ast if query.get("debug") else None,
            "builder": builder_info,
        }
        plan_dict["explain"] = explain
        return QueryPlan(engine=plan_dict["engine"], plan=plan_dict)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        return await execute_query_plan(ctx, plan)


def _extract_ehr_id(ir: AqlQueryIR) -> str | None:
    for pred in ir.predicates:
        if pred.path == "ehr_id" and pred.op in ("eq", "="):
            return pred.value
    return None

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        op_lower = op.lower()
        cfg = ctx.config
        adapters = ctx.adapters or {}
        index_admin = adapters.get("index_admin")
        storage = adapters.get("storage")
        atlas = adapters.get("atlas_search") or adapters.get("text_search")

        if op_lower == "ensure_dictionaries":
            created = []
            warnings = []
            dict_name = cfg.get("coding", {}).get("archetype_ids", {}).get("dictionary", "_codes")
            shortcuts_name = "_shortcuts"
            if index_admin:
                if dict_name:
                    await index_admin.ensure_collection(dict_name)
                    created.append(dict_name)
                await index_admin.ensure_collection(shortcuts_name)
                created.append(shortcuts_name)
            else:
                warnings.append("index_admin adapter not available; cannot ensure collections")
            # ensure placeholder docs exist
            if storage:
                for coll, doc_id in ((dict_name, "codes"), (shortcuts_name, "shortcuts")):
                    if not coll:
                        continue
                    existing = await storage.find_one(coll, {"_id": doc_id})
                    if not existing:
                        await storage.insert_one(coll, {"_id": doc_id, "items": {}})
            return {"ok": True, "created": created, "warnings": warnings}

        if op_lower == "rebuild_codes":
            dict_name = cfg.get("coding", {}).get("archetype_ids", {}).get("dictionary", "_codes")
            if not storage or not dict_name:
                return {"ok": False, "warnings": ["storage adapter not available or dictionary not configured"]}
            docs = await storage.aggregate(cfg.get("collections", {}).get("compositions", {}).get("name"), [{"$limit": payload.get("limit", 1000)}] if payload else [{"$limit": 1000}])
            items = {}
            for doc in docs:
                for code in doc.get("codes", []):
                    items[code] = items.get(code, atcode_to_token(code))  # type: ignore
            await storage.insert_one(dict_name, {"_id": "codes", "items": items, "updated": True})
            cache = (ctx.meta or {}).get("dict_cache") if ctx else None
            if cache is not None:
                cache.pop("codes", None)
            return {"ok": True, "updated": len(items), "warnings": []}

        if op_lower == "rebuild_shortcuts":
            shortcuts_name = "_shortcuts"
            if not storage:
                return {"ok": False, "warnings": ["storage adapter not available"]}
            await storage.insert_one(shortcuts_name, {"_id": "shortcuts", "items": {}, "updated": True})
            cache = (ctx.meta or {}).get("dict_cache") if ctx else None
            if cache is not None:
                cache.pop("shortcuts", None)
            return {"ok": True, "updated": 0, "warnings": ["shortcuts rebuilt as empty; provide source data to populate"]}

        if op_lower == "ensure_atlas_search_index":
            search_cfg = cfg.get("collections", {}).get("search", {})
            definition = search_cfg.get("slim_projection") or {}
            if atlas and search_cfg.get("name") and search_cfg.get("atlas_index_name"):
                res = await atlas.ensure_search_index(search_cfg["name"], search_cfg["atlas_index_name"], definition)
                return {"ok": True, "result": res}
            return {"ok": False, "warnings": ["atlas_search adapter not available or search collection/index not configured"]}

        if op_lower == "rebuild_slim_search_collection":
            if not storage:
                return {"ok": False, "warnings": ["storage adapter not available"]}
            comp_coll = cfg.get("collections", {}).get("compositions", {}).get("name")
            search_coll = cfg.get("collections", {}).get("search", {}).get("name")
            if not comp_coll or not search_coll:
                return {"ok": False, "warnings": ["collections not configured"]}
            batch_size = int(payload.get("batch_size", 100)) if payload else 100
            docs = await storage.aggregate(comp_coll, [{"$limit": batch_size}])
            inserted = 0
            for doc in docs:
                search_doc = {
                    cfg.get("collections", {}).get("search", {}).get("nodes_field", "sn"): doc.get("search_nodes", []),
                    cfg.get("fields", {}).get("search", {}).get("ehr_id", "ehr_id"): doc.get("ehr_id"),
                    cfg.get("fields", {}).get("search", {}).get("comp_id", "cid"): doc.get("_id"),
                }
                await storage.insert_one(search_coll, search_doc)
                inserted += 1
            return {"ok": True, "processed": len(docs), "inserted": inserted, "warnings": []}

        raise ValueError(f"Strategy op '{op}' not supported")
