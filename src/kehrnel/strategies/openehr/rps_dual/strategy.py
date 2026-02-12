from __future__ import annotations

import copy
import inspect
import json
import random
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict

from kehrnel.core.plugin import StrategyPlugin
from kehrnel.core.types import ApplyPlan, ApplyResult, TransformResult, QueryPlan, QueryResult, StrategyContext
from kehrnel.core.manifest import StrategyManifest
from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.core.explain import enrich_explain
from kehrnel.core.errors import KehrnelError
from kehrnel.core.synthetic_model_catalog import resolve_links, resolve_model_specs
from kehrnel.domains.openehr.templates.generator import kehrnelGenerator
from kehrnel.domains.openehr.templates.parser import TemplateParser
from kehrnel.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.strategies.openehr.rps_dual.query.executor import execute as execute_query_plan
from kehrnel.strategies.openehr.rps_dual.services.codes_service import atcode_to_token
from kehrnel.strategies.openehr.rps_dual.services.codes_service import get_codes
from kehrnel.strategies.openehr.rps_dual.services.shortcuts_service import get_shortcuts
from kehrnel.strategies.openehr.rps_dual.config import (
    normalize_config,
    normalize_bulk_config,
    RPSDualConfig,
    BulkConfig,
    build_flattener_config,
    build_coding_opts,
)
from kehrnel.strategies.openehr.rps_dual.config_resolver import resolve_uri, resolve_uri_async
from kehrnel.strategies.openehr.rps_dual.query.compiler import build_query_pipeline


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST_PATH = Path(__file__).parent / "manifest.json"
SCHEMA_PATH = Path(__file__).parent / "schema.json"
DEFAULTS_PATH = Path(__file__).parent / "defaults.json"
BULK_SCHEMA_PATH = Path(__file__).parent / "ingest" / "bulk_schema.json"
BULK_DEFAULTS_PATH = Path(__file__).parent / "ingest" / "bulk_defaults.json"


MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))


class RPSDualStrategy(StrategyPlugin):
    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        self.schema = load_json(SCHEMA_PATH)
        self.defaults = load_json(DEFAULTS_PATH)
        # Load bulk config schemas
        self.bulk_schema = load_json(BULK_SCHEMA_PATH) if BULK_SCHEMA_PATH.exists() else {}
        self.bulk_defaults = load_json(BULK_DEFAULTS_PATH) if BULK_DEFAULTS_PATH.exists() else {}
        # hydrate manifest schemas/defaults
        self.manifest.config_schema = self.schema
        self.manifest.default_config = self.defaults
        self.normalized_config: RPSDualConfig | None = None
        self.normalized_bulk_config: BulkConfig | None = None

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
        # index plans from pack_spec storage indexes
        artifacts = self._augment_indexes_from_spec(ctx, artifacts)
        # indexes from mapping/index hints
        try:
            flattener = CompositionFlattener(
                db=None,
                config={"paths": {"separator": cfg.get("paths", {}).get("separator", ".")}},
                mappings_path=str(Path(__file__).parent / "ingest" / "config" / "flattener_mappings_f.jsonc"),
                mappings_content=ctx.meta.get("mappings") if ctx.meta else None,
            )
            hints = flattener.get_index_hints()
            for _, idx_list in hints.items():
                for idx in idx_list:
                    if isinstance(idx.get("index"), dict) and idx["index"].get("type") == "token":
                        artifacts["search_indexes"].append(
                            {
                                "collection": search.get("name"),
                                "name": search.get("atlas_index_name") or "search_nodes_index",
                                "definition": {"mappings": idx["index"]},
                            }
                        )
        except Exception:
            pass
        return ApplyPlan(artifacts=artifacts)

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        storage = (ctx.adapters or {}).get("storage")
        index_admin = (ctx.adapters or {}).get("index_admin")
        atlas_search = (ctx.adapters or {}).get("atlas_search") or (ctx.adapters or {}).get("text_search")
        created = []
        warnings = []
        skipped = []
        for coll in plan.artifacts.get("collections", []):
            if index_admin:
                await index_admin.ensure_collection(coll)
                created.append(coll)
        for idx in plan.artifacts.get("indexes", []):
            if index_admin:
                res = await index_admin.ensure_indexes(idx.get("collection"), [{"keys": idx.get("keys", []), "options": {}}])
                warnings.extend(res.get("warnings", []))
            else:
                skipped.append({"collection": idx.get("collection"), "reason": "index_admin adapter not available"})
        for si in plan.artifacts.get("search_indexes", []):
            if atlas_search:
                res = await atlas_search.ensure_search_index(si["collection"], si["name"], si.get("definition", {}))
                warnings.extend(res.get("warnings", []))
            else:
                skipped.append({"collection": si.get("collection"), "reason": "atlas_search adapter not available"})
        return ApplyResult(created=created, warnings=warnings, skipped=[s for s in skipped if s])

    def _augment_indexes_from_spec(self, ctx: StrategyContext, artifacts: Dict[str, Any]) -> Dict[str, Any]:
        """Merge indexes defined in pack_spec storage indexes into the plan."""
        spec = getattr(self.manifest, "pack_spec", None) or {}
        stores = (spec.get("storage") or {}).get("stores") or []
        store_profiles = (ctx.meta or {}).get("store_profiles") or {}
        collections_cfg = ctx.config.get("collections", {}) if isinstance(ctx.config, dict) else {}

        def resolve_collection(store: Dict[str, Any]) -> str | None:
            role = store.get("role")
            if role and role in store_profiles:
                return store_profiles[role].get("collection")
            # fallback to config
            if role == "store:compositions":
                return (collections_cfg.get("compositions") or {}).get("name")
            if role == "store:search":
                return (collections_cfg.get("search") or {}).get("name")
            return store.get("destinationType")

        for store in stores:
            if not isinstance(store, dict):
                continue
            coll = resolve_collection(store)
            for idx in store.get("indexes") or []:
                if not isinstance(idx, dict):
                    continue
                idx_type = idx.get("type")
                if idx_type == "btree":
                    fields = idx.get("fields") or []
                    keys = [(f, 1) for f in fields]
                    if coll and keys:
                        artifacts["indexes"].append({"collection": coll, "keys": keys, "options": idx.get("options", {})})
                elif idx_type == "wildcard":
                    field = idx.get("field") or "data"
                    if coll:
                        artifacts["indexes"].append({"collection": coll, "keys": [(f"{field}.$**", 1)], "options": idx.get("options", {})})
                elif idx_type == "search":
                    if coll:
                        artifacts["search_indexes"].append(
                            {
                                "collection": coll,
                                "name": idx.get("name") or store.get("role") or "search_index",
                                "definition": idx.get("definition", {}),
                                "storedSource": idx.get("storedSource"),
                            }
                        )
        return artifacts

    async def _build_flattener_for_context(self, ctx: StrategyContext) -> CompositionFlattener:
        cfg = ctx.config or {}
        adapters = ctx.adapters or {}
        storage = adapters.get("storage")

        strategy_cfg = normalize_config(cfg)
        bulk_cfg = normalize_bulk_config((ctx.meta or {}).get("bulk_config", {}))
        flattener_config = build_flattener_config(strategy_cfg, bulk_cfg)
        coding_cfg = build_coding_opts(strategy_cfg)

        mappings_ref = strategy_cfg.transform.mappings
        mappings_content = (ctx.meta or {}).get("mappings") if ctx and ctx.meta else None
        if mappings_content is None and mappings_ref:
            db = getattr(storage, "db", None)
            mappings_content = await resolve_uri_async(mappings_ref, db, Path(__file__).parent)

        mappings_path = str(Path(__file__).parent / "ingest" / "config" / "flattener_mappings_f.jsonc")
        db = getattr(storage, "db", None)
        return await CompositionFlattener.create(
            db=db,
            config=flattener_config,
            mappings_path=mappings_path,
            mappings_content=mappings_content,
            field_map=None,
            coding_opts=coding_cfg,
        )

    async def _ensure_shortcuts_collection_initialized(
        self,
        *,
        strategy_cfg: RPSDualConfig,
        storage: Any,
        index_admin: Any,
    ) -> Dict[str, Any]:
        """Ensure shortcuts collection and placeholder document exist when enabled."""
        if not strategy_cfg.transform.apply_shortcuts:
            return {"enabled": False, "collection": None, "created": False}
        shortcuts_name = strategy_cfg.collections.shortcuts.name
        if not shortcuts_name:
            return {"enabled": True, "collection": None, "created": False, "warning": "shortcuts collection not configured"}
        if index_admin:
            await index_admin.ensure_collection(shortcuts_name)
        created = False
        existing = await storage.find_one(shortcuts_name, {"_id": "shortcuts"})
        if not existing:
            await storage.insert_one(shortcuts_name, {"_id": "shortcuts", "items": {}, "initialized": True})
            created = True
        return {"enabled": True, "collection": shortcuts_name, "created": created}

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        flattener = await self._build_flattener_for_context(ctx)
        # Normalise payload into the shape expected by the flattener
        comp_obj = payload.get("canonicalJSON") if isinstance(payload, dict) else None
        if comp_obj is None and isinstance(payload, dict):
            comp_obj = payload.get("composition") or payload.get("compositionJSON") or payload
        raw_doc = {
            "_id": payload.get("_id") if isinstance(payload, dict) else "comp-1",
            "ehr_id": (payload.get("ehr_id") if isinstance(payload, dict) else None) or "ehr-1",
            "composition_version": payload.get("composition_version") if isinstance(payload, dict) else None,
            "canonicalJSON": comp_obj or payload,
        }
        base_doc, search_doc = flattener.transform_composition(raw_doc)

        return TransformResult(base=base_doc, search=search_doc, meta={})

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg = ctx.config or {}
        strategy_cfg = normalize_config(cfg)
        storage = (ctx.adapters or {}).get("storage")
        index_admin = (ctx.adapters or {}).get("index_admin")
        shortcuts_init = None
        if storage:
            shortcuts_init = await self._ensure_shortcuts_collection_initialized(
                strategy_cfg=strategy_cfg,
                storage=storage,
                index_admin=index_admin,
            )

        tf = await self.transform(ctx, payload)
        comp_name = cfg.get("collections", {}).get("compositions", {}).get("name")
        search_cfg = cfg.get("collections", {}).get("search", {}) or {}
        search_enabled = search_cfg.get("enabled", True)
        search_name = search_cfg.get("name")

        inserted = {}
        if storage and comp_name and tf.base:
            await storage.insert_one(comp_name, tf.base)
            inserted["base"] = comp_name
        if storage and search_enabled and search_name and tf.search:
            await storage.insert_one(search_name, tf.search)
            inserted["search"] = search_name
        return {"inserted": inserted, "base": tf.base, "search": tf.search, "shortcuts_init": shortcuts_init}

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reverse a flattened base document back to a nested composition (best effort).
        """
        from kehrnel.strategies.openehr.rps_dual.ingest.unflattener import CompositionUnflattener

        cfg = ctx.config or {}

        # Normalize configs using new structure
        strategy_cfg = normalize_config(cfg)
        # For reverse transform, role should default to "secondary" (read-only)
        bulk_cfg = normalize_bulk_config({"role": "secondary", **(ctx.meta or {}).get("bulk_config", {})})

        # Build config for unflattener
        flattener_config = build_flattener_config(strategy_cfg, bulk_cfg)
        coding_cfg = build_coding_opts(strategy_cfg)

        unflattener = await CompositionUnflattener.create(
            db=getattr((ctx.adapters or {}).get("storage"), "db", None),
            config=flattener_config,
            mappings_path=str(Path(__file__).parent / "ingest" / "config" / "flattener_mappings_f.jsonc"),
            mappings_content=(ctx.meta or {}).get("mappings") if ctx and ctx.meta else None,
            coding_opts=coding_cfg,
        )
        base_doc = payload.get("base") if isinstance(payload, dict) else payload
        if not isinstance(base_doc, dict):
            raise ValueError("Payload must include flattened base document under 'base'")
        return {"composition": unflattener.unflatten(base_doc)}

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        ir = AqlQueryIR(**query) if not isinstance(query, AqlQueryIR) else query
        cfg_model = normalize_config(ctx.config)
        explain_domain = (getattr(ctx.manifest, "domain", None) or domain or "").lower() or None
        shortcuts_res = await get_shortcuts(ctx)
        codes_res = await get_codes(ctx)
        warnings = []
        if shortcuts_res.get("missing"):
            warnings.append({"code": "dict_missing_shortcuts", "message": "Shortcuts dictionary missing", "details": {"source": shortcuts_res.get("source")}})
        if codes_res.get("missing"):
            warnings.append({"code": "dict_missing_codes", "message": "Codes dictionary missing", "details": {"source": codes_res.get("source")}})
        slim_cfg = cfg_model.model_dump().get("slim_search") or {}
        bundle_id = slim_cfg.get("bundle_id")
        bundle_refs = (ctx.meta or {}).get("bundle_refs") or {}
        bundle_digest = bundle_refs.get(bundle_id) if bundle_id else None
        # Build query pipeline using the query compiler
        engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info = await build_query_pipeline(ir, cfg_model)
        if ir.scope == "cross_patient":
            post_match = [stage for stage in pipeline[1:] if "$match" in stage]
            if post_match:
                warnings.append({"code": "partial_pushdown", "message": "Some predicates evaluated after $search", "details": {"post_match_stages": len(post_match)}})
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
        config_hash = (ctx.meta or {}).get("config_hash") if ctx else None
        manifest_digest = (ctx.meta or {}).get("manifest_digest") if ctx else None
        explain = {
            "dicts": plan_dict["dicts"],
            "warnings": plan_dict["warnings"],
            "engine": "query_engine",
            "stage0": stage0,
            "schema": schema_cfgs,
            "ast": ast_doc if query.get("debug") else None,
            "builder": builder_info,
            "bundle": {"id": bundle_id, "digest": bundle_digest} if bundle_id else None,
        }
        explain = enrich_explain(
            explain,
            ctx,
            domain=explain_domain or "openehr",
            engine="query_engine",
            scope=ir.scope,
        )
        plan_dict["explain"] = explain
        return QueryPlan(engine=plan_dict["engine"], plan=plan_dict, explain=explain)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        return await execute_query_plan(ctx, plan)

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        op_lower = op.lower()
        cfg = ctx.config
        strategy_cfg = normalize_config(cfg)
        adapters = ctx.adapters or {}
        index_admin = adapters.get("index_admin")
        storage = adapters.get("storage")
        atlas = adapters.get("atlas_search") or adapters.get("text_search")

        if op_lower == "ensure_dictionaries":
            created = []
            warnings = []
            dict_name = strategy_cfg.collections.codes.name
            shortcuts_name = strategy_cfg.collections.shortcuts.name
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
            dict_name = strategy_cfg.collections.codes.name
            if not storage or not dict_name:
                return {"ok": False, "warnings": ["storage adapter not available or dictionary not configured"]}
            docs = await storage.aggregate(strategy_cfg.collections.compositions.name, [{"$limit": payload.get("limit", 1000)}] if payload else [{"$limit": 1000}])
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
            shortcuts_name = strategy_cfg.collections.shortcuts.name
            if not storage:
                return {"ok": False, "warnings": ["storage adapter not available"]}
            await storage.insert_one(shortcuts_name, {"_id": "shortcuts", "items": {}, "updated": True})
            cache = (ctx.meta or {}).get("dict_cache") if ctx else None
            if cache is not None:
                cache.pop("shortcuts", None)
            return {"ok": True, "updated": 0, "warnings": ["shortcuts rebuilt as empty; provide source data to populate"]}

        if op_lower == "ensure_atlas_search_index":
            search_coll = strategy_cfg.collections.search
            atlas_idx = search_coll.atlasIndex
            definition = {}
            if atlas_idx and atlas_idx.definition:
                # Resolve URI if needed
                db = getattr(storage, "db", None)
                definition = await resolve_uri_async(atlas_idx.definition, db, Path(__file__).parent) or {}
            if atlas and search_coll.name and atlas_idx and atlas_idx.name:
                res = await atlas.ensure_search_index(search_coll.name, atlas_idx.name, definition)
                return {"ok": True, "result": res}
            return {"ok": False, "warnings": ["atlas_search adapter not available or search collection/index not configured"]}

        if op_lower == "rebuild_slim_search_collection":
            if not storage:
                return {"ok": False, "warnings": ["storage adapter not available"]}
            comp_coll = strategy_cfg.collections.compositions.name
            search_coll_name = strategy_cfg.collections.search.name
            if not comp_coll or not search_coll_name:
                return {"ok": False, "warnings": ["collections not configured"]}
            batch_size = int(payload.get("batch_size", 100)) if payload else 100
            docs = await storage.aggregate(comp_coll, [{"$limit": batch_size}])
            # Get bundle reference from search collection seed if available
            bundle_id = None
            seed_ref = strategy_cfg.collections.search.atlasIndex
            if seed_ref and isinstance(seed_ref.definition, str):
                bundle_id = seed_ref.definition
            bundle = await self._maybe_load_bundle(bundle_id, ctx)
            inserted = 0
            for doc in docs:
                search_doc = self._apply_bundle_to_composition(bundle, doc, strategy_cfg)
                await storage.insert_one(search_coll_name, search_doc)
                inserted += 1
            return {"ok": True, "processed": len(docs), "inserted": inserted, "warnings": []}

        if op_lower == "synthetic_generate_batch":
            if not storage:
                raise KehrnelError(
                    code="STORAGE_NOT_AVAILABLE",
                    status=503,
                    message="storage adapter not available for synthetic generation",
                )
            patient_count = int(payload.get("patient_count") or payload.get("patients") or 0)
            if patient_count <= 0:
                raise KehrnelError(code="INVALID_INPUT", status=400, message="patient_count must be > 0")
            source_collection = payload.get("source_collection") or "samples"
            source_database = payload.get("source_database") or payload.get("source_db")
            dry_run = bool(payload.get("dry_run", False))
            plan_only = bool(payload.get("plan_only", False))
            generation_mode = str(payload.get("generation_mode") or "auto").strip().lower()
            store_canonical = bool(payload.get("store_canonical", False))
            canonical_collection = str(payload.get("canonical_collection") or "compositions_canonical_synthetic")
            write_batch_size = max(10, int(payload.get("write_batch_size", 250) or 250))
            comp_collection = strategy_cfg.collections.compositions.name
            search_collection = strategy_cfg.collections.search.name
            search_enabled = bool(strategy_cfg.collections.search.enabled and search_collection)
            flattener = await self._build_flattener_for_context(ctx)
            shortcuts_init = None
            if not dry_run and not plan_only:
                shortcuts_init = await self._ensure_shortcuts_collection_initialized(
                    strategy_cfg=strategy_cfg,
                    storage=storage,
                    index_admin=index_admin,
                )
            target_database = getattr(getattr(storage, "db", None), "name", None)

            progress_cb = (ctx.meta or {}).get("progress_cb")
            should_cancel = (ctx.meta or {}).get("should_cancel")

            models = payload.get("models")
            templates = payload.get("templates")
            model_source = payload.get("model_source") or {}
            model_source_database = (
                model_source.get("database_name")
                if isinstance(model_source, dict)
                else None
            )
            model_source_catalog_collection = (
                model_source.get("catalog_collection")
                if isinstance(model_source, dict)
                else None
            )
            model_source_links_collection = (
                model_source.get("links_collection")
                if isinstance(model_source, dict)
                else None
            )
            source_templates = payload.get("source_templates")
            source_sample_size = int(payload.get("source_sample_size", 200) or 200)
            source_min_per_patient = int(payload.get("source_min_per_patient", 1) or 1)
            source_max_per_patient = int(payload.get("source_max_per_patient", source_min_per_patient) or source_min_per_patient)
            source_filter = payload.get("source_filter")

            model_specs: list[Dict[str, Any]] = []
            if isinstance(models, list) and models:
                model_specs = await resolve_model_specs(
                    storage,
                    model_source=model_source if isinstance(model_source, dict) else {},
                    requested_models=models,
                    domain=(getattr(ctx.manifest, "domain", "") or "openEHR"),
                    strategy_id=getattr(ctx.manifest, "id", None),
                )
            elif isinstance(templates, list) and templates:
                for entry in templates:
                    if not isinstance(entry, dict):
                        raise KehrnelError(code="INVALID_INPUT", status=400, message="each template entry must be an object")
                    template_id = str(entry.get("template_id") or entry.get("templateId") or "").strip()
                    if not template_id:
                        raise KehrnelError(code="INVALID_INPUT", status=400, message="template_id is required in each template entry")
                    min_per = int(entry.get("min_per_patient", entry.get("min", 1)))
                    max_per = int(entry.get("max_per_patient", entry.get("max", min_per)))
                    if min_per < 0 or max_per < min_per:
                        raise KehrnelError(
                            code="INVALID_INPUT",
                            status=400,
                            message=f"invalid range for template {template_id}: min={min_per}, max={max_per}",
                        )
                    sample_size = int(entry.get("sample_pool_size", 25) or 25)
                    model_specs.append(
                        {
                            "model_id": template_id,
                            "template_id": template_id,
                            "min": min_per,
                            "max": max_per,
                            "weight": float(entry.get("weight", 1.0)),
                            "sample_size": max(1, sample_size),
                            "catalog": {},
                        }
                    )
            elif generation_mode in ("from_source", "source", "auto"):
                discovered_template_ids: list[str] = []
                if isinstance(source_templates, list) and source_templates:
                    discovered_template_ids = [str(t).strip() for t in source_templates if str(t).strip()]
                else:
                    match_stage: Dict[str, Any] = {}
                    if isinstance(source_filter, dict) and source_filter:
                        match_stage = source_filter
                    pipeline = ([{"$match": match_stage}] if match_stage else []) + [
                        {
                            "$project": {
                                "template_id": {
                                    "$ifNull": [
                                        "$template_id",
                                        {
                                            "$ifNull": [
                                                "$template_name",
                                                {
                                                    "$ifNull": [
                                                        "$tid",
                                                        {
                                                            "$ifNull": [
                                                                "$canonicalJSON.archetype_details.template_id.value",
                                                                "$archetype_details.template_id.value",
                                                            ]
                                                        },
                                                    ]
                                                },
                                            ]
                                        },
                                    ]
                                }
                            }
                        },
                        {"$match": {"template_id": {"$type": "string", "$ne": ""}}},
                        {"$group": {"_id": "$template_id"}},
                        {"$limit": max(1, source_sample_size)},
                    ]
                    rows = await storage.aggregate(source_collection, pipeline) if not source_database else await _aggregate_from_database(
                        storage=storage,
                        database_name=str(source_database),
                        collection_name=source_collection,
                        pipeline=pipeline,
                    )
                    discovered_template_ids = [str(r.get("_id")).strip() for r in rows if str(r.get("_id") or "").strip()]

                if not discovered_template_ids:
                    raise KehrnelError(
                        code="SOURCE_TEMPLATE_NOT_FOUND",
                        status=404,
                        message=f"No templates discovered in source {source_database + '.' if source_database else ''}{source_collection}",
                    )

                if source_min_per_patient < 0 or source_max_per_patient < source_min_per_patient:
                    raise KehrnelError(
                        code="INVALID_INPUT",
                        status=400,
                        message=f"invalid source range: min={source_min_per_patient}, max={source_max_per_patient}",
                    )

                for template_id in discovered_template_ids:
                    model_specs.append(
                        {
                            "model_id": template_id,
                            "template_id": template_id,
                            "min": source_min_per_patient,
                            "max": source_max_per_patient,
                            "weight": 1.0,
                            "sample_size": max(1, source_sample_size),
                            "catalog": {},
                        }
                    )
            else:
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="Provide payload.models, payload.templates, or source-based mode (generation_mode=from_source).",
                )

            links = await resolve_links(
                storage,
                model_source=model_source if isinstance(model_source, dict) else {},
                model_ids=[str(m.get("model_id")) for m in model_specs],
                explicit_links=payload.get("links") if "links" in payload else None,
            )

            template_specs = []
            samples_by_template: Dict[str, list[Dict[str, Any]]] = {}
            template_generators: Dict[str, Any] = {}
            estimated_docs = 0
            for entry in model_specs:
                catalog = entry.get("catalog") or {}
                template_id = str(
                    entry.get("template_id")
                    or catalog.get("template_id")
                    or catalog.get("templateId")
                    or ((catalog.get("template") or {}).get("id") if isinstance(catalog.get("template"), dict) else None)
                    or entry.get("model_id")
                    or ""
                ).strip()
                if not template_id:
                    raise KehrnelError(code="INVALID_INPUT", status=400, message=f"template_id unresolved for model_id={entry.get('model_id')}")
                min_per = int(entry.get("min", 1))
                max_per = int(entry.get("max", min_per))
                sample_size = int(entry.get("sample_size", 25) or 25)
                estimated_docs += patient_count * ((min_per + max_per) // 2)
                template_specs.append(
                    {
                        "model_id": str(entry.get("model_id") or template_id),
                        "template_id": template_id,
                        "min": min_per,
                        "max": max_per,
                        "sample_size": max(1, sample_size),
                        "catalog": catalog,
                    }
                )

            if estimated_docs <= 0:
                estimated_docs = patient_count

            for spec in template_specs:
                template_id = spec["template_id"]
                sample_docs: list[Dict[str, Any]] = []
                prefer_source = generation_mode in ("auto", "from_source", "source")
                prefer_models = generation_mode in ("auto", "from_models", "models")

                if prefer_source:
                    match_conditions = [
                        {"template_id": template_id},
                        {"template_name": template_id},
                        {"tid": template_id},
                        {"canonicalJSON.archetype_details.template_id.value": template_id},
                        {"archetype_details.template_id.value": template_id},
                    ]
                    source_match: Dict[str, Any]
                    if isinstance(source_filter, dict) and source_filter:
                        source_match = {"$and": [source_filter, {"$or": match_conditions}]}
                    else:
                        source_match = {"$or": match_conditions}
                    sample_docs = await storage.aggregate(
                        source_collection,
                        [{"$match": source_match}, {"$sample": {"size": spec["sample_size"]}}],
                    ) if not source_database else await _aggregate_from_database(
                        storage=storage,
                        database_name=str(source_database),
                        collection_name=source_collection,
                        pipeline=[{"$match": source_match}, {"$sample": {"size": spec["sample_size"]}}],
                    )

                if sample_docs:
                    samples_by_template[template_id] = sample_docs
                    continue

                if prefer_models:
                    model_doc = spec.get("catalog") or {}
                    gen = _build_canonical_generator_from_model(model_doc)
                    if gen is not None:
                        template_generators[template_id] = gen
                        continue

                raise KehrnelError(
                    code="SOURCE_TEMPLATE_NOT_FOUND",
                    status=404,
                    message=(
                        f"No generation source for template_id={template_id}. "
                        f"Tried source {source_database + '.' if source_database else ''}{source_collection} "
                        f"and model definition in model_source."
                    ),
                )

            estimated_base_bytes = 0
            estimated_search_bytes = 0
            estimated_canonical_bytes = 0
            for spec in template_specs:
                template_id = spec["template_id"]
                canonical_probe = None
                if template_id in samples_by_template:
                    canonical_probe = _extract_canonical_composition(samples_by_template[template_id][0])
                elif template_id in template_generators:
                    canonical_probe = template_generators[template_id]()
                if not canonical_probe:
                    continue
                probe_doc = {
                    "_id": str(uuid.uuid4()),
                    "ehr_id": f"synthetic-ehr-{uuid.uuid4()}",
                    "template_id": template_id,
                    "canonicalJSON": canonical_probe,
                }
                probe_base, probe_search = flattener.transform_composition(probe_doc)
                avg_docs_for_template = max(1, patient_count * ((spec["min"] + spec["max"]) // 2))
                if store_canonical and canonical_probe:
                    estimated_canonical_bytes += _json_size_bytes(probe_doc) * avg_docs_for_template
                if probe_base:
                    estimated_base_bytes += _json_size_bytes(probe_base) * avg_docs_for_template
                if search_enabled and probe_search:
                    estimated_search_bytes += _json_size_bytes(probe_search) * avg_docs_for_template

            if plan_only:
                return {
                    "ok": True,
                    "plan_only": True,
                    "patients": patient_count,
                    "estimated_docs": estimated_docs,
                    "estimated_canonical_bytes": estimated_canonical_bytes,
                    "estimated_base_bytes": estimated_base_bytes,
                    "estimated_search_bytes": estimated_search_bytes,
                    "estimated_total_bytes": estimated_canonical_bytes + estimated_base_bytes + estimated_search_bytes,
                    "source_collection": source_collection,
                    "source_database": source_database,
                    "target_database": target_database,
                    "target_collections": {
                        "canonical": canonical_collection if store_canonical else None,
                        "compositions": comp_collection,
                        "search": search_collection if search_enabled else None,
                    },
                    "model_source": {
                        "database_name": model_source_database or target_database,
                        "catalog_collection": model_source_catalog_collection,
                        "links_collection": model_source_links_collection,
                    },
                    "models": [{"model_id": s["model_id"], "template_id": s["template_id"], "min": s["min"], "max": s["max"]} for s in template_specs],
                    "links": links,
                }

            inserted_canonical = 0
            inserted_base = 0
            inserted_search = 0
            generated_docs = 0
            by_template: Dict[str, int] = {}
            by_model: Dict[str, int] = {}
            link_applied_count = 0
            last_progress = -1
            canonical_batch: list[Dict[str, Any]] = []
            base_batch: list[Dict[str, Any]] = []
            search_batch: list[Dict[str, Any]] = []

            async def _flush_batches(force: bool = False) -> None:
                nonlocal inserted_canonical, inserted_base, inserted_search
                if dry_run:
                    return
                if store_canonical and canonical_collection and canonical_batch and (force or len(canonical_batch) >= write_batch_size):
                    await storage.insert_many(canonical_collection, canonical_batch)
                    inserted_canonical += len(canonical_batch)
                    canonical_batch.clear()
                if comp_collection and base_batch and (force or len(base_batch) >= write_batch_size):
                    await storage.insert_many(comp_collection, base_batch)
                    inserted_base += len(base_batch)
                    base_batch.clear()
                if search_enabled and search_collection and search_batch and (force or len(search_batch) >= write_batch_size):
                    await storage.insert_many(search_collection, search_batch)
                    inserted_search += len(search_batch)
                    search_batch.clear()

            await _emit_progress(
                progress_cb,
                progress=1,
                phase="running",
                stats={
                    "patients_total": patient_count,
                    "estimated_docs": estimated_docs,
                    "target_database": target_database,
                    "target_collections": {
                        "canonical": canonical_collection if store_canonical else None,
                        "compositions": comp_collection,
                        "search": search_collection if search_enabled else None,
                    },
                    "source": {
                        "database": source_database or target_database,
                        "collection": source_collection,
                    },
                    "model_source": {
                        "database_name": model_source_database or target_database,
                        "catalog_collection": model_source_catalog_collection,
                        "links_collection": model_source_links_collection,
                    },
                    "patientCount": patient_count,
                    "generatedPatients": 0,
                    "generatedDocuments": 0,
                    "modelCount": len(template_specs),
                    "linksApplied": 0,
                },
            )

            for i in range(patient_count):
                if _is_canceled(should_cancel):
                    raise KehrnelError(code="JOB_CANCELED", status=499, message="Synthetic batch canceled by user")

                ehr_id = f"synthetic-ehr-{uuid.uuid4()}"
                repeats: Dict[str, int] = {}
                model_to_template: Dict[str, str] = {}
                for spec in template_specs:
                    model_id = spec["model_id"]
                    template_id = spec["template_id"]
                    repeats[model_id] = random.randint(spec["min"], spec["max"])
                    model_to_template[model_id] = template_id

                for link in links:
                    from_id = str(link.get("from") or link.get("from_model_id") or "").strip()
                    to_id = str(link.get("to") or link.get("to_model_id") or "").strip()
                    if not from_id or not to_id:
                        continue
                    if repeats.get(from_id, 0) <= 0:
                        continue
                    probability = float(link.get("probability", 1.0))
                    if random.random() > max(0.0, min(1.0, probability)):
                        continue
                    min_to = int(link.get("min_to_per_patient", 1))
                    repeats[to_id] = max(repeats.get(to_id, 0), min_to)
                    link_applied_count += 1

                for model_id, repeat in repeats.items():
                    template_id = model_to_template.get(model_id)
                    if not template_id:
                        continue
                    for _ in range(repeat):
                        if template_id in samples_by_template:
                            src_doc = random.choice(samples_by_template[template_id])
                            canonical = _extract_canonical_composition(src_doc)
                        else:
                            canonical = template_generators[template_id]()
                        if not canonical:
                            continue
                        raw_doc = {
                            "_id": str(uuid.uuid4()),
                            "ehr_id": ehr_id,
                            "template_id": template_id,
                            "canonicalJSON": canonical,
                        }
                        if not dry_run and store_canonical and canonical_collection:
                            canonical_batch.append(raw_doc)
                        transformed_base, transformed_search = flattener.transform_composition(raw_doc)
                        if not dry_run and comp_collection and transformed_base:
                            base_batch.append(transformed_base)
                        if not dry_run and search_enabled and transformed_search:
                            search_batch.append(transformed_search)
                        await _flush_batches()
                        generated_docs += 1
                        by_template[template_id] = by_template.get(template_id, 0) + 1
                        by_model[model_id] = by_model.get(model_id, 0) + 1

                progress = min(99, int((generated_docs / max(1, estimated_docs)) * 100))
                if progress != last_progress:
                    await _emit_progress(
                        progress_cb,
                        progress=progress,
                        phase=f"patient {i + 1}/{patient_count}",
                        stats={
                            "patients_completed": i + 1,
                            "generated_docs": generated_docs,
                            "inserted_canonical": inserted_canonical,
                            "inserted_base": inserted_base,
                            "inserted_search": inserted_search,
                            "links_applied": link_applied_count,
                            "patientCount": patient_count,
                            "generatedPatients": i + 1,
                            "generatedDocuments": generated_docs,
                            "modelCount": len(template_specs),
                            "linksApplied": link_applied_count,
                        },
                    )
                    last_progress = progress

            await _flush_batches(force=True)
            await _emit_progress(
                progress_cb,
                progress=100,
                phase="completed",
                stats={
                    "patients_completed": patient_count,
                    "generated_docs": generated_docs,
                    "inserted_canonical": inserted_canonical,
                    "inserted_base": inserted_base,
                    "inserted_search": inserted_search,
                    "links_applied": link_applied_count,
                    "patientCount": patient_count,
                    "generatedPatients": patient_count,
                    "generatedDocuments": generated_docs,
                    "modelCount": len(template_specs),
                    "linksApplied": link_applied_count,
                },
            )
            return {
                "ok": True,
                "dry_run": dry_run,
                "plan_only": False,
                "source_collection": source_collection,
                "source_database": source_database,
                "target_database": target_database,
                "target": {
                    "canonical": canonical_collection if store_canonical else None,
                    "compositions": comp_collection,
                    "search": search_collection if search_enabled else None,
                },
                "model_source": {
                    "database_name": model_source_database or target_database,
                    "catalog_collection": model_source_catalog_collection,
                    "links_collection": model_source_links_collection,
                },
                "shortcuts_init": shortcuts_init,
                "patients": patient_count,
                "generated_docs": generated_docs,
                "inserted_canonical": inserted_canonical,
                "inserted_base": inserted_base,
                "inserted_search": inserted_search,
                "links_applied": link_applied_count,
                "by_template": by_template,
                "by_model": by_model,
            }

        raise ValueError(f"Strategy op '{op}' not supported")

    def _apply_bundle_to_composition(self, bundle: Dict[str, Any], comp: Dict[str, Any], cfg: RPSDualConfig) -> Dict[str, Any]:
        # Get field names from config
        cn_field = cfg.fields.document.cn
        sn_field = cfg.fields.document.sn
        ehr_id_field = cfg.fields.document.ehr_id
        comp_id_field = cfg.fields.document.comp_id

        nodes = comp.get(cn_field, []) or comp.get("cn", [])
        search_nodes = []
        templates = (bundle.get("payload") or {}).get("templates") or []
        for tpl in templates:
            tid = tpl.get("templateId")
            rules = tpl.get("rules") or []
            analytics = tpl.get("analytics_fields") or []
            for node in nodes:
                p = node.get("p") or ""
                data = node.get("data") or {}
                matched = False
                if rules:
                    for rule in rules:
                        when = rule.get("when") or {}
                        chain = when.get("pathChain") or []
                        if chain and all(token in p for token in chain):
                            matched = True
                            break
                elif analytics:
                    matched = True
                if matched:
                    sn_entry = {"p": p, "tid": tid}
                    for field in (rule.get("copy") if matched and rules else []) or []:
                        if field == "p":
                            sn_entry["p"] = p
                        else:
                            sn_entry[field] = _pluck(data, field)
                    if analytics:
                        sn_entry["analytics"] = [{a.get("name"): _pluck(data, a.get("path", ""))} for a in analytics]
                    search_nodes.append(sn_entry)
        return {
            sn_field: search_nodes,
            ehr_id_field: comp.get(ehr_id_field, comp.get("ehr_id")),
            comp_id_field: comp.get(comp_id_field, comp.get("_id")),
            "tid": templates[0].get("templateId") if templates else comp.get("tid"),
        }


    async def _maybe_load_bundle(self, bundle_id: str | None, ctx: StrategyContext) -> Dict[str, Any]:
        if not bundle_id:
            return {}
        store = (ctx.meta or {}).get("bundle_store") if ctx else None
        if store:
            return store.get_bundle(bundle_id)
        path = Path(__file__).parent / "bundles" / f"{bundle_id}.json"
        if path.exists():
            import json as _json
            return _json.loads(path.read_text(encoding="utf-8"))
        raise KehrnelError(code="BUNDLE_NOT_FOUND", status=404, message=f"Bundle not found: {bundle_id}")


def _pluck(data: Dict[str, Any], path: str) -> Any:
    if not path:
        return None
    cur: Any = data
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur.get(part)
        else:
            return None
    return cur


async def _aggregate_from_database(
    *,
    storage: Any,
    database_name: str,
    collection_name: str,
    pipeline: list[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    db = getattr(storage, "db", None)
    client = getattr(db, "client", None) if db is not None else None
    if client is None:
        raise KehrnelError(code="STORAGE_NOT_AVAILABLE", status=503, message="Mongo client not available")
    cursor = client[str(database_name)][collection_name].aggregate(pipeline)
    return [doc async for doc in cursor]


def _extract_canonical_composition(doc: Dict[str, Any]) -> Dict[str, Any] | None:
    if not isinstance(doc, dict):
        return None
    if isinstance(doc.get("canonicalJSON"), dict):
        return copy.deepcopy(doc["canonicalJSON"])
    if isinstance(doc.get("composition"), dict):
        return copy.deepcopy(doc["composition"])
    if doc.get("_type") == "COMPOSITION":
        return copy.deepcopy(doc)
    return None


def _build_canonical_generator_from_model(model_doc: Dict[str, Any]):
    if not isinstance(model_doc, dict):
        return None
    xml_payload = (
        (((model_doc.get("domainData") or {}).get("source") or {}).get("xml") if isinstance(model_doc.get("domainData"), dict) else None)
        or model_doc.get("opt")
        or model_doc.get("template")
    )
    if not isinstance(xml_payload, str) or "<" not in xml_payload:
        return None

    with tempfile.NamedTemporaryFile(suffix=".opt", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write(xml_payload)
        tmp_path = Path(tmp.name)

    parser = TemplateParser(tmp_path)
    try:
        tmp_path.unlink(missing_ok=True)
    except Exception:
        pass
    gen = kehrnelGenerator(parser)

    def _create():
        return gen.generate_random()

    return _create


def _json_size_bytes(obj: Dict[str, Any]) -> int:
    try:
        return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except Exception:
        return 0


async def _emit_progress(cb: Any, *, progress: int | None = None, phase: str | None = None, stats: Dict[str, Any] | None = None) -> None:
    if not cb:
        return
    result = cb(progress=progress, phase=phase, stats=stats)
    if inspect.isawaitable(result):
        await result


def _is_canceled(cb: Any) -> bool:
    if not cb:
        return False
    try:
        return bool(cb())
    except Exception:
        return False
