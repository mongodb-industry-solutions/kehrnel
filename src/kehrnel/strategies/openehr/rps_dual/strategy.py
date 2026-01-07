from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from kehrnel.core.plugin import StrategyPlugin
from kehrnel.core.types import ApplyPlan, ApplyResult, TransformResult, QueryPlan, QueryResult, StrategyContext
from kehrnel.core.manifest import StrategyManifest
from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.core.explain import enrich_explain
from kehrnel.core.errors import KehrnelError
from kehrnel.strategies.openehr.rps_dual.ingest.flattener_f import CompositionFlattener
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

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        cfg = ctx.config or {}
        adapters = ctx.adapters or {}
        storage = adapters.get("storage")

        # Normalize configs using new structure
        strategy_cfg = normalize_config(cfg)
        bulk_cfg = normalize_bulk_config((ctx.meta or {}).get("bulk_config", {}))

        # Build flattener config from normalized models
        flattener_config = build_flattener_config(strategy_cfg, bulk_cfg)
        coding_cfg = build_coding_opts(strategy_cfg)

        # Resolve mappings URI
        mappings_ref = strategy_cfg.transform.mappings
        mappings_content = (ctx.meta or {}).get("mappings") if ctx and ctx.meta else None

        if mappings_content is None and mappings_ref:
            db = getattr(storage, "db", None)
            mappings_content = await resolve_uri_async(mappings_ref, db, Path(__file__).parent)

        mappings_path = str(Path(__file__).parent / "ingest" / "config" / "flattener_mappings_f.jsonc")

        # Try to reuse raw motor database from the storage adapter if available
        db = getattr(storage, "db", None)

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

        flattener = await CompositionFlattener.create(
            db=db,
            config=flattener_config,
            mappings_path=mappings_path,
            mappings_content=mappings_content,
            field_map=None,
            coding_opts=coding_cfg,
        )
        base_doc, search_doc = flattener.transform_composition(raw_doc)

        return TransformResult(base=base_doc, search=search_doc, meta={})

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        tf = await self.transform(ctx, payload)
        storage = (ctx.adapters or {}).get("storage")
        cfg = ctx.config or {}
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
        return {"inserted": inserted, "base": tf.base, "search": tf.search}

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reverse a flattened base document back to a nested composition (best effort).
        """
        from kehrnel.strategies.openehr.rps_dual.ingest.unflattener_f import CompositionUnflattener

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
        # configure legacy compat so vendored transformers can read config
        # Query compiler lives under rps_dual.query (former legacy stack).
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
