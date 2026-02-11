from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from kehrnel.core.plugin import StrategyPlugin
from kehrnel.core.types import ApplyPlan, ApplyResult, TransformResult, QueryPlan, QueryResult, StrategyContext
from kehrnel.core.manifest import StrategyManifest
from kehrnel.core.explain import enrich_explain
from kehrnel.domains.openehr.aql.ir import AqlQueryIR


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST_PATH = Path(__file__).parent / "manifest.json"
SCHEMA_PATH = Path(__file__).parent / "schema.json"
DEFAULTS_PATH = Path(__file__).parent / "defaults.json"


MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))


class RPSSingleStrategy(StrategyPlugin):
    """Simplified reversed-path search strategy using a single collection."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        self.schema = load_json(SCHEMA_PATH)
        self.defaults = load_json(DEFAULTS_PATH)
        self.manifest.config_schema = self.schema
        self.manifest.default_config = self.defaults

    async def validate_config(self, ctx: StrategyContext) -> None:
        return None

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = ctx.config
        comp = cfg.get("collections", {}).get("compositions", {})
        artifacts = {"collections": [], "indexes": []}
        if comp.get("name"):
            artifacts["collections"].append(comp["name"])
            artifacts["indexes"].append({"collection": comp["name"], "keys": [("ehr_id", 1)]})
        return ApplyPlan(artifacts=artifacts)

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        storage = (ctx.adapters or {}).get("storage")
        index_admin = (ctx.adapters or {}).get("index_admin")
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
        return ApplyResult(created=created, warnings=warnings)

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        return TransformResult(base=payload, search=None, meta={})

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        tf = await self.transform(ctx, payload)
        storage = (ctx.adapters or {}).get("storage")
        comp_name = ctx.config.get("collections", {}).get("compositions", {}).get("name")
        if storage and comp_name and tf.base:
            await storage.insert_one(comp_name, tf.base)
            return {"inserted": {"base": comp_name}}
        return {"inserted": {}}

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        ir = AqlQueryIR(**query) if not isinstance(query, AqlQueryIR) else query
        cfg = ctx.config
        comp_coll = cfg.get("collections", {}).get("compositions", {}).get("name")
        atlas_idx = cfg.get("collections", {}).get("search", {}).get("atlas_index_name")
        scope = ir.scope or query.get("scope") or "patient"
        pipeline = []
        if scope == "cross_patient":
            pipeline.append({"$search": {"index": atlas_idx or "search_nodes_index", "text": {"query": "*", "path": cfg.get("fields", {}).get("search", {}).get("path", "p")}}})
        else:
            pipeline.append({"$match": {"domain": (domain or "openehr").lower()}})
        explain = enrich_explain({"builder": {"chosen": "rps_single_dummy"}}, ctx, domain=domain or "openehr", engine="rps_single_dummy", scope=scope)
        return QueryPlan(engine="rps_single_dummy", plan={"collection": comp_coll, "pipeline": pipeline, "explain": explain}, explain=explain)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        storage = (ctx.adapters or {}).get("storage")
        rows = []
        if storage and plan.plan.get("collection"):
            rows = await storage.aggregate(plan.plan["collection"], plan.plan.get("pipeline", []))
        return QueryResult(engine_used=plan.engine, rows=rows, explain=plan.explain)

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]):
        raise ValueError(f"Strategy op '{op}' not supported")
