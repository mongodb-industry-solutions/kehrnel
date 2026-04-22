"""
X12-837 Claim Processing Strategy

Pipeline:
1. parse_x12 → ephemeral structure with control IDs and segments (not persisted by default)
2. map_to_contextobject → creates CO instance
3. flatten_co_to_cn → semi-flatten into cn[] (same shape as openEHR)
4. materialize → one collection doc per transaction

Output document:
{
  "_id": "...",
  "tenant_id": "acme",
  "type": "837P",
  "control": { "isa13": "...", "gs06": "...", "st02": "..." },
  "co": { "id": "Claim837", "version": "1.0.0" },
  "cn": [
    { "p": "claim/patient/memberId/ROOT", "kp": ["claim","patient","memberId"], "data": "111223333" },
    ...
  ]
}
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.types import ApplyPlan, ApplyResult, TransformResult, QueryPlan, QueryResult, StrategyContext
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.explain import enrich_explain
from kehrnel.contextobjects.strategy_support import (
    compile_con2l_runtime,
    negotiate_con2l_runtime,
    resolve_context_contract_runtime,
    summarize_context_map_runtime,
)

from .ingest.canonical_parser import parse_x12_to_transaction


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST_PATH = Path(__file__).parent / "manifest.json"
SCHEMA_PATH = Path(__file__).parent / "schema.json"
DEFAULTS_PATH = Path(__file__).parent / "defaults.json"

MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))


class X12COSingleStrategy(StrategyPlugin):
    """
    X12 837 claim-processing strategy: ephemeral stage-1 + unified cn[] output.

    Single collection with cn[] in the same shape as openEHR for unified queries.
    """

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        self.schema = load_json(SCHEMA_PATH)
        self.defaults = load_json(DEFAULTS_PATH)
        self.manifest.config_schema = self.schema
        self.manifest.default_config = self.defaults
        self.base_path = Path(__file__).parent

    async def validate_config(self, ctx: StrategyContext) -> None:
        return None

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        cfg = ctx.config
        collections = cfg.get("collections", {})
        claims = collections.get("claims", {}).get("name")
        stage1 = collections.get("stage1", {})

        artifacts = {"collections": [], "indexes": []}

        if claims:
            artifacts["collections"].append(claims)

        # Add stage1 collection if enabled
        if stage1.get("enabled") and stage1.get("name"):
            artifacts["collections"].append(stage1["name"])

        # Indexes for claims collection
        indexes = cfg.get("indexes", {})
        for idx in indexes.get("claims", []) or []:
            artifacts["indexes"].append({
                "collection": claims,
                "keys": idx.get("keys", []),
                "options": idx.get("options", {}),
            })

        return ApplyPlan(artifacts=artifacts)

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        index_admin = (ctx.adapters or {}).get("index_admin")
        created = []
        warnings = []

        for coll in plan.artifacts.get("collections", []):
            if index_admin:
                await index_admin.ensure_collection(coll)
                created.append(coll)
            else:
                warnings.append("index_admin adapter missing")
                break

        for idx in plan.artifacts.get("indexes", []):
            if not index_admin or not idx.get("collection"):
                continue
            res = await index_admin.ensure_indexes(
                idx.get("collection"),
                [{"keys": idx.get("keys", []), "options": idx.get("options", {})}],
            )
            warnings.extend(res.get("warnings", []))

        return ApplyResult(created=created, warnings=warnings)

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        """
        Pipeline:
        1. parse_x12 → ephemeral stage-1
        2. map_to_contextobject → CO instance
        3. flatten_co_to_cn → cn[]
        """
        edi_text = payload.get("edi") or payload.get("x12") or payload.get("raw") or payload.get("text")
        tenant_id = payload.get("tenantId") or payload.get("tenant_id")
        meta: Dict[str, Any] = {}

        # Stage 1: Parse X12 (ephemeral)
        stage1 = None
        if edi_text:
            stage1 = parse_x12_to_transaction(
                edi_text,
                tenant_id=tenant_id,
                raw_name=payload.get("rawName") or payload.get("raw_name") or payload.get("filename"),
                received_at=payload.get("receivedAt") or payload.get("received_at"),
            )

        if not stage1:
            meta["warning"] = "missing_x12_payload"
            return TransformResult(base={}, search=None, meta=meta)

        # Stage 2: Map to ContextObject
        co_instance = self._map_to_contextobject(stage1)

        # Stage 3: Flatten to cn[]
        cn = self._flatten_to_cn(co_instance.get("claim", {}))

        # Build output document
        control = {
            "isa13": stage1.get("control", {}).get("isa13", ""),
            "gs06": stage1.get("control", {}).get("gs06", ""),
            "st02": stage1.get("control", {}).get("st02", ""),
        }

        # Generate stable _id from control numbers
        id_source = f"{control['isa13']}:{control['gs06']}:{control['st02']}"
        doc_id = hashlib.sha256(id_source.encode()).hexdigest()[:24]

        output_doc = {
            "_id": doc_id,
            "type": stage1.get("type", "837P"),
            "control": control,
            "co": co_instance.get("co", {"id": "Claim837", "version": "1.0.0"}),
            "cn": cn,
        }

        if tenant_id:
            output_doc["tenant_id"] = tenant_id

        # Stage1 returned in meta for optional persistence
        persist_stage1 = ctx.config.get("transform", {}).get("debug", {}).get("persistStage1", False)
        if persist_stage1:
            meta["stage1"] = stage1

        return TransformResult(base=output_doc, search=None, meta=meta)

    def _map_to_contextobject(self, stage1: Dict[str, Any]) -> Dict[str, Any]:
        """Map parsed X12 to ContextObject instance (Claim837)."""
        tx_type = stage1.get("type", "837P")

        # Extract claim data from stage1 semantic fields
        claim = stage1.get("semantic", {}) or {}

        # If no semantic mapping yet, build basic claim from control info
        if not claim:
            claim = {
                "identifiers": {
                    "controlNumber": stage1.get("control", {}).get("st02", ""),
                    "patientControlNumber": "",
                },
                "type": tx_type,
            }

        return {
            "co": {"id": "Claim837", "version": "1.0.0"},
            "claim": claim,
        }

    def _flatten_to_cn(
        self,
        data: Any,
        path_parts: Optional[List[str]] = None,
        key_path: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Flatten CO instance to cn[] (same shape as openEHR).

        Output format:
        { "p": "claim/patient/memberId/ROOT", "kp": ["claim","patient","memberId"], "data": "value" }
        """
        if path_parts is None:
            path_parts = ["claim"]
        if key_path is None:
            key_path = ["claim"]

        cn: List[Dict[str, Any]] = []
        cfg = getattr(self, "_ctx_config", {}) or {}
        path_cfg = cfg.get("transform", {}).get("pathConfig", {})
        separator = path_cfg.get("separator", "/")
        reverse_path = path_cfg.get("reversePath", True)

        self._flatten_recursive(data, path_parts, key_path, cn, separator, reverse_path)
        return cn

    def _flatten_recursive(
        self,
        data: Any,
        path_parts: List[str],
        key_path: List[str],
        cn: List[Dict[str, Any]],
        separator: str,
        reverse_path: bool,
    ) -> None:
        """Recursively flatten nested structure."""
        if data is None:
            return

        if isinstance(data, dict):
            for key, value in data.items():
                if value is None:
                    continue
                new_path = path_parts + [key]
                new_kp = key_path + [key]

                if isinstance(value, (dict, list)):
                    self._flatten_recursive(value, new_path, new_kp, cn, separator, reverse_path)
                else:
                    self._emit_node(new_path, new_kp, value, cn, separator, reverse_path)

        elif isinstance(data, list):
            for idx, item in enumerate(data):
                if item is None:
                    continue
                idx_str = str(idx)
                new_path = path_parts + [f"[{idx}]"]
                new_kp = key_path + [idx_str]

                if isinstance(item, dict):
                    self._flatten_recursive(item, new_path, new_kp, cn, separator, reverse_path)
                else:
                    self._emit_node(new_path, new_kp, item, cn, separator, reverse_path)
        else:
            self._emit_node(path_parts, key_path, data, cn, separator, reverse_path)

    def _emit_node(
        self,
        path_parts: List[str],
        key_path: List[str],
        value: Any,
        cn: List[Dict[str, Any]],
        separator: str,
        reverse_path: bool,
    ) -> None:
        """Emit a context node."""
        if reverse_path:
            reversed_parts = list(reversed(path_parts))
            path = separator.join(reversed_parts) + separator + "ROOT"
        else:
            path = separator.join(path_parts) + separator + "ROOT"

        cn.append({"p": path, "kp": key_path, "data": value})

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        return TransformResult(base=payload)

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Ingest X12 EDI to claims collection."""
        self._ctx_config = ctx.config
        tf = await self.transform(ctx, payload)
        storage = (ctx.adapters or {}).get("storage")
        cfg = ctx.config
        collections = cfg.get("collections", {})
        claims_name = collections.get("claims", {}).get("name")
        stage1_cfg = collections.get("stage1", {})

        inserted: Dict[str, Any] = {}

        # Write main claims document
        if storage and claims_name and tf.base:
            await storage.insert_one(claims_name, tf.base)
            inserted["claims"] = claims_name

        # Optionally write stage1 for debugging
        persist_stage1 = cfg.get("transform", {}).get("debug", {}).get("persistStage1", False)
        if storage and persist_stage1 and stage1_cfg.get("name") and tf.meta.get("stage1"):
            await storage.insert_one(stage1_cfg["name"], tf.meta["stage1"])
            inserted["stage1"] = stage1_cfg["name"]

        return {"inserted": inserted, "meta": tf.meta}

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        cfg = ctx.config
        collections = cfg.get("collections", {})
        collection = collections.get("claims", {}).get("name")

        filters = query.get("filters") or query.get("filter") or {}
        pipeline = []

        if filters:
            pipeline.append({"$match": filters})
        if query.get("sort"):
            pipeline.append({"$sort": query.get("sort")})
        if query.get("skip"):
            pipeline.append({"$skip": int(query.get("skip"))})
        if query.get("limit"):
            pipeline.append({"$limit": int(query.get("limit"))})

        explain = enrich_explain(
            {"builder": {"chosen": "x12_co_single"}, "scope": "claims"},
            ctx,
            domain=domain or "x12",
            engine="mongo",
            scope="claims",
        )

        return QueryPlan(
            engine="mongo",
            plan={"collection": collection, "pipeline": pipeline, "explain": explain},
            explain=explain,
        )

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        storage = (ctx.adapters or {}).get("storage")
        rows = []
        if storage and plan.plan.get("collection"):
            rows = await storage.aggregate(plan.plan["collection"], plan.plan.get("pipeline", []))
        return QueryResult(engine_used=plan.engine, rows=rows, explain=plan.explain)

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]):
        if op == "resolve_context_contract":
            return await resolve_context_contract_runtime(ctx, payload)
        if op == "compile_con2l":
            return await compile_con2l_runtime(ctx, payload)
        if op == "summarize_object_map":
            return await summarize_context_map_runtime(ctx, payload)
        if op == "negotiate_con2l":
            return await negotiate_con2l_runtime(ctx, payload)
        raise ValueError(f"Strategy op '{op}' not supported")
