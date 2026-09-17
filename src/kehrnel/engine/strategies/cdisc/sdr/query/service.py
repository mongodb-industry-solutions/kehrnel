"""Compilation and execution of governed CDISC Study Query IR."""

from __future__ import annotations

import re
from typing import Any, Dict, List

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.explain import enrich_explain
from kehrnel.engine.core.types import QueryPlan, QueryResult, StrategyContext
from kehrnel.engine.domains.cdisc.query import compile_study_query, encode_page_token

from ..common import MODEL_SCHEMA_VERSION, collections, config, storage_adapter


class QueryService:
    async def compile(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        cfg = config(ctx)
        coll = collections(cfg)
        query_cfg = cfg.get("query") or {}
        payload = dict(query or {})
        page = dict(payload.get("page") or {})
        page.setdefault("limit", int(query_cfg.get("default_limit", 100)))
        if int(page["limit"]) > int(query_cfg.get("max_limit", 1000)):
            raise KehrnelError(
                code="INVALID_CDISC_QUERY",
                status=400,
                message="query page limit exceeds the activated strategy maximum",
            )
        payload["page"] = page
        try:
            plan = compile_study_query(
                payload,
                tenant_id=str(cfg["tenant_id"]),
                collection=coll["records"],
                snapshot_collection=coll["snapshots"],
                model_schema_version=MODEL_SCHEMA_VERSION,
                extra_allowed_paths=set(query_cfg.get("extra_allowed_paths") or []),
            )
        except Exception as exc:
            raise KehrnelError(code="INVALID_CDISC_QUERY", status=400, message=str(exc)) from exc
        explain = enrich_explain(
            {
                "builder": {"chosen": "cdisc_study_query_v1"},
                "governance": plan["governance"],
                "collection": plan["collection"],
            },
            ctx,
            domain=domain or "cdisc",
            engine=plan["engine"],
            scope=plan["scope"],
        )
        plan["explain"] = explain
        return QueryPlan(engine=plan["engine"], plan=plan, explain=explain)

    async def execute(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        storage = storage_adapter(ctx)
        try:
            rows = await storage.aggregate(plan.plan["collection"], plan.plan["pipeline"])
        except Exception as exc:
            raise KehrnelError(
                code="CDISC_QUERY_EXECUTION_FAILED",
                status=502,
                message=str(exc),
                details={"collection": plan.plan.get("collection")},
            ) from exc
        pagination = plan.plan.get("pagination") or {}
        limit = int(pagination.get("limit") or len(rows))
        has_more = len(rows) > limit
        rows = rows[:limit]
        explain = dict(plan.explain or {})
        explain["pagination"] = {
            "limit": limit,
            "hasMore": has_more,
            "nextToken": encode_page_token(int(pagination.get("offset") or 0) + limit) if has_more else None,
        }
        return QueryResult(engine_used=plan.engine, rows=rows, explain=explain)

    async def search(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Run governed lexical, vector, or hybrid retrieval over published records."""

        query = str(payload.get("q") or "").strip()
        mode = str(payload.get("mode") or "hybrid").lower()
        if not query or mode not in {"lexical", "semantic", "hybrid"}:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="q and a valid search mode are required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        semantic_cfg = cfg.get("semantic") or {}
        if not bool(semantic_cfg.get("enabled", True)):
            raise KehrnelError(code="CDISC_SEMANTIC_DISABLED", status=409, message="Semantic retrieval is disabled.")
        coll = collections(cfg)
        limit = min(max(int(payload.get("limit") or 20), 1), int((cfg.get("query") or {}).get("max_limit", 1000)))
        snapshots = await storage.aggregate(
            coll["snapshots"],
            [{"$match": {
                "tenantId": str(cfg["tenant_id"]),
                "modelSchemaVersion": MODEL_SCHEMA_VERSION,
                "state": "published",
            }}],
        )
        snapshot_refs = [item["_id"] for item in snapshots]
        if not snapshot_refs:
            return {"ok": True, "modeRequested": mode, "modeUsed": "none", "rows": [], "explain": {"publishedSnapshots": 0}}
        match: Dict[str, Any] = {
            "tenantId": str(cfg["tenant_id"]),
            "modelSchemaVersion": MODEL_SCHEMA_VERSION,
            "snapshotRef": {"$in": snapshot_refs},
        }
        if payload.get("studyIds"):
            match["studyId"] = {"$in": [str(value) for value in payload["studyIds"]]}
        if payload.get("snapshotIds"):
            match["snapshotId"] = {"$in": [str(value) for value in payload["snapshotIds"]]}
        if payload.get("domains"):
            match["domain"] = {"$in": [str(value).upper() for value in payload["domains"]]}
        if payload.get("profile"):
            match["profile"] = str(payload["profile"]).lower()

        lexical_rows: List[Dict[str, Any]] = []
        lexical_engine = "portable-token"
        if mode in {"lexical", "hybrid"}:
            if (ctx.adapters or {}).get("atlas_search") is not None:
                lexical_engine = "atlas-search"
                lexical_rows = await storage.aggregate(coll["records"], [
                    {"$search": {"index": semantic_cfg.get("lexical_index", "cdisc_semantic_text"), "text": {"query": query, "path": "semantic.text"}}},
                    {"$match": match},
                    {"$limit": limit * 5},
                    {"$project": {"_id": 1, "studyId": 1, "snapshotId": 1, "datasetId": 1, "domain": 1, "facets": 1, "data": 1, "score": {"$meta": "searchScore"}}},
                ])
            else:
                candidates = await storage.aggregate(coll["records"], [
                    {"$match": match}, {"$limit": int(semantic_cfg.get("max_fallback_scan", 10_000))},
                ])
                terms = [term for term in re.findall(r"[a-z0-9]+", query.lower()) if term]
                for item in candidates:
                    text = str((item.get("semantic") or {}).get("text") or " ".join(
                        str(value) for value in (item.get("data") or {}).values() if value not in (None, "")
                    )).lower()
                    score = sum(text.count(term) for term in terms)
                    if score:
                        lexical_rows.append({**item, "score": float(score)})
                lexical_rows.sort(key=lambda item: (-item["score"], str(item.get("_id"))))
                lexical_rows = lexical_rows[: limit * 5]

        vector_rows: List[Dict[str, Any]] = []
        vector_warning = None
        if mode in {"semantic", "hybrid"}:
            try:
                vector_rows = await storage.aggregate(coll["records"], [
                    {"$vectorSearch": {
                        "index": semantic_cfg.get("auto_embed_index", "cdisc_semantic_auto_embed"),
                        "path": "semantic.text", "query": query,
                        "numCandidates": max(limit * 10, 100), "limit": limit * 5, "filter": match,
                    }},
                    {"$project": {"_id": 1, "studyId": 1, "snapshotId": 1, "datasetId": 1, "domain": 1, "facets": 1, "data": 1, "score": {"$meta": "vectorSearchScore"}}},
                ])
            except Exception as exc:
                if mode == "semantic":
                    raise KehrnelError(code="CDISC_SEMANTIC_SEARCH_FAILED", status=502, message=str(exc)) from exc
                vector_warning = f"Atlas Automated Embedding unavailable; hybrid search used lexical results only: {exc}"

        if mode == "lexical" or not vector_rows:
            rows = lexical_rows[:limit]
            mode_used = "lexical"
        elif mode == "semantic":
            rows = vector_rows[:limit]
            mode_used = "semantic"
        else:
            fused: Dict[str, Dict[str, Any]] = {}
            for source, candidates in (("lexical", lexical_rows), ("semantic", vector_rows)):
                for rank, item in enumerate(candidates, start=1):
                    key = str(item.get("_id"))
                    current = fused.setdefault(key, {**item, "score": 0.0, "scoreSources": {}})
                    contribution = 1.0 / (60 + rank)
                    current["score"] += contribution
                    current["scoreSources"][source] = contribution
            rows = sorted(fused.values(), key=lambda item: (-item["score"], str(item.get("_id"))))[:limit]
            mode_used = "hybrid"
        return {
            "ok": True,
            "modeRequested": mode,
            "modeUsed": mode_used,
            "rows": rows,
            "explain": {
                "tenantInjected": True,
                "publishedSnapshotConstraint": True,
                "lexicalEngine": lexical_engine if lexical_rows or mode in {"lexical", "hybrid"} else None,
                "autoEmbedIndex": semantic_cfg.get("auto_embed_index") if vector_rows else None,
                "warning": vector_warning,
            },
        }
