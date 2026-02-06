"""Execute compiled plans using strategy adapters."""
from __future__ import annotations

from typing import Any, Dict, List

from kehrnel.core.types import QueryResult, QueryPlan, StrategyContext


async def execute(ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
    storage = (ctx.adapters or {}).get("storage")
    rows: List[Dict[str, Any]] = []
    explain = {"plan": plan.plan, "pipeline": plan.plan.get("pipeline")}
    try:
        if plan.engine == "mongo_pipeline":
            if storage and plan.plan.get("collection"):
                rows = await storage.aggregate(plan.plan["collection"], plan.plan.get("pipeline", []))
        elif plan.engine.startswith("atlas_search") or plan.engine == "text_search_dual":
            # same aggregate; pipeline starts with $search
            if storage and plan.plan.get("collection"):
                rows = await storage.aggregate(plan.plan["collection"], plan.plan.get("pipeline", []))
    except Exception as exc:  # surface pipeline even on failure
        explain["error"] = str(exc)
    if plan.plan.get("explain"):
        explain.update(plan.plan["explain"])
    return QueryResult(engine_used=plan.engine, rows=rows, explain=explain)
