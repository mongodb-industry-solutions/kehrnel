"""Execute compiled plans using strategy adapters."""
from __future__ import annotations

from typing import Any, Dict, List
from bson.code import Code

from kehrnel.engine.core.types import QueryResult, QueryPlan, StrategyContext


def _prepare_pipeline(value: Any) -> Any:
    if isinstance(value, list):
        return [_prepare_pipeline(item) for item in value]
    if isinstance(value, dict):
        prepared = {key: _prepare_pipeline(item) for key, item in value.items()}
        fn_spec = prepared.get("$function")
        if isinstance(fn_spec, dict) and isinstance(fn_spec.get("body"), str):
            fn_spec["body"] = Code(fn_spec["body"])
        return prepared
    return value


async def execute(ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
    storage = (ctx.adapters or {}).get("storage")
    rows: List[Dict[str, Any]] = []
    prepared_pipeline = _prepare_pipeline(plan.plan.get("pipeline", []))
    explain = {"plan": plan.plan, "pipeline": plan.plan.get("pipeline")}
    try:
        if plan.engine == "mongo_pipeline":
            if storage and plan.plan.get("collection"):
                rows = await storage.aggregate(plan.plan["collection"], prepared_pipeline)
        elif plan.engine.startswith("atlas_search") or plan.engine == "text_search_dual":
            # same aggregate; pipeline starts with $search
            if storage and plan.plan.get("collection"):
                rows = await storage.aggregate(plan.plan["collection"], prepared_pipeline)
    except Exception as exc:  # surface pipeline even on failure
        explain["error"] = str(exc)
    if plan.plan.get("explain"):
        explain.update(plan.plan["explain"])
    return QueryResult(engine_used=plan.engine, rows=rows, explain=explain)
