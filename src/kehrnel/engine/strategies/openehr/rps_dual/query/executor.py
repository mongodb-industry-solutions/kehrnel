"""Execute compiled plans using strategy adapters."""
from __future__ import annotations

from copy import deepcopy
from time import perf_counter
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
    explain = deepcopy(plan.plan.get("explain") or {})
    db_duration_ms = None
    if plan.plan.get("pipeline") is not None:
        explain["pipeline"] = plan.plan.get("pipeline")
    if plan.plan.get("collection") is not None and "collection" not in explain:
        explain["collection"] = plan.plan.get("collection")
    if plan.plan.get("scope") is not None and "scope" not in explain:
        explain["scope"] = plan.plan.get("scope")
    try:
        if plan.engine == "mongo_pipeline":
            if storage and plan.plan.get("collection"):
                db_started = perf_counter()
                rows = await storage.aggregate(plan.plan["collection"], prepared_pipeline)
                db_duration_ms = round((perf_counter() - db_started) * 1000, 2)
        elif plan.engine.startswith("atlas_search") or plan.engine == "text_search_dual":
            # same aggregate; pipeline starts with $search
            if storage and plan.plan.get("collection"):
                db_started = perf_counter()
                rows = await storage.aggregate(plan.plan["collection"], prepared_pipeline)
                db_duration_ms = round((perf_counter() - db_started) * 1000, 2)
    except Exception as exc:  # surface pipeline even on failure
        explain["error"] = str(exc)
    if db_duration_ms is not None:
        explain_timings = dict(explain.get("timings") or {})
        explain_timings["kehrnel_db_ms"] = db_duration_ms
        explain["timings"] = explain_timings
    return QueryResult(engine_used=plan.engine, rows=rows, explain=explain)
