from __future__ import annotations

from typing import Any, Dict

from kehrnel.core.plugin import StrategyPlugin
from kehrnel.core.types import QueryPlan, QueryResult, StrategyContext
from kehrnel.core.explain import enrich_explain


class FHIRResourceFirstStrategy(StrategyPlugin):
    def __init__(self, manifest):
        self.manifest = manifest

    async def validate_config(self, ctx: StrategyContext):
        # minimal validation already done upstream
        return True

    async def plan(self, ctx: StrategyContext):
        return {}

    async def apply(self, ctx: StrategyContext, plan=None):
        return {}

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]):
        return {}

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]):
        return {}

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]):
        return {}

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        dval = (domain or "fhir").lower()
        pipeline = [{"$match": {"domain": dval}}]
        explain = {
            "builder": {"chosen": "fhir_dummy"},
            "scope": query.get("scope") or "unknown",
            "reason": "fhir_dummy_strategy",
        }
        explain = enrich_explain(explain, ctx, domain=dval, engine="fhir_dummy", scope=query.get("scope") or "unknown")
        return QueryPlan(engine="fhir_dummy", plan={"pipeline": pipeline, "explain": explain}, explain=explain)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        return QueryResult(engine_used=plan.engine, rows=[], explain=plan.plan.get("explain"))

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]):
        raise ValueError(f"Strategy op '{op}' not supported")
