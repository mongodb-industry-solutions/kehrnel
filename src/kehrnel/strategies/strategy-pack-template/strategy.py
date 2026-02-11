from __future__ import annotations

from typing import Any, Dict

from kehrnel.core.plugin import StrategyPlugin
from kehrnel.core.types import ApplyPlan, ApplyResult, TransformResult, QueryPlan, QueryResult, StrategyContext
from kehrnel.core.explain import enrich_explain


class TemplateStrategy(StrategyPlugin):
    """Minimal example strategy implementation."""

    def __init__(self, manifest):
        self.manifest = manifest

    async def validate_config(self, ctx: StrategyContext):
        return True

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        return ApplyPlan(artifacts={})

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        return ApplyResult(created=[], warnings=[])

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        return TransformResult(base=payload)

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {"ok": True}

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        pipeline = [{"$match": {"domain": (domain or "example").lower()}}]
        explain = enrich_explain({"builder": {"chosen": "template"}}, ctx, domain=domain or "example", engine="template", scope=query.get("scope") or "unknown")
        return QueryPlan(engine="template", plan={"pipeline": pipeline, "explain": explain}, explain=explain)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        return QueryResult(engine_used=plan.engine, rows=[], explain=plan.explain)

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]):
        raise ValueError(f"Strategy op '{op}' not supported")
