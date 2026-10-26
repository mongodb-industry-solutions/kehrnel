from __future__ import annotations

from typing import Any, Dict

from .types import QueryPlan, QueryResult, ApplyPlan, ApplyResult, TransformResult, StrategyContext


class StrategyPlugin:
    """
    Base interface for strategy plugins. Methods are async-first; sync implementations are allowed.
    """

    async def validate_config(self, ctx: StrategyContext) -> None:
        return None

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        raise NotImplementedError

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        raise NotImplementedError

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        raise NotImplementedError

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        raise NotImplementedError

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        raise NotImplementedError

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        raise NotImplementedError

    async def execute_authoritative_write(
        self,
        ctx: StrategyContext,
        request: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute one governed record write through the strategy's native boundary.

        This is deliberately separate from ``ingest``: ingestion prepares or
        imports data, while an authoritative write must preserve the domain's
        version, concurrency, provenance and acknowledgement semantics.
        """
        raise NotImplementedError

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Strategy-specific operation dispatcher. Default: not supported.
        """
        raise NotImplementedError(f"Strategy op '{op}' not supported")
