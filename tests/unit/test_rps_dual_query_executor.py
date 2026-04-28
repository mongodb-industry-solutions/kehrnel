from __future__ import annotations

import pytest

from kehrnel.engine.core.types import QueryPlan, StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.query.executor import execute


class _FakeStorage:
    async def aggregate(self, _collection, _pipeline):
        return [{"ok": True}]


@pytest.mark.asyncio
async def test_execute_query_result_explain_keeps_single_pipeline_copy():
    ctx = StrategyContext(
        environment_id="env-test",
        config={},
        adapters={"storage": _FakeStorage()},
        meta={},
    )
    plan = QueryPlan(
        engine="mongo_pipeline",
        plan={
            "collection": "compositions_rps",
            "scope": "cross_patient",
            "pipeline": [{"$match": {"tid": "air_adverse_reaction_record_v1"}}],
            "explain": {
                "engine": "query_engine",
                "builder": {"chosen": "pipeline_builder"},
            },
        },
    )

    result = await execute(ctx, plan)

    assert result.engine_used == "mongo_pipeline"
    assert result.rows == [{"ok": True}]
    assert result.explain["pipeline"] == [{"$match": {"tid": "air_adverse_reaction_record_v1"}}]
    assert "plan" not in result.explain
    assert result.explain["builder"]["chosen"] == "pipeline_builder"
    assert isinstance(result.explain["timings"]["kehrnel_db_ms"], float)
    assert result.explain["timings"]["kehrnel_db_ms"] >= 0
