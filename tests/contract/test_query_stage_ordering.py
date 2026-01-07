import pytest

from kehrnel.core.types import StrategyContext
from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.domains.openehr.aql.ir import AqlQueryIR


def strategy_ctx():
    cfg = load_json(DEFAULTS_PATH)
    return RPSDualStrategy(MANIFEST), StrategyContext(environment_id="env", config=cfg)


@pytest.mark.asyncio
async def test_patient_query_starts_with_match():
    strat, ctx = strategy_ctx()
    ir = AqlQueryIR(
        scope="patient",
        predicates=[{"path": "ehr_id", "op": "eq", "value": "p1"}],
        select=[{"path": "ehr_id", "alias": "ehr_id"}],
    )
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$match" in pipeline[0], "patient query must start with $match"


@pytest.mark.asyncio
async def test_cross_patient_query_starts_with_search():
    strat, ctx = strategy_ctx()
    ir = AqlQueryIR(
        scope="cross_patient",
        predicates=[{"path": "text", "op": "eq", "value": "hello"}],
        select=[{"path": "ehr_id", "alias": "ehr_id"}],
    )
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$search" in pipeline[0], "$search must be first stage for cross-patient queries"
