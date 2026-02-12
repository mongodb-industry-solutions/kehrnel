from pathlib import Path

import pytest

from kehrnel.core.errors import KehrnelError
from kehrnel.core.types import StrategyContext
from kehrnel.strategies.openehr.rps_dual.strategy import DEFAULTS_PATH, MANIFEST, RPSDualStrategy, load_json
from tests.helpers.fixture_storage import FixtureStorage


@pytest.mark.asyncio
async def test_compile_query_accepts_raw_aql_and_compiles_patient_pipeline():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage}, manifest=MANIFEST)

    raw_aql = """
    SELECT c/uid/value AS uid
    FROM EHR e CONTAINS COMPOSITION c
    WHERE e/ehr_id/value = 'p1'
    """
    plan = await strat.compile_query(ctx, "openehr", {"aql": raw_aql, "debug": True})
    pipeline = plan.plan.get("pipeline", [])

    assert pipeline
    assert "$match" in pipeline[0]
    assert plan.explain is not None
    assert plan.explain.get("scope") == "patient"
    assert plan.explain.get("aql") is not None
    assert isinstance(plan.explain.get("ast"), dict)


@pytest.mark.asyncio
async def test_compile_query_rejects_invalid_aql_before_execute():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage}, manifest=MANIFEST)

    with pytest.raises(KehrnelError) as exc_info:
        await strat.compile_query(ctx, "openehr", {"aql": "SELECT FROM"})

    assert exc_info.value.code in {"AQL_PARSE_ERROR", "AQL_AST_INVALID", "AQL_NOT_SUPPORTED"}
