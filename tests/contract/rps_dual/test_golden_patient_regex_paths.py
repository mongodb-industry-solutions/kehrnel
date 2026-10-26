import pytest

from kehrnel.engine.domains.openehr.aql.parse import parse_aql
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from tests.helpers.fixture_storage import FixtureStorage
from pathlib import Path


@pytest.mark.asyncio
async def test_patient_root_path_uses_single_elem_match():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    strat = RPSDualStrategy(MANIFEST)
    aql = "select Centro as Centro from compositions where ehr_id = 'p1'"
    ir = parse_aql(aql)
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert "$match" in pipeline[0]
    match_stage = pipeline[0]["$match"]
    assert match_stage["ehr_id"] == "p1"
    assert match_stage["cn"]["$elemMatch"]["p"]["$regex"] == "^[^:]+$"
