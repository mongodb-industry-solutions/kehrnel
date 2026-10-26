import pytest

from kehrnel.engine.domains.openehr.aql.parse import parse_aql
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from tests.helpers.fixture_storage import FixtureStorage
from pathlib import Path


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Compatibility patient regex/elemMatch parity pending", strict=False)
async def test_patient_regex_and_all_elem_match():
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
    assert "cn" in match_stage or any("$all" in v for v in match_stage.values())
    # ensure regex present in elemMatch
    found_regex = False
    for v in match_stage.values():
        if isinstance(v, dict) and "$all" in v:
            for item in v["$all"]:
                if "$elemMatch" in item and "p" in item["$elemMatch"]:
                    if isinstance(item["$elemMatch"]["p"], dict) and "$regex" in item["$elemMatch"]["p"]:
                        found_regex = True
    assert found_regex
