import pytest

from kehrnel.engine.domains.openehr.aql.parse import parse_aql
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from tests.helpers.fixture_storage import FixtureStorage
from pathlib import Path


@pytest.mark.asyncio
async def test_cross_patient_search_and_automatic_lookup():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    strat = RPSDualStrategy(MANIFEST)
    aql = "select Centro as Centro from compositions where text = 'hello'"
    ir = parse_aql(aql)
    ir.scope = "cross_patient"
    ir.select = [
        {
            "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
            "alias": "Centro",
        }
    ]
    ir.predicates = [
        {
            "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
            "op": "eq",
            "value": "VAL1",
        },
        {"path": "med_ac/time/value", "op": "gt", "value": "2020-01-01T00:00:00"},
    ]
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert "$search" in pipeline[0]
    search_stage = pipeline[0]["$search"]
    assert "compound" in search_stage
    assert len(search_stage["compound"].get("must", [])) == 2
    assert any("$lookup" in stage for stage in pipeline)
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project
    assert "Centro" in project["$project"]
