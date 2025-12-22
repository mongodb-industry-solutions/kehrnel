import pytest
from kehrnel.protocols.openehr.aql.parse import parse_aql
from kehrnel.core.types import StrategyContext
from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from tests.helpers.fixture_storage import FixtureStorage
from pathlib import Path


@pytest.mark.asyncio
async def test_legacy_patient_pipeline_shape():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    ir = parse_aql("select Centro as Centro from compositions")
    # Force patient scope with multiple node predicates to require $all grouping
    ir.scope = "patient"
    ir.select = [
        {
            "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
            "alias": "Centro",
            "agg": "first",
        }
    ]
    ir.predicates = [
        {"path": "ehr_id", "op": "eq", "value": "p1"},
        {"path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string", "op": "eq", "value": "VAL1"},
        {"path": "med_ac/time/value", "op": "gt", "value": "2020-01-01T00:00:00"},
    ]
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$match" in pipeline[0], "patient pipeline must start with $match"
    match_stage = pipeline[0]["$match"]
    assert any("$all" in v for v in match_stage.values()), "expected $all with $elemMatch"
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project is not None
    centro_proj = project["$project"]["Centro"]
    cond = centro_proj["$let"]["vars"]["target_element"]["$first"]["$filter"]["cond"]
    assert "$regexMatch" in cond
