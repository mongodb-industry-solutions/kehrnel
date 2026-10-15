import pytest

from kehrnel.engine.domains.openehr.aql.parse import parse_aql
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.engine.core.types import StrategyContext


@pytest.mark.asyncio
async def test_vaccination_cross_patient_projection_lookup():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg)
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
    assert pipeline, "pipeline expected"
    assert "$search" in pipeline[0], "$search must be first"
    assert any("$lookup" in stage for stage in pipeline), "$lookup should be inferred from the projection"
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project, "projection expected"
    assert project["$project"]["Centro"] == "$data.v.df.cs"
