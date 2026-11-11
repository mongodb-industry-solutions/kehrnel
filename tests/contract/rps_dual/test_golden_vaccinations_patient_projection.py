import pytest

from kehrnel.engine.domains.openehr.aql.parse import parse_aql
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.engine.core.types import StrategyContext


@pytest.mark.asyncio
async def test_vaccination_patient_projection_shape():
    cfg = load_json(DEFAULTS_PATH)
    storage = None
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    aql = "select Centro as Centro, FechaAdmin as FechaAdmin from compositions where ehr_id = 'p1'"
    ir = parse_aql(aql)
    ir.select = [
        {
            "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
            "alias": "Centro",
        },
        {"path": "med_ac/time/value", "alias": "FechaAdmin"},
    ]
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$match" in pipeline[0]
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project, "projection expected"
    proj_fields = project["$project"]
    assert "Centro" in proj_fields
    assert "FechaAdmin" in proj_fields
    assert proj_fields["Centro"] == "$data.v.df.cs"
    assert proj_fields["FechaAdmin"] == "$data.time.v"
