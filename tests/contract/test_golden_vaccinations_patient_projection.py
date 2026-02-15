import pytest

from kehrnel.domains.openehr.aql.parse import parse_aql
from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.core.types import StrategyContext


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Compatibility projection field casing parity pending", strict=False)
async def test_vaccination_patient_projection_shape():
    cfg = load_json(DEFAULTS_PATH)
    storage = None
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    aql = "select Centro as Centro, FechaAdmin as FechaAdmin from compositions where ehr_id = 'p1'"
    ir = parse_aql(aql)
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$match" in pipeline[0]
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project, "projection expected"
    proj_fields = project["$project"]
    assert "Centro" in proj_fields
    # ensure projection uses regexMatch/filter structure
    centro_expr = proj_fields["Centro"]["$first"]["$map"]["input"]["$filter"]["cond"]
    assert "$regexMatch" in centro_expr
