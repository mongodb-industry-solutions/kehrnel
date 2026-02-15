import pytest

from kehrnel.domains.openehr.aql.parse import parse_aql
from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.core.types import StrategyContext


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Compatibility lookup/projection parity pending", strict=False)
async def test_vaccination_cross_patient_projection_lookup():
    cfg = load_json(DEFAULTS_PATH)
    cfg.setdefault("query_engine", {})["lookup_full_composition"] = True
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg)
    aql = "select Centro as Centro from compositions where text = 'hello'"
    ir = parse_aql(aql)
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$search" in pipeline[0], "$search must be first"
    assert any("$lookup" in stage for stage in pipeline), "$lookup expected when lookup_full_composition is enabled"
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project, "projection expected"
    # projection should reference comp.cn when lookup is used
    proj_expr = project["$project"]["Centro"]["$first"]["$map"]["input"]
    assert isinstance(proj_expr, str) and proj_expr.startswith("$comp.cn")
