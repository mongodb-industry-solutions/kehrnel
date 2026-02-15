import pytest

from kehrnel.domains.openehr.aql.parse import parse_aql
from kehrnel.core.types import StrategyContext
from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from tests.helpers.fixture_storage import FixtureStorage
from pathlib import Path


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Compatibility $search compound filter/lookup parity pending", strict=False)
async def test_cross_patient_search_embedded_and_lookup():
    cfg = load_json(DEFAULTS_PATH)
    cfg.setdefault("query_engine", {})["lookup_full_composition"] = True
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    strat = RPSDualStrategy(MANIFEST)
    aql = "select Centro as Centro from compositions where text = 'hello'"
    ir = parse_aql(aql)
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert "$search" in pipeline[0]
    search_stage = pipeline[0]["$search"]
    assert "compound" in search_stage
    filters = search_stage["compound"].get("filter", [])
    assert any("embeddedDocument" in f for f in filters)
    assert any("$lookup" in stage for stage in pipeline)
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project
    proj_expr = project["$project"]["Centro"]["$first"]["$map"]["input"]
    assert isinstance(proj_expr, str)
