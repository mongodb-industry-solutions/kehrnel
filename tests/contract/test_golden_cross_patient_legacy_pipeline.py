import pytest
from kehrnel.engine.domains.openehr.aql.parse import parse_aql
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from tests.helpers.fixture_storage import FixtureStorage
from pathlib import Path


@pytest.mark.asyncio
async def test_compatibility_cross_patient_pipeline_shape():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    ir = parse_aql("select Centro as Centro from compositions where text = 'hello'")
    # ensure predicate present for search and mark cross_patient scope
    ir.scope = "cross_patient"
    ir.select = [
        {
            "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
            "alias": "Centro",
            "agg": "first",
        }
    ]
    # add two predicates to force compound filter
    ir.predicates = [
        {"path": "text", "op": "eq", "value": "hello"},
        {"path": "med_ac/time/value", "op": "gt", "value": "2020-01-01T00:00:00"},
    ]
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$search" in pipeline[0], "cross-patient pipeline must start with $search"
    search_stage = pipeline[0]["$search"]
    assert "compound" in search_stage, "expected compound.filter in search stage"
    # ensure lookup/projection follow when enabled
    has_lookup = any("$lookup" in stage for stage in pipeline)
    assert has_lookup is True or cfg.get("query_engine", {}).get("lookup_full_composition") is False
