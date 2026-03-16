import pytest

from kehrnel.domains.openehr.aql.parse import parse_aql
from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.core.types import StrategyContext


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Compatibility embeddedDocument grouping not yet restored", strict=False)
async def test_cross_patient_aql_compiles_to_search_first():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg)
    ir = parse_aql("select t as t from compositions where text = 'hello'")
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$search" in pipeline[0], "cross-patient should start with $search"
    # embeddedDocument grouping should appear for predicates
    search_stage = pipeline[0]["$search"]
    compound = search_stage.get("compound", {})
    filters = compound.get("filter", [])
    assert any("embeddedDocument" in f for f in filters), "embeddedDocument grouping expected"
