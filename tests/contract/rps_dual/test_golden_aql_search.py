import pytest

from kehrnel.engine.domains.openehr.aql.parse import parse_aql
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.engine.core.types import StrategyContext


@pytest.mark.asyncio
async def test_cross_patient_aql_uses_search_for_non_metadata_predicates():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg)
    ir = parse_aql("select t as t from compositions where text = 'hello'")
    ir.scope = "cross_patient"
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
    assert "$search" in pipeline[0], "cross-patient should start with $search"
    search_stage = pipeline[0]["$search"]
    compound = search_stage.get("compound", {})
    assert len(compound.get("must", [])) == 2
    assert plan.explain["builder"]["reason"] == "scope_cross_patient"
