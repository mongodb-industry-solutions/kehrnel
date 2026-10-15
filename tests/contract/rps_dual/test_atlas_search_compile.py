import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR


def strategy_with_config():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    return strat, cfg


@pytest.mark.asyncio
async def test_cross_patient_embedded_document_pipeline():
    strat, cfg = strategy_with_config()
    ctx = StrategyContext(environment_id="env", config=cfg)
    ir = AqlQueryIR(
        scope="cross_patient",
        predicates=[{"path": "admin/path", "op": "eq", "value": "abc"}],
        select=[{"path": "ehr_id", "alias": "ehr_id"}],
    )
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "Pipeline should not be empty"
    assert list(pipeline[0].keys())[0] == "$search"
    search = pipeline[0]["$search"]
    assert search["index"] == "search_nodes_index"
    assert search["text"] == {"query": "abc", "path": "data.path"}
    assert plan.explain["builder"]["chosen"] == "search_pipeline_builder"
    assert plan.explain["builder"]["reason"] == "scope_cross_patient"


@pytest.mark.asyncio
async def test_lookup_is_automatically_added_for_full_composition_projection():
    strat, cfg = strategy_with_config()
    ctx = StrategyContext(environment_id="env", config=cfg)
    ir = AqlQueryIR(
        scope="cross_patient",
        predicates=[{"path": "med_ac/time/value", "op": "gt", "value": "2020-01-01T00:00:00"}],
        select=[
            {
                "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
                "alias": "Centro",
            }
        ],
    )
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert "$search" in pipeline[0]
    assert any("$lookup" in stage for stage in pipeline), "lookup should be inferred from the projection"


@pytest.mark.asyncio
async def test_post_match_added_for_unsupported_predicate():
    strat, cfg = strategy_with_config()
    ctx = StrategyContext(environment_id="env", config=cfg)
    ir = AqlQueryIR(
        scope="cross_patient",
        predicates=[{"path": "ehr_id", "op": "contains", "value": "p"}],
        select=[{"path": "ehr_id", "alias": "ehr_id"}],
    )
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert len(pipeline) >= 2
    assert "$match" in pipeline[1]
