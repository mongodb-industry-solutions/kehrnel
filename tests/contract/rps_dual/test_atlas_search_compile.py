import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR


def strategy_with_config():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    return strat, cfg


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Compatibility search pipeline parity pending ($search compound filter shape)", strict=False)
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
    filters = pipeline[0]["$search"]["compound"]["filter"]
    assert any("embeddedDocument" in f for f in filters)
    embedded = next(f for f in filters if "embeddedDocument" in f)
    wildcard_filter = embedded["embeddedDocument"]["operator"]["compound"]["filter"][0]["wildcard"]
    assert wildcard_filter["path"].endswith(".p")


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Compatibility lookup behavior parity pending", strict=False)
async def test_lookup_appended_when_configured():
    strat, cfg = strategy_with_config()
    cfg.setdefault("query_engine", {})["lookup_full_composition"] = True
    ctx = StrategyContext(environment_id="env", config=cfg)
    ir = AqlQueryIR(
        scope="cross_patient",
        predicates=[{"path": "ehr_id", "op": "eq", "value": "p1"}],
        select=[{"path": "ehr_id", "alias": "ehr_id"}],
    )
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert any("$lookup" in stage for stage in pipeline), "lookup should be present after $search when configured"


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Compatibility post-filter behavior parity pending", strict=False)
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
