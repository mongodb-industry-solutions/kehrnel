import json
from pathlib import Path

import pytest

from kehrnel.core.types import StrategyContext
from kehrnel.domains.openehr.aql.parse import parse_aql
from kehrnel.strategies.openehr.rps_dual.strategy import DEFAULTS_PATH, MANIFEST, RPSDualStrategy, load_json
from tests.helpers.fixture_storage import FixtureStorage


def _required_keys():
    return {"engine", "domain", "strategy_id", "strategy_version", "activation_id", "config_hash", "manifest_digest", "scope"}


def _assert_explain_metadata(plan_explain: dict):
    missing = _required_keys() - plan_explain.keys()
    assert not missing, f"Missing explain metadata: {missing}"
    assert plan_explain["engine"] == "query_engine"
    assert isinstance(plan_explain["config_hash"], str) and plan_explain["config_hash"]
    assert plan_explain["manifest_digest"]


@pytest.mark.asyncio
async def test_compile_explain_contains_engine_and_activation_metadata():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    strat = RPSDualStrategy(MANIFEST)
    meta = {"activation_id": "act-123", "config_hash": "override-hash", "manifest_digest": "override-digest"}
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage}, manifest=MANIFEST, meta=meta)

    # patient scope
    ir_patient = parse_aql("select Centro as Centro from compositions")
    ir_patient.scope = "patient"
    ir_patient.predicates = [{"path": "ehr_id", "op": "eq", "value": "p1"}]
    patient_plan = await strat.compile_query(ctx, "openehr", ir_patient.to_dict())
    _assert_explain_metadata(patient_plan.explain)
    assert patient_plan.explain["scope"] == "patient"

    # cross-patient scope
    ir_cross = parse_aql("select Centro as Centro from compositions where text = 'hello'")
    ir_cross.scope = "cross_patient"
    ir_cross.predicates = [{"path": "text", "op": "eq", "value": "hello"}]
    cross_plan = await strat.compile_query(ctx, "openehr", ir_cross.to_dict())
    _assert_explain_metadata(cross_plan.explain)
    assert cross_plan.explain["scope"] == "cross_patient"
