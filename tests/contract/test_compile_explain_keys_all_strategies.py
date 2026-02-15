from pathlib import Path

import pytest

from kehrnel.core.types import StrategyContext
from kehrnel.domains.openehr.aql.parse import parse_aql
from kehrnel.strategies.openehr.rps_dual.strategy import DEFAULTS_PATH, MANIFEST as RPS_MANIFEST, RPSDualStrategy, load_json
from kehrnel.strategies.fhir.resource_first.strategy import FHIRResourceFirstStrategy
from kehrnel.strategies.genomics.variant_first.strategy import GenomicsVariantFirstStrategy
from tests.helpers.fixture_storage import FixtureStorage


REQUIRED_KEYS = {"engine", "domain", "strategy_id", "strategy_version", "activation_id", "config_hash", "manifest_digest", "scope"}


def _assert_keys(explain: dict):
    missing = REQUIRED_KEYS - explain.keys()
    assert not missing, f"missing explain keys: {missing}"
    assert explain["config_hash"]
    assert explain["manifest_digest"]


@pytest.mark.asyncio
async def test_explain_keys_rps_dual():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    strat = RPSDualStrategy(RPS_MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage}, manifest=RPS_MANIFEST, meta={"activation_id": "act-rps"})
    ir = parse_aql("select Centro as Centro from compositions")
    ir.scope = "patient"
    ir.predicates = [{"path": "ehr_id", "op": "eq", "value": "p1"}]
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    _assert_keys(plan.explain)
    assert plan.explain["engine"] == "query_engine"
    assert plan.explain["scope"] == "patient"


@pytest.mark.asyncio
async def test_explain_keys_fhir_resource_first():
    strat_manifest = strat_manifest_loader("fhir.resource_first")
    strat = FHIRResourceFirstStrategy(manifest=strat_manifest)
    ctx = StrategyContext(environment_id="env", config={}, manifest=strat_manifest, meta={"activation_id": "act-fhir"})
    plan = await strat.compile_query(ctx, "fhir", {"scope": "patient"})
    _assert_keys(plan.explain)
    assert plan.explain["domain"] == "fhir"
    assert plan.explain["engine"] == "fhir_dummy"


@pytest.mark.asyncio
async def test_explain_keys_genomics_variant_first():
    strat_manifest = strat_manifest_loader("genomics.variant_first")
    strat = GenomicsVariantFirstStrategy(manifest=strat_manifest)
    ctx = StrategyContext(environment_id="env", config={}, manifest=strat_manifest, meta={"activation_id": "act-geno"})
    plan = await strat.compile_query(ctx, "genomics", {"scope": "patient"})
    _assert_keys(plan.explain)
    assert plan.explain["domain"] == "genomics"
    assert plan.explain["engine"] == "genomics_dummy"


def strat_manifest_loader(strategy_id: str):
    # load manifests using the same loader as API
    from kehrnel.api.app import _load_manifests

    manifests, _, _ = _load_manifests()
    by_id = {m.id: m for m in manifests}
    if strategy_id not in by_id:
        raise AssertionError(f"strategy manifest not found: {strategy_id}")
    return by_id[strategy_id]
