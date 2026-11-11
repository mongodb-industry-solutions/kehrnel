import json
from pathlib import Path

from kehrnel.engine.core.explain import enrich_explain
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.types import StrategyContext


ROOT = Path(__file__).resolve().parents[2]


def _manifest(relative_path: str) -> StrategyManifest:
    payload = json.loads((ROOT / relative_path).read_text(encoding="utf-8"))
    return StrategyManifest.model_validate(payload)


def test_cdisc_manifest_distinguishes_native_compiled_and_hybrid_interactions():
    manifest = _manifest("src/kehrnel/engine/strategies/cdisc/sdr/manifest.json")

    modes = {contract.id: contract.mode for contract in manifest.interaction_contracts}

    assert modes == {
        "cdisc.dataset-exchange": "native",
        "cdisc.study-query": "compiled",
        "cdisc.study-analysis": "compiled",
        "cdisc.operational-evidence": "hybrid",
    }


def test_fhir_query_explain_identifies_the_native_interaction_contract():
    manifest = _manifest(
        "src/kehrnel/engine/strategies/fhir/resource_store/specification/manifest.json"
    )
    ctx = StrategyContext(environment_id="test", config={}, manifest=manifest, meta={})

    explain = enrich_explain(
        {"builder": {"chosen": "fhir_mql"}},
        ctx,
        domain="fhir",
        engine="fhir_mql",
        scope="Observation",
    )

    assert explain["interaction"] == {
        "id": "fhir.rest-search",
        "name": "FHIR REST and Search",
        "mode": "native",
        "kind": "resource-api",
        "authority": "HL7 FHIR",
        "standard": "FHIR R4, R5 and R6",
        "contract": "FHIR RESTful API and Search",
    }


def test_cdisc_query_explain_identifies_the_compiled_interaction_contract():
    manifest = _manifest("src/kehrnel/engine/strategies/cdisc/sdr/manifest.json")
    ctx = StrategyContext(environment_id="test", config={}, manifest=manifest, meta={})

    explain = enrich_explain(
        {"builder": {"chosen": "cdisc_study_query_v1"}},
        ctx,
        domain="cdisc",
        engine="mongo_pipeline",
        scope="study",
    )

    assert explain["interaction"]["id"] == "cdisc.study-query"
    assert explain["interaction"]["mode"] == "compiled"
    assert explain["interaction"]["contract"] == "cdisc-query/v1"


def test_unmatched_engine_does_not_invent_interaction_provenance():
    manifest = _manifest("src/kehrnel/engine/strategies/cdisc/sdr/manifest.json")
    ctx = StrategyContext(environment_id="test", config={}, manifest=manifest, meta={})

    explain = enrich_explain({}, ctx, domain="cdisc", engine="unknown")

    assert "interaction" not in explain
