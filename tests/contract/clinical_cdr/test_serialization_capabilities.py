"""Contract tests for T5 (canonical serialization + OperationOutcome),
T6 (capability catalog), and T7 (no Mongo executes on unsupported search)."""

from __future__ import annotations

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext

from kehrnel.engine.strategies.fhir.clinical_cdr import query as fhir_query
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import serialization as ser
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge

pytest.importorskip("fhir_search_to_mql")


def _ctx(*, config: dict | None = None) -> StrategyContext:
    return StrategyContext(
        environment_id="test-env",
        config=config or {"database": "fhir_test"},
        bindings={"db": {"provider": "mongodb", "uri": "mongodb://localhost:27017", "database": "fhir_test"}},
        manifest=MANIFEST,
    )


# ── T5: canonical serialization ───────────────────────────────────────────────

def test_canonical_resource_strips_operational_keeps_primitive_extensions():
    doc = {
        "_id": "abc123",
        "resourceType": "Patient",
        "id": "pat-1",
        "birthDate": "1980-01-01",
        "_birthDate": {"extension": [{"url": "x", "valueString": "approx"}]},  # primitive ext — KEEP
        "_search": {"patient": "pat-1"},
        "_compartments": ["Patient/pat-1"],
        "_stored_at": "2026-08-22T00:00:00Z",
        "_fhir_resource_type": "Patient",
        "_kehrnel": {"storage_schema_version": "1"},
        "_custom": {"customerScore": 7},
        "_enrichments": {"cohort": "example"},
    }
    out = ser.canonical_resource(doc)
    # operational fields removed
    for k in ("_id", "_search", "_compartments", "_stored_at", "_fhir_resource_type", "_kehrnel", "_custom", "_enrichments"):
        assert k not in out
    # canonical + primitive extension preserved
    assert out["resourceType"] == "Patient"
    assert out["id"] == "pat-1"
    assert out["_birthDate"] == {"extension": [{"url": "x", "valueString": "approx"}]}


def test_searchset_bundle_shape():
    b = ser.searchset_bundle([{"resourceType": "Patient", "id": "p1", "_id": "x"}], total=1)
    assert b["resourceType"] == "Bundle"
    assert b["type"] == "searchset"
    assert b["total"] == 1
    assert b["entry"][0]["resource"] == {"resourceType": "Patient", "id": "p1"}


def test_operation_outcome_maps_code():
    oo = ser.operation_outcome(code="FHIR_SEARCH_UNSUPPORTED_PARAM", message="bad param")
    assert oo["resourceType"] == "OperationOutcome"
    assert oo["issue"][0]["code"] == "not-supported"
    assert oo["issue"][0]["diagnostics"] == "bad param"


def test_execution_summary_is_bounded_no_id_arrays():
    plan_body = {"collection": "Patient", "filter": {"_multi_step": [{"query": {}}, {"query": {}}]}}
    explain = {"total": 3, "returned": 3, "execution": {"collection": "Patient", "limit": 10, "skip": 0}}
    resp = ser.build_search_response(plan_body=plan_body, engine_used="fhir_mql", rows=[], explain=explain)
    assert resp["contract_version"] == ser.SEARCH_CONTRACT_VERSION
    summ = resp["execution_summary"]
    assert summ["multi_step"] == {"stage_count": 2}  # bounded — count only, no id lists
    assert "mongo_execution_stats" not in resp  # privileged-only


# ── T6: capability catalog ────────────────────────────────────────────────────

def test_fhir_capabilities_reports_distinct_sets():
    cat = fhir_query.fhir_capabilities(_ctx())
    assert cat["ok"] and cat["fhir_version"] == "R5"
    assert "Patient" in cat["searchable_resource_types"]
    assert "Patient" in cat["generatable_resource_types"]
    assert "Patient" in cat["synthetic_writable_resource_types"]
    assert cat["ingest_supported"] is True
    assert cat["write_supported"] is True
    assert "Patient" in cat["storable_resource_types"]
    assert set(cat["storable_resource_types"]) == set(cat["searchable_resource_types"])
    assert set(cat["recipe_resource_types"]) < set(cat["storable_resource_types"])
    assert set(cat["storable_resource_types"]) < set(cat["schema_supported_resource_types"])
    assert set(cat["synthetic_writable_resource_types"]) <= set(cat["generatable_resource_types"])
    assert set(cat["generation_only_resource_types"]) == (
        set(cat["generatable_resource_types"])
        - set(cat["synthetic_writable_resource_types"])
    )
    assert cat["capability_counts"]["synthetic_writable"] == len(
        cat["synthetic_writable_resource_types"]
    )
    assert "does not imply persistence" in cat["capability_semantics"][
        "generatable_resource_types"
    ]
    assert set(cat["validation_levels"]) == {"structure", "base"}
    assert cat["conformance_mode"] == "fhir-core"
    assert cat["implementation_guide_packages"] == []
    assert cat["available_profiles"] == []
    assert cat["profile_conformance"] is False
    assert "_count" in cat["supported_result_controls"]
    assert cat["chaining_supported"] is True
    assert cat["reverse_chaining_supported"] is True
    assert cat["chaining_limits"]["maximum_hops"] == 1
    assert set(cat["handling_modes"]) == {"strict", "lenient"}


# ── T7: unsupported search must NOT reach Mongo ───────────────────────────────

@pytest.mark.asyncio
async def test_unsupported_search_never_builds_mongo_context(monkeypatch):
    """Fail-closed proof: build_mql_context must not be called for a bad search."""
    calls = {"n": 0}

    def _boom(*args, **kwargs):
        calls["n"] += 1
        raise AssertionError("Mongo context must not be built for an unsupported search")

    monkeypatch.setattr(bridge, "build_mql_context", _boom)

    from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import FHIRClinicalCDRStrategy
    strat = FHIRClinicalCDRStrategy(MANIFEST)
    with pytest.raises(KehrnelError) as exc:
        await strat.run_op(_ctx(), "fhir_search", {"resource_type": "Patient", "criteria": {"totally_unsupported": "x"}})
    assert exc.value.code == "FHIR_SEARCH_UNSUPPORTED_PARAM"
    assert calls["n"] == 0


# ── T3: storage-adapter seam (persist/read/serialize) ─────────────────────────

def test_storage_adapter_protocol_and_readpath():
    from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.storage_adapter import (
        FHIRStorageAdapter, MongoFHIRStorageAdapter,
    )

    class _FakeColl:
        def find_one(self, q, projection=None):
            assert projection["_custom"] == 0
            assert projection["_enrichments"] == 0
            return {"_id": "x", "resourceType": "Patient", "id": q["id"], "_search": {"a": 1}}

    class _FakeDb:
        def __getitem__(self, name):
            assert name == "fhir_Patient"
            return _FakeColl()

    adapter = MongoFHIRStorageAdapter(_FakeDb(), collection_prefix="fhir_")
    assert isinstance(adapter, FHIRStorageAdapter)  # satisfies the Protocol

    # serialize strips operational fields
    assert adapter.serialize({"_id": "x", "resourceType": "Patient", "id": "p1", "_search": {}}) == {
        "resourceType": "Patient", "id": "p1"
    }
    # read returns canonical resource
    assert adapter.read("Patient", "p1") == {"resourceType": "Patient", "id": "p1"}
    assert callable(adapter.persist)
