"""R4 full package-backed capability contract.

R4 is now backed by the bundled fhir.schema.v4.json and 146 R4 search configs.
These tests verify the upgraded R4 support tier.
"""

from __future__ import annotations

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.resource_store.scripts.query import (
    compile_fhir_query,
    fhir_capabilities,
    fhir_list_search_params,
)
from kehrnel.engine.strategies.fhir.resource_store.scripts.import_resources import (
    fhir_import_resources,
)
from kehrnel.engine.strategies.fhir.resource_store.scripts.resource_catalog import (
    fhir_resource_catalog,
)
from kehrnel.engine.strategies.fhir.resource_store.scripts.validation import (
    available_validation_levels,
    validate_level,
)
from kehrnel.engine.strategies.fhir.resource_store.strategy import MANIFEST


def _ctx() -> StrategyContext:
    return StrategyContext(
        environment_id="r4-test",
        config={
            "database": "fhir_r4_test",
            "schema_version": "R4",
        },
        bindings={},
        manifest=MANIFEST,
    )


def test_r4_reports_package_backed_release_evidence_and_scope():
    capabilities = fhir_capabilities(_ctx())

    assert capabilities["release_support"]["support_tier"] == "package-backed"
    assert capabilities["release_support"]["base_schema_validation"] is True
    assert capabilities["validation_levels"] == ["structure", "base"]
    # R4 is now fully backed: 146 schema types, 126 storable (schema ∩ search configs)
    assert len(capabilities["storable_resource_types"]) > 2
    assert "Patient" in capabilities["storable_resource_types"]
    assert "Observation" in capabilities["storable_resource_types"]
    assert capabilities["generatable_resource_types"] == []
    assert capabilities["synthetic_writable_resource_types"] == []
    # Chaining and compartment search are still not enabled for R4
    assert capabilities["chaining_supported"] is False
    assert capabilities["reverse_chaining_supported"] is False
    assert capabilities["chaining_limits"]["maximum_hops"] == 0
    assert any(
        item["code"] == "compartment and chained search"
        for item in capabilities["unsupported_interactions"]
    )
    assert capabilities["compartments"]["Patient"]["supported"] is False
    assert capabilities["compartments"]["Patient"]["searchable_resource_types"] == []


def test_r4_catalog_reflects_full_r4_schema():
    catalog = fhir_resource_catalog(_ctx(), {})
    # R4 is now schema-backed: catalog reflects the full 146-resource schema
    assert catalog["resource_count"] > 2
    assert catalog["release_support"]["support_tier"] == "package-backed"
    patient = fhir_resource_catalog(_ctx(), {"resource_type": "Patient"})
    assert patient["resource"]["structure"]["root"] == "Patient"
    assert patient["resource"]["capabilities"]["searchable"] is True


def test_r4_validation_levels_include_base():
    # R4 is now schema-backed; both structure and base are available
    assert available_validation_levels("R4") == ("structure", "base")
    # base validation must succeed (not raise)
    assert validate_level("base", "R4") == "base"
    assert validate_level("structure", "R4") == "structure"

    # R4 Patient now has the full 25-parameter search config
    names = {
        item["name"]
        for item in fhir_list_search_params(_ctx(), {"resource_type": "Patient"})[
            "parameters"
        ]
    }
    assert "identifier" in names
    # organization is now a supported R4 Patient search parameter
    assert "organization" in names


@pytest.mark.asyncio
async def test_r4_compile_accepts_full_r4_search_params():
    # Patient with name — still works
    plan = await compile_fhir_query(
        _ctx(), "fhir", {"resource_type": "Patient", "criteria": {"name": "Smith"}}
    )
    assert plan.plan["resource_type"] == "Patient"

    # organization is now a valid R4 Patient search parameter
    plan = await compile_fhir_query(
        _ctx(),
        "fhir",
        {
            "resource_type": "Patient",
            "criteria": {"organization": "Organization/1"},
        },
    )
    assert plan.plan["resource_type"] == "Patient"

    # Condition is now in the full R4 scope
    plan = await compile_fhir_query(
        _ctx(), "fhir", {"resource_type": "Condition", "criteria": {"_id": "c1"}}
    )
    assert plan.plan["resource_type"] == "Condition"


@pytest.mark.asyncio
async def test_r4_patient_can_be_structurally_projected_in_a_dry_run():
    report = await fhir_import_resources(
        _ctx(),
        {
            "resource": {
                "resourceType": "Patient",
                "id": "r4-patient",
                "name": [{"family": "R4", "given": ["Minimal"]}],
            },
            "dry_run": True,
        },
    )
    assert report["ok"] is True
    assert report["committed"] is False
    assert report["fhir_release"] == "R4"
    assert report["validation"]["level"] == "structure"
    assert report["search_projection"]["projected"] == 1


@pytest.mark.asyncio
async def test_universal_ingest_uses_the_same_import_pipeline():
    from kehrnel.engine.strategies.fhir.resource_store.strategy import (
        FHIRResourceStoreStrategy,
    )

    report = await FHIRResourceStoreStrategy(MANIFEST).ingest(
        _ctx(),
        {
            "documents": [
                {
                    "resourceType": "Patient",
                    "id": "r4-cli-patient",
                    "name": [{"family": "CLI"}],
                }
            ],
            "dry_run": True,
        },
    )

    assert report["ok"] is True
    assert report["committed"] is False
    assert report["resource_counts"] == {"Patient": 1}
