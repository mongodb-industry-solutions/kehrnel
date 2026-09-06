"""Truthful provisional R4 capability contract."""

from __future__ import annotations

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.query import (
    compile_fhir_query,
    fhir_capabilities,
    fhir_list_search_params,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.import_resources import (
    fhir_import_resources,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.resource_catalog import (
    fhir_resource_catalog,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.validation import (
    available_validation_levels,
    validate_level,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST


def _ctx() -> StrategyContext:
    return StrategyContext(
        environment_id="r4-test",
        config={
            "database": "fhir_r4_test",
            "schema_version": "R4",
            "collections": {"mode": "per_resource_type"},
        },
        bindings={},
        manifest=MANIFEST,
    )


def test_r4_reports_minimal_release_evidence_and_scope():
    capabilities = fhir_capabilities(_ctx())

    assert capabilities["release_support"]["support_tier"] == "minimal"
    assert capabilities["release_support"]["base_schema_validation"] is False
    assert capabilities["validation_levels"] == ["structure"]
    assert capabilities["storable_resource_types"] == ["Observation", "Patient"]
    assert capabilities["generatable_resource_types"] == []
    assert capabilities["synthetic_writable_resource_types"] == []


def test_r4_catalog_has_small_package_independent_models():
    catalog = fhir_resource_catalog(_ctx(), {})
    assert catalog["resource_count"] == 2
    assert catalog["source"] == "kehrnel.fhir_r4_minimal_contract"
    assert catalog["release_support"]["support_tier"] == "minimal"
    patient_summary = next(
        item for item in catalog["resources"] if item["resource_type"] == "Patient"
    )
    assert patient_summary["search_parameter_count"] == 8
    patient = fhir_resource_catalog(_ctx(), {"resource_type": "Patient"})
    assert patient["resource"]["structure"]["root"] == "Patient"
    assert patient["resource"]["capabilities"]["searchable"] is True


def test_r4_validation_and_search_are_fail_closed():
    assert available_validation_levels("R4") == ("structure",)
    with pytest.raises(ValueError):
        validate_level("base", "R4")

    names = {
        item["name"]
        for item in fhir_list_search_params(_ctx(), {"resource_type": "Patient"})[
            "parameters"
        ]
    }
    assert "identifier" in names
    assert "organization" not in names


@pytest.mark.asyncio
async def test_r4_compile_accepts_reviewed_param_and_rejects_everything_else():
    plan = await compile_fhir_query(
        _ctx(), "fhir", {"resource_type": "Patient", "criteria": {"name": "Smith"}}
    )
    assert plan.plan["resource_type"] == "Patient"

    with pytest.raises(KehrnelError) as exc_info:
        await compile_fhir_query(
            _ctx(),
            "fhir",
            {
                "resource_type": "Patient",
                "criteria": {"organization": "Organization/1"},
            },
        )
    assert exc_info.value.code == "FHIR_R4_SEARCH_OUTSIDE_MINIMAL_SCOPE"

    with pytest.raises(KehrnelError) as exc_info:
        await compile_fhir_query(
            _ctx(), "fhir", {"resource_type": "Condition", "criteria": {"_id": "c1"}}
        )
    assert exc_info.value.code == "FHIR_R4_RESOURCE_NOT_IN_MINIMAL_SCOPE"


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
    from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import (
        FHIRClinicalCDRStrategy,
    )

    report = await FHIRClinicalCDRStrategy(MANIFEST).ingest(
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
