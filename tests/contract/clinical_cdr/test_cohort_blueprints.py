"""Contract tests for patient-centred FHIR cohort blueprints."""

from __future__ import annotations

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.cohort_blueprints import (
    BLUEPRINT_CONTRACT_VERSION,
    fhir_cohort_catalog,
    fhir_cohort_plan,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.generation import (
    synthetic_generate_batch,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import (
    FHIRClinicalCDRStrategy,
    MANIFEST,
)


def _ctx(*, release: str = "R5") -> StrategyContext:
    return StrategyContext(
        environment_id="test-env",
        config={"database": "fhir_test", "schema_version": release},
        bindings={
            "db": {
                "provider": "mongodb",
                "uri": "mongodb://localhost:27017",
                "database": "fhir_test",
            }
        },
        manifest=MANIFEST,
    )


def test_cohort_catalog_discloses_versioned_curated_assets():
    result = fhir_cohort_catalog(_ctx(), {})

    assert result["contract_version"] == BLUEPRINT_CONTRACT_VERSION
    assert result["asset_count"] == 3
    assert {asset["id"] for asset in result["assets"]} == {
        "cardiometabolic-monitoring",
        "oncology-care-pathway",
        "payer-claims-journey",
    }
    assert all(asset["maturity"] == "curated-demo" for asset in result["assets"])


def test_cohort_plan_is_deterministic_and_reviewable():
    payload = {
        "blueprint_id": "cardiometabolic-monitoring",
        "patients": 7,
        "seed": 91,
        "history_years": 4,
    }
    first = fhir_cohort_plan(_ctx(), payload)
    second = fhir_cohort_plan(_ctx(), payload)

    assert first == second
    assert first["cohort"]["patients"] == 7
    assert first["planned"]["Patient"] == 7
    assert first["planned"]["Observation"] >= 56
    assert first["plan_digest"].startswith("sha256:")
    assert first["quality_contract"]["epidemiological_validity"] == "not-claimed"
    assert (
        first["quality_contract"]["profile_validation"]
        == "not-implemented-base-schema-only"
    )
    assert first["execution"] == {
        "generatable": True,
        "persistable": True,
        "preview_only_resource_types": [],
    }


def test_cohort_plan_applies_population_distribution_and_rule_overrides():
    result = fhir_cohort_plan(
        _ctx(),
        {
            "blueprint_id": "cardiometabolic-monitoring",
            "patients": 3,
            "population": {
                "age_bands": [{"min": 60, "max": 70, "weight": 1.0}],
                "gender_distribution": {"female": 1.0},
            },
            "per_patient_resources": {
                "Observation": {"min": 20, "max": 20, "probability": 1.0}
            },
            "clinical_rules": [
                {"id": "longitudinal-dates-v1"},
                {"id": "blood-pressure-panel-v1", "fraction": 1.0},
            ],
        },
    )

    assert result["population"]["gender_distribution"] == {"female": 1.0}
    observation = next(
        item
        for item in result["per_patient_distributions"]
        if item["resource_type"] == "Observation"
    )
    assert observation["planned_total"] == 60
    assert result["clinical_rules"][-1]["fraction"] == 1.0


def test_cohort_plan_rejects_r4_until_generator_support_exists():
    with pytest.raises(KehrnelError) as exc:
        fhir_cohort_plan(
            _ctx(release="R4"),
            {"blueprint_id": "cardiometabolic-monitoring"},
        )

    assert exc.value.code == "FHIR_COHORT_RELEASE_UNSUPPORTED"


def test_inline_blueprint_must_satisfy_the_public_contract():
    with pytest.raises(KehrnelError) as exc:
        fhir_cohort_plan(
            _ctx(),
            {
                "blueprint": {
                    "id": "incomplete-blueprint",
                    "defaults": {
                        "patients": 1,
                        "history_years": 1,
                        "reference_date": "2026-01-01",
                        "seed": 1,
                    },
                }
            },
        )

    assert exc.value.code == "FHIR_COHORT_BLUEPRINT_INVALID"
    assert "public contract" in str(exc.value)


@pytest.mark.asyncio
async def test_cardiometabolic_cohort_dry_run_has_patient_graph_and_bp_evidence():
    result = await synthetic_generate_batch(
        _ctx(),
        {
            "cohort": {
                "blueprint_id": "cardiometabolic-monitoring",
                "patients": 2,
                "seed": 123,
            },
            "dry_run": True,
            "include_sample": True,
            "sample_limit": 100,
        },
    )

    assert result["ok"] is True
    assert result["generated"]["Patient"] == 2
    assert result["quality_report"]["status"] == "passed"
    assert result["quality_report"]["checks"]["relative_reference_integrity"][
        "passed"
    ] is True
    assert result["quality_report"]["checks"]["patient_linkage"]["passed"] is True
    longitudinal = result["quality_report"]["checks"]["longitudinal_window"]
    assert longitudinal["passed"] is True
    assert longitudinal["resources_checked"] > 0
    assert longitudinal["outside_window_count"] == 0
    bp = result["quality_report"]["checks"]["blood_pressure_consistency"]
    assert bp["applicable"] is True
    assert bp["panels_checked"] > 0
    assert bp["passed"] is True

    patient_ids = {
        resource["id"]
        for resource in result["sample_resources"]
        if resource.get("resourceType") == "Patient"
    }
    observation_subjects = {
        resource.get("subject", {}).get("reference", "").removeprefix("Patient/")
        for resource in result["sample_resources"]
        if resource.get("resourceType") == "Observation"
    }
    assert observation_subjects
    assert observation_subjects <= patient_ids


@pytest.mark.parametrize("release", ["R5", "R6"])
@pytest.mark.parametrize(
    "blueprint_id",
    [
        "cardiometabolic-monitoring",
        "oncology-care-pathway",
        "payer-claims-journey",
    ],
)
@pytest.mark.asyncio
async def test_all_starter_cohorts_are_schema_valid(
    release: str, blueprint_id: str
):
    result = await synthetic_generate_batch(
        _ctx(release=release),
        {
            "cohort": {
                "blueprint_id": blueprint_id,
                "patients": 1,
                "seed": 7,
            },
            "dry_run": True,
        },
    )

    assert result["quality_report"]["status"] == "passed"
    assert result["generation_conformance"]["passed"] is True
    schema = result["quality_report"]["checks"]["base_schema_conformance"]
    assert schema["passed"] is True
    assert schema["optional_values_removed"] == 0
    assert result["quality_report"]["checks"]["patient_linkage"]["passed"] is True
    assert result["quality_report"]["checks"]["longitudinal_window"]["passed"] is True


@pytest.mark.asyncio
async def test_strategy_dispatches_cohort_catalog_and_plan():
    strategy = FHIRClinicalCDRStrategy(MANIFEST)

    catalog = await strategy.run_op(_ctx(), "fhir_cohort_catalog", {})
    plan = await strategy.run_op(
        _ctx(),
        "fhir_cohort_plan",
        {"blueprint_id": "payer-claims-journey", "patients": 3, "seed": 44},
    )

    assert catalog["asset_count"] == 3
    assert plan["planned"]["Patient"] == 3
    assert plan["planned"]["Coverage"] == 3
