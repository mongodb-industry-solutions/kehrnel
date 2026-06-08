"""Comprehensive integration tests for NutritionIntake search parameters."""
from __future__ import annotations

import copy
from typing import Any, Dict

import pytest

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def converter() -> FHIRSearchConverter:
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.fixture
def rich_nutrition_intake() -> Dict[str, Any]:
    return {
        "resourceType": "NutritionIntake",
        "id": "ni-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "reportedReference": {"reference": "Practitioner/prac-1"},
        "occurrenceDateTime": "2024-07-15T12:00:00Z",
        "code": {"coding": [{"code": "meal"}]},
        "identifier": [{"value": "NI-001"}],
        "consumedItem": [
            {
                "nutritionProduct": {
                    "concept": {"coding": [{"code": "apple"}]}
                }
            }
        ],
    }


class TestNutritionIntakeReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("NutritionIntake", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_source(self, converter):
        q = converter.convert("NutritionIntake", "source=prac-1")
        assert "_search.sourceId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("NutritionIntake", "encounter=enc-1")
        assert "_search.encounterId" in str(q)


class TestNutritionIntakeTokenParameters:
    def test_code(self, converter):
        q = converter.convert("NutritionIntake", "code=meal")
        assert "meal" in str(q)

    def test_nutrition(self, converter):
        q = converter.convert("NutritionIntake", "nutrition=apple")
        assert "apple" in str(q)

    def test_status(self, converter):
        q = converter.convert("NutritionIntake", "status=completed")
        assert q == {"status": "completed"}


class TestNutritionIntakeDateParameters:
    def test_date(self, converter):
        q = converter.convert("NutritionIntake", "date=ge2024-07-01")
        assert "occurrenceDateTime" in str(q)


class TestNutritionIntakeCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "NutritionIntake")
        assert q == {"_compartments.Patient": "pat-1"}


class TestNutritionIntakeDenormalization:
    def test_rich(self, denormalizer, rich_nutrition_intake):
        out = denormalizer.denormalize(copy.deepcopy(rich_nutrition_intake))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["sourceId"] == "prac-1"
        assert "apple" in s["nutritionProductConcept_codes"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
