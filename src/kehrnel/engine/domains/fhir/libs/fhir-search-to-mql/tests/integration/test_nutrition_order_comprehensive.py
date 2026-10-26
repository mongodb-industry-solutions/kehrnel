"""
Comprehensive integration tests for ALL NutritionOrder search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "NutritionOrder")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/NutritionOrder.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 14 search parameters in ``configs/NutritionOrder.yaml``.

Compartments (precomputed): Patient, Practitioner, Encounter.
"""
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
def rich_nutrition_order() -> Dict[str, Any]:
    return {
        "resourceType": "NutritionOrder",
        "id": "nutrition-order-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "dateTime": "2024-07-15T10:00:00Z",
        "subject": {"reference": "Patient/pat-1"},
        "orderer": {"reference": "Practitioner/prac-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "groupIdentifier": {
            "system": "http://hospital.org/group",
            "value": "GRP-NO-001",
        },
        "identifier": [
            {"system": "http://hospital.org/nutrition", "value": "NO-001"}
        ],
        "oralDiet": {
            "type": [{"coding": [{"system": "http://snomed.info/sct", "code": "226211001"}]}]
        },
        "enteralFormula": {
            "baseFormulaType": {
                "concept": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "226783000"}]
                }
            },
            "additive": [
                {
                    "type": {
                        "concept": {
                            "coding": [
                                {"system": "http://snomed.info/sct", "code": "226789001"}
                            ]
                        }
                    }
                }
            ],
        },
        "supplement": [
            {
                "type": {
                    "concept": {
                        "coding": [
                            {"system": "http://snomed.info/sct", "code": "226352002"}
                        ]
                    }
                }
            }
        ],
    }


@pytest.fixture
def minimal_nutrition_order() -> Dict[str, Any]:
    return {
        "resourceType": "NutritionOrder",
        "id": "nutrition-order-min",
        "status": "active",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestNutritionOrderReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("NutritionOrder", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("NutritionOrder", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_provider(self, converter):
        q = converter.convert("NutritionOrder", "provider=prac-1")
        assert "_search.providerId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("NutritionOrder", "encounter=enc-1")
        assert "_search.encounterId" in str(q)


class TestNutritionOrderTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("NutritionOrder", "identifier=NO-001")
        assert "NO-001" in str(q)

    def test_group_identifier(self, converter):
        q = converter.convert("NutritionOrder", "group-identifier=GRP-NO-001")
        assert "GRP-NO-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("NutritionOrder", "status=active")
        assert "active" in str(q)

    def test_additive(self, converter):
        q = converter.convert("NutritionOrder", "additive=226789001")
        assert "additiveConcept_codes" in str(q)

    def test_formula(self, converter):
        q = converter.convert("NutritionOrder", "formula=226783000")
        assert "formulaConcept_codes" in str(q)

    def test_oraldiet(self, converter):
        q = converter.convert("NutritionOrder", "oraldiet=226211001")
        assert "oralDietType_codes" in str(q)

    def test_supplement(self, converter):
        q = converter.convert("NutritionOrder", "supplement=226352002")
        assert "supplementConcept_codes" in str(q)


class TestNutritionOrderDateParameters:
    def test_datetime(self, converter):
        q = converter.convert("NutritionOrder", "datetime=ge2024-07-01")
        assert "dateTime" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("NutritionOrder", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestNutritionOrderCommonParameters:
    def test_id(self, converter):
        q = converter.convert("NutritionOrder", "_id=nutrition-order-rich")
        assert "nutrition-order-rich" in str(q)


class TestNutritionOrderDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_nutrition_order):
        out = denormalizer.denormalize(minimal_nutrition_order)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_nutrition_order):
        out = denormalizer.denormalize(rich_nutrition_order)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["subjectId"] == "pat-1"
        assert s["providerId"] == "prac-1"
        assert s["encounterId"] == "enc-1"
        assert "NO-001" in s["identifier_values"]
        assert "GRP-NO-001" in s["groupIdentifier_values"]
        assert "226211001" in s["oralDietType_codes"]
        assert "226783000" in s["formulaConcept_codes"]
        assert "226789001" in s["additiveConcept_codes"]
        assert "226352002" in s["supplementConcept_codes"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert out["_compartments"]["Practitioner"] == ["prac-1"]
        assert out["_compartments"]["Encounter"] == ["enc-1"]

    def test_input_not_mutated(self, denormalizer, rich_nutrition_order):
        original = copy.deepcopy(rich_nutrition_order)
        denormalizer.denormalize(rich_nutrition_order)
        assert rich_nutrition_order == original
