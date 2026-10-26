"""
Comprehensive integration tests for ALL AllergyIntolerance search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "AllergyIntolerance")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/AllergyIntolerance.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json

Exercises 17 search parameters in ``configs/AllergyIntolerance.yaml``.

Compartments (precomputed): Patient, Practitioner, Device.
"""
from __future__ import annotations

import copy
from datetime import datetime
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
def rich_allergy() -> Dict[str, Any]:
    return {
        "resourceType": "AllergyIntolerance",
        "id": "ai-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "clinicalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical",
                    "code": "active",
                }
            ]
        },
        "verificationStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification",
                    "code": "confirmed",
                }
            ]
        },
        "type": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/allergy-intolerance-type",
                    "code": "allergy",
                }
            ]
        },
        "category": ["food"],
        "criticality": "high",
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "91935009"}]
        },
        "patient": {"reference": "Patient/pat-1"},
        "recordedDate": "2024-07-01T10:00:00Z",
        "lastOccurrence": "2024-06-15T08:00:00Z",
        "identifier": [{"system": "http://hospital.org/ai", "value": "AI-001"}],
        "participant": [{"actor": {"reference": "Practitioner/prac-1"}}],
        "reaction": [
            {
                "substance": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "227493005"}]
                },
                "manifestation": [
                    {
                        "concept": {
                            "coding": [{"system": "http://snomed.info/sct", "code": "39579001"}]
                        },
                        "reference": {"reference": "Observation/obs-1"},
                    }
                ],
                "severity": "severe",
                "exposureRoute": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "26643006"}]
                },
            }
        ],
    }


@pytest.fixture
def minimal_allergy() -> Dict[str, Any]:
    return {
        "resourceType": "AllergyIntolerance",
        "id": "ai-min",
        "patient": {"reference": "Patient/pat-min"},
        "code": {"coding": [{"code": "91935009"}]},
    }


class TestAllergyIntoleranceReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("AllergyIntolerance", "patient=pat-1")
        assert "_search.patientId" in str(q)
        assert "pat-1" in str(q)

    def test_participant(self, converter):
        q = converter.convert("AllergyIntolerance", "participant=prac-1")
        assert "_search.participantIds" in str(q)

    def test_manifestation_reference(self, converter):
        q = converter.convert("AllergyIntolerance", "manifestation-reference=obs-1")
        assert "_search.manifestationReferenceIds" in str(q)


class TestAllergyIntoleranceTokenParameters:
    def test_clinical_status(self, converter):
        q = converter.convert("AllergyIntolerance", "clinical-status=active")
        assert "active" in str(q)

    def test_code(self, converter):
        q = converter.convert("AllergyIntolerance", "code=91935009")
        assert "91935009" in str(q)

    def test_reaction_substance_via_code(self, converter):
        q = converter.convert("AllergyIntolerance", "code=227493005")
        assert "227493005" in str(q)

    def test_category(self, converter):
        q = converter.convert("AllergyIntolerance", "category=food")
        assert "food" in str(q)

    def test_criticality(self, converter):
        assert converter.convert("AllergyIntolerance", "criticality=high") == {
            "criticality": "high"
        }

    def test_severity(self, converter):
        q = converter.convert("AllergyIntolerance", "severity=severe")
        assert "severe" in str(q)

    def test_route(self, converter):
        q = converter.convert("AllergyIntolerance", "route=26643006")
        assert "26643006" in str(q)

    def test_verification_status(self, converter):
        q = converter.convert("AllergyIntolerance", "verification-status=confirmed")
        assert "confirmed" in str(q)

    def test_type(self, converter):
        q = converter.convert("AllergyIntolerance", "type=allergy")
        assert "allergy" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("AllergyIntolerance", "identifier=AI-001")
        assert "AI-001" in str(q)


class TestAllergyIntoleranceDateParameters:
    def test_date_recorded(self, converter):
        q = converter.convert("AllergyIntolerance", "date=ge2024-07-01")
        assert "recordedDate" in str(q)

    def test_last_date(self, converter):
        q = converter.convert("AllergyIntolerance", "last-date=ge2024-06-01")
        assert "lastOccurrence" in str(q)


class TestAllergyIntoleranceDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_allergy):
        out = denormalizer.denormalize(minimal_allergy)
        s = out["_search"]
        assert s["patientId"] == "pat-min"
        assert "91935009" in s["code_codes"]

    def test_rich_fields(self, denormalizer, rich_allergy):
        out = denormalizer.denormalize(rich_allergy)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "active" in s["clinicalStatus_codes"]
        assert "confirmed" in s["verificationStatus_codes"]
        assert "91935009" in s["code_codes"]
        assert "227493005" in s["reactionSubstance_codes"]
        assert "food" in s["category_values"]
        assert "prac-1" in s["participantIds"]
        assert "39579001" in s["manifestationCode_codes"]
        assert "obs-1" in s["manifestationReferenceIds"]
        assert "severe" in s["severity_values"]
        assert "26643006" in s["route_codes"]

    def test_input_not_mutated(self, denormalizer, rich_allergy):
        original = copy.deepcopy(rich_allergy)
        denormalizer.denormalize(rich_allergy)
        assert rich_allergy == original


class TestAllergyIntolerancePrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_allergy):
        out = denormalizer.denormalize(rich_allergy)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_allergy):
        out = denormalizer.denormalize(rich_allergy)
        assert "prac-1" in out["_compartments"]["Practitioner"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "AllergyIntolerance", "clinical-status=active"
        )
        assert "_compartments.Patient" in str(q)


def _mongo_available() -> bool:
    try:
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.server_info()
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.mongodb
@pytest.mark.skipif(not _mongo_available(), reason="MongoDB not running")
class TestAllergyIntoleranceMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["allergyintolerance_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "AllergyIntolerance",
            "id": "e2e-ai-1",
            "clinicalStatus": {
                "coding": [{"code": "active"}],
            },
            "patient": {"reference": "Patient/p1"},
            "code": {"coding": [{"code": "91935009"}]},
            "recordedDate": datetime(2024, 7, 1, 10, 0, 0),
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_clinical_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(
            seeded.find(conv.convert("AllergyIntolerance", "clinical-status=active"))
        )
        assert len(results) == 1
        assert results[0]["id"] == "e2e-ai-1"
