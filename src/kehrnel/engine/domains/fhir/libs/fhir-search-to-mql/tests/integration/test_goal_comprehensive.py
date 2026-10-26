"""
Comprehensive integration tests for ALL Goal search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Goal")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Goal.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 13 search parameters in ``configs/Goal.yaml``.

Compartments (precomputed): Patient.
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
def rich_goal() -> Dict[str, Any]:
    return {
        "resourceType": "Goal",
        "id": "goal-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "lifecycleStatus": "active",
        "achievementStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/goal-achievement",
                    "code": "in-progress",
                }
            ]
        },
        "identifier": [{"system": "http://hospital.org/goal", "value": "GOAL-001"}],
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/goal-category",
                        "code": "dietary",
                    }
                ]
            }
        ],
        "description": {
            "coding": [{"system": "http://snomed.info/sct", "code": "406156006"}],
            "text": "Reduce body weight",
        },
        "subject": {"reference": "Patient/pat-1"},
        "startDate": "2024-07-01",
        "addresses": [{"reference": "Condition/cond-1"}],
        "target": [
            {
                "measure": {
                    "coding": [{"system": "http://loinc.org", "code": "29463-7"}]
                },
                "dueDate": "2024-12-31",
            }
        ],
    }


@pytest.fixture
def minimal_goal() -> Dict[str, Any]:
    return {
        "resourceType": "Goal",
        "id": "goal-min",
        "lifecycleStatus": "planned",
        "description": {"coding": [{"code": "406156006"}]},
        "subject": {"reference": "Patient/pat-min"},
    }


class TestGoalReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Goal", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Goal", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_addresses(self, converter):
        q = converter.convert("Goal", "addresses=cond-1")
        assert "_search.addressesIds" in str(q)


class TestGoalTokenParameters:
    def test_lifecycle_status(self, converter):
        assert converter.convert("Goal", "lifecycle-status=active") == {
            "lifecycleStatus": "active"
        }

    def test_achievement_status(self, converter):
        q = converter.convert("Goal", "achievement-status=in-progress")
        assert "in-progress" in str(q)

    def test_description_code(self, converter):
        q = converter.convert("Goal", "description=406156006")
        assert "406156006" in str(q)

    def test_description_text(self, converter):
        q = converter.convert("Goal", "description:text=weight")
        assert "description" in str(q).lower()

    def test_category(self, converter):
        q = converter.convert("Goal", "category=dietary")
        assert "dietary" in str(q)

    def test_target_measure(self, converter):
        q = converter.convert("Goal", "target-measure=29463-7")
        assert "29463-7" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Goal", "identifier=GOAL-001")
        assert "GOAL-001" in str(q)


class TestGoalDateParameters:
    def test_start_date(self, converter):
        q = converter.convert("Goal", "start-date=ge2024-07-01")
        assert "startDate" in str(q)

    def test_target_date(self, converter):
        q = converter.convert("Goal", "target-date=le2024-12-31")
        assert "targetDueDate" in str(q)


class TestGoalDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_goal):
        out = denormalizer.denormalize(minimal_goal)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert "406156006" in s["description_codes"]

    def test_rich_fields(self, denormalizer, rich_goal):
        out = denormalizer.denormalize(rich_goal)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "in-progress" in s["achievementStatus_codes"]
        assert "dietary" in s["category_codes"]
        assert "406156006" in s["description_codes"]
        assert "reduce body weight" in s["description_text_lower"]
        assert "cond-1" in s["addressesIds"]
        assert "29463-7" in s["targetMeasure_codes"]
        assert "2024-12-31" in s["targetDueDate"] or any(
            "2024-12-31" in str(v) for v in s.get("targetDueDate", [])
        )

    def test_input_not_mutated(self, denormalizer, rich_goal):
        original = copy.deepcopy(rich_goal)
        denormalizer.denormalize(rich_goal)
        assert rich_goal == original


class TestGoalPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_goal):
        out = denormalizer.denormalize(rich_goal)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Goal", "lifecycle-status=active"
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
class TestGoalMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["goal_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "Goal",
            "id": "e2e-goal-1",
            "lifecycleStatus": "active",
            "description": {"coding": [{"code": "406156006"}]},
            "subject": {"reference": "Patient/p1"},
            "startDate": datetime(2024, 7, 1),
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_lifecycle_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("Goal", "lifecycle-status=active")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-goal-1"
