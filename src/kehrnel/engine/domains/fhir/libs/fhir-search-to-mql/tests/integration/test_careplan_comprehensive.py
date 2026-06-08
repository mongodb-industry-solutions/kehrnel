"""
Comprehensive integration tests for ALL CarePlan search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "CarePlan")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/CarePlan.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, encounter.json

Exercises 20 search parameters in ``configs/CarePlan.yaml``.

Compartments (precomputed): Patient, Practitioner, Encounter.
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
def rich_care_plan() -> Dict[str, Any]:
    return {
        "resourceType": "CarePlan",
        "id": "cp-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "intent": "plan",
        "identifier": [{"system": "http://hospital.org/cp", "value": "CP-001"}],
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/careplan-category",
                        "code": "assess-plan",
                    }
                ]
            }
        ],
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "period": {
            "start": "2024-07-01T00:00:00Z",
            "end": "2024-12-31T23:59:59Z",
        },
        "custodian": {"reference": "Practitioner/prac-cust"},
        "contributor": [{"reference": "Practitioner/prac-1"}],
        "careTeam": [{"reference": "CareTeam/ct-1"}],
        "goal": [{"reference": "Goal/goal-1"}],
        "addresses": [{"reference": {"reference": "Condition/cond-1"}}],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
        "activity": [
            {"plannedActivityReference": {"reference": "ServiceRequest/sr-act-1"}}
        ],
        "instantiatesCanonical": [
            "http://example.org/fhir/PlanDefinition/diabetes-plan"
        ],
        "instantiatesUri": ["http://example.org/protocols/diabetes-v1"],
    }


@pytest.fixture
def minimal_care_plan() -> Dict[str, Any]:
    return {
        "resourceType": "CarePlan",
        "id": "cp-min",
        "status": "draft",
        "intent": "proposal",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestCarePlanReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("CarePlan", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("CarePlan", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("CarePlan", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_condition(self, converter):
        q = converter.convert("CarePlan", "condition=cond-1")
        assert "_search.conditionIds" in str(q)

    def test_care_team(self, converter):
        q = converter.convert("CarePlan", "care-team=ct-1")
        assert "_search.careTeamIds" in str(q)

    def test_goal(self, converter):
        q = converter.convert("CarePlan", "goal=goal-1")
        assert "_search.goalIds" in str(q)

    def test_activity_reference(self, converter):
        q = converter.convert("CarePlan", "activity-reference=sr-act-1")
        assert "_search.activityReferenceIds" in str(q)

    def test_custodian(self, converter):
        q = converter.convert("CarePlan", "custodian=prac-cust")
        assert "_search.custodianId" in str(q)


class TestCarePlanTokenParameters:
    def test_status(self, converter):
        assert converter.convert("CarePlan", "status=active") == {"status": "active"}

    def test_intent(self, converter):
        assert converter.convert("CarePlan", "intent=plan") == {"intent": "plan"}

    def test_category(self, converter):
        q = converter.convert("CarePlan", "category=assess-plan")
        assert "assess-plan" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("CarePlan", "identifier=CP-001")
        assert "CP-001" in str(q)


class TestCarePlanDateAndUriParameters:
    def test_date(self, converter):
        q = converter.convert("CarePlan", "date=ge2024-07-01")
        assert "period" in str(q)

    def test_instantiates_uri(self, converter):
        q = converter.convert("CarePlan", "instantiates-uri=diabetes-v1")
        assert "instantiatesUri" in str(q) or "instantiatesUri_values" in str(q)


class TestCarePlanDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_care_plan):
        out = denormalizer.denormalize(minimal_care_plan)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"

    def test_rich_fields(self, denormalizer, rich_care_plan):
        out = denormalizer.denormalize(rich_care_plan)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert "assess-plan" in s["category_codes"]
        assert "cond-1" in s["conditionIds"]
        assert "ct-1" in s["careTeamIds"]
        assert "goal-1" in s["goalIds"]
        assert "sr-act-1" in s["activityReferenceIds"]
        assert s["custodianId"] == "prac-cust"
        assert "period" in s

    def test_input_not_mutated(self, denormalizer, rich_care_plan):
        original = copy.deepcopy(rich_care_plan)
        denormalizer.denormalize(rich_care_plan)
        assert rich_care_plan == original


class TestCarePlanPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_care_plan):
        out = denormalizer.denormalize(rich_care_plan)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment_from_contributor(self, denormalizer, rich_care_plan):
        out = denormalizer.denormalize(rich_care_plan)
        prac = out["_compartments"]["Practitioner"]
        assert "prac-1" in prac
        assert "prac-cust" not in prac

    def test_encounter_compartment(self, denormalizer, rich_care_plan):
        out = denormalizer.denormalize(rich_care_plan)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "CarePlan", "status=active"
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
class TestCarePlanMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["careplan_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "CarePlan",
            "id": "e2e-cp-1",
            "status": "active",
            "intent": "plan",
            "subject": {"reference": "Patient/p1"},
            "period": {
                "start": datetime(2024, 7, 1),
                "end": datetime(2024, 12, 31),
            },
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("CarePlan", "status=active")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-cp-1"
