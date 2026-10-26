"""
Comprehensive integration tests for ALL MedicationRequest search parameters per FHIR R5.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "MedicationRequest")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/MedicationRequest.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 18 search parameters in ``configs/MedicationRequest.yaml``
(composite ``combo-date`` deferred):

  References (7): encounter, intended-dispenser, intended-performer,
    medication, patient, requester, subject
  Tokens (8): category, code, group-identifier, identifier,
    intended-performertype, intent, priority, status
  Dates (1): authoredon
  Common (2): _id, _lastUpdated

Compartments (precomputed): Patient, Practitioner, Device, Encounter.
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
def rich_medication_request() -> Dict[str, Any]:
    return {
        "resourceType": "MedicationRequest",
        "id": "mr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "intent": "order",
        "priority": "routine",
        "identifier": [{"system": "http://hospital.org/mr", "value": "MR-001"}],
        "groupIdentifier": {"system": "http://hospital.org/grp", "value": "GRP-99"},
        "category": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/medicationrequest-category", "code": "outpatient"}]}
        ],
        "medication": {
            "concept": {
                "coding": [{"system": "http://snomed.info/sct", "code": "319785009"}]
            },
            "reference": {"reference": "Medication/med-1"},
        },
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "authoredOn": "2024-07-01T10:00:00Z",
        "requester": {"reference": "Practitioner/prac-1"},
        "performer": [
            {"reference": "Practitioner/prac-2"},
            {"reference": "Organization/org-pharm"},
        ],
        "performerType": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/dispense-request-performer-role",
                    "code": "pharmacist",
                }
            ]
        },
        "dispenseRequest": {
            "dispenser": {"reference": "Organization/org-dispense"},
        },
    }


@pytest.fixture
def minimal_medication_request() -> Dict[str, Any]:
    return {
        "resourceType": "MedicationRequest",
        "id": "mr-min",
        "status": "draft",
        "intent": "proposal",
        "subject": {"reference": "Patient/pat-min"},
        "medication": {
            "concept": {"coding": [{"code": "319785009"}]},
        },
    }


class TestMedicationRequestReferenceParameters:
    def test_patient_bare_id(self, converter):
        q = converter.convert("MedicationRequest", "patient=pat-1")
        s = str(q)
        assert "$or" in s
        assert "pat-1" in s

    def test_subject_typed(self, converter):
        q = converter.convert("MedicationRequest", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_medication_reference(self, converter):
        q = converter.convert("MedicationRequest", "medication=med-1")
        assert "_search.medicationReferenceIds" in str(q)

    def test_requester(self, converter):
        q = converter.convert("MedicationRequest", "requester=prac-1")
        assert "_search.requesterId" in str(q)

    def test_intended_performer(self, converter):
        q = converter.convert("MedicationRequest", "intended-performer=prac-2")
        assert "_search.intendedPerformerIds" in str(q)

    def test_intended_dispenser(self, converter):
        q = converter.convert("MedicationRequest", "intended-dispenser=org-dispense")
        s = str(q)
        assert "_search.intendedDispenserId" in s
        assert "org-dispense" in s

    def test_encounter(self, converter):
        q = converter.convert("MedicationRequest", "encounter=enc-1")
        assert "_search.encounterId" in str(q)


class TestMedicationRequestTokenParameters:
    def test_status(self, converter):
        assert converter.convert("MedicationRequest", "status=active") == {
            "status": "active"
        }

    def test_intent(self, converter):
        assert converter.convert("MedicationRequest", "intent=order") == {"intent": "order"}

    def test_priority(self, converter):
        assert converter.convert("MedicationRequest", "priority=routine") == {
            "priority": "routine"
        }

    def test_code(self, converter):
        q = converter.convert("MedicationRequest", "code=319785009")
        assert "319785009" in str(q)

    def test_category(self, converter):
        q = converter.convert("MedicationRequest", "category=outpatient")
        assert "outpatient" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("MedicationRequest", "identifier=MR-001")
        assert "MR-001" in str(q)

    def test_group_identifier(self, converter):
        q = converter.convert("MedicationRequest", "group-identifier=GRP-99")
        assert "GRP-99" in str(q)

    def test_intended_performer_type(self, converter):
        q = converter.convert("MedicationRequest", "intended-performertype=pharmacist")
        assert "pharmacist" in str(q)

    def test_id(self, converter):
        q = converter.convert("MedicationRequest", "_id=mr-rich")
        assert "mr-rich" in str(q)


class TestMedicationRequestDateParameters:
    def test_authoredon_ge(self, converter):
        q = converter.convert("MedicationRequest", "authoredon=ge2024-07-01")
        assert "authoredOn" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("MedicationRequest", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestMedicationRequestComplexQueries:
    def test_status_patient_code(self, converter):
        q = converter.convert(
            "MedicationRequest",
            "status=active&patient=pat-1&code=319785009",
        )
        s = str(q)
        assert "active" in s
        assert "pat-1" in s
        assert "319785009" in s


class TestMedicationRequestDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_medication_request):
        out = denormalizer.denormalize(minimal_medication_request)
        s = out["_search"]
        assert s["subjectId"] == "pat-min"
        assert s["patientId"] == "pat-min"
        assert "319785009" in s["medicationConcept_codes"]
        assert "category_codes" not in s

    def test_rich_fields(self, denormalizer, rich_medication_request):
        out = denormalizer.denormalize(rich_medication_request)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["requesterId"] == "prac-1"
        assert s["encounterId"] == "enc-1"
        assert "319785009" in s["medicationConcept_codes"]
        assert "med-1" in s["medicationReferenceIds"]
        assert "prac-2" in s["intendedPerformerIds"]
        assert s["intendedDispenserId"] == "org-dispense"
        assert "GRP-99" in s["groupIdentifier_values"]
        assert "outpatient" in s["category_codes"]
        assert "pharmacist" in s["performerType_codes"]

    def test_input_not_mutated(self, denormalizer, rich_medication_request):
        original = copy.deepcopy(rich_medication_request)
        denormalizer.denormalize(rich_medication_request)
        assert rich_medication_request == original


class TestMedicationRequestPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_medication_request):
        out = denormalizer.denormalize(rich_medication_request)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment_requester_only(
        self, denormalizer, rich_medication_request
    ):
        out = denormalizer.denormalize(rich_medication_request)
        prac = out["_compartments"]["Practitioner"]
        assert "prac-1" in prac
        assert "prac-2" not in prac

    def test_encounter_compartment(self, denormalizer, rich_medication_request):
        out = denormalizer.denormalize(rich_medication_request)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "MedicationRequest", "status=active"
        )
        s = str(q)
        assert "_compartments.Patient" in s
        assert "pat-1" in s

    def test_practitioner_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Practitioner", "prac-1", "MedicationRequest"
        )
        assert q == {"_compartments.Practitioner": "prac-1"}

    def test_encounter_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Encounter", "enc-1", "MedicationRequest"
        )
        assert q == {"_compartments.Encounter": "enc-1"}


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
class TestMedicationRequestMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["medicationrequest_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "MedicationRequest",
            "id": "e2e-mr-1",
            "status": "active",
            "intent": "order",
            "subject": {"reference": "Patient/p1"},
            "medication": {
                "concept": {"coding": [{"code": "319785009"}]},
                "reference": {"reference": "Medication/med-1"},
            },
            "authoredOn": datetime(2024, 7, 1, 10, 0, 0),
            "requester": {"reference": "Practitioner/dr1"},
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("MedicationRequest", "status=active")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-mr-1"

    def test_patient_compartment_e2e(self, seeded):
        conv = FHIRSearchConverter()
        mql = conv.convert_with_compartment(
            "Patient", "p1", "MedicationRequest", "status=active"
        )
        results = list(seeded.find(mql))
        assert len(results) == 1
