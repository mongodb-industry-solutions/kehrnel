"""
Comprehensive integration tests for ALL MedicationAdministration search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "MedicationAdministration")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/MedicationAdministration.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 17 search parameters in ``configs/MedicationAdministration.yaml``:

  References (8): device, encounter, medication, patient, performer,
    reason-given, request, subject
  Tokens (6): code, identifier, performer-device-code, reason-given-code,
    reason-not-given, status
  Dates (1): date
  Common (2): _id, _lastUpdated

R5 note: occurrence elements are spelled ``occurence*`` in the published schema.
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
def rich_medication_administration() -> Dict[str, Any]:
    return {
        "resourceType": "MedicationAdministration",
        "id": "ma-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "identifier": [{"system": "http://hospital.org/ma", "value": "MA-001"}],
        "medication": {
            "concept": {
                "coding": [{"system": "http://snomed.info/sct", "code": "319785009"}]
            },
            "reference": {"reference": "Medication/med-1"},
        },
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "request": {"reference": "MedicationRequest/mr-1"},
        "occurenceDateTime": "2024-07-15T09:00:00Z",
        "device": [{"reference": {"reference": "Device/pump-1"}}],
        "performer": [
            {
                "actor": {
                    "concept": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "706699008"}]
                    },
                    "reference": {"reference": "Practitioner/prac-1"},
                }
            }
        ],
        "reason": [
            {
                "concept": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "386661006"}]
                },
                "reference": {"reference": "Condition/cond-1"},
            }
        ],
        "statusReason": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "182849000"}]}
        ],
    }


@pytest.fixture
def minimal_medication_administration() -> Dict[str, Any]:
    return {
        "resourceType": "MedicationAdministration",
        "id": "ma-min",
        "status": "in-progress",
        "subject": {"reference": "Patient/pat-min"},
        "medication": {
            "concept": {"coding": [{"code": "319785009"}]},
        },
    }


class TestMedicationAdministrationReferenceParameters:
    def test_patient_bare_id(self, converter):
        q = converter.convert("MedicationAdministration", "patient=pat-1")
        s = str(q)
        assert "$or" in s
        assert "pat-1" in s

    def test_subject_typed(self, converter):
        q = converter.convert("MedicationAdministration", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_medication_reference(self, converter):
        q = converter.convert("MedicationAdministration", "medication=med-1")
        assert "_search.medicationReferenceIds" in str(q)

    def test_performer_practitioner(self, converter):
        q = converter.convert("MedicationAdministration", "performer=prac-1")
        assert "_search.performerIds" in str(q)

    def test_device(self, converter):
        q = converter.convert("MedicationAdministration", "device=pump-1")
        assert "_search.deviceIds" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("MedicationAdministration", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_request(self, converter):
        q = converter.convert("MedicationAdministration", "request=mr-1")
        assert "_search.requestId" in str(q)

    def test_reason_given_reference(self, converter):
        q = converter.convert("MedicationAdministration", "reason-given=cond-1")
        assert "_search.reasonGivenIds" in str(q)


class TestMedicationAdministrationTokenParameters:
    def test_status(self, converter):
        assert converter.convert("MedicationAdministration", "status=completed") == {
            "status": "completed"
        }

    def test_code(self, converter):
        q = converter.convert("MedicationAdministration", "code=319785009")
        assert "319785009" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("MedicationAdministration", "identifier=MA-001")
        assert "MA-001" in str(q)

    def test_performer_device_code(self, converter):
        q = converter.convert("MedicationAdministration", "performer-device-code=706699008")
        assert "706699008" in str(q)

    def test_reason_given_code(self, converter):
        q = converter.convert("MedicationAdministration", "reason-given-code=386661006")
        assert "386661006" in str(q)

    def test_reason_not_given(self, converter):
        q = converter.convert("MedicationAdministration", "reason-not-given=182849000")
        assert "182849000" in str(q)

    def test_id(self, converter):
        q = converter.convert("MedicationAdministration", "_id=ma-rich")
        assert "ma-rich" in str(q)


class TestMedicationAdministrationDateParameters:
    def test_date_ge(self, converter):
        q = converter.convert("MedicationAdministration", "date=ge2024-07-15")
        s = str(q)
        assert "occurenceDateTime" in s or "occurencePeriod" in s

    def test_last_updated(self, converter):
        q = converter.convert("MedicationAdministration", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestMedicationAdministrationDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_medication_administration):
        out = denormalizer.denormalize(minimal_medication_administration)
        s = out["_search"]
        assert s["subjectId"] == "pat-min"
        assert s["patientId"] == "pat-min"
        assert "319785009" in s["medicationConcept_codes"]

    def test_rich_fields(self, denormalizer, rich_medication_administration):
        out = denormalizer.denormalize(rich_medication_administration)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert s["requestId"] == "mr-1"
        assert "319785009" in s["medicationConcept_codes"]
        assert "med-1" in s["medicationReferenceIds"]
        assert "prac-1" in s["performerIds"]
        assert "pump-1" in s["deviceIds"]
        assert "386661006" in s["reasonGivenCode_codes"]
        assert "cond-1" in s["reasonGivenIds"]
        assert "182849000" in s["statusReason_codes"]
        assert "706699008" in s["performerActorConcept_codes"]

    def test_input_not_mutated(self, denormalizer, rich_medication_administration):
        original = copy.deepcopy(rich_medication_administration)
        denormalizer.denormalize(rich_medication_administration)
        assert rich_medication_administration == original


class TestMedicationAdministrationPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_medication_administration):
        out = denormalizer.denormalize(rich_medication_administration)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_medication_administration):
        out = denormalizer.denormalize(rich_medication_administration)
        assert "prac-1" in out["_compartments"]["Practitioner"]

    def test_device_compartment_admin_device_and_performer(
        self, denormalizer, rich_medication_administration
    ):
        out = denormalizer.denormalize(rich_medication_administration)
        dev = out["_compartments"]["Device"]
        assert "pump-1" in dev

    def test_encounter_compartment(self, denormalizer, rich_medication_administration):
        out = denormalizer.denormalize(rich_medication_administration)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "MedicationAdministration", "status=completed"
        )
        s = str(q)
        assert "_compartments.Patient" in s

    def test_practitioner_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Practitioner", "prac-1", "MedicationAdministration"
        )
        assert q == {"_compartments.Practitioner": "prac-1"}

    def test_device_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Device", "pump-1", "MedicationAdministration"
        )
        assert q == {"_compartments.Device": "pump-1"}


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
class TestMedicationAdministrationMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["medicationadministration_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "MedicationAdministration",
            "id": "e2e-ma-1",
            "status": "completed",
            "subject": {"reference": "Patient/p1"},
            "medication": {
                "concept": {"coding": [{"code": "319785009"}]},
                "reference": {"reference": "Medication/med-1"},
            },
            "occurenceDateTime": datetime(2024, 7, 15, 9, 0, 0),
            "encounter": {"reference": "Encounter/enc1"},
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(
            seeded.find(conv.convert("MedicationAdministration", "status=completed"))
        )
        assert len(results) == 1
        assert results[0]["id"] == "e2e-ma-1"

    def test_encounter_compartment_e2e(self, seeded):
        conv = FHIRSearchConverter()
        mql = conv.convert_with_compartment(
            "Encounter", "enc1", "MedicationAdministration", "status=completed"
        )
        results = list(seeded.find(mql))
        assert len(results) == 1
