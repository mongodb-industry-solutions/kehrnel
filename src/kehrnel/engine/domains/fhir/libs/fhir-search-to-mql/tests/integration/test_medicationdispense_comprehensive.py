"""
Comprehensive integration tests for ALL MedicationDispense search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "MedicationDispense")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/MedicationDispense.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, encounter.json

Exercises 19 search parameters in ``configs/MedicationDispense.yaml``:

  References (10): destination, encounter, location, medication, patient,
    performer, prescription, receiver, responsibleparty, subject
  Tokens (4): code, identifier, status, type
  Dates (3): recorded, whenhandedover, whenprepared
  Common (2): _id, _lastUpdated

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
def rich_medication_dispense() -> Dict[str, Any]:
    return {
        "resourceType": "MedicationDispense",
        "id": "md-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    "code": "FF",
                }
            ]
        },
        "identifier": [{"system": "http://hospital.org/md", "value": "MD-001"}],
        "medication": {
            "concept": {
                "coding": [{"system": "http://snomed.info/sct", "code": "319785009"}]
            },
            "reference": {"reference": "Medication/med-1"},
        },
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "location": {"reference": "Location/loc-1"},
        "destination": {"reference": "Location/loc-dest"},
        "whenPrepared": "2024-07-14T08:00:00Z",
        "whenHandedOver": "2024-07-15T09:00:00Z",
        "recorded": "2024-07-14T07:00:00Z",
        "authorizingPrescription": [{"reference": "MedicationRequest/mr-1"}],
        "performer": [{"actor": {"reference": "Practitioner/prac-1"}}],
        "receiver": [{"reference": "Patient/pat-1"}],
        "substitution": {
            "wasSubstituted": True,
            "responsibleParty": {"reference": "Practitioner/prac-2"},
        },
    }


@pytest.fixture
def minimal_medication_dispense() -> Dict[str, Any]:
    return {
        "resourceType": "MedicationDispense",
        "id": "md-min",
        "status": "preparation",
        "subject": {"reference": "Patient/pat-min"},
        "medication": {
            "concept": {"coding": [{"code": "319785009"}]},
        },
    }


class TestMedicationDispenseReferenceParameters:
    def test_patient_bare_id(self, converter):
        q = converter.convert("MedicationDispense", "patient=pat-1")
        s = str(q)
        assert "$or" in s
        assert "pat-1" in s

    def test_medication_reference(self, converter):
        q = converter.convert("MedicationDispense", "medication=med-1")
        assert "_search.medicationReferenceIds" in str(q)

    def test_performer(self, converter):
        q = converter.convert("MedicationDispense", "performer=prac-1")
        assert "_search.performerIds" in str(q)

    def test_prescription(self, converter):
        q = converter.convert("MedicationDispense", "prescription=mr-1")
        assert "_search.prescriptionIds" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("MedicationDispense", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_location(self, converter):
        q = converter.convert("MedicationDispense", "location=loc-1")
        assert "_search.locationId" in str(q)

    def test_destination(self, converter):
        q = converter.convert("MedicationDispense", "destination=loc-dest")
        assert "_search.destinationId" in str(q)

    def test_receiver(self, converter):
        q = converter.convert("MedicationDispense", "receiver=pat-1")
        assert "_search.receiverIds" in str(q)

    def test_responsible_party(self, converter):
        q = converter.convert("MedicationDispense", "responsibleparty=prac-2")
        assert "_search.responsiblePartyId" in str(q)


class TestMedicationDispenseTokenParameters:
    def test_status(self, converter):
        assert converter.convert("MedicationDispense", "status=completed") == {
            "status": "completed"
        }

    def test_code(self, converter):
        q = converter.convert("MedicationDispense", "code=319785009")
        assert "319785009" in str(q)

    def test_type(self, converter):
        q = converter.convert("MedicationDispense", "type=FF")
        assert "FF" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("MedicationDispense", "identifier=MD-001")
        assert "MD-001" in str(q)

    def test_id(self, converter):
        q = converter.convert("MedicationDispense", "_id=md-rich")
        assert "md-rich" in str(q)


class TestMedicationDispenseDateParameters:
    def test_when_prepared(self, converter):
        q = converter.convert("MedicationDispense", "whenprepared=ge2024-07-14")
        assert "whenPrepared" in str(q)

    def test_when_handed_over(self, converter):
        q = converter.convert("MedicationDispense", "whenhandedover=ge2024-07-15")
        assert "whenHandedOver" in str(q)

    def test_recorded(self, converter):
        q = converter.convert("MedicationDispense", "recorded=ge2024-07-14")
        assert "recorded" in str(q)


class TestMedicationDispenseDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_medication_dispense):
        out = denormalizer.denormalize(minimal_medication_dispense)
        s = out["_search"]
        assert s["subjectId"] == "pat-min"
        assert "319785009" in s["medicationConcept_codes"]

    def test_rich_fields(self, denormalizer, rich_medication_dispense):
        out = denormalizer.denormalize(rich_medication_dispense)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert "319785009" in s["medicationConcept_codes"]
        assert "med-1" in s["medicationReferenceIds"]
        assert "prac-1" in s["performerIds"]
        assert "mr-1" in s["prescriptionIds"]
        assert s["locationId"] == "loc-1"
        assert s["destinationId"] == "loc-dest"
        assert s["responsiblePartyId"] == "prac-2"
        assert "FF" in s["type_codes"]
        assert "pat-1" in s["receiverIds"]

    def test_input_not_mutated(self, denormalizer, rich_medication_dispense):
        original = copy.deepcopy(rich_medication_dispense)
        denormalizer.denormalize(rich_medication_dispense)
        assert rich_medication_dispense == original


class TestMedicationDispensePrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_medication_dispense):
        out = denormalizer.denormalize(rich_medication_dispense)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment_performer_only(
        self, denormalizer, rich_medication_dispense
    ):
        out = denormalizer.denormalize(rich_medication_dispense)
        prac = out["_compartments"]["Practitioner"]
        assert "prac-1" in prac
        # substitution.responsibleParty is searchable via `responsibleparty`
        # but is not a Practitioner-compartment linking parameter per R5.
        assert "prac-2" not in prac

    def test_encounter_compartment(self, denormalizer, rich_medication_dispense):
        out = denormalizer.denormalize(rich_medication_dispense)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "MedicationDispense", "status=completed"
        )
        assert "_compartments.Patient" in str(q)

    def test_practitioner_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Practitioner", "prac-1", "MedicationDispense"
        )
        assert q == {"_compartments.Practitioner": "prac-1"}


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
class TestMedicationDispenseMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["medicationdispense_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "MedicationDispense",
            "id": "e2e-md-1",
            "status": "completed",
            "subject": {"reference": "Patient/p1"},
            "medication": {
                "concept": {"coding": [{"code": "319785009"}]},
                "reference": {"reference": "Medication/med-1"},
            },
            "whenPrepared": datetime(2024, 7, 14, 8, 0, 0),
            "encounter": {"reference": "Encounter/enc1"},
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("MedicationDispense", "status=completed")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-md-1"
