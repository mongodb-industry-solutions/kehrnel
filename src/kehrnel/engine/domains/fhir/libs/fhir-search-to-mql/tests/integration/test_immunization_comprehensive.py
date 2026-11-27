"""
Comprehensive integration tests for ALL Immunization search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Immunization")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Immunization.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, encounter.json

Exercises 18 search parameters in ``configs/Immunization.yaml``.

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
def rich_immunization() -> Dict[str, Any]:
    return {
        "resourceType": "Immunization",
        "id": "imm-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "vaccineCode": {
            "coding": [{"system": "http://hl7.org/fhir/sid/cvx", "code": "140"}],
            "text": "Influenza vaccine",
        },
        "patient": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "occurrenceDateTime": "2024-07-15T10:00:00Z",
        "lotNumber": "LOT-2024-001",
        "location": {"reference": "Location/loc-1"},
        "manufacturer": {"reference": "Organization/org-mfr"},
        "identifier": [{"system": "http://hospital.org/imm", "value": "IMM-001"}],
        "performer": [{"actor": {"reference": "Practitioner/prac-1"}}],
        "reason": [
            {
                "concept": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "429060002"}]
                },
                "reference": {"reference": "Condition/cond-1"},
            }
        ],
        "statusReason": {
            "coding": [{"code": "immunity"}]
        },
        "protocolApplied": [
            {
                "series": "Standard 2024",
                "targetDisease": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "6142004"}]
                },
            }
        ],
        "reaction": [
            {
                "date": "2024-07-16T08:00:00Z",
                "manifestation": {"reference": {"reference": "Observation/obs-rx-1"}},
            }
        ],
    }


@pytest.fixture
def minimal_immunization() -> Dict[str, Any]:
    return {
        "resourceType": "Immunization",
        "id": "imm-min",
        "status": "completed",
        "vaccineCode": {"coding": [{"code": "140"}]},
        "patient": {"reference": "Patient/pat-min"},
    }


class TestImmunizationReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Immunization", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_performer(self, converter):
        q = converter.convert("Immunization", "performer=prac-1")
        assert "_search.performerIds" in str(q)

    def test_location(self, converter):
        q = converter.convert("Immunization", "location=loc-1")
        assert "_search.locationId" in str(q)

    def test_manufacturer(self, converter):
        q = converter.convert("Immunization", "manufacturer=org-mfr")
        assert "_search.manufacturerId" in str(q)

    def test_reaction(self, converter):
        q = converter.convert("Immunization", "reaction=obs-rx-1")
        assert "_search.reactionIds" in str(q)

    def test_reason_reference(self, converter):
        q = converter.convert("Immunization", "reason-reference=cond-1")
        assert "_search.reasonReferenceIds" in str(q)


class TestImmunizationTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Immunization", "status=completed") == {
            "status": "completed"
        }

    def test_vaccine_code(self, converter):
        q = converter.convert("Immunization", "vaccine-code=140")
        assert "140" in str(q)

    def test_reason_code(self, converter):
        q = converter.convert("Immunization", "reason-code=429060002")
        assert "429060002" in str(q)

    def test_target_disease(self, converter):
        q = converter.convert("Immunization", "target-disease=6142004")
        assert "6142004" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Immunization", "identifier=IMM-001")
        assert "IMM-001" in str(q)


class TestImmunizationStringParameters:
    def test_lot_number(self, converter):
        q = converter.convert("Immunization", "lot-number=LOT-2024")
        assert "_search.lotNumber_lower" in str(q)
        assert "lot-2024" in str(q).lower()

    def test_series(self, converter):
        q = converter.convert("Immunization", "series=Standard")
        assert "series" in str(q).lower()


class TestImmunizationDateParameters:
    def test_date(self, converter):
        q = converter.convert("Immunization", "date=ge2024-07-15")
        assert "occurrenceDateTime" in str(q)

    def test_reaction_date(self, converter):
        q = converter.convert("Immunization", "reaction-date=ge2024-07-16")
        assert "reactionDate" in str(q)


class TestImmunizationDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_immunization):
        out = denormalizer.denormalize(minimal_immunization)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert "140" in s["vaccineCode_codes"]

    def test_rich_fields(self, denormalizer, rich_immunization):
        out = denormalizer.denormalize(rich_immunization)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "140" in s["vaccineCode_codes"]
        assert "prac-1" in s["performerIds"]
        assert s["locationId"] == "loc-1"
        assert s["manufacturerId"] == "org-mfr"
        assert "429060002" in s["reasonCode_codes"]
        assert "cond-1" in s["reasonReferenceIds"]
        assert "6142004" in s["targetDisease_codes"]
        assert "obs-rx-1" in s["reactionIds"]
        assert "lot-2024-001" in s["lotNumber_lower"]
        assert "standard 2024" in s["series_lower"]

    def test_input_not_mutated(self, denormalizer, rich_immunization):
        original = copy.deepcopy(rich_immunization)
        denormalizer.denormalize(rich_immunization)
        assert rich_immunization == original


class TestImmunizationPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_immunization):
        out = denormalizer.denormalize(rich_immunization)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_immunization):
        out = denormalizer.denormalize(rich_immunization)
        assert "prac-1" in out["_compartments"]["Practitioner"]

    def test_encounter_compartment(self, denormalizer, rich_immunization):
        out = denormalizer.denormalize(rich_immunization)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Immunization", "status=completed"
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
class TestImmunizationMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["immunization_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "Immunization",
            "id": "e2e-imm-1",
            "status": "completed",
            "vaccineCode": {"coding": [{"code": "140"}]},
            "patient": {"reference": "Patient/p1"},
            "occurrenceDateTime": datetime(2024, 7, 15, 10, 0, 0),
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("Immunization", "status=completed")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-imm-1"
