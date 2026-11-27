"""
Comprehensive integration tests for ALL Procedure search parameters per FHIR R5.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Procedure")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Procedure.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 19 search parameters in ``configs/Procedure.yaml``:

  References (10): based-on, encounter, instantiates-canonical, location,
    part-of, patient, performer, reason-reference, report, subject
  Tokens (5): category, code, identifier, reason-code, status
  Dates (1): date
  URI (1): instantiates-uri
  Common (2): _id, _lastUpdated

R5 structural notes:
  * ``performer[]`` is BackboneElement — search uses ``performer.actor``.
  * ``reason[]`` is CodeableReference (``reason-code`` / ``reason-reference``).
  * ``occurrence[x]`` — ``date`` uses ``occurrenceDateTime`` and
    ``_search.occurrencePeriod`` for Period overlap.

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
def rich_procedure() -> Dict[str, Any]:
    return {
        "resourceType": "Procedure",
        "id": "proc-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "identifier": [{"system": "http://hospital.org/proc", "value": "PROC-001"}],
        "category": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "103693007"}]}
        ],
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "80146002"}]
        },
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "performer": [
            {"actor": {"reference": "Practitioner/prac-1"}},
            {"actor": {"reference": "Device/dev-1"}},
        ],
        "reason": [
            {
                "concept": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "109006"}]
                },
                "reference": {"reference": "Condition/cond-1"},
            }
        ],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
        "partOf": [{"reference": "Observation/obs-1"}],
        "location": {"reference": "Location/loc-1"},
        "report": [{"reference": "DiagnosticReport/dr-1"}],
        "occurrenceDateTime": "2024-07-15T09:00:00Z",
        "occurrencePeriod": {
            "start": "2024-07-15T08:00:00Z",
            "end": "2024-07-15T10:00:00Z",
        },
        "instantiatesCanonical": [
            "http://example.org/fhir/ActivityDefinition/appendectomy"
        ],
        "instantiatesUri": ["http://example.org/protocols/appendectomy-v1"],
    }


@pytest.fixture
def minimal_procedure() -> Dict[str, Any]:
    return {
        "resourceType": "Procedure",
        "id": "proc-min",
        "status": "preparation",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestProcedureReferenceParameters:
    def test_patient_bare_id(self, converter):
        q = converter.convert("Procedure", "patient=pat-1")
        s = str(q)
        assert "$or" in s
        assert "pat-1" in s

    def test_subject_typed(self, converter):
        q = converter.convert("Procedure", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_subject_bare_id(self, converter):
        q = converter.convert("Procedure", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_performer_practitioner(self, converter):
        q = converter.convert("Procedure", "performer=prac-1")
        assert "_search.performerIds" in str(q)
        assert "prac-1" in str(q)

    def test_performer_device(self, converter):
        q = converter.convert("Procedure", "performer=Device/dev-1")
        s = str(q)
        assert "_search.performerIds" in s
        assert "dev-1" in s

    def test_encounter(self, converter):
        q = converter.convert("Procedure", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("Procedure", "based-on=sr-1")
        assert "_search.basedOnIds" in str(q)

    def test_part_of(self, converter):
        q = converter.convert("Procedure", "part-of=obs-1")
        assert "_search.partOfIds" in str(q)

    def test_location(self, converter):
        q = converter.convert("Procedure", "location=loc-1")
        s = str(q)
        assert "_search.locationId" in s
        assert "loc-1" in s

    def test_reason_reference(self, converter):
        q = converter.convert("Procedure", "reason-reference=cond-1")
        assert "_search.reasonReferenceIds" in str(q)

    def test_report(self, converter):
        q = converter.convert("Procedure", "report=dr-1")
        assert "_search.reportIds" in str(q)

    def test_instantiates_canonical(self, converter):
        q = converter.convert(
            "Procedure",
            "instantiates-canonical=ActivityDefinition/appendectomy",
        )
        s = str(q)
        assert "instantiatesCanonical" in s or "instantiatesCanonical_values" in s


class TestProcedureTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Procedure", "status=completed") == {"status": "completed"}

    def test_code(self, converter):
        q = converter.convert("Procedure", "code=80146002")
        assert "80146002" in str(q)

    def test_category(self, converter):
        q = converter.convert("Procedure", "category=103693007")
        assert "103693007" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Procedure", "identifier=PROC-001")
        assert "PROC-001" in str(q)

    def test_reason_code(self, converter):
        q = converter.convert("Procedure", "reason-code=109006")
        assert "109006" in str(q)

    def test_id(self, converter):
        q = converter.convert("Procedure", "_id=proc-rich")
        assert "proc-rich" in str(q)


class TestProcedureDateAndUriParameters:
    def test_date_ge_datetime(self, converter):
        q = converter.convert("Procedure", "date=ge2024-07-15")
        s = str(q)
        assert "occurrenceDateTime" in s or "occurrencePeriod" in s

    def test_date_period_overlap(self, converter):
        q = converter.convert("Procedure", "date=le2024-12-31")
        assert "occurrencePeriod" in str(q) or "occurrenceDateTime" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Procedure", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)

    def test_instantiates_uri(self, converter):
        q = converter.convert(
            "Procedure",
            "instantiates-uri=http://example.org/protocols/appendectomy-v1",
        )
        s = str(q)
        assert "instantiatesUri" in s or "instantiatesUri_values" in s


class TestProcedureComplexQueries:
    def test_status_and_patient(self, converter):
        q = converter.convert("Procedure", "status=completed&patient=pat-1")
        s = str(q)
        assert "completed" in s
        assert "pat-1" in s

    def test_date_and_code(self, converter):
        q = converter.convert(
            "Procedure",
            "date=ge2024-07-01&code=80146002&status=completed",
        )
        s = str(q)
        assert "80146002" in s
        assert "completed" in s


class TestProcedureDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_procedure):
        out = denormalizer.denormalize(minimal_procedure)
        s = out["_search"]
        assert s["subjectId"] == "pat-min"
        assert s["patientId"] == "pat-min"
        assert "code_codes" not in s

    def test_rich_fields(self, denormalizer, rich_procedure):
        out = denormalizer.denormalize(rich_procedure)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert "prac-1" in s["performerIds"]
        assert "dev-1" in s["performerIds"]
        assert "80146002" in s["code_codes"]
        assert "103693007" in s["category_codes"]
        assert "109006" in s["reasonCode_codes"]
        assert "cond-1" in s["reasonReferenceIds"]
        assert "sr-1" in s["basedOnIds"]
        assert "obs-1" in s["partOfIds"]
        assert s["locationId"] == "loc-1"
        assert "dr-1" in s["reportIds"]
        assert "occurrencePeriod" in s

    def test_input_not_mutated(self, denormalizer, rich_procedure):
        original = copy.deepcopy(rich_procedure)
        denormalizer.denormalize(rich_procedure)
        assert rich_procedure == original

    def test_only_search_and_compartments_added(self, denormalizer, rich_procedure):
        original = set(rich_procedure.keys())
        out = denormalizer.denormalize(rich_procedure)
        added = set(out.keys()) - original
        assert added.issubset({"_search", "_compartments"})


class TestProcedurePrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_procedure):
        out = denormalizer.denormalize(rich_procedure)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_procedure):
        out = denormalizer.denormalize(rich_procedure)
        assert "prac-1" in out["_compartments"]["Practitioner"]

    def test_device_compartment(self, denormalizer, rich_procedure):
        out = denormalizer.denormalize(rich_procedure)
        assert "dev-1" in out["_compartments"]["Device"]

    def test_encounter_compartment(self, denormalizer, rich_procedure):
        out = denormalizer.denormalize(rich_procedure)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_not_in_practitioner_compartment(self, denormalizer, rich_procedure):
        out = denormalizer.denormalize(rich_procedure)
        assert "pat-1" not in out["_compartments"]["Practitioner"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Procedure", "status=completed"
        )
        s = str(q)
        assert "_compartments.Patient" in s
        assert "pat-1" in s

    def test_practitioner_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Practitioner", "prac-1", "Procedure")
        assert q == {"_compartments.Practitioner": "prac-1"}

    def test_device_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Device", "dev-1", "Procedure")
        assert q == {"_compartments.Device": "dev-1"}

    def test_encounter_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Encounter", "enc-1", "Procedure")
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
class TestProcedureMongoDB:
    _PROCEDURES = [
        {
            "resourceType": "Procedure",
            "id": "e2e-proc-1",
            "status": "completed",
            "subject": {"reference": "Patient/p1"},
            "code": {"coding": [{"code": "80146002"}]},
            "occurrenceDateTime": datetime(2024, 7, 15, 9, 0, 0),
            "performer": [{"actor": {"reference": "Practitioner/dr1"}}],
            "encounter": {"reference": "Encounter/enc1"},
        },
        {
            "resourceType": "Procedure",
            "id": "e2e-proc-2",
            "status": "in-progress",
            "subject": {"reference": "Patient/p2"},
            "code": {"coding": [{"code": "71388002"}]},
            "occurrenceDateTime": datetime(2024, 1, 10, 8, 0, 0),
        },
    ]

    @pytest.fixture(scope="class")
    def mongo_col(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        col = db["procedure_e2e"]
        col.delete_many({})
        yield col
        col.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_col):
        dn = ResourceDenormalizer()
        docs = [dn.denormalize(copy.deepcopy(s)) for s in self._PROCEDURES]
        mongo_col.insert_many(docs)
        return mongo_col

    @pytest.fixture(scope="class")
    def conv(self):
        return FHIRSearchConverter()

    def test_status_lookup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Procedure", "status=completed")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-proc-1"

    def test_patient_and_code(self, seeded, conv):
        q = conv.convert("Procedure", "patient=p1&code=80146002")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-proc-1"

    def test_patient_compartment_e2e(self, seeded, conv):
        mql = conv.convert_with_compartment(
            "Patient", "p1", "Procedure", "status=completed"
        )
        results = list(seeded.find(mql))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-proc-1"

    def test_encounter_compartment_e2e(self, seeded, conv):
        mql = conv.convert_with_compartment(
            "Encounter", "enc1", "Procedure", "status=completed"
        )
        results = list(seeded.find(mql))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-proc-1"
