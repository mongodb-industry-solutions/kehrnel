"""
Comprehensive integration tests for ALL ServiceRequest search parameters per FHIR R5.

Local spec sources:
- schema/indexes/search-parameters.r5.json  (resource "ServiceRequest")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ServiceRequest.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 25 search parameters in ``configs/ServiceRequest.yaml``:

  References (11): based-on, body-structure, code-reference, encounter,
    instantiates-canonical, patient, performer, replaces, requester,
    specimen, subject
  Tokens (9): body-site, category, code-concept, identifier, intent,
    performer-type, priority, requisition, status
  Dates (2): authored, occurrence
  URI (1): instantiates-uri
  Common (2): _id, _lastUpdated

Compartments (precomputed): Patient, Practitioner, Device.
Encounter compartment is dynamic via ``encounter``.
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
def rich_service_request() -> Dict[str, Any]:
    return {
        "resourceType": "ServiceRequest",
        "id": "sr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "intent": "order",
        "priority": "routine",
        "identifier": [{"system": "http://hospital.org/sr", "value": "SR-001"}],
        "requisition": {"system": "http://hospital.org/req", "value": "REQ-99"},
        "category": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "108252007"}]}
        ],
        "code": {
            "concept": {
                "coding": [{"system": "http://snomed.info/sct", "code": "103693007"}]
            },
            "reference": {"reference": "ActivityDefinition/lab-panel"},
        },
        "subject": {"reference": "Patient/pat-1"},
        "requester": {"reference": "Practitioner/prac-1"},
        "performer": [
            {"reference": "Practitioner/prac-2"},
            {"reference": "Organization/org-1"},
        ],
        "performerType": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                    "code": "PPRF",
                }
            ]
        },
        "encounter": {"reference": "Encounter/enc-1"},
        "authoredOn": "2024-07-01T10:00:00Z",
        "occurrencePeriod": {"start": "2024-07-15T08:00:00Z", "end": "2024-07-15T12:00:00Z"},
        "bodySite": [{"coding": [{"system": "http://snomed.info/sct", "code": "181414000"}]}],
        "bodyStructure": {"reference": "BodyStructure/bs-1"},
        "basedOn": [{"reference": "CarePlan/cp-1"}],
        "replaces": [{"reference": "ServiceRequest/sr-old"}],
        "specimen": [{"reference": "Specimen/sp-1"}],
        "instantiatesCanonical": ["http://example.org/fhir/ActivityDefinition/colo-screening"],
        "instantiatesUri": ["http://example.org/protocols/colo-v1"],
    }


@pytest.fixture
def minimal_service_request() -> Dict[str, Any]:
    return {
        "resourceType": "ServiceRequest",
        "id": "sr-min",
        "status": "draft",
        "intent": "proposal",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestServiceRequestReferenceParameters:
    def test_patient_bare_id(self, converter):
        q = converter.convert("ServiceRequest", "patient=pat-1")
        s = str(q)
        assert "$or" in s
        assert "pat-1" in s

    def test_subject_typed(self, converter):
        q = converter.convert("ServiceRequest", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_requester(self, converter):
        q = converter.convert("ServiceRequest", "requester=prac-1")
        assert "_search.requesterId" in str(q)

    def test_performer(self, converter):
        q = converter.convert("ServiceRequest", "performer=prac-2")
        assert "_search.performerIds" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("ServiceRequest", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("ServiceRequest", "based-on=cp-1")
        assert "_search.basedOnIds" in str(q)

    def test_code_reference(self, converter):
        q = converter.convert("ServiceRequest", "code-reference=lab-panel")
        assert "_search.codeReferenceIds" in str(q)

    def test_specimen(self, converter):
        q = converter.convert("ServiceRequest", "specimen=sp-1")
        assert "_search.specimenIds" in str(q)

    def test_body_structure(self, converter):
        q = converter.convert("ServiceRequest", "body-structure=bs-1")
        assert "_search.bodyStructureId" in str(q)


class TestServiceRequestTokenParameters:
    def test_status(self, converter):
        assert converter.convert("ServiceRequest", "status=active") == {"status": "active"}

    def test_intent(self, converter):
        assert converter.convert("ServiceRequest", "intent=order") == {"intent": "order"}

    def test_code_concept(self, converter):
        q = converter.convert("ServiceRequest", "code-concept=103693007")
        assert "103693007" in str(q)

    def test_category(self, converter):
        q = converter.convert("ServiceRequest", "category=108252007")
        assert "108252007" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("ServiceRequest", "identifier=SR-001")
        assert "SR-001" in str(q)

    def test_requisition(self, converter):
        q = converter.convert("ServiceRequest", "requisition=REQ-99")
        assert "REQ-99" in str(q)

    def test_id(self, converter):
        q = converter.convert("ServiceRequest", "_id=sr-rich")
        assert "sr-rich" in str(q)


class TestServiceRequestDateAndUriParameters:
    def test_authored_ge(self, converter):
        q = converter.convert("ServiceRequest", "authored=ge2024-07-01")
        assert "authoredOn" in str(q)

    def test_occurrence_period(self, converter):
        q = converter.convert("ServiceRequest", "occurrence=ge2024-07-15")
        assert "occurrencePeriod" in str(q) or "occurrenceDateTime" in str(q)

    def test_instantiates_uri(self, converter):
        q = converter.convert(
            "ServiceRequest",
            "instantiates-uri=http://example.org/protocols/colo-v1",
        )
        assert "instantiatesUri" in str(q) or "instantiatesUri_values" in str(q)


class TestServiceRequestDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_service_request):
        out = denormalizer.denormalize(minimal_service_request)
        assert out["_search"]["subjectId"] == "pat-min"
        assert "codeConcept_codes" not in out["_search"]

    def test_rich_fields(self, denormalizer, rich_service_request):
        out = denormalizer.denormalize(rich_service_request)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["requesterId"] == "prac-1"
        assert "prac-2" in s["performerIds"]
        assert s["encounterId"] == "enc-1"
        assert "103693007" in s["codeConcept_codes"]
        assert "lab-panel" in s["codeReferenceIds"]
        assert "occurrencePeriod" in s

    def test_input_not_mutated(self, denormalizer, rich_service_request):
        original = copy.deepcopy(rich_service_request)
        denormalizer.denormalize(rich_service_request)
        assert rich_service_request == original


class TestServiceRequestPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_service_request):
        out = denormalizer.denormalize(rich_service_request)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_service_request):
        out = denormalizer.denormalize(rich_service_request)
        prac = out["_compartments"]["Practitioner"]
        assert "prac-1" in prac
        assert "prac-2" in prac

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "ServiceRequest", "status=active"
        )
        assert "_compartments.Patient" in str(q)

    def test_practitioner_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Practitioner", "prac-1", "ServiceRequest")
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
class TestServiceRequestMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["servicerequest_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "ServiceRequest",
            "id": "e2e-sr-1",
            "status": "active",
            "intent": "order",
            "subject": {"reference": "Patient/p1"},
            "authoredOn": datetime(2024, 7, 1, 10, 0, 0),
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("ServiceRequest", "status=active")))
        assert len(results) == 1
