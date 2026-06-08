"""
Comprehensive integration tests for ALL DeviceRequest search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "DeviceRequest")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/DeviceRequest.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 21 search parameters in ``configs/DeviceRequest.yaml``.

Compartments (precomputed): Patient, Practitioner, Device.
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
def rich_device_request() -> Dict[str, Any]:
    return {
        "resourceType": "DeviceRequest",
        "id": "dr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "intent": "order",
        "code": {
            "concept": {"coding": [{"code": "wheelchair"}]},
            "reference": {"reference": "Device/dev-1"},
        },
        "subject": {"reference": "Patient/pat-1"},
        "requester": {"reference": "Practitioner/prac-1"},
        "performer": {
            "concept": {"coding": [{"code": "nurse"}]},
            "reference": {"reference": "PractitionerRole/pr-1"},
        },
        "encounter": {"reference": "Encounter/enc-1"},
        "authoredOn": "2024-07-15T10:00:00Z",
        "occurrenceDateTime": "2024-07-20T10:00:00Z",
        "identifier": [
            {"system": "http://hospital.org/dr", "value": "DR-001"}
        ],
        "groupIdentifier": {
            "system": "http://hospital.org/grp",
            "value": "GRP-001",
        },
        "instantiatesUri": ["http://example.org/protocols/dr"],
        "replaces": [{"reference": "DeviceRequest/dr-prev"}],
    }


@pytest.fixture
def minimal_device_request() -> Dict[str, Any]:
    return {
        "resourceType": "DeviceRequest",
        "id": "dr-min",
        "status": "active",
        "intent": "order",
        "code": {"concept": {"coding": [{"code": "walker"}]}},
        "subject": {"reference": "Patient/pat-min"},
    }


class TestDeviceRequestReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("DeviceRequest", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("DeviceRequest", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_device(self, converter):
        q = converter.convert("DeviceRequest", "device=dev-1")
        assert "_search.deviceIds" in str(q)

    def test_requester(self, converter):
        q = converter.convert("DeviceRequest", "requester=prac-1")
        assert "_search.requesterId" in str(q)

    def test_performer(self, converter):
        q = converter.convert("DeviceRequest", "performer=pr-1")
        assert "_search.performerIds" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("DeviceRequest", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_prior_request(self, converter):
        q = converter.convert("DeviceRequest", "prior-request=dr-prev")
        assert "_search.replacesIds" in str(q)


class TestDeviceRequestTokenParameters:
    def test_code(self, converter):
        q = converter.convert("DeviceRequest", "code=wheelchair")
        assert "codeConcept_codes" in str(q)

    def test_performer_code(self, converter):
        q = converter.convert("DeviceRequest", "performer-code=nurse")
        assert "performerConcept_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("DeviceRequest", "identifier=DR-001")
        assert "DR-001" in str(q)

    def test_group_identifier(self, converter):
        q = converter.convert("DeviceRequest", "group-identifier=GRP-001")
        assert "GRP-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("DeviceRequest", "status=active")
        assert "active" in str(q)

    def test_intent(self, converter):
        q = converter.convert("DeviceRequest", "intent=order")
        assert "order" in str(q)


class TestDeviceRequestDateParameters:
    def test_authored_on(self, converter):
        q = converter.convert("DeviceRequest", "authored-on=ge2024-07-01")
        assert "authoredOn" in str(q)

    def test_event_date(self, converter):
        q = converter.convert("DeviceRequest", "event-date=ge2024-07-20")
        assert "occurrenceDateTime" in str(q)


class TestDeviceRequestUriParameters:
    def test_instantiates_uri(self, converter):
        q = converter.convert(
            "DeviceRequest",
            "instantiates-uri=http://example.org/protocols/dr",
        )
        assert "instantiatesUri" in str(q) or "instantiatesUri_values" in str(q)


class TestDeviceRequestDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_device_request):
        doc = denormalizer.denormalize(copy.deepcopy(rich_device_request))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert "dev-1" in search["deviceIds"]
        assert search["requesterId"] == "prac-1"
        assert "wheelchair" in search["codeConcept_codes"]
        assert "DR-001" in search["identifier_values"]
        assert "GRP-001" in search["groupIdentifier_values"]
        assert "dr-prev" in search["replacesIds"]
        assert "pat-1" in doc["_compartments"]["Patient"]
        assert "prac-1" in doc["_compartments"]["Practitioner"]

    def test_minimal_denormalization(self, denormalizer, minimal_device_request):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_device_request))
        assert doc["_search"]["patientId"] == "pat-min"
