"""
Comprehensive integration tests for ALL Communication search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Communication")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Communication.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 18 search parameters in ``configs/Communication.yaml``.

Compartments (precomputed): Patient, Practitioner, Encounter, Device.
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
def rich_communication() -> Dict[str, Any]:
    return {
        "resourceType": "Communication",
        "id": "comm-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "sender": {"reference": "Practitioner/prac-1"},
        "recipient": [{"reference": "Practitioner/prac-2"}],
        "sent": "2024-07-15T09:00:00Z",
        "received": "2024-07-15T09:05:00Z",
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/communication-category",
                        "code": "notification",
                    }
                ]
            }
        ],
        "medium": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationMode",
                        "code": "WRITTEN",
                    }
                ]
            }
        ],
        "topic": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "371535009",
                }
            ]
        },
        "identifier": [
            {"system": "http://hospital.org/comm", "value": "COMM-001"}
        ],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
        "partOf": [{"reference": "Communication/parent-1"}],
        "instantiatesCanonical": ["http://example.org/PlanDefinition/pd-1"],
        "instantiatesUri": ["http://example.org/protocols/alert"],
    }


@pytest.fixture
def minimal_communication() -> Dict[str, Any]:
    return {
        "resourceType": "Communication",
        "id": "comm-min",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestCommunicationReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Communication", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Communication", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("Communication", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_sender(self, converter):
        q = converter.convert("Communication", "sender=prac-1")
        assert "_search.senderId" in str(q)

    def test_recipient(self, converter):
        q = converter.convert("Communication", "recipient=prac-2")
        assert "_search.recipientIds" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("Communication", "based-on=sr-1")
        assert "_search.basedOnIds" in str(q)

    def test_part_of(self, converter):
        q = converter.convert("Communication", "part-of=parent-1")
        assert "_search.partOfIds" in str(q)

    def test_instantiates_canonical(self, converter):
        q = converter.convert(
            "Communication",
            "instantiates-canonical=http://example.org/PlanDefinition/pd-1",
        )
        assert "instantiatesCanonical" in str(q)


class TestCommunicationTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Communication", "identifier=COMM-001")
        assert "COMM-001" in str(q)

    def test_category(self, converter):
        q = converter.convert("Communication", "category=notification")
        assert "notification" in str(q)

    def test_medium(self, converter):
        q = converter.convert("Communication", "medium=WRITTEN")
        assert "WRITTEN" in str(q)

    def test_topic(self, converter):
        q = converter.convert("Communication", "topic=371535009")
        assert "371535009" in str(q)

    def test_status(self, converter):
        q = converter.convert("Communication", "status=completed")
        assert "completed" in str(q)


class TestCommunicationDateParameters:
    def test_sent(self, converter):
        q = converter.convert("Communication", "sent=ge2024-07-01")
        assert "sent" in str(q)

    def test_received(self, converter):
        q = converter.convert("Communication", "received=ge2024-07-01")
        assert "received" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Communication", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestCommunicationUriParameters:
    def test_instantiates_uri(self, converter):
        q = converter.convert(
            "Communication",
            "instantiates-uri=http://example.org/protocols/alert",
        )
        assert "instantiatesUri" in str(q)


class TestCommunicationCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Communication", "_id=comm-rich")
        assert "comm-rich" in str(q)


class TestCommunicationDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_communication):
        out = denormalizer.denormalize(minimal_communication)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_communication):
        out = denormalizer.denormalize(rich_communication)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["subjectId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert s["senderId"] == "prac-1"
        assert "prac-2" in s["recipientIds"]
        assert "notification" in s["category_codes"]
        assert "WRITTEN" in s["medium_codes"]
        assert "371535009" in s["topic_codes"]
        assert "COMM-001" in s["identifier_values"]
        assert "sr-1" in s["basedOnIds"]
        assert "parent-1" in s["partOfIds"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert "prac-1" in out["_compartments"]["Practitioner"]
        assert out["_compartments"]["Encounter"] == ["enc-1"]

    def test_input_not_mutated(self, denormalizer, rich_communication):
        original = copy.deepcopy(rich_communication)
        denormalizer.denormalize(rich_communication)
        assert rich_communication == original
