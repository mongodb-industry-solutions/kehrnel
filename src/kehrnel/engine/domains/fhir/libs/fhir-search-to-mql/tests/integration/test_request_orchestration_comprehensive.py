"""Comprehensive integration tests for RequestOrchestration search parameters."""
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
def rich_request_orchestration() -> Dict[str, Any]:
    return {
        "resourceType": "RequestOrchestration",
        "id": "ro-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "intent": "order",
        "priority": "routine",
        "subject": {"reference": "Patient/pat-1"},
        "author": {"reference": "Practitioner/prac-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "authoredOn": "2024-07-15T09:00:00Z",
        "code": {"coding": [{"code": "protocol"}]},
        "identifier": [{"value": "RO-001"}],
        "groupIdentifier": {"value": "GRP-RO-001"},
        "basedOn": [{"reference": "CarePlan/cp-1"}],
        "instantiatesCanonical": ["http://example.org/PlanDefinition/pd-1"],
        "instantiatesUri": ["http://example.org/protocols/ro-1"],
        "action": [
            {
                "participant": [
                    {"actorReference": {"reference": "Practitioner/part-1"}}
                ]
            }
        ],
    }


class TestRequestOrchestrationReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("RequestOrchestration", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_author(self, converter):
        q = converter.convert("RequestOrchestration", "author=prac-1")
        assert "_search.authorId" in str(q)

    def test_participant(self, converter):
        q = converter.convert("RequestOrchestration", "participant=part-1")
        assert "_search.participantIds" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("RequestOrchestration", "based-on=cp-1")
        assert "_search.basedOnIds" in str(q)


class TestRequestOrchestrationTokenParameters:
    def test_code(self, converter):
        q = converter.convert("RequestOrchestration", "code=protocol")
        assert "protocol" in str(q)

    def test_status(self, converter):
        q = converter.convert("RequestOrchestration", "status=active")
        assert q == {"status": "active"}


class TestRequestOrchestrationDateParameters:
    def test_authored(self, converter):
        q = converter.convert("RequestOrchestration", "authored=ge2024-07-01")
        assert "authoredOn" in str(q)


class TestRequestOrchestrationCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "RequestOrchestration")
        assert q == {"_compartments.Patient": "pat-1"}


class TestRequestOrchestrationDenormalization:
    def test_rich(self, denormalizer, rich_request_orchestration):
        out = denormalizer.denormalize(copy.deepcopy(rich_request_orchestration))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["authorId"] == "prac-1"
        assert "part-1" in s["participantIds"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
