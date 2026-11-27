"""Comprehensive integration tests for EnrollmentRequest search parameters."""
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
def rich_enrollment_request() -> Dict[str, Any]:
    return {
        "resourceType": "EnrollmentRequest",
        "id": "enr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "candidate": {"reference": "Patient/pat-1"},
        "identifier": [{"value": "ENR-001"}],
    }


class TestEnrollmentRequestReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("EnrollmentRequest", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("EnrollmentRequest", "subject=pat-1")
        assert "_search.candidateId" in str(q)


class TestEnrollmentRequestTokenParameters:
    def test_status(self, converter):
        assert converter.convert("EnrollmentRequest", "status=active") == {"status": "active"}

    def test_identifier(self, converter):
        q = converter.convert("EnrollmentRequest", "identifier=ENR-001")
        assert "ENR-001" in str(q)


class TestEnrollmentRequestCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "EnrollmentRequest")
        assert q == {"_compartments.Patient": "pat-1"}


class TestEnrollmentRequestDenormalization:
    def test_rich(self, denormalizer, rich_enrollment_request):
        out = denormalizer.denormalize(copy.deepcopy(rich_enrollment_request))
        assert out["_search"]["candidateId"] == "pat-1"
        assert out["_compartments"]["Patient"] == ["pat-1"]
