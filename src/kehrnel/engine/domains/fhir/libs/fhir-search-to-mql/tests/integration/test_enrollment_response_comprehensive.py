"""Comprehensive integration tests for EnrollmentResponse search parameters."""
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
def rich_enrollment_response() -> Dict[str, Any]:
    return {
        "resourceType": "EnrollmentResponse",
        "id": "enres-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "request": {"reference": "EnrollmentRequest/enr-1"},
        "identifier": [{"value": "ENRES-001"}],
    }


class TestEnrollmentResponseReferenceParameters:
    def test_request(self, converter):
        q = converter.convert("EnrollmentResponse", "request=enr-1")
        assert "_search.requestId" in str(q)


class TestEnrollmentResponseTokenParameters:
    def test_status(self, converter):
        assert converter.convert("EnrollmentResponse", "status=active") == {"status": "active"}

    def test_identifier(self, converter):
        q = converter.convert("EnrollmentResponse", "identifier=ENRES-001")
        assert "ENRES-001" in str(q)


class TestEnrollmentResponseDenormalization:
    def test_rich(self, denormalizer, rich_enrollment_response):
        out = denormalizer.denormalize(copy.deepcopy(rich_enrollment_response))
        assert out["_search"]["requestId"] == "enr-1"
