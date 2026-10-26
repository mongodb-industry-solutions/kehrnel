"""
Comprehensive integration tests for ALL CoverageEligibilityResponse search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "CoverageEligibilityResponse")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/CoverageEligibilityResponse.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 11 search parameters in ``configs/CoverageEligibilityResponse.yaml``.

Compartments (precomputed): Patient.
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
def rich_ceres() -> Dict[str, Any]:
    return {
        "resourceType": "CoverageEligibilityResponse",
        "id": "ceres-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "outcome": "complete",
        "patient": {"reference": "Patient/pat-1"},
        "insurer": {"reference": "Organization/org-ins"},
        "request": {"reference": "CoverageEligibilityRequest/cer-1"},
        "requestor": {"reference": "Practitioner/pr-req"},
        "created": "2024-07-15",
        "disposition": "Eligible for coverage",
        "identifier": [
            {"system": "http://hospital.org/ceres", "value": "CERES-001"}
        ],
    }


@pytest.fixture
def minimal_ceres() -> Dict[str, Any]:
    return {
        "resourceType": "CoverageEligibilityResponse",
        "id": "ceres-min",
        "status": "active",
        "outcome": "complete",
        "patient": {"reference": "Patient/pat-min"},
        "insurer": {"reference": "Organization/org-min"},
        "request": {"reference": "CoverageEligibilityRequest/cer-min"},
    }


class TestCoverageEligibilityResponseReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("CoverageEligibilityResponse", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_request(self, converter):
        q = converter.convert("CoverageEligibilityResponse", "request=cer-1")
        assert "_search.requestId" in str(q)

    def test_insurer(self, converter):
        q = converter.convert("CoverageEligibilityResponse", "insurer=org-ins")
        assert "_search.insurerId" in str(q)

    def test_requestor(self, converter):
        q = converter.convert("CoverageEligibilityResponse", "requestor=pr-req")
        assert "_search.requestorId" in str(q)


class TestCoverageEligibilityResponseTokenParameters:
    def test_status(self, converter):
        assert converter.convert(
            "CoverageEligibilityResponse", "status=active"
        ) == {"status": "active"}

    def test_outcome(self, converter):
        assert converter.convert(
            "CoverageEligibilityResponse", "outcome=complete"
        ) == {"outcome": "complete"}

    def test_identifier(self, converter):
        q = converter.convert(
            "CoverageEligibilityResponse", "identifier=CERES-001"
        )
        assert "CERES-001" in str(q)


class TestCoverageEligibilityResponseStringParameters:
    def test_disposition_default(self, converter):
        q = converter.convert(
            "CoverageEligibilityResponse", "disposition=eligible"
        )
        assert "disposition_lower" in str(q)

    def test_disposition_exact(self, converter):
        q = converter.convert(
            "CoverageEligibilityResponse", "disposition:exact=Eligible for coverage"
        )
        assert "disposition" in str(q)


class TestCoverageEligibilityResponseDateParameters:
    def test_created(self, converter):
        q = converter.convert(
            "CoverageEligibilityResponse", "created=ge2024-07-01"
        )
        assert "created" in str(q)


class TestCoverageEligibilityResponseDenormalization:
    def test_rich(self, denormalizer, rich_ceres):
        out = denormalizer.denormalize(copy.deepcopy(rich_ceres))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["requestId"] == "cer-1"
        assert s["insurerId"] == "org-ins"
        assert s["requestorId"] == "pr-req"
        assert s["disposition"] == "Eligible for coverage"
        assert "CERES-001" in s["identifier_values"]
        assert "pat-1" in out["_compartments"]["Patient"]
