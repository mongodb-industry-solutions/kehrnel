"""
Comprehensive integration tests for ALL CoverageEligibilityRequest search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "CoverageEligibilityRequest")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/CoverageEligibilityRequest.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 9 search parameters in ``configs/CoverageEligibilityRequest.yaml``.

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
def rich_cer() -> Dict[str, Any]:
    return {
        "resourceType": "CoverageEligibilityRequest",
        "id": "cer-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "patient": {"reference": "Patient/pat-1"},
        "insurer": {"reference": "Organization/org-ins"},
        "enterer": {"reference": "Practitioner/pr-enterer"},
        "provider": {"reference": "Practitioner/pr-provider"},
        "facility": {"reference": "Location/loc-1"},
        "created": "2024-07-15",
        "identifier": [
            {"system": "http://hospital.org/cer", "value": "CER-001"}
        ],
    }


@pytest.fixture
def minimal_cer() -> Dict[str, Any]:
    return {
        "resourceType": "CoverageEligibilityRequest",
        "id": "cer-min",
        "status": "active",
        "patient": {"reference": "Patient/pat-min"},
        "insurer": {"reference": "Organization/org-min"},
    }


class TestCoverageEligibilityRequestReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("CoverageEligibilityRequest", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_enterer(self, converter):
        q = converter.convert("CoverageEligibilityRequest", "enterer=pr-enterer")
        assert "_search.entererId" in str(q)

    def test_provider(self, converter):
        q = converter.convert("CoverageEligibilityRequest", "provider=pr-provider")
        assert "_search.providerId" in str(q)

    def test_facility(self, converter):
        q = converter.convert("CoverageEligibilityRequest", "facility=loc-1")
        assert "_search.facilityId" in str(q)


class TestCoverageEligibilityRequestTokenParameters:
    def test_status(self, converter):
        assert converter.convert(
            "CoverageEligibilityRequest", "status=active"
        ) == {"status": "active"}

    def test_identifier(self, converter):
        q = converter.convert("CoverageEligibilityRequest", "identifier=CER-001")
        assert "CER-001" in str(q)

    def test_id(self, converter):
        q = converter.convert("CoverageEligibilityRequest", "_id=cer-rich")
        assert "cer-rich" in str(q)


class TestCoverageEligibilityRequestDateParameters:
    def test_created(self, converter):
        q = converter.convert("CoverageEligibilityRequest", "created=ge2024-07-01")
        assert "created" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert(
            "CoverageEligibilityRequest", "_lastUpdated=ge2024-08-01"
        )
        assert "meta.lastUpdated" in str(q)


class TestCoverageEligibilityRequestDenormalization:
    def test_rich(self, denormalizer, rich_cer):
        out = denormalizer.denormalize(copy.deepcopy(rich_cer))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["entererId"] == "pr-enterer"
        assert s["providerId"] == "pr-provider"
        assert s["facilityId"] == "loc-1"
        assert "CER-001" in s["identifier_values"]
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_minimal(self, denormalizer, minimal_cer):
        out = denormalizer.denormalize(copy.deepcopy(minimal_cer))
        assert out["_search"]["patientId"] == "pat-min"


class TestCoverageEligibilityRequestCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "CoverageEligibilityRequest"
        )
        assert q == {"_compartments.Patient": "pat-1"}
