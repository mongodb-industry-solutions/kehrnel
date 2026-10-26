"""
Comprehensive integration tests for ALL ClaimResponse search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "ClaimResponse")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ClaimResponse.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 13 search parameters in ``configs/ClaimResponse.yaml``.

Compartments (precomputed): Patient, Practitioner.
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
def rich_claim_response() -> Dict[str, Any]:
    return {
        "resourceType": "ClaimResponse",
        "id": "cr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "use": "claim",
        "outcome": "complete",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                    "code": "professional",
                }
            ]
        },
        "patient": {"reference": "Patient/pat-1"},
        "created": "2024-07-15",
        "insurer": {"reference": "Organization/org-ins"},
        "requestor": {"reference": "Practitioner/pr-req"},
        "request": {"reference": "Claim/claim-1"},
        "identifier": [
            {"system": "http://hospital.org/claimresponse", "value": "CR-001"}
        ],
        "disposition": "Claim processed successfully",
        "payment": {"date": "2024-08-01"},
    }


@pytest.fixture
def minimal_claim_response() -> Dict[str, Any]:
    return {
        "resourceType": "ClaimResponse",
        "id": "cr-min",
        "status": "active",
        "use": "claim",
        "outcome": "queued",
        "type": {"coding": [{"code": "professional"}]},
        "patient": {"reference": "Patient/pat-min"},
    }


class TestClaimResponseReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("ClaimResponse", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_insurer(self, converter):
        q = converter.convert("ClaimResponse", "insurer=org-ins")
        assert "_search.insurerId" in str(q)

    def test_request(self, converter):
        q = converter.convert("ClaimResponse", "request=claim-1")
        assert "_search.requestId" in str(q)

    def test_requestor(self, converter):
        q = converter.convert("ClaimResponse", "requestor=pr-req")
        assert "_search.requestorId" in str(q)


class TestClaimResponseTokenParameters:
    def test_status(self, converter):
        assert converter.convert("ClaimResponse", "status=active") == {
            "status": "active"
        }

    def test_use(self, converter):
        assert converter.convert("ClaimResponse", "use=claim") == {"use": "claim"}

    def test_outcome(self, converter):
        assert converter.convert("ClaimResponse", "outcome=complete") == {
            "outcome": "complete"
        }

    def test_identifier(self, converter):
        q = converter.convert("ClaimResponse", "identifier=CR-001")
        assert "CR-001" in str(q)


class TestClaimResponseStringParameters:
    def test_disposition(self, converter):
        q = converter.convert("ClaimResponse", "disposition=processed")
        assert "disposition" in str(q).lower()

    def test_disposition_exact(self, converter):
        q = converter.convert("ClaimResponse", "disposition:exact=Claim processed")
        assert "_search.disposition" in str(q)


class TestClaimResponseDateParameters:
    def test_created(self, converter):
        q = converter.convert("ClaimResponse", "created=ge2024-07-15")
        assert "created" in str(q)

    def test_payment_date(self, converter):
        q = converter.convert("ClaimResponse", "payment-date=ge2024-08-01")
        assert "payment.date" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("ClaimResponse", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestClaimResponseCommonParameters:
    def test_id(self, converter):
        q = converter.convert("ClaimResponse", "_id=cr-rich")
        assert "cr-rich" in str(q)


class TestClaimResponseDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_claim_response):
        out = denormalizer.denormalize(minimal_claim_response)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"

    def test_rich_fields(self, denormalizer, rich_claim_response):
        out = denormalizer.denormalize(rich_claim_response)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["insurerId"] == "org-ins"
        assert s["requestorId"] == "pr-req"
        assert s["requestId"] == "claim-1"
        assert s["requestType"] == "Claim"
        assert "CR-001" in s["identifier_values"]
        assert s["disposition"] == "Claim processed successfully"
        assert s["disposition_lower"] == "claim processed successfully"

    def test_input_not_mutated(self, denormalizer, rich_claim_response):
        original = copy.deepcopy(rich_claim_response)
        denormalizer.denormalize(rich_claim_response)
        assert rich_claim_response == original


class TestClaimResponsePrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_claim_response):
        out = denormalizer.denormalize(rich_claim_response)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_claim_response):
        out = denormalizer.denormalize(rich_claim_response)
        assert "pr-req" in out["_compartments"]["Practitioner"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "ClaimResponse", "status=active"
        )
        assert "_compartments.Patient" in str(q)
