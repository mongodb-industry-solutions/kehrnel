"""
Comprehensive integration tests for ALL PaymentNotice search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "PaymentNotice")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/PaymentNotice.yaml

Exercises 9 search parameters in ``configs/PaymentNotice.yaml``.

No precomputed compartments (Practitioner linking param `provider` in
compartment JSON does not match R5 `reporter` field).
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
def rich_payment_notice() -> Dict[str, Any]:
    return {
        "resourceType": "PaymentNotice",
        "id": "pn-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "amount": {"value": 250.0, "currency": "USD"},
        "recipient": {"reference": "Organization/org-1"},
        "created": "2024-07-15T10:00:00Z",
        "reporter": {"reference": "Practitioner/prac-1"},
        "request": {"reference": "Claim/claim-1"},
        "response": {"reference": "ClaimResponse/cr-1"},
        "identifier": [
            {"system": "http://hospital.org/pn", "value": "PN-001"}
        ],
        "paymentStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/paymentstatus",
                    "code": "paid",
                }
            ]
        },
    }


@pytest.fixture
def minimal_payment_notice() -> Dict[str, Any]:
    return {
        "resourceType": "PaymentNotice",
        "id": "pn-min",
        "status": "active",
        "amount": {"value": 1, "currency": "USD"},
        "recipient": {"reference": "Organization/org-min"},
    }


class TestPaymentNoticeReferenceParameters:
    def test_reporter(self, converter):
        q = converter.convert("PaymentNotice", "reporter=prac-1")
        assert "_search.reporterId" in str(q)

    def test_request(self, converter):
        q = converter.convert("PaymentNotice", "request=claim-1")
        assert "_search.requestId" in str(q)

    def test_response(self, converter):
        q = converter.convert("PaymentNotice", "response=cr-1")
        assert "_search.responseId" in str(q)


class TestPaymentNoticeTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("PaymentNotice", "identifier=PN-001")
        assert "PN-001" in str(q)

    def test_payment_status(self, converter):
        q = converter.convert("PaymentNotice", "payment-status=paid")
        assert "paymentStatus_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("PaymentNotice", "status=active")
        assert "active" in str(q)

    def test_id(self, converter):
        q = converter.convert("PaymentNotice", "_id=pn-rich")
        assert "pn-rich" in str(q)


class TestPaymentNoticeDateParameters:
    def test_created(self, converter):
        q = converter.convert("PaymentNotice", "created=ge2024-07-01")
        assert "created" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("PaymentNotice", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestPaymentNoticeDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_payment_notice):
        doc = denormalizer.denormalize(copy.deepcopy(rich_payment_notice))
        search = doc["_search"]
        assert search["reporterId"] == "prac-1"
        assert search["requestId"] == "claim-1"
        assert search["responseId"] == "cr-1"
        assert "paid" in search["paymentStatus_codes"]
        assert "PN-001" in search["identifier_values"]
        assert "_compartments" not in doc

    def test_minimal_denormalization(self, denormalizer, minimal_payment_notice):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_payment_notice))
        assert doc.get("_search", {}) == {}
