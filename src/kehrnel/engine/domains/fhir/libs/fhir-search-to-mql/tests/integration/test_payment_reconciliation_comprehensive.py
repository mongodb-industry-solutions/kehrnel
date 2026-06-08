"""
Comprehensive integration tests for ALL PaymentReconciliation search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "PaymentReconciliation")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/PaymentReconciliation.yaml

Exercises 12 search parameters in ``configs/PaymentReconciliation.yaml``.

Compartments (precomputed): Practitioner.
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
def rich_payment_reconciliation() -> Dict[str, Any]:
    return {
        "resourceType": "PaymentReconciliation",
        "id": "pr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "outcome": "complete",
        "type": {"coding": [{"code": "payment"}]},
        "amount": {"value": 500.0, "currency": "USD"},
        "created": "2024-07-15T10:00:00Z",
        "disposition": "Payment processed successfully",
        "requestor": {"reference": "Practitioner/prac-1"},
        "request": {"reference": "Task/task-1"},
        "paymentIssuer": {"reference": "Organization/org-1"},
        "identifier": [
            {"system": "http://hospital.org/pr", "value": "PR-001"}
        ],
        "allocation": [
            {
                "account": {"reference": "Account/acct-1"},
                "encounter": {"reference": "Encounter/enc-1"},
            }
        ],
    }


@pytest.fixture
def minimal_payment_reconciliation() -> Dict[str, Any]:
    return {
        "resourceType": "PaymentReconciliation",
        "id": "pr-min",
        "status": "active",
        "type": {"coding": [{"code": "payment"}]},
        "amount": {"value": 1, "currency": "USD"},
    }


class TestPaymentReconciliationReferenceParameters:
    def test_requestor(self, converter):
        q = converter.convert("PaymentReconciliation", "requestor=prac-1")
        assert "_search.requestorId" in str(q)

    def test_request(self, converter):
        q = converter.convert("PaymentReconciliation", "request=task-1")
        assert "_search.requestId" in str(q)

    def test_payment_issuer(self, converter):
        q = converter.convert("PaymentReconciliation", "payment-issuer=org-1")
        assert "_search.paymentIssuerId" in str(q)

    def test_allocation_account(self, converter):
        q = converter.convert("PaymentReconciliation", "allocation-account=acct-1")
        assert "_search.allocationAccountIds" in str(q)

    def test_allocation_encounter(self, converter):
        q = converter.convert(
            "PaymentReconciliation", "allocation-encounter=enc-1"
        )
        assert "_search.allocationEncounterIds" in str(q)


class TestPaymentReconciliationTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("PaymentReconciliation", "identifier=PR-001")
        assert "PR-001" in str(q)

    def test_outcome(self, converter):
        q = converter.convert("PaymentReconciliation", "outcome=complete")
        assert "complete" in str(q)

    def test_status(self, converter):
        q = converter.convert("PaymentReconciliation", "status=active")
        assert "active" in str(q)

    def test_id(self, converter):
        q = converter.convert("PaymentReconciliation", "_id=pr-rich")
        assert "pr-rich" in str(q)


class TestPaymentReconciliationStringParameters:
    def test_disposition(self, converter):
        q = converter.convert(
            "PaymentReconciliation", "disposition=processed"
        )
        assert "disposition_lower" in str(q)

    def test_disposition_exact(self, converter):
        q = converter.convert(
            "PaymentReconciliation",
            "disposition:exact=Payment processed successfully",
        )
        assert "_search.disposition" in str(q)


class TestPaymentReconciliationDateParameters:
    def test_created(self, converter):
        q = converter.convert("PaymentReconciliation", "created=ge2024-07-01")
        assert "created" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("PaymentReconciliation", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestPaymentReconciliationDenormalization:
    def test_rich_denormalization(
        self, denormalizer, rich_payment_reconciliation
    ):
        doc = denormalizer.denormalize(
            copy.deepcopy(rich_payment_reconciliation)
        )
        search = doc["_search"]
        assert search["requestorId"] == "prac-1"
        assert search["requestId"] == "task-1"
        assert search["paymentIssuerId"] == "org-1"
        assert "acct-1" in search["allocationAccountIds"]
        assert "enc-1" in search["allocationEncounterIds"]
        assert "PR-001" in search["identifier_values"]
        assert "payment processed successfully" in search["disposition_lower"]
        assert "Practitioner" in doc["_compartments"]
        assert "prac-1" in doc["_compartments"]["Practitioner"]

    def test_minimal_denormalization(
        self, denormalizer, minimal_payment_reconciliation
    ):
        doc = denormalizer.denormalize(
            copy.deepcopy(minimal_payment_reconciliation)
        )
        assert doc.get("_search", {}) == {}
