"""
Comprehensive integration tests for ALL Invoice search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Invoice")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Invoice.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 15 search parameters in ``configs/Invoice.yaml``.

Compartments (precomputed): Patient, Practitioner, RelatedPerson.
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
def rich_invoice() -> Dict[str, Any]:
    return {
        "resourceType": "Invoice",
        "id": "inv-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "issued",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/invoice-type",
                    "code": "invoice",
                }
            ]
        },
        "subject": {"reference": "Patient/pat-1"},
        "recipient": {"reference": "RelatedPerson/rp-1"},
        "account": {"reference": "Account/acct-1"},
        "issuer": {"reference": "Organization/org-issuer"},
        "date": "2024-07-15T10:00:00Z",
        "identifier": [
            {"system": "http://hospital.org/inv", "value": "INV-001"}
        ],
        "participant": [
            {
                "actor": {"reference": "Practitioner/prac-1"},
                "role": {"coding": [{"code": "author"}]},
            }
        ],
        "totalGross": {"value": 500.0, "currency": "USD"},
        "totalNet": {"value": 450.0, "currency": "USD"},
    }


@pytest.fixture
def minimal_invoice() -> Dict[str, Any]:
    return {
        "resourceType": "Invoice",
        "id": "inv-min",
        "status": "issued",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestInvoiceReferenceParameters:
    def test_account(self, converter):
        q = converter.convert("Invoice", "account=acct-1")
        assert "_search.accountId" in str(q)

    def test_issuer(self, converter):
        q = converter.convert("Invoice", "issuer=org-issuer")
        assert "_search.issuerId" in str(q)

    def test_participant(self, converter):
        q = converter.convert("Invoice", "participant=prac-1")
        assert "_search.participantIds" in str(q)

    def test_patient(self, converter):
        q = converter.convert("Invoice", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_recipient(self, converter):
        q = converter.convert("Invoice", "recipient=rp-1")
        assert "_search.recipientId" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Invoice", "subject=pat-1")
        assert "_search.subjectId" in str(q)


class TestInvoiceTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Invoice", "identifier=INV-001")
        assert "INV-001" in str(q)

    def test_participant_role(self, converter):
        q = converter.convert("Invoice", "participant-role=author")
        assert "participantRole_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("Invoice", "status=issued")
        assert "issued" in str(q)

    def test_type(self, converter):
        q = converter.convert("Invoice", "type=invoice")
        assert "type_codes" in str(q)

    def test_id(self, converter):
        q = converter.convert("Invoice", "_id=inv-rich")
        assert "inv-rich" in str(q)


class TestInvoiceQuantityParameters:
    def test_totalgross(self, converter):
        q = converter.convert("Invoice", "totalgross=500")
        assert "totalGross" in str(q)

    def test_totalnet(self, converter):
        q = converter.convert("Invoice", "totalnet=450")
        assert "totalNet" in str(q)


class TestInvoiceDateParameters:
    def test_date(self, converter):
        q = converter.convert("Invoice", "date=ge2024-07-01")
        assert "date" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Invoice", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestInvoiceDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_invoice):
        doc = denormalizer.denormalize(copy.deepcopy(rich_invoice))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["accountId"] == "acct-1"
        assert search["issuerId"] == "org-issuer"
        assert search["recipientId"] == "rp-1"
        assert "prac-1" in search["participantIds"]
        assert "author" in search["participantRole_codes"]
        assert "invoice" in search["type_codes"]
        assert "Patient" in doc["_compartments"]
        assert "prac-1" in doc["_compartments"]["Practitioner"]
        assert "rp-1" in doc["_compartments"]["RelatedPerson"]

    def test_minimal_denormalization(self, denormalizer, minimal_invoice):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_invoice))
        assert doc["_search"]["patientId"] == "pat-min"
