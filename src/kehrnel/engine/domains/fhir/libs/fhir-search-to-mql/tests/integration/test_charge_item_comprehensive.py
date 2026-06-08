"""
Comprehensive integration tests for ALL ChargeItem search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "ChargeItem")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ChargeItem.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 20 search parameters in ``configs/ChargeItem.yaml``.

Compartments (precomputed): Patient, Practitioner, Device, Encounter.
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
def rich_charge_item() -> Dict[str, Any]:
    return {
        "resourceType": "ChargeItem",
        "id": "ci-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "billable",
        "code": {
            "coding": [
                {
                    "system": "http://www.ama-assn.org/go/cpt",
                    "code": "99213",
                }
            ]
        },
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "enterer": {"reference": "Practitioner/prac-1"},
        "occurrenceDateTime": "2024-07-15T10:00:00Z",
        "enteredDate": "2024-07-14T09:00:00Z",
        "identifier": [
            {"system": "http://hospital.org/ci", "value": "CI-001"}
        ],
        "account": [{"reference": "Account/acct-1"}],
        "performingOrganization": {"reference": "Organization/org-perf"},
        "requestingOrganization": {"reference": "Organization/org-req"},
        "performer": [
            {
                "actor": {"reference": "Practitioner/prac-perf"},
                "function": {
                    "coding": [{"code": "performer"}],
                },
            }
        ],
        "service": [{"reference": {"reference": "Procedure/proc-1"}}],
        "quantity": {"value": 1, "unit": "each"},
        "totalPriceComponent": {
            "factor": 1.5,
            "amount": {"value": 120.0, "currency": "USD"},
        },
    }


@pytest.fixture
def minimal_charge_item() -> Dict[str, Any]:
    return {
        "resourceType": "ChargeItem",
        "id": "ci-min",
        "status": "billable",
        "code": {"coding": [{"code": "99213"}]},
        "subject": {"reference": "Patient/pat-min"},
    }


class TestChargeItemReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("ChargeItem", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("ChargeItem", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("ChargeItem", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_enterer(self, converter):
        q = converter.convert("ChargeItem", "enterer=prac-1")
        assert "_search.entererId" in str(q)

    def test_performer_actor(self, converter):
        q = converter.convert("ChargeItem", "performer-actor=prac-perf")
        assert "_search.performerActorIds" in str(q)

    def test_account(self, converter):
        q = converter.convert("ChargeItem", "account=acct-1")
        assert "_search.accountIds" in str(q)

    def test_service(self, converter):
        q = converter.convert("ChargeItem", "service=proc-1")
        assert "_search.serviceReferenceIds" in str(q)

    def test_performing_organization(self, converter):
        q = converter.convert("ChargeItem", "performing-organization=org-perf")
        assert "_search.performingOrganizationId" in str(q)

    def test_requesting_organization(self, converter):
        q = converter.convert("ChargeItem", "requesting-organization=org-req")
        assert "_search.requestingOrganizationId" in str(q)


class TestChargeItemTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("ChargeItem", "identifier=CI-001")
        assert "CI-001" in str(q)

    def test_code(self, converter):
        q = converter.convert("ChargeItem", "code=99213")
        assert "code_codes" in str(q)

    def test_performer_function(self, converter):
        q = converter.convert("ChargeItem", "performer-function=performer")
        assert "performerFunction_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("ChargeItem", "status=billable")
        assert "billable" in str(q)

    def test_id(self, converter):
        q = converter.convert("ChargeItem", "_id=ci-rich")
        assert "ci-rich" in str(q)


class TestChargeItemNumberAndQuantityParameters:
    def test_factor_override(self, converter):
        q = converter.convert("ChargeItem", "factor-override=1.5")
        assert "factorOverride_values" in str(q)

    def test_price_override(self, converter):
        q = converter.convert("ChargeItem", "price-override=120")
        assert "totalPriceComponent.amount" in str(q)

    def test_quantity(self, converter):
        q = converter.convert("ChargeItem", "quantity=1")
        assert "quantity" in str(q)


class TestChargeItemDateParameters:
    def test_occurrence(self, converter):
        q = converter.convert("ChargeItem", "occurrence=ge2024-07-01")
        assert "occurrenceDateTime" in str(q) or "occurrencePeriod" in str(q)

    def test_entered_date(self, converter):
        q = converter.convert("ChargeItem", "entered-date=ge2024-07-01")
        assert "enteredDate" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("ChargeItem", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestChargeItemDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_charge_item):
        doc = denormalizer.denormalize(copy.deepcopy(rich_charge_item))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["encounterId"] == "enc-1"
        assert search["entererId"] == "prac-1"
        assert "acct-1" in search["accountIds"]
        assert "proc-1" in search["serviceReferenceIds"]
        assert "prac-perf" in search["performerActorIds"]
        assert "performer" in search["performerFunction_codes"]
        assert "99213" in search["code_codes"]
        assert 1.5 in search["factorOverride_values"]
        assert "Patient" in doc["_compartments"]
        assert "prac-1" in doc["_compartments"]["Practitioner"]

    def test_minimal_denormalization(self, denormalizer, minimal_charge_item):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_charge_item))
        assert doc["_search"]["patientId"] == "pat-min"
