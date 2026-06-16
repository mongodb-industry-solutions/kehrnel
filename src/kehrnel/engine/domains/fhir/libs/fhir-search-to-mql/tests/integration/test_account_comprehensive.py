"""
Comprehensive integration tests for ALL Account search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Account")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Account.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 12 search parameters in ``configs/Account.yaml``.

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
def rich_account() -> Dict[str, Any]:
    return {
        "resourceType": "Account",
        "id": "acct-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "name": "Inpatient Account",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    "code": "PBILLACCT",
                }
            ]
        },
        "subject": [{"reference": "Patient/pat-1"}],
        "owner": {"reference": "Organization/org-1"},
        "servicePeriod": {
            "start": "2024-07-01",
            "end": "2024-12-31",
        },
        "identifier": [
            {"system": "http://hospital.org/acct", "value": "ACCT-001"}
        ],
        "guarantor": [{"party": {"reference": "RelatedPerson/rp-1"}}],
        "relatedAccount": [{"account": {"reference": "Account/acct-parent"}}],
    }


@pytest.fixture
def minimal_account() -> Dict[str, Any]:
    return {
        "resourceType": "Account",
        "id": "acct-min",
        "status": "active",
    }


class TestAccountReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Account", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Account", "subject=pat-1")
        assert "_search.subjectIds" in str(q)

    def test_owner(self, converter):
        q = converter.convert("Account", "owner=org-1")
        assert "_search.ownerId" in str(q)

    def test_guarantor(self, converter):
        q = converter.convert("Account", "guarantor=rp-1")
        assert "_search.guarantorIds" in str(q)

    def test_relatedaccount(self, converter):
        q = converter.convert("Account", "relatedaccount=acct-parent")
        assert "_search.relatedAccountIds" in str(q)


class TestAccountTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Account", "identifier=ACCT-001")
        assert "ACCT-001" in str(q)

    def test_type(self, converter):
        q = converter.convert("Account", "type=PBILLACCT")
        assert "type_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("Account", "status=active")
        assert "active" in str(q)

    def test_id(self, converter):
        q = converter.convert("Account", "_id=acct-rich")
        assert "acct-rich" in str(q)


class TestAccountStringParameters:
    def test_name(self, converter):
        q = converter.convert("Account", "name=Inpatient")
        assert "name_lower" in str(q)

    def test_name_exact(self, converter):
        q = converter.convert("Account", "name:exact=Inpatient Account")
        assert "_search.name" in str(q)


class TestAccountDateParameters:
    def test_period(self, converter):
        q = converter.convert("Account", "period=ge2024-07-01")
        assert "servicePeriod" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Account", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestAccountDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_account):
        doc = denormalizer.denormalize(copy.deepcopy(rich_account))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert "pat-1" in search["subjectIds"]
        assert search["ownerId"] == "org-1"
        assert "rp-1" in search["guarantorIds"]
        assert "acct-parent" in search["relatedAccountIds"]
        assert "PBILLACCT" in search["type_codes"]
        assert "ACCT-001" in search["identifier_values"]
        assert "inpatient account" in search["name_lower"]
        assert "Patient" in doc["_compartments"]
        assert "pat-1" in doc["_compartments"]["Patient"]

    def test_minimal_denormalization(self, denormalizer, minimal_account):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_account))
        assert doc.get("_search", {}) == {}
