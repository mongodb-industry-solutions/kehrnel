"""Comprehensive integration tests for InsurancePlan search parameters."""
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
def rich_insurance_plan() -> Dict[str, Any]:
    return {
        "resourceType": "InsurancePlan",
        "id": "ip-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "name": "Gold Plan",
        "alias": ["GP-2024"],
        "type": [{"coding": [{"code": "medical"}]}],
        "ownedBy": {"reference": "Organization/org-owner"},
        "administeredBy": {"reference": "Organization/org-admin"},
        "identifier": [{"value": "IP-001"}],
        "contact": [
            {
                "address": {
                    "city": "Boston",
                    "state": "MA",
                    "postalCode": "02101",
                    "country": "US",
                    "use": "work",
                }
            }
        ],
        "endpoint": [{"reference": "Endpoint/ep-1"}],
    }


class TestInsurancePlanStringParameters:
    def test_name(self, converter):
        q = converter.convert("InsurancePlan", "name=gold")
        assert "name_lower" in str(q)

    def test_address_city(self, converter):
        q = converter.convert("InsurancePlan", "address-city=boston")
        assert "addressCity_lower" in str(q)


class TestInsurancePlanTokenParameters:
    def test_status(self, converter):
        assert converter.convert("InsurancePlan", "status=active") == {"status": "active"}

    def test_type(self, converter):
        q = converter.convert("InsurancePlan", "type=medical")
        assert "type_codes" in str(q)


class TestInsurancePlanReferenceParameters:
    def test_owned_by(self, converter):
        q = converter.convert("InsurancePlan", "owned-by=org-owner")
        assert "_search.ownedById" in str(q)


class TestInsurancePlanDenormalization:
    def test_rich(self, denormalizer, rich_insurance_plan):
        out = denormalizer.denormalize(copy.deepcopy(rich_insurance_plan))
        s = out["_search"]
        assert s["ownedById"] == "org-owner"
        assert "Boston" in s["addressCity"]
        assert "gold" in s["name_lower"]
