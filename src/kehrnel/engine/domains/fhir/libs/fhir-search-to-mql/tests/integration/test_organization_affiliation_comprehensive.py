"""Comprehensive integration tests for OrganizationAffiliation search parameters."""
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
def rich_oa() -> Dict[str, Any]:
    return {
        "resourceType": "OrganizationAffiliation",
        "id": "oa-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "active": True,
        "organization": {"reference": "Organization/org-primary"},
        "participatingOrganization": {"reference": "Organization/org-part"},
        "code": [{"coding": [{"code": "provider"}]}],
        "specialty": [{"coding": [{"code": "cardio"}]}],
        "period": {"start": "2024-01-01", "end": "2025-12-31"},
        "identifier": [{"system": "http://hospital.org/oa", "value": "OA-001"}],
        "contact": [{"telecom": [{"system": "email", "value": "affil@example.org"}]}],
        "endpoint": [{"reference": "Endpoint/ep-1"}],
        "location": [{"reference": "Location/loc-1"}],
        "network": [{"reference": "Organization/net-1"}],
        "healthcareService": [{"reference": "HealthcareService/hs-1"}],
    }


@pytest.fixture
def minimal_oa() -> Dict[str, Any]:
    return {"resourceType": "OrganizationAffiliation", "id": "oa-min"}


class TestOrganizationAffiliationTokenParameters:
    def test_active_bool(self, converter):
        assert converter.convert("OrganizationAffiliation", "active=true") == {"active": True}

    def test_role(self, converter):
        q = converter.convert("OrganizationAffiliation", "role=provider")
        assert "code_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("OrganizationAffiliation", "identifier=OA-001")
        assert "OA-001" in str(q)


class TestOrganizationAffiliationReferenceParameters:
    def test_primary_organization(self, converter):
        q = converter.convert("OrganizationAffiliation", "primary-organization=org-primary")
        assert "_search.organizationId" in str(q)

    def test_participating_organization(self, converter):
        q = converter.convert("OrganizationAffiliation", "participating-organization=org-part")
        assert "org-part" in str(q)

    def test_service(self, converter):
        q = converter.convert("OrganizationAffiliation", "service=hs-1")
        assert "_search.healthcareServiceIds" in str(q)


class TestOrganizationAffiliationDateParameters:
    def test_date(self, converter):
        q = converter.convert("OrganizationAffiliation", "date=ge2024-01-01")
        assert "period" in str(q)


class TestOrganizationAffiliationDenormalization:
    def test_rich(self, denormalizer, rich_oa):
        out = denormalizer.denormalize(copy.deepcopy(rich_oa))
        s = out["_search"]
        assert s["organizationId"] == "org-primary"
        assert s["participatingOrganizationId"] == "org-part"
        assert "provider" in s["code_codes"]
        assert "affil@example.org" in s["email"]

    def test_minimal_sparse(self, denormalizer, minimal_oa):
        out = denormalizer.denormalize(copy.deepcopy(minimal_oa))
        assert out.get("_search", {}) == {}
