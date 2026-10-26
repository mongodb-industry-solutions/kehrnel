"""Comprehensive integration tests for Endpoint search parameters."""
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
def rich_endpoint() -> Dict[str, Any]:
    return {
        "resourceType": "Endpoint",
        "id": "ep-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "name": "FHIR REST Endpoint",
        "connectionType": [{"coding": [{"code": "hl7-fhir-rest"}]}],
        "managingOrganization": {"reference": "Organization/org-1"},
        "identifier": [{"value": "EP-001"}],
        "payload": [{"type": [{"coding": [{"code": "application/fhir+json"}]}]}],
    }


class TestEndpointStringParameters:
    def test_name_default(self, converter):
        q = converter.convert("Endpoint", "name=fhir")
        assert "name_lower" in str(q)

    def test_name_exact(self, converter):
        q = converter.convert("Endpoint", "name:exact=FHIR REST Endpoint")
        assert "_search.name" in str(q)


class TestEndpointTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Endpoint", "status=active") == {"status": "active"}

    def test_connection_type(self, converter):
        q = converter.convert("Endpoint", "connection-type=hl7-fhir-rest")
        assert "hl7-fhir-rest" in str(q)


class TestEndpointReferenceParameters:
    def test_organization(self, converter):
        q = converter.convert("Endpoint", "organization=org-1")
        assert "_search.managingOrganizationId" in str(q)


class TestEndpointDenormalization:
    def test_rich(self, denormalizer, rich_endpoint):
        out = denormalizer.denormalize(copy.deepcopy(rich_endpoint))
        s = out["_search"]
        assert s["managingOrganizationId"] == "org-1"
        assert "hl7-fhir-rest" in s["connectionType_codes"]
        assert s["name_lower"] == "fhir rest endpoint"
