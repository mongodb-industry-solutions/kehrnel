"""
Comprehensive integration tests for ALL HealthcareService search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "HealthcareService")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/HealthcareService.yaml

Exercises 17 search parameters in ``configs/HealthcareService.yaml``.

No R5 compartments — HealthcareService is not in compartment definitions.
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
def rich_healthcare_service() -> Dict[str, Any]:
    return {
        "resourceType": "HealthcareService",
        "id": "hs-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "active": True,
        "name": "Cardiology Consultation",
        "providedBy": {"reference": "Organization/org-1"},
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/service-category",
                        "code": "17",
                    }
                ]
            }
        ],
        "type": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "11429006",
                    }
                ]
            }
        ],
        "specialty": [
            {"coding": [{"system": "http://snomed.info/sct", "code": "394579002"}]}
        ],
        "location": [{"reference": "Location/loc-1"}],
        "coverageArea": [{"reference": "Location/loc-coverage"}],
        "endpoint": [{"reference": "Endpoint/ep-1"}],
        "offeredIn": [{"reference": "HealthcareService/hs-parent"}],
        "identifier": [{"system": "http://hospital.org/hs", "value": "HS-001"}],
        "characteristic": [
            {"coding": [{"code": "telemedicine"}]}
        ],
        "communication": [
            {"coding": [{"system": "urn:ietf:bcp:47", "code": "en"}]}
        ],
        "program": [{"coding": [{"code": "cancer"}]}],
        "eligibility": [{"code": {"coding": [{"code": "retired"}]}}],
    }


@pytest.fixture
def minimal_healthcare_service() -> Dict[str, Any]:
    return {
        "resourceType": "HealthcareService",
        "id": "hs-min",
        "name": "Walk-in Clinic",
    }


class TestHealthcareServiceStringParameters:
    def test_name(self, converter):
        q = converter.convert("HealthcareService", "name=cardio")
        assert "name_lower" in str(q)

    def test_name_exact(self, converter):
        q = converter.convert("HealthcareService", "name:exact=Cardiology Consultation")
        assert "_search.name" in str(q)


class TestHealthcareServiceReferenceParameters:
    def test_organization(self, converter):
        q = converter.convert("HealthcareService", "organization=org-1")
        assert "_search.organizationId" in str(q)

    def test_location(self, converter):
        q = converter.convert("HealthcareService", "location=loc-1")
        assert "_search.locationIds" in str(q)

    def test_coverage_area(self, converter):
        q = converter.convert("HealthcareService", "coverage-area=loc-coverage")
        assert "_search.coverageAreaIds" in str(q)

    def test_endpoint(self, converter):
        q = converter.convert("HealthcareService", "endpoint=ep-1")
        assert "_search.endpointIds" in str(q)

    def test_offered_in(self, converter):
        q = converter.convert("HealthcareService", "offered-in=hs-parent")
        assert "_search.offeredInIds" in str(q)


class TestHealthcareServiceTokenParameters:
    def test_active(self, converter):
        assert converter.convert("HealthcareService", "active=true") == {"active": True}

    def test_service_category(self, converter):
        q = converter.convert("HealthcareService", "service-category=17")
        assert "17" in str(q)

    def test_service_type(self, converter):
        q = converter.convert("HealthcareService", "service-type=11429006")
        assert "11429006" in str(q)

    def test_specialty(self, converter):
        q = converter.convert("HealthcareService", "specialty=394579002")
        assert "394579002" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("HealthcareService", "identifier=HS-001")
        assert "HS-001" in str(q)

    def test_eligibility(self, converter):
        q = converter.convert("HealthcareService", "eligibility=retired")
        assert "retired" in str(q)

    def test_characteristic(self, converter):
        q = converter.convert("HealthcareService", "characteristic=telemedicine")
        assert "telemedicine" in str(q)

    def test_communication(self, converter):
        q = converter.convert("HealthcareService", "communication=en")
        assert "en" in str(q)

    def test_program(self, converter):
        q = converter.convert("HealthcareService", "program=cancer")
        assert "cancer" in str(q)


class TestHealthcareServiceCommonParameters:
    def test_id(self, converter):
        q = converter.convert("HealthcareService", "_id=hs-rich")
        assert "hs-rich" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("HealthcareService", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestHealthcareServiceDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_healthcare_service):
        out = denormalizer.denormalize(minimal_healthcare_service)
        s = out.get("_search", {})
        assert s["name_lower"] == "walk-in clinic"

    def test_rich_fields(self, denormalizer, rich_healthcare_service):
        out = denormalizer.denormalize(rich_healthcare_service)
        s = out["_search"]
        assert s["name"] == "Cardiology Consultation"
        assert s["organizationId"] == "org-1"
        assert "17" in s["category_codes"]
        assert "11429006" in s["type_codes"]
        assert "394579002" in s["specialty_codes"]
        assert "loc-1" in s["locationIds"]
        assert "loc-coverage" in s["coverageAreaIds"]
        assert "ep-1" in s["endpointIds"]
        assert "hs-parent" in s["offeredInIds"]
        assert "HS-001" in s["identifier_values"]
        assert "retired" in s["eligibility_codes"]
        assert "telemedicine" in s["characteristic_codes"]
        assert "en" in s["communication_codes"]
        assert "cancer" in s["program_codes"]

    def test_input_not_mutated(self, denormalizer, rich_healthcare_service):
        original = copy.deepcopy(rich_healthcare_service)
        denormalizer.denormalize(rich_healthcare_service)
        assert rich_healthcare_service == original
