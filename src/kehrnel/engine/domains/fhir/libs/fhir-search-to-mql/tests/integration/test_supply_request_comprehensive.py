"""
Comprehensive integration tests for ALL SupplyRequest search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "SupplyRequest")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/SupplyRequest.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 10 search parameters in ``configs/SupplyRequest.yaml``.

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
def rich_supply_request() -> Dict[str, Any]:
    return {
        "resourceType": "SupplyRequest",
        "id": "sr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "category": {"coding": [{"code": "central"}]},
        "authoredOn": "2024-07-15T10:00:00Z",
        "deliverFor": {"reference": "Patient/pat-1"},
        "deliverTo": {"reference": "Location/loc-1"},
        "requester": {"reference": "Practitioner/prac-1"},
        "supplier": [{"reference": "Organization/org-1"}],
        "item": {"concept": {"coding": [{"code": "gloves"}]}},
        "quantity": {"value": 100},
        "identifier": [
            {"system": "http://hospital.org/sr", "value": "SR-001"}
        ],
    }


@pytest.fixture
def minimal_supply_request() -> Dict[str, Any]:
    return {
        "resourceType": "SupplyRequest",
        "id": "sr-min",
        "status": "draft",
        "deliverFor": {"reference": "Patient/pat-min"},
        "deliverTo": {"reference": "Patient/pat-min"},
        "item": {"concept": {"coding": [{"code": "mask"}]}},
        "quantity": {"value": 1},
    }


class TestSupplyRequestTokenParameters:
    def test_category(self, converter):
        q = converter.convert("SupplyRequest", "category=central")
        assert "category_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("SupplyRequest", "identifier=SR-001")
        assert "SR-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("SupplyRequest", "status=active")
        assert q == {"status": "active"}


class TestSupplyRequestReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("SupplyRequest", "patient=pat-1")
        assert "deliverForId" in str(q)

    def test_subject(self, converter):
        q = converter.convert("SupplyRequest", "subject=Location/loc-1")
        assert "loc-1" in str(q)

    def test_requester(self, converter):
        q = converter.convert("SupplyRequest", "requester=prac-1")
        assert "prac-1" in str(q)

    def test_supplier(self, converter):
        q = converter.convert("SupplyRequest", "supplier=org-1")
        assert "org-1" in str(q)


class TestSupplyRequestDateParameters:
    def test_date_ge(self, converter):
        q = converter.convert("SupplyRequest", "date=ge2024-01-01")
        assert "$gte" in str(q)
        assert "authoredOn" in str(q)


class TestSupplyRequestDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_supply_request):
        doc = denormalizer.denormalize(copy.deepcopy(rich_supply_request))
        search = doc["_search"]
        assert search["deliverForId"] == "pat-1"
        assert search["deliverToId"] == "loc-1"
        assert search["requesterId"] == "prac-1"
        assert "org-1" in search["supplierIds"]
        assert "central" in search["category_codes"]
        assert "prac-1" in doc["_compartments"]["Practitioner"]

    def test_minimal_sparse_output(self, denormalizer, minimal_supply_request):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_supply_request))
        assert "identifier_values" not in doc.get("_search", {})


class TestSupplyRequestCompartmentRouting:
    def test_patient_compartment_on_deliver_to(self, denormalizer, minimal_supply_request):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_supply_request))
        assert "pat-min" in doc["_compartments"]["Patient"]
