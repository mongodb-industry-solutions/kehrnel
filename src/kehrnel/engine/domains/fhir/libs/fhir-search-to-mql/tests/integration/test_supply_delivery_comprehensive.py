"""
Comprehensive integration tests for ALL SupplyDelivery search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "SupplyDelivery")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/SupplyDelivery.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 7 search parameters in ``configs/SupplyDelivery.yaml``.

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
def rich_supply_delivery() -> Dict[str, Any]:
    return {
        "resourceType": "SupplyDelivery",
        "id": "sd-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "patient": {"reference": "Patient/pat-1"},
        "supplier": {"reference": "Practitioner/prac-1"},
        "receiver": [{"reference": "PractitionerRole/pr-1"}],
        "identifier": [
            {"system": "http://hospital.org/sd", "value": "SD-001"}
        ],
    }


@pytest.fixture
def minimal_supply_delivery() -> Dict[str, Any]:
    return {
        "resourceType": "SupplyDelivery",
        "id": "sd-min",
        "status": "in-progress",
        "patient": {"reference": "Patient/pat-min"},
    }


class TestSupplyDeliveryTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("SupplyDelivery", "identifier=SD-001")
        assert "SD-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("SupplyDelivery", "status=completed")
        assert q == {"status": "completed"}


class TestSupplyDeliveryReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("SupplyDelivery", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_supplier(self, converter):
        q = converter.convert("SupplyDelivery", "supplier=prac-1")
        assert "prac-1" in str(q)

    def test_receiver(self, converter):
        q = converter.convert("SupplyDelivery", "receiver=pr-1")
        assert "pr-1" in str(q)


class TestSupplyDeliveryDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_supply_delivery):
        doc = denormalizer.denormalize(copy.deepcopy(rich_supply_delivery))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["supplierId"] == "prac-1"
        assert "pr-1" in search["receiverIds"]
        assert "SD-001" in search["identifier_values"]
        assert "pat-1" in doc["_compartments"]["Patient"]
        assert "prac-1" in doc["_compartments"]["Practitioner"]

    def test_minimal_sparse_output(self, denormalizer, minimal_supply_delivery):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_supply_delivery))
        assert "supplierId" not in doc.get("_search", {})


class TestSupplyDeliveryCompartmentRouting:
    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "SupplyDelivery")
        assert q == {"_compartments.Patient": "pat-1"}
