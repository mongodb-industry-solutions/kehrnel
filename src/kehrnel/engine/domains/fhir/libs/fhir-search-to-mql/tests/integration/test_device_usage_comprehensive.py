"""
Comprehensive integration tests for ALL DeviceUsage search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "DeviceUsage")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/DeviceUsage.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 6 search parameters in ``configs/DeviceUsage.yaml``.

Compartments (precomputed): Patient, Practitioner, Device.
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
def rich_device_usage() -> Dict[str, Any]:
    return {
        "resourceType": "DeviceUsage",
        "id": "du-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "patient": {"reference": "Patient/pat-1"},
        "device": {
            "concept": {"coding": [{"code": "glucose-monitor"}]},
            "reference": {"reference": "Device/dev-1"},
        },
        "identifier": [
            {"system": "http://hospital.org/du", "value": "DU-001"}
        ],
    }


@pytest.fixture
def minimal_device_usage() -> Dict[str, Any]:
    return {
        "resourceType": "DeviceUsage",
        "id": "du-min",
        "status": "active",
        "patient": {"reference": "Patient/pat-min"},
        "device": {"concept": {"coding": [{"code": "walker"}]}},
    }


class TestDeviceUsageTokenParameters:
    def test_device(self, converter):
        q = converter.convert("DeviceUsage", "device=glucose-monitor")
        assert "deviceConcept_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("DeviceUsage", "identifier=DU-001")
        assert "DU-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("DeviceUsage", "status=active")
        assert q == {"status": "active"}

    def test_id(self, converter):
        q = converter.convert("DeviceUsage", "_id=du-rich")
        assert "du-rich" in str(q)


class TestDeviceUsageReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("DeviceUsage", "patient=pat-1")
        assert "pat-1" in str(q)


class TestDeviceUsageDateParameters:
    def test_last_updated(self, converter):
        q = converter.convert("DeviceUsage", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestDeviceUsageDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_device_usage):
        doc = denormalizer.denormalize(copy.deepcopy(rich_device_usage))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert "glucose-monitor" in search["deviceConcept_codes"]
        assert "dev-1" in search["deviceIds"]
        assert "DU-001" in search["identifier_values"]
        assert "pat-1" in doc["_compartments"]["Patient"]
        assert "dev-1" in doc["_compartments"]["Device"]

    def test_minimal_sparse_output(self, denormalizer, minimal_device_usage):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_device_usage))
        assert "identifier_values" not in doc.get("_search", {})


class TestDeviceUsageCompartmentRouting:
    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "DeviceUsage")
        assert q == {"_compartments.Patient": "pat-1"}
