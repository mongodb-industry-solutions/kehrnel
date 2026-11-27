"""
Comprehensive integration tests for ALL DeviceDispense search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "DeviceDispense")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/DeviceDispense.yaml

Exercises 7 search parameters in ``configs/DeviceDispense.yaml``.
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
def rich_device_dispense() -> Dict[str, Any]:
    return {
        "resourceType": "DeviceDispense",
        "id": "dd-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "subject": {"reference": "Patient/pat-1"},
        "device": {"concept": {"coding": [{"code": "insulin-pump"}]}},
        "identifier": [
            {"system": "http://hospital.org/dd", "value": "DD-001"}
        ],
    }


@pytest.fixture
def minimal_device_dispense() -> Dict[str, Any]:
    return {
        "resourceType": "DeviceDispense",
        "id": "dd-min",
        "status": "in-progress",
        "subject": {"reference": "Patient/pat-min"},
        "device": {"concept": {"coding": [{"code": "cpap"}]}},
    }


class TestDeviceDispenseTokenParameters:
    def test_code(self, converter):
        q = converter.convert("DeviceDispense", "code=insulin-pump")
        assert "deviceConcept_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("DeviceDispense", "identifier=DD-001")
        assert "DD-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("DeviceDispense", "status=completed")
        assert q == {"status": "completed"}


class TestDeviceDispenseReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("DeviceDispense", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("DeviceDispense", "subject=Patient/pat-1")
        assert "pat-1" in str(q)


class TestDeviceDispenseDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_device_dispense):
        doc = denormalizer.denormalize(copy.deepcopy(rich_device_dispense))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["subjectId"] == "pat-1"
        assert "insulin-pump" in search["deviceConcept_codes"]
        assert "DD-001" in search["identifier_values"]

    def test_minimal_sparse_output(self, denormalizer, minimal_device_dispense):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_device_dispense))
        assert "identifier_values" not in doc.get("_search", {})
