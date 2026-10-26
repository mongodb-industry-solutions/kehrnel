"""Comprehensive integration tests for VisionPrescription search parameters."""
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
def rich_vision_prescription() -> Dict[str, Any]:
    return {
        "resourceType": "VisionPrescription",
        "id": "vp-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "dateWritten": "2024-07-15T10:00:00Z",
        "patient": {"reference": "Patient/pat-1"},
        "prescriber": {"reference": "PractitionerRole/pr-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "identifier": [{"value": "VP-001"}],
        "lensSpecification": [{"eye": "right", "sphere": -1.0}],
    }


class TestVisionPrescriptionReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("VisionPrescription", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_prescriber(self, converter):
        q = converter.convert("VisionPrescription", "prescriber=pr-1")
        assert "_search.prescriberId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("VisionPrescription", "encounter=enc-1")
        assert "_search.encounterId" in str(q)


class TestVisionPrescriptionTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("VisionPrescription", "identifier=VP-001")
        assert "VP-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("VisionPrescription", "status=active")
        assert q == {"status": "active"}


class TestVisionPrescriptionDateParameters:
    def test_datewritten(self, converter):
        q = converter.convert("VisionPrescription", "datewritten=ge2024-07-01")
        assert "dateWritten" in str(q)


class TestVisionPrescriptionCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "VisionPrescription")
        assert q == {"_compartments.Patient": "pat-1"}


class TestVisionPrescriptionDenormalization:
    def test_rich(self, denormalizer, rich_vision_prescription):
        out = denormalizer.denormalize(copy.deepcopy(rich_vision_prescription))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["prescriberId"] == "pr-1"
        assert s["encounterId"] == "enc-1"
        assert "VP-001" in s["identifier_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
