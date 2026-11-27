"""Comprehensive integration tests for Basic search parameters."""
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
def rich_basic() -> Dict[str, Any]:
    return {
        "resourceType": "Basic",
        "id": "basic-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "code": {"coding": [{"code": "referral"}]},
        "subject": {"reference": "Patient/pat-1"},
        "author": {"reference": "Practitioner/prac-1"},
        "created": "2024-07-15",
        "identifier": [{"value": "BASIC-001"}],
    }


class TestBasicReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Basic", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_author(self, converter):
        q = converter.convert("Basic", "author=prac-1")
        assert "_search.authorId" in str(q)


class TestBasicTokenParameters:
    def test_code(self, converter):
        q = converter.convert("Basic", "code=referral")
        assert "referral" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Basic", "identifier=BASIC-001")
        assert "BASIC-001" in str(q)


class TestBasicDateParameters:
    def test_created(self, converter):
        q = converter.convert("Basic", "created=ge2024-07-01")
        assert "created" in str(q)


class TestBasicCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "Basic")
        assert q == {"_compartments.Patient": "pat-1"}


class TestBasicDenormalization:
    def test_rich(self, denormalizer, rich_basic):
        out = denormalizer.denormalize(copy.deepcopy(rich_basic))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["authorId"] == "prac-1"
        assert "referral" in s["code_codes"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
