"""
Comprehensive integration tests for ALL BodyStructure search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "BodyStructure")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/BodyStructure.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 7 search parameters in ``configs/BodyStructure.yaml``.

Compartments (precomputed): Patient.
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
def rich_body_structure() -> Dict[str, Any]:
    return {
        "resourceType": "BodyStructure",
        "id": "bs-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "patient": {"reference": "Patient/pat-1"},
        "morphology": {"coding": [{"code": "morph-1"}]},
        "includedStructure": [
            {"structure": {"coding": [{"code": "arm"}]}}
        ],
        "excludedStructure": [
            {"structure": {"coding": [{"code": "finger"}]}}
        ],
        "identifier": [
            {"system": "http://hospital.org/bs", "value": "BS-001"}
        ],
    }


@pytest.fixture
def minimal_body_structure() -> Dict[str, Any]:
    return {
        "resourceType": "BodyStructure",
        "id": "bs-min",
        "patient": {"reference": "Patient/pat-min"},
        "includedStructure": [
            {"structure": {"coding": [{"code": "torso"}]}}
        ],
    }


class TestBodyStructureReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("BodyStructure", "patient=pat-1")
        assert "pat-1" in str(q)


class TestBodyStructureTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("BodyStructure", "identifier=BS-001")
        assert "BS-001" in str(q)

    def test_morphology(self, converter):
        q = converter.convert("BodyStructure", "morphology=morph-1")
        assert "morphology_codes" in str(q)

    def test_included_structure(self, converter):
        q = converter.convert("BodyStructure", "included_structure=arm")
        assert "includedStructure_codes" in str(q)

    def test_excluded_structure(self, converter):
        q = converter.convert("BodyStructure", "excluded_structure=finger")
        assert "excludedStructure_codes" in str(q)

    def test_id(self, converter):
        q = converter.convert("BodyStructure", "_id=bs-rich")
        assert "bs-rich" in str(q)


class TestBodyStructureDateParameters:
    def test_last_updated(self, converter):
        q = converter.convert("BodyStructure", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestBodyStructureDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_body_structure):
        doc = denormalizer.denormalize(copy.deepcopy(rich_body_structure))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert "arm" in search["includedStructure_codes"]
        assert "finger" in search["excludedStructure_codes"]
        assert "morph-1" in search["morphology_codes"]
        assert "BS-001" in search["identifier_values"]
        assert "pat-1" in doc["_compartments"]["Patient"]

    def test_minimal_denormalization(self, denormalizer, minimal_body_structure):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_body_structure))
        assert doc["_search"]["patientId"] == "pat-min"
