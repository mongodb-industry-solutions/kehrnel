"""
Comprehensive integration tests for ALL Flag search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Flag")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Flag.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 10 search parameters in ``configs/Flag.yaml``.

Compartments (precomputed): Patient, Practitioner, Encounter, Device.
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
def rich_flag() -> Dict[str, Any]:
    return {
        "resourceType": "Flag",
        "id": "flag-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "code": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "304379003",
                }
            ]
        },
        "subject": {"reference": "Patient/pat-1"},
        "author": {"reference": "Practitioner/prac-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "period": {
            "start": "2024-07-01T00:00:00Z",
            "end": "2024-12-31T23:59:59Z",
        },
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/flag-category",
                        "code": "safety",
                    }
                ]
            }
        ],
        "identifier": [{"system": "http://hospital.org/flag", "value": "FLAG-001"}],
    }


@pytest.fixture
def minimal_flag() -> Dict[str, Any]:
    return {
        "resourceType": "Flag",
        "id": "flag-min",
        "status": "active",
        "code": {"coding": [{"code": "304379003"}]},
        "subject": {"reference": "Patient/pat-min"},
    }


class TestFlagReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Flag", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Flag", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_author(self, converter):
        q = converter.convert("Flag", "author=prac-1")
        assert "_search.authorId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("Flag", "encounter=enc-1")
        assert "_search.encounterId" in str(q)


class TestFlagTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Flag", "identifier=FLAG-001")
        assert "FLAG-001" in str(q)

    def test_category(self, converter):
        q = converter.convert("Flag", "category=safety")
        assert "safety" in str(q)

    def test_status(self, converter):
        q = converter.convert("Flag", "status=active")
        assert "active" in str(q)


class TestFlagDateParameters:
    def test_date(self, converter):
        q = converter.convert("Flag", "date=ge2024-07-01")
        assert "period" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Flag", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestFlagCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Flag", "_id=flag-rich")
        assert "flag-rich" in str(q)


class TestFlagDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_flag):
        out = denormalizer.denormalize(minimal_flag)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_flag):
        out = denormalizer.denormalize(rich_flag)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["subjectId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert s["authorId"] == "prac-1"
        assert "safety" in s["category_codes"]
        assert "FLAG-001" in s["identifier_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert "prac-1" in out["_compartments"]["Practitioner"]
        assert out["_compartments"]["Encounter"] == ["enc-1"]

    def test_input_not_mutated(self, denormalizer, rich_flag):
        original = copy.deepcopy(rich_flag)
        denormalizer.denormalize(rich_flag)
        assert rich_flag == original
