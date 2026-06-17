"""
Comprehensive integration tests for ALL DetectedIssue search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "DetectedIssue")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/DetectedIssue.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 11 search parameters in ``configs/DetectedIssue.yaml``.

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
def rich_detected_issue() -> Dict[str, Any]:
    return {
        "resourceType": "DetectedIssue",
        "id": "di-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "final",
        "code": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    "code": "DRG",
                }
            ]
        },
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                        "code": "drug-drug",
                    }
                ]
            }
        ],
        "subject": {"reference": "Patient/pat-1"},
        "author": {"reference": "Practitioner/prac-1"},
        "identifiedDateTime": "2024-07-15T10:00:00Z",
        "identifier": [
            {"system": "http://hospital.org/di", "value": "DI-001"}
        ],
        "implicated": [{"reference": "MedicationRequest/mr-1"}],
    }


@pytest.fixture
def minimal_detected_issue() -> Dict[str, Any]:
    return {
        "resourceType": "DetectedIssue",
        "id": "di-min",
        "status": "final",
        "code": {"coding": [{"code": "DRG"}]},
        "subject": {"reference": "Patient/pat-min"},
    }


class TestDetectedIssueReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("DetectedIssue", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("DetectedIssue", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_author(self, converter):
        q = converter.convert("DetectedIssue", "author=prac-1")
        assert "_search.authorId" in str(q)

    def test_implicated(self, converter):
        q = converter.convert("DetectedIssue", "implicated=mr-1")
        assert "_search.implicatedIds" in str(q)


class TestDetectedIssueTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("DetectedIssue", "identifier=DI-001")
        assert "DI-001" in str(q)

    def test_code(self, converter):
        q = converter.convert("DetectedIssue", "code=DRG")
        assert "code_codes" in str(q)

    def test_category(self, converter):
        q = converter.convert("DetectedIssue", "category=drug-drug")
        assert "category_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("DetectedIssue", "status=final")
        assert "final" in str(q)

    def test_id(self, converter):
        q = converter.convert("DetectedIssue", "_id=di-rich")
        assert "di-rich" in str(q)


class TestDetectedIssueDateParameters:
    def test_identified(self, converter):
        q = converter.convert("DetectedIssue", "identified=ge2024-07-01")
        assert "identifiedDateTime" in str(q) or "identifiedPeriod" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("DetectedIssue", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestDetectedIssueDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_detected_issue):
        doc = denormalizer.denormalize(copy.deepcopy(rich_detected_issue))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["authorId"] == "prac-1"
        assert "mr-1" in search["implicatedIds"]
        assert "DRG" in search["code_codes"]
        assert "drug-drug" in search["category_codes"]
        assert "DI-001" in search["identifier_values"]
        assert "Patient" in doc["_compartments"]
        assert "pat-1" in doc["_compartments"]["Patient"]

    def test_minimal_denormalization(self, denormalizer, minimal_detected_issue):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_detected_issue))
        assert doc["_search"]["patientId"] == "pat-min"
