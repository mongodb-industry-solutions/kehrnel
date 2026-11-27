"""
Comprehensive integration tests for ALL ResearchSubject search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "ResearchSubject")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ResearchSubject.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 9 search parameters in ``configs/ResearchSubject.yaml``.

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
def rich_research_subject() -> Dict[str, Any]:
    return {
        "resourceType": "ResearchSubject",
        "id": "rsub-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "study": {"reference": "ResearchStudy/rs-1"},
        "subject": {"reference": "Patient/pat-1"},
        "period": {"start": "2024-01-15", "end": "2024-12-31"},
        "identifier": [
            {"system": "http://hospital.org/rsub", "value": "RSUB-001"}
        ],
        "progress": [
            {
                "subjectState": {
                    "coding": [{"code": "on-study"}],
                },
            }
        ],
    }


@pytest.fixture
def minimal_research_subject() -> Dict[str, Any]:
    return {
        "resourceType": "ResearchSubject",
        "id": "rsub-min",
        "status": "active",
        "study": {"reference": "ResearchStudy/rs-min"},
        "subject": {"reference": "Patient/pat-min"},
    }


class TestResearchSubjectReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("ResearchSubject", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_study(self, converter):
        q = converter.convert("ResearchSubject", "study=rs-1")
        assert "_search.studyId" in str(q)

    def test_subject(self, converter):
        q = converter.convert("ResearchSubject", "subject=pat-1")
        assert "_search.subjectId" in str(q)


class TestResearchSubjectTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("ResearchSubject", "identifier=RSUB-001")
        assert "RSUB-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("ResearchSubject", "status=active")
        assert "active" in str(q)

    def test_subject_state(self, converter):
        q = converter.convert("ResearchSubject", "subject_state=on-study")
        assert "progressSubjectState_codes" in str(q)

    def test_id(self, converter):
        q = converter.convert("ResearchSubject", "_id=rsub-rich")
        assert "rsub-rich" in str(q)


class TestResearchSubjectDateParameters:
    def test_date(self, converter):
        q = converter.convert("ResearchSubject", "date=ge2024-01-01")
        assert "period" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("ResearchSubject", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestResearchSubjectDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_research_subject):
        doc = denormalizer.denormalize(copy.deepcopy(rich_research_subject))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["studyId"] == "rs-1"
        assert search["subjectId"] == "pat-1"
        assert "on-study" in search["progressSubjectState_codes"]
        assert "Patient" in doc["_compartments"]
        assert "pat-1" in doc["_compartments"]["Patient"]

    def test_minimal_denormalization(self, denormalizer, minimal_research_subject):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_research_subject))
        assert doc["_search"]["patientId"] == "pat-min"
