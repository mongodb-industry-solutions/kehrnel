"""Comprehensive integration tests for GenomicStudy search parameters."""
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
def rich_genomic_study() -> Dict[str, Any]:
    return {
        "resourceType": "GenomicStudy",
        "id": "gs-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "registered",
        "subject": {"reference": "Patient/pat-1"},
        "identifier": [{"value": "GS-001"}],
        "analysis": [
            {
                "focus": [{"reference": "Condition/cond-1"}],
            }
        ],
    }


class TestGenomicStudyReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("GenomicStudy", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("GenomicStudy", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_focus(self, converter):
        q = converter.convert("GenomicStudy", "focus=cond-1")
        assert "_search.focusIds" in str(q)


class TestGenomicStudyTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("GenomicStudy", "identifier=GS-001")
        assert "GS-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("GenomicStudy", "status=registered")
        assert q == {"status": "registered"}


class TestGenomicStudyDenormalization:
    def test_rich(self, denormalizer, rich_genomic_study):
        out = denormalizer.denormalize(copy.deepcopy(rich_genomic_study))
        s = out["_search"]
        assert s["subjectId"] == "pat-1"
        assert s["patientId"] == "pat-1"
        assert "cond-1" in s["focusIds"]
        assert out.get("_compartments", {}) == {}
