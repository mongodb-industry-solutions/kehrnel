"""Comprehensive integration tests for MeasureReport search parameters."""
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
def rich_measure_report() -> Dict[str, Any]:
    return {
        "resourceType": "MeasureReport",
        "id": "mr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "complete",
        "date": "2024-07-15T10:00:00Z",
        "subject": {"reference": "Patient/pat-1"},
        "reporter": {"reference": "Practitioner/prac-1"},
        "location": {"reference": "Location/loc-1"},
        "measure": "http://example.org/Measure/diabetes",
        "period": {"start": "2024-07-01", "end": "2024-07-31"},
        "identifier": [{"value": "MR-001"}],
        "evaluatedResource": [{"reference": "Observation/obs-1"}],
    }


class TestMeasureReportReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("MeasureReport", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_reporter(self, converter):
        q = converter.convert("MeasureReport", "reporter=prac-1")
        assert "_search.reporterId" in str(q)

    def test_evaluated_resource(self, converter):
        q = converter.convert("MeasureReport", "evaluated-resource=obs-1")
        assert "_search.evaluatedResourceIds" in str(q)

    def test_measure(self, converter):
        q = converter.convert("MeasureReport", "measure=diabetes")
        assert "_search.measureCanonical_values" in str(q)


class TestMeasureReportTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("MeasureReport", "identifier=MR-001")
        assert "MR-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("MeasureReport", "status=complete")
        assert q == {"status": "complete"}


class TestMeasureReportDateParameters:
    def test_period(self, converter):
        q = converter.convert("MeasureReport", "period=ge2024-07-01")
        assert "_search.period" in str(q)


class TestMeasureReportCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "MeasureReport")
        assert q == {"_compartments.Patient": "pat-1"}


class TestMeasureReportDenormalization:
    def test_rich(self, denormalizer, rich_measure_report):
        out = denormalizer.denormalize(copy.deepcopy(rich_measure_report))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["reporterId"] == "prac-1"
        assert "obs-1" in s["evaluatedResourceIds"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
