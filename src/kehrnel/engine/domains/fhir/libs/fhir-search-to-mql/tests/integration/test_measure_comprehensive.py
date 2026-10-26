"""Comprehensive integration tests for Measure search parameters."""
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
def rich_measure() -> Dict[str, Any]:
    return {
        "resourceType": "Measure",
        "id": "measure-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "name": "diabetes-measure",
        "title": "Diabetes Measure",
        "publisher": "Acme Quality",
        "description": "Diabetes quality measure",
        "url": "http://example.org/Measure/diabetes",
        "version": "1.0",
        "date": "2024-01-01",
        "identifier": [{"value": "MEAS-001"}],
        "jurisdiction": [{"coding": [{"code": "US"}]}],
        "topic": [{"coding": [{"code": "diabetes"}]}],
        "useContext": [
            {
                "code": {"code": "focus"},
                "valueCodeableConcept": {"coding": [{"code": "ambulatory"}]},
            }
        ],
        "effectivePeriod": {"start": "2024-01-01", "end": "2024-12-31"},
        "library": ["http://example.org/Library/lib-1"],
        "relatedArtifact": [
            {
                "type": "depends-on",
                "resource": {"reference": "Library/lib-dep"},
            }
        ],
    }


class TestMeasureReferenceParameters:
    def test_depends_on(self, converter):
        q = converter.convert("Measure", "depends-on=lib-dep")
        assert "_search.dependsOnIds" in str(q)


class TestMeasureTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Measure", "identifier=MEAS-001")
        assert "MEAS-001" in str(q)

    def test_context(self, converter):
        q = converter.convert("Measure", "context=ambulatory")
        assert "ambulatory" in str(q)

    def test_status(self, converter):
        q = converter.convert("Measure", "status=active")
        assert q == {"status": "active"}


class TestMeasureStringParameters:
    def test_title(self, converter):
        q = converter.convert("Measure", "title=Diabetes")
        assert "_search.title_lower" in str(q)


class TestMeasureDateParameters:
    def test_effective(self, converter):
        q = converter.convert("Measure", "effective=ge2024-01-01")
        assert "_search.effectivePeriod" in str(q)


class TestMeasureDenormalization:
    def test_rich(self, denormalizer, rich_measure):
        out = denormalizer.denormalize(copy.deepcopy(rich_measure))
        s = out["_search"]
        assert "lib-dep" in s["dependsOnIds"]
        assert "http://example.org/Library/lib-1" in s["library_values"]
        assert "ambulatory" in s["context_codes"]
        assert out.get("_compartments", {}) == {}
