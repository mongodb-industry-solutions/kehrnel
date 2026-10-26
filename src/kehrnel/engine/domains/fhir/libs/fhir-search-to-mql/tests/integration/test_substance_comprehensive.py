"""
Comprehensive integration tests for ALL Substance search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Substance")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Substance.yaml

Exercises 10 search parameters in ``configs/Substance.yaml``.

No R5 compartments — Substance is not in compartment definitions.
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
def rich_substance() -> Dict[str, Any]:
    return {
        "resourceType": "Substance",
        "id": "sub-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "code": {
            "concept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "387517004",
                    }
                ]
            },
            "reference": {"reference": "SubstanceDefinition/sd-aspirin"},
        },
        "category": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/substance-category", "code": "chemical"}]}
        ],
        "identifier": [{"system": "http://hospital.org/substance", "value": "SUB-001"}],
        "expiry": "2025-12-31",
        "quantity": {"value": 500, "unit": "mg", "system": "http://unitsofmeasure.org", "code": "mg"},
        "ingredient": [
            {
                "substanceCodeableConcept": {
                    "coding": [{"code": "387207008"}]
                },
                "substanceReference": {"reference": "Substance/sub-salicylic"},
            }
        ],
    }


@pytest.fixture
def minimal_substance() -> Dict[str, Any]:
    return {
        "resourceType": "Substance",
        "id": "sub-min",
        "status": "active",
        "code": {"concept": {"coding": [{"code": "387517004"}]}},
    }


class TestSubstanceReferenceParameters:
    def test_code_reference(self, converter):
        q = converter.convert("Substance", "code-reference=sd-aspirin")
        assert "_search.codeReferenceIds" in str(q)

    def test_substance_reference(self, converter):
        q = converter.convert("Substance", "substance-reference=sub-salicylic")
        assert "_search.substanceReferenceIds" in str(q)


class TestSubstanceTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Substance", "status=active") == {"status": "active"}

    def test_code(self, converter):
        q = converter.convert("Substance", "code=387517004")
        assert "387517004" in str(q)

    def test_code_ingredient_concept(self, converter):
        q = converter.convert("Substance", "code=387207008")
        assert "387207008" in str(q)

    def test_category(self, converter):
        q = converter.convert("Substance", "category=chemical")
        assert "chemical" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Substance", "identifier=SUB-001")
        assert "SUB-001" in str(q)


class TestSubstanceDateParameters:
    def test_expiry(self, converter):
        q = converter.convert("Substance", "expiry=le2025-12-31")
        assert "expiry" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Substance", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestSubstanceQuantityParameters:
    def test_quantity(self, converter):
        q = converter.convert("Substance", "quantity=500|http://unitsofmeasure.org|mg")
        assert "quantity" in str(q)


class TestSubstanceCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Substance", "_id=sub-rich")
        assert "sub-rich" in str(q)


class TestSubstanceDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_substance):
        out = denormalizer.denormalize(minimal_substance)
        s = out.get("_search", {})
        assert "387517004" in s["code_codes"]

    def test_rich_fields(self, denormalizer, rich_substance):
        out = denormalizer.denormalize(rich_substance)
        s = out["_search"]
        assert "387517004" in s["code_codes"]
        assert "387207008" in s["code_codes"]
        assert "sd-aspirin" in s["codeReferenceIds"]
        assert "sub-salicylic" in s["substanceReferenceIds"]
        assert "chemical" in s["category_codes"]
        assert "SUB-001" in s["identifier_values"]

    def test_input_not_mutated(self, denormalizer, rich_substance):
        original = copy.deepcopy(rich_substance)
        denormalizer.denormalize(rich_substance)
        assert rich_substance == original
