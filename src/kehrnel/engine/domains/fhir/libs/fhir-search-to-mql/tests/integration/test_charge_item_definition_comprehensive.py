"""Comprehensive integration tests for ChargeItemDefinition search parameters."""
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
def rich_charge_item_definition() -> Dict[str, Any]:
    return {
        "resourceType": "ChargeItemDefinition",
        "id": "cid-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "url": "http://example.org/ChargeItemDefinition/lab-panel",
        "version": "1.0",
        "title": "Lab Panel Fee",
        "publisher": "Acme Billing",
        "description": "Standard lab panel charge",
        "date": "2024-06-01",
        "jurisdiction": [{"coding": [{"code": "US"}]}],
        "identifier": [{"value": "CID-001"}],
        "useContext": [
            {
                "code": {"code": "focus"},
                "valueCodeableConcept": {"coding": [{"code": "ambulatory"}]},
            }
        ],
        "applicability": [
            {"effectivePeriod": {"start": "2024-01-01", "end": "2025-12-31"}}
        ],
    }


class TestChargeItemDefinitionStringParameters:
    def test_title(self, converter):
        q = converter.convert("ChargeItemDefinition", "title=lab")
        assert "title_lower" in str(q)

    def test_publisher(self, converter):
        q = converter.convert("ChargeItemDefinition", "publisher=acme")
        assert "publisher_lower" in str(q)


class TestChargeItemDefinitionTokenParameters:
    def test_status(self, converter):
        assert converter.convert("ChargeItemDefinition", "status=active") == {"status": "active"}

    def test_context(self, converter):
        q = converter.convert("ChargeItemDefinition", "context=ambulatory")
        assert "context_codes" in str(q)

    def test_jurisdiction(self, converter):
        q = converter.convert("ChargeItemDefinition", "jurisdiction=US")
        assert "US" in str(q)


class TestChargeItemDefinitionUriParameters:
    def test_url(self, converter):
        q = converter.convert(
            "ChargeItemDefinition",
            "url=http://example.org/ChargeItemDefinition/lab-panel",
        )
        assert "url" in str(q)


class TestChargeItemDefinitionDateParameters:
    def test_effective(self, converter):
        q = converter.convert("ChargeItemDefinition", "effective=ge2024-01-01")
        assert "effectivePeriod" in str(q)


class TestChargeItemDefinitionDenormalization:
    def test_rich(self, denormalizer, rich_charge_item_definition):
        out = denormalizer.denormalize(copy.deepcopy(rich_charge_item_definition))
        s = out["_search"]
        assert "ambulatory" in s["context_codes"]
        assert "US" in s["jurisdiction_codes"]
        assert s["title_lower"] == "lab panel fee"

    def test_publisher_reference_coerced_to_string(self, denormalizer):
        """Legacy gen data may store publisher as Reference; denorm must index a string."""
        doc = {
            "resourceType": "ChargeItemDefinition",
            "id": "cid-ref-pub",
            "status": "active",
            "url": "http://example.org/ChargeItemDefinition/x",
            "publisher": {
                "reference": "Organization/org-1",
                "display": "Acme Billing",
            },
        }
        out = denormalizer.denormalize(copy.deepcopy(doc))
        assert out["_search"]["publisher"] == "Acme Billing"
        assert out["_search"]["publisher_lower"] == "acme billing"
