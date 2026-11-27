"""
Comprehensive integration tests for ALL BiologicallyDerivedProduct search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "BiologicallyDerivedProduct")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/BiologicallyDerivedProduct.yaml

Exercises 10 search parameters in ``configs/BiologicallyDerivedProduct.yaml``.
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
def rich_bdp() -> Dict[str, Any]:
    return {
        "resourceType": "BiologicallyDerivedProduct",
        "id": "bdp-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "productCode": {"coding": [{"code": "E0398"}]},
        "productCategory": {"code": "organ"},
        "productStatus": {"code": "available"},
        "biologicalSourceEvent": {
            "system": "http://hospital.org/bse",
            "value": "BSE-001",
        },
        "identifier": [
            {"system": "http://hospital.org/bdp", "value": "SN-12345"}
        ],
        "collection": {
            "collector": {"reference": "Practitioner/prac-1"},
        },
        "request": [{"reference": "ServiceRequest/sr-1"}],
    }


@pytest.fixture
def minimal_bdp() -> Dict[str, Any]:
    return {
        "resourceType": "BiologicallyDerivedProduct",
        "id": "bdp-min",
        "productCode": {"coding": [{"code": "blood"}]},
    }


class TestBiologicallyDerivedProductTokenParameters:
    def test_code(self, converter):
        q = converter.convert("BiologicallyDerivedProduct", "code=E0398")
        assert "productCode_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("BiologicallyDerivedProduct", "identifier=SN-12345")
        assert "SN-12345" in str(q)

    def test_serial_number(self, converter):
        q = converter.convert("BiologicallyDerivedProduct", "serial-number=SN-12345")
        assert "SN-12345" in str(q)

    def test_biological_source_event(self, converter):
        q = converter.convert(
            "BiologicallyDerivedProduct", "biological-source-event=BSE-001"
        )
        assert "BSE-001" in str(q)

    def test_product_category(self, converter):
        q = converter.convert("BiologicallyDerivedProduct", "product-category=organ")
        assert "productCategory_codes" in str(q)

    def test_product_status(self, converter):
        q = converter.convert("BiologicallyDerivedProduct", "product-status=available")
        assert "productStatus_codes" in str(q)


class TestBiologicallyDerivedProductReferenceParameters:
    def test_collector(self, converter):
        q = converter.convert("BiologicallyDerivedProduct", "collector=prac-1")
        assert "prac-1" in str(q)

    def test_request(self, converter):
        q = converter.convert("BiologicallyDerivedProduct", "request=sr-1")
        assert "sr-1" in str(q)


class TestBiologicallyDerivedProductDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_bdp):
        doc = denormalizer.denormalize(copy.deepcopy(rich_bdp))
        search = doc["_search"]
        assert "E0398" in search["productCode_codes"]
        assert "organ" in search["productCategory_codes"]
        assert "available" in search["productStatus_codes"]
        assert "BSE-001" in search["biologicalSourceEvent_values"]
        assert "SN-12345" in search["identifier_values"]
        assert search["collectorId"] == "prac-1"
        assert "sr-1" in search["requestIds"]

    def test_minimal_sparse_output(self, denormalizer, minimal_bdp):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_bdp))
        assert "identifier_values" not in doc.get("_search", {})
