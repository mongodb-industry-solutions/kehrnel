"""
Comprehensive integration tests for ALL Medication search parameters per FHIR R5.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Medication")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Medication.yaml

Exercises 12 search parameters in ``configs/Medication.yaml``:

  References (2): ingredient, marketingauthorizationholder
  Tokens (7): code, form, identifier, ingredient-code, lot-number,
    serial-number, status
  Dates (1): expiration-date
  Common (2): _id, _lastUpdated

R5 structural notes:
  * ``ingredient[].item`` is CodeableReference.
  * ``batch`` (0..1) supplies ``lot-number`` and ``expiration-date``.
  * ``serial-number`` searches ``identifier`` per the R5 expression.

No precomputed compartments (Medication is not a compartment member).
"""
from __future__ import annotations

import copy
from datetime import datetime
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
def rich_medication() -> Dict[str, Any]:
    return {
        "resourceType": "Medication",
        "id": "med-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "identifier": [
            {"system": "http://hospital.org/med", "value": "MED-001"},
            {"type": {"coding": [{"code": "SNO"}]}, "value": "SN-999"},
        ],
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "319785009"}]
        },
        "doseForm": {
            "coding": [{"system": "http://snomed.info/sct", "code": "385055001"}]
        },
        "batch": {
            "lotNumber": "LOT-42",
            "expirationDate": "2026-12-31T00:00:00Z",
        },
        "ingredient": [
            {
                "item": {
                    "concept": {
                        "coding": [
                            {"system": "http://snomed.info/sct", "code": "387517004"}
                        ]
                    },
                    "reference": {"reference": "Substance/sub-1"},
                }
            },
            {
                "item": {
                    "reference": {"reference": "Medication/med-base"},
                }
            },
        ],
        "marketingAuthorizationHolder": {"reference": "Organization/org-1"},
    }


@pytest.fixture
def minimal_medication() -> Dict[str, Any]:
    return {
        "resourceType": "Medication",
        "id": "med-min",
        "status": "inactive",
    }


class TestMedicationReferenceParameters:
    def test_ingredient_substance(self, converter):
        q = converter.convert("Medication", "ingredient=sub-1")
        assert "_search.ingredientIds" in str(q)
        assert "sub-1" in str(q)

    def test_ingredient_medication(self, converter):
        q = converter.convert("Medication", "ingredient=Medication/med-base")
        s = str(q)
        assert "_search.ingredientIds" in s
        assert "med-base" in s

    def test_marketing_authorization_holder(self, converter):
        q = converter.convert("Medication", "marketingauthorizationholder=org-1")
        s = str(q)
        assert "_search.marketingAuthorizationHolderId" in s
        assert "org-1" in s


class TestMedicationTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Medication", "status=active") == {"status": "active"}

    def test_code(self, converter):
        q = converter.convert("Medication", "code=319785009")
        assert "319785009" in str(q)

    def test_form(self, converter):
        q = converter.convert("Medication", "form=385055001")
        assert "385055001" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Medication", "identifier=MED-001")
        assert "MED-001" in str(q)

    def test_ingredient_code(self, converter):
        q = converter.convert("Medication", "ingredient-code=387517004")
        assert "387517004" in str(q)

    def test_lot_number(self, converter):
        q = converter.convert("Medication", "lot-number=LOT-42")
        assert "LOT-42" in str(q)

    def test_serial_number(self, converter):
        q = converter.convert("Medication", "serial-number=SN-999")
        assert "SN-999" in str(q)

    def test_id(self, converter):
        q = converter.convert("Medication", "_id=med-rich")
        assert "med-rich" in str(q)


class TestMedicationDateParameters:
    def test_expiration_date_ge(self, converter):
        q = converter.convert("Medication", "expiration-date=ge2026-01-01")
        assert "_search.batchExpirationDate" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Medication", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestMedicationComplexQueries:
    def test_status_and_code(self, converter):
        q = converter.convert("Medication", "status=active&code=319785009")
        s = str(q)
        assert "active" in s
        assert "319785009" in s


class TestMedicationDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_medication):
        out = denormalizer.denormalize(minimal_medication)
        search = out.get("_search", {})
        assert "status" not in search
        assert out["status"] == "inactive"
        assert "code_codes" not in search

    def test_rich_fields(self, denormalizer, rich_medication):
        out = denormalizer.denormalize(rich_medication)
        s = out["_search"]
        assert "319785009" in s["code_codes"]
        assert "385055001" in s["form_codes"]
        assert "MED-001" in s["identifier_values"]
        assert "SN-999" in s["identifier_values"]
        assert "LOT-42" in s["lotNumber_values"]
        assert s["batchExpirationDate"] == "2026-12-31T00:00:00Z"
        assert "387517004" in s["ingredientCode_codes"]
        assert "sub-1" in s["ingredientIds"]
        assert "med-base" in s["ingredientIds"]
        assert s["marketingAuthorizationHolderId"] == "org-1"
        assert "_compartments" not in out

    def test_input_not_mutated(self, denormalizer, rich_medication):
        original = copy.deepcopy(rich_medication)
        denormalizer.denormalize(rich_medication)
        assert rich_medication == original

    def test_only_search_added(self, denormalizer, rich_medication):
        original = set(rich_medication.keys())
        out = denormalizer.denormalize(rich_medication)
        added = set(out.keys()) - original
        assert added == {"_search"}


def _mongo_available() -> bool:
    try:
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.server_info()
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.mongodb
@pytest.mark.skipif(not _mongo_available(), reason="MongoDB not running")
class TestMedicationMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["medication_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "Medication",
            "id": "e2e-med-1",
            "status": "active",
            "code": {"coding": [{"code": "319785009"}]},
            "batch": {
                "expirationDate": datetime(2026, 12, 31, 0, 0, 0),
            },
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("Medication", "status=active")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-med-1"

    def test_expiration_date_lookup(self, seeded):
        conv = FHIRSearchConverter()
        q = conv.convert("Medication", "expiration-date=ge2026-06-01")
        results = list(seeded.find(q))
        assert len(results) == 1
