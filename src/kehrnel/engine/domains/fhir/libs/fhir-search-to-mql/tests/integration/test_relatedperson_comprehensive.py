"""
Comprehensive integration tests for ALL RelatedPerson search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "RelatedPerson")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/RelatedPerson.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, relatedperson.json

Exercises 21 search parameters in ``configs/RelatedPerson.yaml``.

Compartments (precomputed): Patient, RelatedPerson.
"""
from __future__ import annotations

import copy
from typing import Any, Dict

import pytest

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from fhir_search_to_mql.denormalizer.extractors.phonetic import soundex

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def converter() -> FHIRSearchConverter:
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.fixture
def rich_relatedperson() -> Dict[str, Any]:
    return {
        "resourceType": "RelatedPerson",
        "id": "rp-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "active": True,
        "patient": {"reference": "Patient/pat-1"},
        "relationship": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": "WIFE",
                    }
                ]
            }
        ],
        "name": [{"family": "Smith", "given": ["Jane"], "text": "Jane Smith"}],
        "telecom": [
            {"system": "phone", "value": "555-0199"},
            {"system": "email", "value": "jane.smith@example.org"},
        ],
        "gender": "female",
        "birthDate": "1985-03-20",
        "address": [
            {
                "use": "home",
                "line": ["42 Oak St"],
                "city": "Springfield",
                "state": "IL",
                "postalCode": "62701",
                "country": "USA",
            }
        ],
        "identifier": [{"system": "http://hospital.org/rp", "value": "RP-001"}],
    }


@pytest.fixture
def minimal_relatedperson() -> Dict[str, Any]:
    return {
        "resourceType": "RelatedPerson",
        "id": "rp-min",
        "patient": {"reference": "Patient/pat-min"},
    }


class TestRelatedPersonReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("RelatedPerson", "patient=pat-1")
        assert "pat-1" in str(q)


class TestRelatedPersonStringParameters:
    def test_name(self, converter):
        q = converter.convert("RelatedPerson", "name=smith")
        assert "familyName_lower" in str(q)

    def test_family(self, converter):
        q = converter.convert("RelatedPerson", "family=Smith")
        assert "familyName_lower" in str(q)

    def test_given(self, converter):
        q = converter.convert("RelatedPerson", "given=Jane")
        assert "givenNames_lower" in str(q)

    def test_address_city(self, converter):
        q = converter.convert("RelatedPerson", "address-city=Spring")
        assert "addressCity_lower" in str(q)

    def test_phonetic(self, converter):
        code = soundex("Smith")
        q = converter.convert("RelatedPerson", f"phonetic={code}")
        assert "phonetic_codes" in str(q)


class TestRelatedPersonTokenParameters:
    def test_active(self, converter):
        assert converter.convert("RelatedPerson", "active=true") == {"active": True}

    def test_gender(self, converter):
        assert converter.convert("RelatedPerson", "gender=female") == {"gender": "female"}

    def test_relationship(self, converter):
        q = converter.convert("RelatedPerson", "relationship=WIFE")
        assert "WIFE" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("RelatedPerson", "identifier=RP-001")
        assert "RP-001" in str(q)

    def test_email(self, converter):
        q = converter.convert("RelatedPerson", "email=jane.smith@example.org")
        assert "jane.smith@example.org" in str(q)

    def test_phone(self, converter):
        q = converter.convert("RelatedPerson", "phone=555-0199")
        assert "555-0199" in str(q)

    def test_telecom(self, converter):
        q = converter.convert("RelatedPerson", "telecom=555-0199")
        assert "555-0199" in str(q)

    def test_address_use(self, converter):
        q = converter.convert("RelatedPerson", "address-use=home")
        assert "home" in str(q)


class TestRelatedPersonDateParameters:
    def test_birthdate(self, converter):
        q = converter.convert("RelatedPerson", "birthdate=ge1980-01-01")
        assert "birthDate" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("RelatedPerson", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestRelatedPersonCommonParameters:
    def test_id(self, converter):
        q = converter.convert("RelatedPerson", "_id=rp-rich")
        assert "rp-rich" in str(q)


class TestRelatedPersonDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_relatedperson):
        out = denormalizer.denormalize(minimal_relatedperson)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["RelatedPerson"] == ["rp-min"]

    def test_rich_fields(self, denormalizer, rich_relatedperson):
        out = denormalizer.denormalize(rich_relatedperson)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "WIFE" in s["relationship_codes"]
        assert "Smith" in s["familyName"]
        assert "jane.smith@example.org" in s["email"]
        assert "555-0199" in s["phone"]
        assert "RP-001" in s["identifier_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert out["_compartments"]["RelatedPerson"] == ["rp-rich"]

    def test_input_not_mutated(self, denormalizer, rich_relatedperson):
        original = copy.deepcopy(rich_relatedperson)
        denormalizer.denormalize(rich_relatedperson)
        assert rich_relatedperson == original
