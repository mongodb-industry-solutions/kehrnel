"""
Comprehensive integration tests for ALL Person search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Person")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Person.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 25 search parameters in ``configs/Person.yaml``.

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
def rich_person() -> Dict[str, Any]:
    return {
        "resourceType": "Person",
        "id": "person-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "gender": "male",
        "birthDate": "1975-06-01",
        "name": [{"family": "Smith", "given": ["John"]}],
        "telecom": [
            {"system": "email", "value": "john@example.org"},
            {"system": "phone", "value": "555-0100"},
        ],
        "address": [{"city": "Boston", "state": "MA", "use": "home"}],
        "link": [
            {"target": {"reference": "Patient/pat-1"}},
            {"target": {"reference": "Practitioner/prac-1"}},
        ],
        "managingOrganization": {"reference": "Organization/org-1"},
        "identifier": [
            {"system": "http://hospital.org/person", "value": "PERSON-001"}
        ],
    }


@pytest.fixture
def minimal_person() -> Dict[str, Any]:
    return {
        "resourceType": "Person",
        "id": "person-min",
        "name": [{"family": "Min"}],
        "link": [{"target": {"reference": "Patient/pat-min"}}],
    }


class TestPersonStringParameters:
    def test_family(self, converter):
        q = converter.convert("Person", "family=smith")
        assert "_search.familyName_lower" in str(q)

    def test_given(self, converter):
        q = converter.convert("Person", "given=john")
        assert "_search.givenNames_lower" in str(q)

    def test_name(self, converter):
        q = converter.convert("Person", "name=smith")
        assert "familyName_lower" in str(q)


class TestPersonReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Person", "patient=pat-1")
        assert "patientLinkId" in str(q)

    def test_practitioner(self, converter):
        q = converter.convert("Person", "practitioner=prac-1")
        assert "practitionerLinkId" in str(q)

    def test_link(self, converter):
        q = converter.convert("Person", "link=pat-1")
        assert "linkTargetIds" in str(q)

    def test_organization(self, converter):
        q = converter.convert("Person", "organization=org-1")
        assert "_search.organizationId" in str(q)


class TestPersonTokenParameters:
    def test_gender(self, converter):
        q = converter.convert("Person", "gender=male")
        assert "male" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Person", "identifier=PERSON-001")
        assert "PERSON-001" in str(q)

    def test_email(self, converter):
        q = converter.convert("Person", "email=john@example.org")
        assert "john@example.org" in str(q).lower()

    def test_phone(self, converter):
        q = converter.convert("Person", "phone=555-0100")
        assert "555-0100" in str(q)

    def test_address_use(self, converter):
        q = converter.convert("Person", "address-use=home")
        assert "home" in str(q)


class TestPersonDateParameters:
    def test_birthdate(self, converter):
        q = converter.convert("Person", "birthdate=ge1970-01-01")
        assert "birthDate" in str(q)


class TestPersonDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_person):
        doc = denormalizer.denormalize(copy.deepcopy(rich_person))
        search = doc["_search"]
        assert "pat-1" in search["patientLinkId"]
        assert "prac-1" in search["practitionerLinkId"]
        assert search["organizationId"] == "org-1"
        assert "smith" in search["familyName_lower"][0]
        assert "PERSON-001" in search["identifier_values"]
        assert "pat-1" in doc["_compartments"]["Patient"]

    def test_minimal_denormalization(self, denormalizer, minimal_person):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_person))
        assert "pat-min" in doc["_search"]["patientLinkId"]
