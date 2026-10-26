"""
Comprehensive integration tests for ALL FamilyMemberHistory search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "FamilyMemberHistory")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/FamilyMemberHistory.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 11 search parameters in ``configs/FamilyMemberHistory.yaml``.

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
def rich_family_member_history() -> Dict[str, Any]:
    return {
        "resourceType": "FamilyMemberHistory",
        "id": "fmh-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "date": "2024-07-15",
        "patient": {"reference": "Patient/pat-1"},
        "relationship": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                    "code": "FTH",
                }
            ]
        },
        "sex": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/administrative-gender",
                    "code": "male",
                }
            ]
        },
        "identifier": [
            {"system": "http://hospital.org/fmh", "value": "FMH-001"}
        ],
        "condition": [
            {
                "code": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "44054006",
                        }
                    ]
                }
            }
        ],
        "instantiatesCanonical": ["http://example.org/PlanDefinition/fmh-plan"],
        "instantiatesUri": ["http://example.org/protocols/fmh"],
    }


@pytest.fixture
def minimal_family_member_history() -> Dict[str, Any]:
    return {
        "resourceType": "FamilyMemberHistory",
        "id": "fmh-min",
        "status": "completed",
        "patient": {"reference": "Patient/pat-min"},
        "relationship": {"coding": [{"code": "FTH"}]},
    }


class TestFamilyMemberHistoryReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("FamilyMemberHistory", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_instantiates_canonical(self, converter):
        q = converter.convert(
            "FamilyMemberHistory",
            "instantiates-canonical=http://example.org/PlanDefinition/fmh-plan",
        )
        assert "instantiatesCanonical" in str(q)


class TestFamilyMemberHistoryTokenParameters:
    def test_code(self, converter):
        q = converter.convert("FamilyMemberHistory", "code=44054006")
        assert "conditionCode_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("FamilyMemberHistory", "identifier=FMH-001")
        assert "FMH-001" in str(q)

    def test_relationship(self, converter):
        q = converter.convert("FamilyMemberHistory", "relationship=FTH")
        assert "relationship_codes" in str(q)

    def test_sex(self, converter):
        q = converter.convert("FamilyMemberHistory", "sex=male")
        assert "sex_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("FamilyMemberHistory", "status=completed")
        assert "completed" in str(q)


class TestFamilyMemberHistoryDateParameters:
    def test_date(self, converter):
        q = converter.convert("FamilyMemberHistory", "date=ge2024-07-01")
        assert "date" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("FamilyMemberHistory", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestFamilyMemberHistoryUriParameters:
    def test_instantiates_uri(self, converter):
        q = converter.convert(
            "FamilyMemberHistory",
            "instantiates-uri=http://example.org/protocols/fmh",
        )
        assert "instantiatesUri" in str(q)


class TestFamilyMemberHistoryCommonParameters:
    def test_id(self, converter):
        q = converter.convert("FamilyMemberHistory", "_id=fmh-rich")
        assert "fmh-rich" in str(q)


class TestFamilyMemberHistoryDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_family_member_history):
        out = denormalizer.denormalize(minimal_family_member_history)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert "FTH" in s["relationship_codes"]
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_family_member_history):
        out = denormalizer.denormalize(rich_family_member_history)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "FTH" in s["relationship_codes"]
        assert "male" in s["sex_codes"]
        assert "44054006" in s["conditionCode_codes"]
        assert "FMH-001" in s["identifier_values"]
        assert "http://example.org/PlanDefinition/fmh-plan" in s[
            "instantiatesCanonical_values"
        ]
        assert "http://example.org/protocols/fmh" in s["instantiatesUri_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]

    def test_input_not_mutated(self, denormalizer, rich_family_member_history):
        original = copy.deepcopy(rich_family_member_history)
        denormalizer.denormalize(rich_family_member_history)
        assert rich_family_member_history == original
