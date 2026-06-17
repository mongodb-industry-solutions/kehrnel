"""
Comprehensive integration tests for ALL RiskAssessment search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "RiskAssessment")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/RiskAssessment.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, encounter.json

Exercises 12 search parameters in ``configs/RiskAssessment.yaml``.

Compartments (precomputed): Patient, Encounter.
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
def rich_risk_assessment() -> Dict[str, Any]:
    return {
        "resourceType": "RiskAssessment",
        "id": "ra-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "final",
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "condition": {"reference": "Condition/cond-1"},
        "performer": {"reference": "Practitioner/prac-1"},
        "occurrenceDateTime": "2024-07-15T10:00:00Z",
        "method": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/risk-assessment-method",
                    "code": "clinical",
                }
            ]
        },
        "identifier": [{"system": "http://hospital.org/ra", "value": "RA-001"}],
        "prediction": [
            {
                "probabilityDecimal": 0.42,
                "qualitativeRisk": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/risk-probability",
                            "code": "moderate",
                        }
                    ]
                },
            }
        ],
    }


@pytest.fixture
def minimal_risk_assessment() -> Dict[str, Any]:
    return {
        "resourceType": "RiskAssessment",
        "id": "ra-min",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestRiskAssessmentReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("RiskAssessment", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("RiskAssessment", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_condition(self, converter):
        q = converter.convert("RiskAssessment", "condition=cond-1")
        assert "_search.conditionId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("RiskAssessment", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_performer(self, converter):
        q = converter.convert("RiskAssessment", "performer=prac-1")
        assert "_search.performerId" in str(q)


class TestRiskAssessmentTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("RiskAssessment", "identifier=RA-001")
        assert "RA-001" in str(q)

    def test_method(self, converter):
        q = converter.convert("RiskAssessment", "method=clinical")
        assert "clinical" in str(q)

    def test_risk(self, converter):
        q = converter.convert("RiskAssessment", "risk=moderate")
        assert "moderate" in str(q)


class TestRiskAssessmentNumberParameters:
    def test_probability(self, converter):
        q = converter.convert("RiskAssessment", "probability=0.42")
        assert "probabilityDecimal_values" in str(q)

    def test_probability_gt(self, converter):
        q = converter.convert("RiskAssessment", "probability=gt0.5")
        assert "probabilityDecimal_values" in str(q)
        assert "$gt" in str(q)


class TestRiskAssessmentDateParameters:
    def test_date(self, converter):
        q = converter.convert("RiskAssessment", "date=ge2024-07-01")
        assert "occurrenceDateTime" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("RiskAssessment", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestRiskAssessmentCommonParameters:
    def test_id(self, converter):
        q = converter.convert("RiskAssessment", "_id=ra-rich")
        assert "ra-rich" in str(q)


class TestRiskAssessmentDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_risk_assessment):
        out = denormalizer.denormalize(minimal_risk_assessment)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_risk_assessment):
        out = denormalizer.denormalize(rich_risk_assessment)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["conditionId"] == "cond-1"
        assert s["encounterId"] == "enc-1"
        assert s["performerId"] == "prac-1"
        assert "clinical" in s["method_codes"]
        assert "moderate" in s["risk_codes"]
        assert 0.42 in s["probabilityDecimal_values"]
        assert "RA-001" in s["identifier_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert out["_compartments"]["Encounter"] == ["enc-1"]

    def test_input_not_mutated(self, denormalizer, rich_risk_assessment):
        original = copy.deepcopy(rich_risk_assessment)
        denormalizer.denormalize(rich_risk_assessment)
        assert rich_risk_assessment == original
