"""
Comprehensive integration tests for ALL ImmunizationRecommendation search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "ImmunizationRecommendation")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ImmunizationRecommendation.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 10 search parameters in ``configs/ImmunizationRecommendation.yaml``.

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
def rich_immunization_recommendation() -> Dict[str, Any]:
    return {
        "resourceType": "ImmunizationRecommendation",
        "id": "ir-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "patient": {"reference": "Patient/pat-1"},
        "date": "2024-07-15T10:00:00Z",
        "identifier": [
            {"system": "http://hospital.org/ir", "value": "IR-001"}
        ],
        "recommendation": [
            {
                "forecastStatus": {"coding": [{"code": "due"}]},
                "vaccineCode": [{"coding": [{"code": "flu"}]}],
                "targetDisease": [{"coding": [{"code": "6142004"}]}],
                "supportingImmunization": [
                    {"reference": "Immunization/imm-1"}
                ],
                "supportingPatientInformation": [
                    {"reference": "Observation/obs-1"}
                ],
            }
        ],
    }


@pytest.fixture
def minimal_immunization_recommendation() -> Dict[str, Any]:
    return {
        "resourceType": "ImmunizationRecommendation",
        "id": "ir-min",
        "patient": {"reference": "Patient/pat-min"},
        "recommendation": [
            {
                "forecastStatus": {"coding": [{"code": "due"}]},
                "vaccineCode": [{"coding": [{"code": "flu"}]}],
            }
        ],
    }


class TestImmunizationRecommendationReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("ImmunizationRecommendation", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_support(self, converter):
        q = converter.convert("ImmunizationRecommendation", "support=imm-1")
        assert "supportingImmunizationIds" in str(q)

    def test_information(self, converter):
        q = converter.convert("ImmunizationRecommendation", "information=obs-1")
        assert "supportingPatientInformationIds" in str(q)


class TestImmunizationRecommendationTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("ImmunizationRecommendation", "identifier=IR-001")
        assert "IR-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("ImmunizationRecommendation", "status=due")
        assert "forecastStatus_codes" in str(q)

    def test_vaccine_type(self, converter):
        q = converter.convert("ImmunizationRecommendation", "vaccine-type=flu")
        assert "vaccineCode_codes" in str(q)

    def test_target_disease(self, converter):
        q = converter.convert("ImmunizationRecommendation", "target-disease=6142004")
        assert "6142004" in str(q)

    def test_id(self, converter):
        q = converter.convert("ImmunizationRecommendation", "_id=ir-rich")
        assert "ir-rich" in str(q)


class TestImmunizationRecommendationDateParameters:
    def test_date(self, converter):
        q = converter.convert("ImmunizationRecommendation", "date=ge2024-07-01")
        assert "date" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("ImmunizationRecommendation", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestImmunizationRecommendationDenormalization:
    def test_rich_denormalization(
        self, denormalizer, rich_immunization_recommendation
    ):
        doc = denormalizer.denormalize(
            copy.deepcopy(rich_immunization_recommendation)
        )
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert "due" in search["forecastStatus_codes"]
        assert "flu" in search["vaccineCode_codes"]
        assert "imm-1" in search["supportingImmunizationIds"]
        assert "obs-1" in search["supportingPatientInformationIds"]
        assert "pat-1" in doc["_compartments"]["Patient"]

    def test_minimal_denormalization(
        self, denormalizer, minimal_immunization_recommendation
    ):
        doc = denormalizer.denormalize(
            copy.deepcopy(minimal_immunization_recommendation)
        )
        assert doc["_search"]["patientId"] == "pat-min"
