"""
Comprehensive integration tests for ALL EpisodeOfCare search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "EpisodeOfCare")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/EpisodeOfCare.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json

Exercises 14 search parameters in ``configs/EpisodeOfCare.yaml``.

Compartments (precomputed): Patient, Practitioner.
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
def rich_episode() -> Dict[str, Any]:
    return {
        "resourceType": "EpisodeOfCare",
        "id": "eoc-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "patient": {"reference": "Patient/pat-1"},
        "managingOrganization": {"reference": "Organization/org-1"},
        "careManager": {"reference": "Practitioner/prac-1"},
        "period": {"start": "2024-01-01", "end": "2024-12-31"},
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/episodeofcare-type",
                        "code": "hacc",
                    }
                ]
            }
        ],
        "identifier": [{"system": "http://hospital.org/eoc", "value": "EOC-001"}],
        "diagnosis": [
            {
                "condition": {
                    "concept": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "44054006"}]
                    },
                    "reference": {"reference": "Condition/cond-1"},
                }
            }
        ],
        "reason": [
            {
                "value": {
                    "concept": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "185349003"}]
                    },
                    "reference": {"reference": "Encounter/enc-1"},
                }
            }
        ],
        "referralRequest": [{"reference": "ServiceRequest/sr-1"}],
    }


@pytest.fixture
def minimal_episode() -> Dict[str, Any]:
    return {
        "resourceType": "EpisodeOfCare",
        "id": "eoc-min",
        "patient": {"reference": "Patient/pat-min"},
    }


class TestEpisodeOfCareReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("EpisodeOfCare", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_organization(self, converter):
        q = converter.convert("EpisodeOfCare", "organization=org-1")
        assert "_search.organizationId" in str(q)

    def test_care_manager(self, converter):
        q = converter.convert("EpisodeOfCare", "care-manager=prac-1")
        assert "_search.careManagerId" in str(q)

    def test_diagnosis_reference(self, converter):
        q = converter.convert("EpisodeOfCare", "diagnosis-reference=cond-1")
        assert "_search.diagnosisReferenceIds" in str(q)

    def test_reason_reference(self, converter):
        q = converter.convert("EpisodeOfCare", "reason-reference=enc-1")
        assert "_search.reasonReferenceIds" in str(q)

    def test_incoming_referral(self, converter):
        q = converter.convert("EpisodeOfCare", "incoming-referral=sr-1")
        assert "_search.referralRequestIds" in str(q)


class TestEpisodeOfCareTokenParameters:
    def test_status(self, converter):
        assert converter.convert("EpisodeOfCare", "status=active") == {"status": "active"}

    def test_type(self, converter):
        q = converter.convert("EpisodeOfCare", "type=hacc")
        assert "hacc" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("EpisodeOfCare", "identifier=EOC-001")
        assert "EOC-001" in str(q)

    def test_diagnosis_code(self, converter):
        q = converter.convert("EpisodeOfCare", "diagnosis-code=44054006")
        assert "44054006" in str(q)

    def test_reason_code(self, converter):
        q = converter.convert("EpisodeOfCare", "reason-code=185349003")
        assert "185349003" in str(q)


class TestEpisodeOfCareDateParameters:
    def test_date(self, converter):
        q = converter.convert("EpisodeOfCare", "date=ge2024-06-01")
        assert "period" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("EpisodeOfCare", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestEpisodeOfCareCommonParameters:
    def test_id(self, converter):
        q = converter.convert("EpisodeOfCare", "_id=eoc-rich")
        assert "eoc-rich" in str(q)


class TestEpisodeOfCareDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_episode):
        out = denormalizer.denormalize(minimal_episode)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_episode):
        out = denormalizer.denormalize(rich_episode)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["organizationId"] == "org-1"
        assert s["careManagerId"] == "prac-1"
        assert "44054006" in s["diagnosisCode_codes"]
        assert "cond-1" in s["diagnosisReferenceIds"]
        assert "185349003" in s["reasonCode_codes"]
        assert "enc-1" in s["reasonReferenceIds"]
        assert "sr-1" in s["referralRequestIds"]
        assert "hacc" in s["type_codes"]
        assert "EOC-001" in s["identifier_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert out["_compartments"]["Practitioner"] == ["prac-1"]

    def test_input_not_mutated(self, denormalizer, rich_episode):
        original = copy.deepcopy(rich_episode)
        denormalizer.denormalize(rich_episode)
        assert rich_episode == original
