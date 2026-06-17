"""
Comprehensive integration tests for ALL ClinicalImpression search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "ClinicalImpression")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ClinicalImpression.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 14 search parameters in ``configs/ClinicalImpression.yaml``.

Compartments (precomputed): Patient, Practitioner, Device, Encounter.
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
def rich_clinical_impression() -> Dict[str, Any]:
    return {
        "resourceType": "ClinicalImpression",
        "id": "ci-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "date": "2024-07-15T10:00:00Z",
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "performer": {"reference": "Practitioner/prac-1"},
        "previous": {"reference": "ClinicalImpression/ci-prev"},
        "identifier": [{"system": "http://hospital.org/ci", "value": "CI-001"}],
        "problem": [{"reference": "Condition/cond-1"}],
        "supportingInfo": [{"reference": "Observation/obs-1"}],
        "finding": [
            {
                "item": {
                    "concept": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "386661006"}]
                    },
                    "reference": {"reference": "Observation/obs-finding"},
                }
            }
        ],
    }


@pytest.fixture
def minimal_clinical_impression() -> Dict[str, Any]:
    return {
        "resourceType": "ClinicalImpression",
        "id": "ci-min",
        "status": "completed",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestClinicalImpressionReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("ClinicalImpression", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("ClinicalImpression", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("ClinicalImpression", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_performer(self, converter):
        q = converter.convert("ClinicalImpression", "performer=prac-1")
        assert "_search.performerId" in str(q)

    def test_previous(self, converter):
        q = converter.convert("ClinicalImpression", "previous=ci-prev")
        assert "_search.previousId" in str(q)

    def test_problem(self, converter):
        q = converter.convert("ClinicalImpression", "problem=cond-1")
        assert "_search.problemIds" in str(q)

    def test_finding_ref(self, converter):
        q = converter.convert("ClinicalImpression", "finding-ref=obs-finding")
        assert "_search.findingReferenceIds" in str(q)

    def test_supporting_info(self, converter):
        q = converter.convert("ClinicalImpression", "supporting-info=obs-1")
        assert "_search.supportingInfoIds" in str(q)


class TestClinicalImpressionTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("ClinicalImpression", "identifier=CI-001")
        assert "CI-001" in str(q)

    def test_finding_code(self, converter):
        q = converter.convert("ClinicalImpression", "finding-code=386661006")
        assert "findingConcept_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("ClinicalImpression", "status=completed")
        assert "completed" in str(q)


class TestClinicalImpressionDateParameters:
    def test_date(self, converter):
        q = converter.convert("ClinicalImpression", "date=ge2024-07-01")
        assert "date" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("ClinicalImpression", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestClinicalImpressionCommonParameters:
    def test_id(self, converter):
        q = converter.convert("ClinicalImpression", "_id=ci-rich")
        assert "ci-rich" in str(q)


class TestClinicalImpressionDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_clinical_impression):
        out = denormalizer.denormalize(minimal_clinical_impression)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_clinical_impression):
        out = denormalizer.denormalize(rich_clinical_impression)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert s["performerId"] == "prac-1"
        assert s["previousId"] == "ci-prev"
        assert "cond-1" in s["problemIds"]
        assert "obs-1" in s["supportingInfoIds"]
        assert "386661006" in s["findingConcept_codes"]
        assert "obs-finding" in s["findingReferenceIds"]
        assert "CI-001" in s["identifier_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert out["_compartments"]["Practitioner"] == ["prac-1"]
        assert out["_compartments"]["Encounter"] == ["enc-1"]

    def test_input_not_mutated(self, denormalizer, rich_clinical_impression):
        original = copy.deepcopy(rich_clinical_impression)
        denormalizer.denormalize(rich_clinical_impression)
        assert rich_clinical_impression == original
