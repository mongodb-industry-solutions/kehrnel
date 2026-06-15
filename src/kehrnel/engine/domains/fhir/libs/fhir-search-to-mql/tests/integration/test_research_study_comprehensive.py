"""
Comprehensive integration tests for ALL ResearchStudy search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "ResearchStudy")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ResearchStudy.yaml

Exercises 27 search parameters in ``configs/ResearchStudy.yaml``.

No precomputed compartments (practitioner.json `principalinvestigator`
does not match R5 `associatedParty` / search index).
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
def rich_research_study() -> Dict[str, Any]:
    return {
        "resourceType": "ResearchStudy",
        "id": "rs-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "title": "Hypertension Trial",
        "name": "HTN-Study",
        "description": "A phase II hypertension study",
        "phase": {"coding": [{"code": "phase-2"}]},
        "period": {"start": "2024-01-01", "end": "2025-12-31"},
        "identifier": [
            {"system": "http://hospital.org/rs", "value": "RS-001"}
        ],
        "condition": [{"coding": [{"code": "38341003"}]}],
        "keyword": [{"coding": [{"code": "hypertension"}]}],
        "region": [{"coding": [{"code": "US"}]}],
        "studyDesign": [{"coding": [{"code": "interventional"}]}],
        "classifier": [{"coding": [{"code": "nct"}]}],
        "focus": [
            {
                "concept": {
                    "coding": [{"code": "med-focus"}],
                },
                "reference": {
                    "reference": "Medication/med-1",
                },
            }
        ],
        "objective": [
            {
                "description": "Lower blood pressure",
                "type": {"coding": [{"code": "primary"}]},
            }
        ],
        "progressStatus": [
            {
                "state": {"coding": [{"code": "recruiting"}]},
                "actual": True,
                "period": {"start": "2024-06-01", "end": "2024-12-31"},
            }
        ],
        "recruitment": {
            "targetNumber": 100,
            "actualNumber": 42,
            "eligibility": {"reference": "Group/grp-1"},
        },
        "protocol": [{"reference": "PlanDefinition/pd-1"}],
        "site": [{"reference": "Location/loc-1"}],
        "partOf": [{"reference": "ResearchStudy/rs-parent"}],
    }


@pytest.fixture
def minimal_research_study() -> Dict[str, Any]:
    return {
        "resourceType": "ResearchStudy",
        "id": "rs-min",
        "status": "active",
        "title": "Minimal Study",
    }


class TestResearchStudyStringParameters:
    def test_description(self, converter):
        q = converter.convert("ResearchStudy", "description=hypertension")
        assert "description_lower" in str(q)

    def test_name(self, converter):
        q = converter.convert("ResearchStudy", "name=HTN")
        assert "name_lower" in str(q)

    def test_title(self, converter):
        q = converter.convert("ResearchStudy", "title=Trial")
        assert "title_lower" in str(q)

    def test_objective_description(self, converter):
        q = converter.convert("ResearchStudy", "objective-description=pressure")
        assert "objectiveDescription_lower" in str(q)


class TestResearchStudyReferenceParameters:
    def test_eligibility(self, converter):
        q = converter.convert("ResearchStudy", "eligibility=grp-1")
        assert "_search.eligibilityId" in str(q)

    def test_focus_reference(self, converter):
        q = converter.convert("ResearchStudy", "focus-reference=med-1")
        assert "_search.focusReferenceIds" in str(q)

    def test_part_of(self, converter):
        q = converter.convert("ResearchStudy", "part-of=rs-parent")
        assert "_search.partOfIds" in str(q)

    def test_protocol(self, converter):
        q = converter.convert("ResearchStudy", "protocol=pd-1")
        assert "_search.protocolIds" in str(q)

    def test_site(self, converter):
        q = converter.convert("ResearchStudy", "site=loc-1")
        assert "_search.siteIds" in str(q)


class TestResearchStudyTokenParameters:
    def test_classifier(self, converter):
        q = converter.convert("ResearchStudy", "classifier=nct")
        assert "classifier_codes" in str(q)

    def test_condition(self, converter):
        q = converter.convert("ResearchStudy", "condition=38341003")
        assert "condition_codes" in str(q)

    def test_focus_code(self, converter):
        q = converter.convert("ResearchStudy", "focus-code=med-focus")
        assert "focusConcept_codes" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("ResearchStudy", "identifier=RS-001")
        assert "RS-001" in str(q)

    def test_keyword(self, converter):
        q = converter.convert("ResearchStudy", "keyword=hypertension")
        assert "keyword_codes" in str(q)

    def test_objective_type(self, converter):
        q = converter.convert("ResearchStudy", "objective-type=primary")
        assert "objectiveType_codes" in str(q)

    def test_phase(self, converter):
        q = converter.convert("ResearchStudy", "phase=phase-2")
        assert "phase_codes" in str(q)

    def test_region(self, converter):
        q = converter.convert("ResearchStudy", "region=US")
        assert "region_codes" in str(q)

    def test_status(self, converter):
        q = converter.convert("ResearchStudy", "status=active")
        assert "active" in str(q)

    def test_study_design(self, converter):
        q = converter.convert("ResearchStudy", "study-design=interventional")
        assert "studyDesign_codes" in str(q)

    def test_id(self, converter):
        q = converter.convert("ResearchStudy", "_id=rs-rich")
        assert "rs-rich" in str(q)


class TestResearchStudyNumberParameters:
    def test_recruitment_actual(self, converter):
        q = converter.convert("ResearchStudy", "recruitment-actual=42")
        assert "recruitmentActualNumber_values" in str(q)

    def test_recruitment_target(self, converter):
        q = converter.convert("ResearchStudy", "recruitment-target=100")
        assert "recruitmentTargetNumber_values" in str(q)


class TestResearchStudyDateParameters:
    def test_date(self, converter):
        q = converter.convert("ResearchStudy", "date=ge2024-01-01")
        assert "period" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("ResearchStudy", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestResearchStudyCompositeParameters:
    def test_progress_status_state_actual(self, converter):
        q = converter.convert(
            "ResearchStudy", "progress-status-state-actual=recruiting$true"
        )
        s = str(q)
        assert "progressStatusState_codes" in s
        assert "progressStatusActual_values" in s

    def test_progress_status_state_period(self, converter):
        q = converter.convert(
            "ResearchStudy",
            "progress-status-state-period=recruiting$2024-06-01",
        )
        s = str(q)
        assert "progressStatusState_codes" in s
        assert "progressStatusPeriod" in s


class TestResearchStudyDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_research_study):
        doc = denormalizer.denormalize(copy.deepcopy(rich_research_study))
        search = doc["_search"]
        assert search["title"] == "Hypertension Trial"
        assert "38341003" in search["condition_codes"]
        assert "med-focus" in search["focusConcept_codes"]
        assert "med-1" in search["focusReferenceIds"]
        assert "recruiting" in search["progressStatusState_codes"]
        assert True in search["progressStatusActual_values"]
        assert search["recruitmentActualNumber_values"] == 42
        assert search["recruitmentTargetNumber_values"] == 100
        assert search["eligibilityId"] == "grp-1"
        assert "pd-1" in search["protocolIds"]
        assert "loc-1" in search["siteIds"]

    def test_minimal_denormalization(self, denormalizer, minimal_research_study):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_research_study))
        assert doc["_search"]["title"] == "Minimal Study"
