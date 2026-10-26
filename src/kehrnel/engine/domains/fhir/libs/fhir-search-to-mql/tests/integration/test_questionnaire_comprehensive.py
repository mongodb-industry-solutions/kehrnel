"""
Comprehensive integration tests for ALL Questionnaire search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Questionnaire")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Questionnaire.yaml

Exercises 20 shipped search parameters in ``configs/Questionnaire.yaml``
(3 composite/quantity params deferred).

Compartments: none.
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
def rich_questionnaire() -> Dict[str, Any]:
    return {
        "resourceType": "Questionnaire",
        "id": "quest-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "version": "1.0",
        "url": "http://example.org/Questionnaire/phq9",
        "name": "phq9",
        "title": "PHQ-9 Questionnaire",
        "publisher": "Example Health",
        "description": "Patient health questionnaire",
        "date": "2024-06-01",
        "subjectType": ["Patient"],
        "code": [{"system": "http://loinc.org", "code": "44249-1"}],
        "jurisdiction": [{"coding": [{"code": "US"}]}],
        "effectivePeriod": {"start": "2024-01-01", "end": "2025-12-31"},
        "identifier": [
            {"system": "http://hospital.org/quest", "value": "Q-001"}
        ],
        "useContext": [
            {
                "code": {"system": "http://terminology.hl7.org/CodeSystem/usage-context-type", "code": "venue"},
                "valueCodeableConcept": {"coding": [{"code": "ambulatory"}]},
            }
        ],
        "item": [
            {
                "linkId": "1",
                "code": [{"code": "item-code-1"}],
                "definition": "http://example.org/item/1",
            }
        ],
    }


@pytest.fixture
def minimal_questionnaire() -> Dict[str, Any]:
    return {
        "resourceType": "Questionnaire",
        "id": "quest-min",
        "status": "draft",
    }


class TestQuestionnaireTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Questionnaire", "status=active") == {
            "status": "active"
        }

    def test_questionnaire_code(self, converter):
        q = converter.convert("Questionnaire", "questionnaire-code=44249-1")
        assert "44249-1" in str(q)

    def test_combo_code(self, converter):
        q = converter.convert("Questionnaire", "combo-code=item-code-1")
        assert "comboCode_codes" in str(q)

    def test_item_code(self, converter):
        q = converter.convert("Questionnaire", "item-code=item-code-1")
        assert "itemCode_codes" in str(q)

    def test_context(self, converter):
        q = converter.convert("Questionnaire", "context=ambulatory")
        assert "context_codes" in str(q)

    def test_context_type(self, converter):
        q = converter.convert("Questionnaire", "context-type=venue")
        assert "contextType_codes" in str(q)

    def test_jurisdiction(self, converter):
        q = converter.convert("Questionnaire", "jurisdiction=US")
        assert "jurisdiction_codes" in str(q)

    def test_subject_type(self, converter):
        q = converter.convert("Questionnaire", "subject-type=Patient")
        assert "subjectType" in str(q)

    def test_version(self, converter):
        assert converter.convert("Questionnaire", "version=1.0") == {
            "version": "1.0"
        }

    def test_identifier(self, converter):
        q = converter.convert("Questionnaire", "identifier=Q-001")
        assert "Q-001" in str(q)


class TestQuestionnaireStringParameters:
    def test_title(self, converter):
        q = converter.convert("Questionnaire", "title=PHQ")
        assert "title_lower" in str(q)

    def test_name(self, converter):
        q = converter.convert("Questionnaire", "name=phq")
        assert "name_lower" in str(q)

    def test_publisher(self, converter):
        q = converter.convert("Questionnaire", "publisher=Example")
        assert "publisher_lower" in str(q)

    def test_description(self, converter):
        q = converter.convert("Questionnaire", "description=health")
        assert "description_lower" in str(q)


class TestQuestionnaireUriParameters:
    def test_url(self, converter):
        q = converter.convert(
            "Questionnaire",
            "url=http://example.org/Questionnaire/phq9",
        )
        assert "url" in str(q)

    def test_definition(self, converter):
        q = converter.convert(
            "Questionnaire",
            "definition=http://example.org/item/1",
        )
        assert "itemDefinition_values" in str(q)


class TestQuestionnaireDateParameters:
    def test_date(self, converter):
        q = converter.convert("Questionnaire", "date=ge2024-06-01")
        assert "date" in str(q)

    def test_effective(self, converter):
        q = converter.convert("Questionnaire", "effective=ge2024-01-01")
        assert "effectivePeriod" in str(q)


class TestQuestionnaireDenormalization:
    def test_rich(self, denormalizer, rich_questionnaire):
        out = denormalizer.denormalize(copy.deepcopy(rich_questionnaire))
        s = out["_search"]
        assert "44249-1" in s["questionnaireCode_codes"]
        assert "item-code-1" in s["itemCode_codes"]
        assert "ambulatory" in s["context_codes"]
        assert "venue" in s["contextType_codes"]
        assert "US" in s["jurisdiction_codes"]
        assert out["subjectType"] == ["Patient"]
        assert "http://example.org/item/1" in s["itemDefinition_values"]
        assert "_compartments" not in out

    def test_minimal_sparse(self, denormalizer, minimal_questionnaire):
        out = denormalizer.denormalize(copy.deepcopy(minimal_questionnaire))
        search = out.get("_search", {})
        assert "questionnaireCode_codes" not in search
