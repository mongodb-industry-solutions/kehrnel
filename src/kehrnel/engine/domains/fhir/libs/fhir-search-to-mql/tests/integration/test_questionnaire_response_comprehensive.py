"""
Comprehensive integration tests for ALL QuestionnaireResponse search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "QuestionnaireResponse")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/QuestionnaireResponse.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 14 search parameters in ``configs/QuestionnaireResponse.yaml``.

Compartments (precomputed): Patient, Practitioner, Encounter, Device.
"""
from __future__ import annotations

import copy
from typing import Any, Dict

import pytest

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer

pytestmark = pytest.mark.integration

_IS_SUBJECT_EXT = (
    "http://hl7.org/fhir/StructureDefinition/questionnaireresponse-isSubject"
)


@pytest.fixture(scope="module")
def converter() -> FHIRSearchConverter:
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.fixture
def rich_questionnaire_response() -> Dict[str, Any]:
    return {
        "resourceType": "QuestionnaireResponse",
        "id": "qr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "questionnaire": "Questionnaire/quest-1",
        "authored": "2024-07-15T10:00:00Z",
        "subject": {"reference": "Patient/pat-1"},
        "author": {"reference": "Practitioner/prac-1"},
        "source": {"reference": "Practitioner/prac-2"},
        "encounter": {"reference": "Encounter/enc-1"},
        "identifier": [
            {"system": "http://hospital.org/qr", "value": "QR-001"}
        ],
        "basedOn": [{"reference": "CarePlan/cp-1"}],
        "partOf": [{"reference": "Observation/obs-1"}],
        "item": [
            {
                "linkId": "subject-item",
                "extension": [{"url": _IS_SUBJECT_EXT, "valueBoolean": True}],
                "answer": [{"valueReference": {"reference": "Patient/pat-subj"}}],
            }
        ],
    }


@pytest.fixture
def minimal_questionnaire_response() -> Dict[str, Any]:
    return {
        "resourceType": "QuestionnaireResponse",
        "id": "qr-min",
        "status": "completed",
        "questionnaire": "Questionnaire/quest-min",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestQuestionnaireResponseReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("QuestionnaireResponse", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("QuestionnaireResponse", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_author(self, converter):
        q = converter.convert("QuestionnaireResponse", "author=prac-1")
        assert "_search.authorId" in str(q)

    def test_source(self, converter):
        q = converter.convert("QuestionnaireResponse", "source=prac-2")
        assert "_search.sourceId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("QuestionnaireResponse", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("QuestionnaireResponse", "based-on=cp-1")
        assert "_search.basedOnIds" in str(q)

    def test_part_of(self, converter):
        q = converter.convert("QuestionnaireResponse", "part-of=obs-1")
        assert "_search.partOfIds" in str(q)

    def test_questionnaire(self, converter):
        q = converter.convert(
            "QuestionnaireResponse", "questionnaire=Questionnaire/quest-1"
        )
        assert "quest-1" in str(q) or "Questionnaire/quest-1" in str(q)

    def test_item_subject(self, converter):
        q = converter.convert("QuestionnaireResponse", "item-subject=pat-subj")
        assert "_search.itemSubjectIds" in str(q)


class TestQuestionnaireResponseTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("QuestionnaireResponse", "identifier=QR-001")
        assert "QR-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("QuestionnaireResponse", "status=completed")
        assert "completed" in str(q)

    def test_id(self, converter):
        q = converter.convert("QuestionnaireResponse", "_id=qr-rich")
        assert "qr-rich" in str(q)


class TestQuestionnaireResponseDateParameters:
    def test_authored(self, converter):
        q = converter.convert("QuestionnaireResponse", "authored=ge2024-07-01")
        assert "authored" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("QuestionnaireResponse", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestQuestionnaireResponseDenormalization:
    def test_rich_denormalization(
        self, denormalizer, rich_questionnaire_response
    ):
        doc = denormalizer.denormalize(copy.deepcopy(rich_questionnaire_response))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["authorId"] == "prac-1"
        assert search["sourceId"] == "prac-2"
        assert search["encounterId"] == "enc-1"
        assert "cp-1" in search["basedOnIds"]
        assert "obs-1" in search["partOfIds"]
        assert "Questionnaire/quest-1" in search["questionnaire_values"]
        assert "pat-subj" in search["itemSubjectIds"]
        assert "QR-001" in search["identifier_values"]
        assert "Patient" in doc["_compartments"]
        assert "pat-1" in doc["_compartments"]["Patient"]

    def test_minimal_denormalization(
        self, denormalizer, minimal_questionnaire_response
    ):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_questionnaire_response))
        assert doc["_search"]["patientId"] == "pat-min"
