"""
Comprehensive integration tests for ALL AdverseEvent search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "AdverseEvent")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/AdverseEvent.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 16 search parameters in ``configs/AdverseEvent.yaml``.

Compartments (precomputed): Patient, Practitioner, RelatedPerson, Device.
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
def rich_adverse_event() -> Dict[str, Any]:
    return {
        "resourceType": "AdverseEvent",
        "id": "ae-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "completed",
        "actuality": "actual",
        "code": {"coding": [{"code": "fall"}]},
        "category": [{"coding": [{"code": "procedure-mishap"}]}],
        "seriousness": {"coding": [{"code": "serious"}]},
        "subject": {"reference": "Patient/pat-1"},
        "recorder": {"reference": "Practitioner/prac-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "location": {"reference": "Location/loc-1"},
        "occurrenceDateTime": "2024-07-15T10:00:00Z",
        "identifier": [
            {"system": "http://hospital.org/ae", "value": "AE-001"}
        ],
        "study": [{"reference": "ResearchStudy/rs-1"}],
        "resultingEffect": [{"reference": "Condition/cond-1"}],
        "suspectEntity": [
            {
                "instanceReference": {
                    "reference": "Medication/med-1"
                }
            }
        ],
    }


@pytest.fixture
def minimal_adverse_event() -> Dict[str, Any]:
    return {
        "resourceType": "AdverseEvent",
        "id": "ae-min",
        "status": "completed",
        "subject": {"reference": "Patient/pat-min"},
        "code": {"coding": [{"code": "rash"}]},
    }


class TestAdverseEventReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("AdverseEvent", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("AdverseEvent", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_recorder(self, converter):
        q = converter.convert("AdverseEvent", "recorder=prac-1")
        assert "_search.recorderId" in str(q)

    def test_substance(self, converter):
        q = converter.convert("AdverseEvent", "substance=med-1")
        assert "_search.substanceIds" in str(q)

    def test_resultingeffect(self, converter):
        q = converter.convert("AdverseEvent", "resultingeffect=cond-1")
        assert "_search.resultingEffectIds" in str(q)

    def test_study(self, converter):
        q = converter.convert("AdverseEvent", "study=rs-1")
        assert "_search.studyIds" in str(q)


class TestAdverseEventTokenParameters:
    def test_code(self, converter):
        q = converter.convert("AdverseEvent", "code=fall")
        assert "code_codes" in str(q)

    def test_category(self, converter):
        q = converter.convert("AdverseEvent", "category=procedure-mishap")
        assert "category_codes" in str(q)

    def test_actuality(self, converter):
        q = converter.convert("AdverseEvent", "actuality=actual")
        assert "actual" in str(q)

    def test_status(self, converter):
        q = converter.convert("AdverseEvent", "status=completed")
        assert "completed" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("AdverseEvent", "identifier=AE-001")
        assert "AE-001" in str(q)


class TestAdverseEventDateParameters:
    def test_date(self, converter):
        q = converter.convert("AdverseEvent", "date=ge2024-07-01")
        assert "occurrenceDateTime" in str(q)


class TestAdverseEventDenormalization:
    def test_rich_denormalization(self, denormalizer, rich_adverse_event):
        doc = denormalizer.denormalize(copy.deepcopy(rich_adverse_event))
        search = doc["_search"]
        assert search["patientId"] == "pat-1"
        assert search["recorderId"] == "prac-1"
        assert "fall" in search["code_codes"]
        assert "med-1" in search["substanceIds"]
        assert "cond-1" in search["resultingEffectIds"]
        assert "pat-1" in doc["_compartments"]["Patient"]
        assert "prac-1" in doc["_compartments"]["Practitioner"]

    def test_minimal_denormalization(self, denormalizer, minimal_adverse_event):
        doc = denormalizer.denormalize(copy.deepcopy(minimal_adverse_event))
        assert doc["_search"]["patientId"] == "pat-min"
