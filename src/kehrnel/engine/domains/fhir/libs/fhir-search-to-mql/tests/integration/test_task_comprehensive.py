"""
Comprehensive integration tests for ALL Task search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Task")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Task.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

Exercises 24 search parameters in ``configs/Task.yaml``.

Compartments (precomputed): Patient, Practitioner, Encounter, Device.
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
def rich_task() -> Dict[str, Any]:
    return {
        "resourceType": "Task",
        "id": "task-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "in-progress",
        "intent": "order",
        "priority": "routine",
        "for": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "focus": {"reference": "ServiceRequest/sr-1"},
        "owner": {"reference": "Practitioner/prac-1"},
        "requester": {"reference": "Practitioner/prac-2"},
        "authoredOn": "2024-07-15T09:00:00Z",
        "lastModified": "2024-07-16T10:00:00Z",
        "executionPeriod": {
            "start": "2024-07-15T09:00:00Z",
            "end": "2024-07-20T17:00:00Z",
        },
        "code": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "103693007",
                }
            ]
        },
        "businessStatus": {
            "coding": [{"code": "in-progress"}]
        },
        "identifier": [{"system": "http://hospital.org/task", "value": "TASK-001"}],
        "groupIdentifier": {"system": "http://hospital.org/group", "value": "GRP-1"},
        "basedOn": [{"reference": "CarePlan/cp-1"}],
        "partOf": [{"reference": "Task/parent-1"}],
        "performer": [{"actor": {"reference": "Practitioner/prac-3"}}],
        "requestedPerformer": [
            {
                "concept": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/task-performer-type",
                            "code": "performer",
                        }
                    ]
                },
                "reference": {"reference": "Practitioner/prac-4"},
            }
        ],
        "output": [
            {
                "type": {"text": "result"},
                "valueReference": {"reference": "Observation/obs-1"},
            }
        ],
    }


@pytest.fixture
def minimal_task() -> Dict[str, Any]:
    return {
        "resourceType": "Task",
        "id": "task-min",
        "for": {"reference": "Patient/pat-min"},
    }


class TestTaskReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Task", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Task", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("Task", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_focus(self, converter):
        q = converter.convert("Task", "focus=sr-1")
        assert "_search.focusId" in str(q)

    def test_owner(self, converter):
        q = converter.convert("Task", "owner=prac-1")
        assert "_search.ownerId" in str(q)

    def test_requester(self, converter):
        q = converter.convert("Task", "requester=prac-2")
        assert "_search.requesterId" in str(q)

    def test_actor(self, converter):
        q = converter.convert("Task", "actor=prac-3")
        assert "_search.actorIds" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("Task", "based-on=cp-1")
        assert "_search.basedOnIds" in str(q)

    def test_part_of(self, converter):
        q = converter.convert("Task", "part-of=parent-1")
        assert "_search.partOfIds" in str(q)

    def test_output(self, converter):
        q = converter.convert("Task", "output=obs-1")
        assert "_search.outputReferenceIds" in str(q)

    def test_requestedperformer_reference(self, converter):
        q = converter.convert("Task", "requestedperformer-reference=prac-4")
        assert "_search.requestedPerformerReferenceIds" in str(q)


class TestTaskTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Task", "identifier=TASK-001")
        assert "TASK-001" in str(q)

    def test_group_identifier(self, converter):
        q = converter.convert("Task", "group-identifier=GRP-1")
        assert "GRP-1" in str(q)

    def test_code(self, converter):
        q = converter.convert("Task", "code=103693007")
        assert "103693007" in str(q)

    def test_business_status(self, converter):
        q = converter.convert("Task", "business-status=in-progress")
        assert "in-progress" in str(q)

    def test_status(self, converter):
        q = converter.convert("Task", "status=in-progress")
        assert "in-progress" in str(q)

    def test_intent(self, converter):
        q = converter.convert("Task", "intent=order")
        assert "order" in str(q)

    def test_priority(self, converter):
        q = converter.convert("Task", "priority=routine")
        assert "routine" in str(q)

    def test_performer_token(self, converter):
        q = converter.convert("Task", "performer=performer")
        assert "requestedPerformerConcept_codes" in str(q)


class TestTaskDateParameters:
    def test_authored_on(self, converter):
        q = converter.convert("Task", "authored-on=ge2024-07-01")
        assert "authoredOn" in str(q)

    def test_modified(self, converter):
        q = converter.convert("Task", "modified=ge2024-07-01")
        assert "lastModified" in str(q)

    def test_period(self, converter):
        q = converter.convert("Task", "period=ge2024-07-01")
        assert "executionPeriod" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Task", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestTaskCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Task", "_id=task-rich")
        assert "task-rich" in str(q)


class TestTaskDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_task):
        out = denormalizer.denormalize(minimal_task)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_task):
        out = denormalizer.denormalize(rich_task)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["subjectId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert s["focusId"] == "sr-1"
        assert s["ownerId"] == "prac-1"
        assert s["requesterId"] == "prac-2"
        assert "prac-3" in s["actorIds"]
        assert "cp-1" in s["basedOnIds"]
        assert "parent-1" in s["partOfIds"]
        assert "obs-1" in s["outputReferenceIds"]
        assert "103693007" in s["code_codes"]
        assert "in-progress" in s["businessStatus_codes"]
        assert "performer" in s["requestedPerformerConcept_codes"]
        assert "prac-4" in s["requestedPerformerReferenceIds"]
        assert "TASK-001" in s["identifier_values"]
        assert "GRP-1" in s["groupIdentifier_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert "prac-1" in out["_compartments"]["Practitioner"]
        assert out["_compartments"]["Encounter"] == ["enc-1"]

    def test_input_not_mutated(self, denormalizer, rich_task):
        original = copy.deepcopy(rich_task)
        denormalizer.denormalize(rich_task)
        assert rich_task == original
