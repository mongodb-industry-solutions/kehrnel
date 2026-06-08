"""
Comprehensive integration tests for ALL AuditEvent search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "AuditEvent")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/AuditEvent.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, device.json

Exercises 17 search parameters in ``configs/AuditEvent.yaml``.

Compartments (precomputed): Patient, Device.
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
def rich_audit_event() -> Dict[str, Any]:
    return {
        "resourceType": "AuditEvent",
        "id": "ae-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "action": "R",
        "recorded": "2024-07-15T10:00:00Z",
        "code": {
            "coding": [
                {
                    "system": "http://dicom.nema.org/resources/ontology/DCM",
                    "code": "110100",
                }
            ]
        },
        "category": [{"coding": [{"code": "rest"}]}],
        "patient": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "agent": [
            {
                "who": {"reference": "Practitioner/prac-1"},
                "requestor": True,
                "role": [{"coding": [{"code": "implementer"}]}],
                "policy": ["http://example.org/policy/audit"],
                "authorization": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-ActReason",
                                "code": "ETREAT",
                            }
                        ]
                    }
                ],
            },
            {"who": {"reference": "Device/dev-1"}},
        ],
        "source": {"observer": {"reference": "Device/dev-1"}},
        "entity": [
            {
                "what": {"reference": "Patient/pat-1"},
                "role": {"coding": [{"code": "1"}]},
            }
        ],
        "outcome": {"code": {"code": "0"}},
        "authorization": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActReason",
                        "code": "PATADMIN",
                    }
                ]
            }
        ],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
    }


@pytest.fixture
def minimal_audit_event() -> Dict[str, Any]:
    return {
        "resourceType": "AuditEvent",
        "id": "ae-min",
        "action": "R",
        "code": {"coding": [{"code": "110100"}]},
        "agent": [{"who": {"reference": "Practitioner/prac-min"}}],
        "source": {"observer": {"reference": "Device/dev-min"}},
        "patient": {"reference": "Patient/pat-min"},
    }


class TestAuditEventReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("AuditEvent", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_agent(self, converter):
        q = converter.convert("AuditEvent", "agent=prac-1")
        assert "_search.agentIds" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("AuditEvent", "encounter=enc-1")
        assert "_search.encounterId" in str(q)

    def test_entity(self, converter):
        q = converter.convert("AuditEvent", "entity=pat-1")
        assert "_search.entityWhatIds" in str(q)

    def test_source(self, converter):
        q = converter.convert("AuditEvent", "source=dev-1")
        assert "_search.sourceObserverId" in str(q)

    def test_based_on(self, converter):
        q = converter.convert("AuditEvent", "based-on=sr-1")
        assert "_search.basedOnIds" in str(q)


class TestAuditEventTokenParameters:
    def test_action(self, converter):
        q = converter.convert("AuditEvent", "action=R")
        assert "R" in str(q)

    def test_code(self, converter):
        q = converter.convert("AuditEvent", "code=110100")
        assert "110100" in str(q)

    def test_category(self, converter):
        q = converter.convert("AuditEvent", "category=rest")
        assert "rest" in str(q)

    def test_agent_role(self, converter):
        q = converter.convert("AuditEvent", "agent-role=implementer")
        assert "implementer" in str(q)

    def test_entity_role(self, converter):
        q = converter.convert("AuditEvent", "entity-role=1")
        assert "entityRole_codes" in str(q)

    def test_outcome(self, converter):
        q = converter.convert("AuditEvent", "outcome=0")
        assert "outcome_codes" in str(q)

    def test_purpose(self, converter):
        q = converter.convert("AuditEvent", "purpose=PATADMIN")
        assert "purpose_codes" in str(q)


class TestAuditEventDateParameters:
    def test_date(self, converter):
        q = converter.convert("AuditEvent", "date=ge2024-07-01")
        assert "recorded" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("AuditEvent", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestAuditEventUriParameters:
    def test_policy(self, converter):
        q = converter.convert(
            "AuditEvent",
            "policy=http://example.org/policy/audit",
        )
        assert "policy_values" in str(q)


class TestAuditEventCommonParameters:
    def test_id(self, converter):
        q = converter.convert("AuditEvent", "_id=ae-rich")
        assert "ae-rich" in str(q)


class TestAuditEventDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_audit_event):
        out = denormalizer.denormalize(minimal_audit_event)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_audit_event):
        out = denormalizer.denormalize(rich_audit_event)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["encounterId"] == "enc-1"
        assert "prac-1" in s["agentIds"]
        assert s["sourceObserverId"] == "dev-1"
        assert "110100" in s["code_codes"]
        assert "rest" in s["category_codes"]
        assert "implementer" in s["agentRole_codes"]
        assert "pat-1" in s["entityWhatIds"]
        assert "1" in s["entityRole_codes"]
        assert "0" in s["outcome_codes"]
        assert "PATADMIN" in s["purpose_codes"]
        assert "ETREAT" in s["purpose_codes"]
        assert "http://example.org/policy/audit" in s["policy_values"]
        assert "sr-1" in s["basedOnIds"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
        assert "dev-1" in out["_compartments"]["Device"]

    def test_input_not_mutated(self, denormalizer, rich_audit_event):
        original = copy.deepcopy(rich_audit_event)
        denormalizer.denormalize(rich_audit_event)
        assert rich_audit_event == original
