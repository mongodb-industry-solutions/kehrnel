"""Comprehensive integration tests for Provenance search parameters."""
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
def rich_provenance() -> Dict[str, Any]:
    return {
        "resourceType": "Provenance",
        "id": "prov-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "target": [{"reference": "Observation/obs-1"}],
        "recorded": "2024-07-15T10:00:00Z",
        "occurredDateTime": "2024-07-14T08:00:00Z",
        "activity": {"coding": [{"code": "CREATE"}]},
        "patient": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "agent": [
            {
                "who": {"reference": "Practitioner/prac-1"},
                "role": [{"coding": [{"code": "author"}]}],
                "type": {"coding": [{"code": "practitioner"}]},
            }
        ],
        "entity": [{"what": {"reference": "Device/dev-1"}}],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
        "signature": [{"type": [{"code": "ProofOfOrigin"}]}],
    }


class TestProvenanceReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Provenance", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_target(self, converter):
        q = converter.convert("Provenance", "target=obs-1")
        assert "_search.targetIds" in str(q)

    def test_agent(self, converter):
        q = converter.convert("Provenance", "agent=prac-1")
        assert "_search.agentIds" in str(q)


class TestProvenanceTokenParameters:
    def test_activity(self, converter):
        q = converter.convert("Provenance", "activity=CREATE")
        assert "CREATE" in str(q)

    def test_agent_role(self, converter):
        q = converter.convert("Provenance", "agent-role=author")
        assert "agentRole_codes" in str(q)

    def test_signature_type(self, converter):
        q = converter.convert("Provenance", "signature-type=ProofOfOrigin")
        assert "signatureType_codes" in str(q)


class TestProvenanceDateParameters:
    def test_recorded(self, converter):
        q = converter.convert("Provenance", "recorded=ge2024-07-01")
        assert "recorded" in str(q)

    def test_when(self, converter):
        q = converter.convert("Provenance", "when=ge2024-07-01")
        assert "occurredDateTime" in str(q)


class TestProvenanceCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "Provenance")
        assert q == {"_compartments.Patient": "pat-1"}


class TestProvenanceDenormalization:
    def test_rich(self, denormalizer, rich_provenance):
        out = denormalizer.denormalize(copy.deepcopy(rich_provenance))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "obs-1" in s["targetIds"]
        assert "prac-1" in s["agentIds"]
        assert out["_compartments"]["Patient"] == ["pat-1"]
