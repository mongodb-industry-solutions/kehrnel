"""
Comprehensive integration tests for ALL Claim search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Claim")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Claim.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, encounter.json

Exercises 19 search parameters in ``configs/Claim.yaml``.

Compartments (precomputed): Patient, Practitioner, Encounter.
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
def rich_claim() -> Dict[str, Any]:
    return {
        "resourceType": "Claim",
        "id": "claim-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "use": "claim",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/claim-type",
                    "code": "professional",
                }
            ]
        },
        "patient": {"reference": "Patient/pat-1"},
        "created": "2024-07-15",
        "enterer": {"reference": "Practitioner/pr-enterer"},
        "provider": {"reference": "Practitioner/pr-provider"},
        "insurer": {"reference": "Organization/org-ins"},
        "facility": {"reference": "Location/loc-1"},
        "priority": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/processpriority", "code": "normal"}]
        },
        "identifier": [{"system": "http://hospital.org/claim", "value": "CLM-001"}],
        "payee": {"party": {"reference": "Practitioner/pr-payee"}},
        "careTeam": [{"provider": {"reference": "Practitioner/pr-ct"}}],
        "item": [
            {
                "encounter": [{"reference": "Encounter/enc-1"}],
                "udi": [{"reference": "Device/dev-item"}],
                "detail": [
                    {
                        "udi": [{"reference": "Device/dev-detail"}],
                        "subDetail": [
                            {"udi": [{"reference": "Device/dev-sub"}]},
                        ],
                    }
                ],
            }
        ],
        "procedure": [{"udi": [{"reference": "Device/dev-proc"}]}],
    }


@pytest.fixture
def minimal_claim() -> Dict[str, Any]:
    return {
        "resourceType": "Claim",
        "id": "claim-min",
        "status": "active",
        "use": "claim",
        "type": {"coding": [{"code": "professional"}]},
        "patient": {"reference": "Patient/pat-min"},
    }


class TestClaimReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Claim", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_provider(self, converter):
        q = converter.convert("Claim", "provider=pr-provider")
        assert "_search.providerId" in str(q)

    def test_enterer(self, converter):
        q = converter.convert("Claim", "enterer=pr-enterer")
        assert "_search.entererId" in str(q)

    def test_insurer(self, converter):
        q = converter.convert("Claim", "insurer=org-ins")
        assert "_search.insurerId" in str(q)

    def test_facility(self, converter):
        q = converter.convert("Claim", "facility=loc-1")
        assert "_search.facilityId" in str(q)

    def test_payee(self, converter):
        q = converter.convert("Claim", "payee=pr-payee")
        assert "_search.payeePartyId" in str(q)

    def test_care_team(self, converter):
        q = converter.convert("Claim", "care-team=pr-ct")
        assert "_search.careTeamProviderIds" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("Claim", "encounter=enc-1")
        assert "_search.itemEncounterIds" in str(q)

    def test_item_udi(self, converter):
        q = converter.convert("Claim", "item-udi=dev-item")
        assert "_search.itemUdiIds" in str(q)

    def test_detail_udi(self, converter):
        q = converter.convert("Claim", "detail-udi=dev-detail")
        assert "_search.detailUdiIds" in str(q)

    def test_subdetail_udi(self, converter):
        q = converter.convert("Claim", "subdetail-udi=dev-sub")
        assert "_search.subdetailUdiIds" in str(q)

    def test_procedure_udi(self, converter):
        q = converter.convert("Claim", "procedure-udi=dev-proc")
        assert "_search.procedureUdiIds" in str(q)


class TestClaimTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Claim", "status=active") == {"status": "active"}

    def test_use(self, converter):
        assert converter.convert("Claim", "use=claim") == {"use": "claim"}

    def test_priority(self, converter):
        q = converter.convert("Claim", "priority=normal")
        assert "normal" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Claim", "identifier=CLM-001")
        assert "CLM-001" in str(q)


class TestClaimDateParameters:
    def test_created(self, converter):
        q = converter.convert("Claim", "created=ge2024-07-15")
        assert "created" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Claim", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestClaimCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Claim", "_id=claim-rich")
        assert "claim-rich" in str(q)


class TestClaimDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_claim):
        out = denormalizer.denormalize(minimal_claim)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"

    def test_rich_fields(self, denormalizer, rich_claim):
        out = denormalizer.denormalize(rich_claim)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["providerId"] == "pr-provider"
        assert s["entererId"] == "pr-enterer"
        assert s["insurerId"] == "org-ins"
        assert s["facilityId"] == "loc-1"
        assert s["payeePartyId"] == "pr-payee"
        assert "pr-ct" in s["careTeamProviderIds"]
        assert "enc-1" in s["itemEncounterIds"]
        assert "dev-item" in s["itemUdiIds"]
        assert "dev-detail" in s["detailUdiIds"]
        assert "dev-sub" in s["subdetailUdiIds"]
        assert "dev-proc" in s["procedureUdiIds"]
        assert "normal" in s["priority_codes"]
        assert "CLM-001" in s["identifier_values"]

    def test_input_not_mutated(self, denormalizer, rich_claim):
        original = copy.deepcopy(rich_claim)
        denormalizer.denormalize(rich_claim)
        assert rich_claim == original


class TestClaimPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_claim):
        out = denormalizer.denormalize(rich_claim)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_claim):
        out = denormalizer.denormalize(rich_claim)
        prac = out["_compartments"]["Practitioner"]
        assert "pr-provider" in prac
        assert "pr-enterer" in prac
        assert "pr-payee" in prac
        assert "pr-ct" in prac

    def test_encounter_compartment(self, denormalizer, rich_claim):
        out = denormalizer.denormalize(rich_claim)
        assert "enc-1" in out["_compartments"]["Encounter"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Claim", "status=active"
        )
        assert "_compartments.Patient" in str(q)
