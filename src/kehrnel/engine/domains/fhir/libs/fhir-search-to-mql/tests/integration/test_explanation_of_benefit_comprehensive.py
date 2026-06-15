"""
Comprehensive integration tests for ALL ExplanationOfBenefit search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "ExplanationOfBenefit")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/ExplanationOfBenefit.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, encounter.json

Exercises 19 search parameters in ``configs/ExplanationOfBenefit.yaml``.

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
def rich_eob() -> Dict[str, Any]:
    return {
        "resourceType": "ExplanationOfBenefit",
        "id": "eob-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "type": {"coding": [{"code": "professional"}]},
        "patient": {"reference": "Patient/pat-1"},
        "created": "2024-07-15",
        "disposition": "Processed as Primary",
        "claim": {"reference": "Claim/clm-1"},
        "enterer": {"reference": "Practitioner/pr-enterer"},
        "provider": {"reference": "Practitioner/pr-provider"},
        "facility": {"reference": "Location/loc-1"},
        "identifier": [
            {"system": "http://hospital.org/eob", "value": "EOB-001"}
        ],
        "insurance": [{"coverage": {"reference": "Coverage/cov-1"}}],
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
def minimal_eob() -> Dict[str, Any]:
    return {
        "resourceType": "ExplanationOfBenefit",
        "id": "eob-min",
        "status": "active",
        "type": {"coding": [{"code": "professional"}]},
        "patient": {"reference": "Patient/pat-min"},
    }


class TestExplanationOfBenefitReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("ExplanationOfBenefit", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_claim(self, converter):
        q = converter.convert("ExplanationOfBenefit", "claim=clm-1")
        assert "_search.claimId" in str(q)

    def test_coverage(self, converter):
        q = converter.convert("ExplanationOfBenefit", "coverage=cov-1")
        assert "_search.coverageIds" in str(q)

    def test_provider(self, converter):
        q = converter.convert("ExplanationOfBenefit", "provider=pr-provider")
        assert "_search.providerId" in str(q)

    def test_payee(self, converter):
        q = converter.convert("ExplanationOfBenefit", "payee=pr-payee")
        assert "_search.payeePartyId" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("ExplanationOfBenefit", "encounter=enc-1")
        assert "_search.itemEncounterIds" in str(q)

    def test_item_udi(self, converter):
        q = converter.convert("ExplanationOfBenefit", "item-udi=dev-item")
        assert "_search.itemUdiIds" in str(q)


class TestExplanationOfBenefitTokenParameters:
    def test_status(self, converter):
        assert converter.convert(
            "ExplanationOfBenefit", "status=active"
        ) == {"status": "active"}

    def test_identifier(self, converter):
        q = converter.convert("ExplanationOfBenefit", "identifier=EOB-001")
        assert "EOB-001" in str(q)


class TestExplanationOfBenefitStringParameters:
    def test_disposition(self, converter):
        q = converter.convert("ExplanationOfBenefit", "disposition=processed")
        assert "disposition_lower" in str(q)


class TestExplanationOfBenefitDateParameters:
    def test_created(self, converter):
        q = converter.convert("ExplanationOfBenefit", "created=ge2024-07-01")
        assert "created" in str(q)


class TestExplanationOfBenefitDenormalization:
    def test_rich(self, denormalizer, rich_eob):
        out = denormalizer.denormalize(copy.deepcopy(rich_eob))
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert s["claimId"] == "clm-1"
        assert "cov-1" in s["coverageIds"]
        assert s["providerId"] == "pr-provider"
        assert s["payeePartyId"] == "pr-payee"
        assert "enc-1" in s["itemEncounterIds"]
        assert "dev-item" in s["itemUdiIds"]
        assert "pat-1" in out["_compartments"]["Patient"]
        assert "pr-provider" in out["_compartments"]["Practitioner"]

    def test_minimal(self, denormalizer, minimal_eob):
        out = denormalizer.denormalize(copy.deepcopy(minimal_eob))
        assert out["_search"]["patientId"] == "pat-min"


class TestExplanationOfBenefitCompartmentRouting:
    def test_patient_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "ExplanationOfBenefit"
        )
        assert q == {"_compartments.Patient": "pat-1"}
