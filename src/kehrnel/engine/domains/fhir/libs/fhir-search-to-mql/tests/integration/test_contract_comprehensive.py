"""
Comprehensive integration tests for ALL Contract search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Contract")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Contract.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 12 search parameters in ``configs/Contract.yaml``.

Compartments (precomputed): Patient.
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
def rich_contract() -> Dict[str, Any]:
    return {
        "resourceType": "Contract",
        "id": "contract-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "executed",
        "issued": "2024-07-15T10:00:00Z",
        "url": "http://example.org/contracts/contract-rich",
        "instantiatesUri": "http://example.org/contract-templates/base",
        "subject": [{"reference": "Patient/pat-1"}],
        "signer": [
            {
                "party": {"reference": "Practitioner/prac-1"},
                "type": {"code": "SELF"},
            }
        ],
        "authority": [{"reference": "Organization/org-1"}],
        "domain": [{"reference": "Location/loc-1"}],
        "identifier": [{"system": "http://hospital.org/contract", "value": "CTR-001"}],
    }


@pytest.fixture
def minimal_contract() -> Dict[str, Any]:
    return {
        "resourceType": "Contract",
        "id": "contract-min",
        "status": "executed",
        "subject": [{"reference": "Patient/pat-min"}],
    }


class TestContractReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("Contract", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("Contract", "subject=pat-1")
        assert "_search.subjectIds" in str(q)

    def test_signer(self, converter):
        q = converter.convert("Contract", "signer=prac-1")
        assert "_search.signerIds" in str(q)

    def test_authority(self, converter):
        q = converter.convert("Contract", "authority=org-1")
        assert "_search.authorityIds" in str(q)

    def test_domain(self, converter):
        q = converter.convert("Contract", "domain=loc-1")
        assert "_search.domainIds" in str(q)


class TestContractTokenParameters:
    def test_identifier(self, converter):
        q = converter.convert("Contract", "identifier=CTR-001")
        assert "CTR-001" in str(q)

    def test_status(self, converter):
        q = converter.convert("Contract", "status=executed")
        assert "executed" in str(q)


class TestContractDateParameters:
    def test_issued(self, converter):
        q = converter.convert("Contract", "issued=ge2024-07-01")
        assert "issued" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Contract", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestContractUriParameters:
    def test_instantiates(self, converter):
        q = converter.convert(
            "Contract",
            "instantiates=http://example.org/contract-templates/base",
        )
        assert "instantiatesUri" in str(q)

    def test_url(self, converter):
        q = converter.convert(
            "Contract",
            "url=http://example.org/contracts/contract-rich",
        )
        assert "url" in str(q)


class TestContractCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Contract", "_id=contract-rich")
        assert "contract-rich" in str(q)


class TestContractDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_contract):
        out = denormalizer.denormalize(minimal_contract)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"
        assert out["_compartments"]["Patient"] == ["pat-min"]

    def test_rich_fields(self, denormalizer, rich_contract):
        out = denormalizer.denormalize(rich_contract)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "pat-1" in s["subjectIds"]
        assert "prac-1" in s["signerIds"]
        assert "org-1" in s["authorityIds"]
        assert "loc-1" in s["domainIds"]
        assert "CTR-001" in s["identifier_values"]
        assert out["_compartments"]["Patient"] == ["pat-1"]

    def test_input_not_mutated(self, denormalizer, rich_contract):
        original = copy.deepcopy(rich_contract)
        denormalizer.denormalize(rich_contract)
        assert rich_contract == original
