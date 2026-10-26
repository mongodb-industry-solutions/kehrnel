"""
Comprehensive integration tests for ALL Coverage search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Coverage")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Coverage.yaml
- src/fhir_search_to_mql/compartments/definitions/patient.json

Exercises 15 search parameters in ``configs/Coverage.yaml``.

Compartments (precomputed): Patient.
"""
from __future__ import annotations

import copy
from datetime import datetime
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
def rich_coverage() -> Dict[str, Any]:
    return {
        "resourceType": "Coverage",
        "id": "cov-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "beneficiary": {"reference": "Patient/pat-1"},
        "insurer": {"reference": "Organization/org-ins"},
        "subscriber": {"reference": "Patient/p-sub"},
        "policyHolder": {"reference": "Patient/p-holder"},
        "dependent": "01",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    "code": "EHCPOL",
                }
            ]
        },
        "identifier": [{"system": "http://hospital.org/cov", "value": "COV-001"}],
        "subscriberId": [{"system": "http://payer.org/sub", "value": "SUB-001"}],
        "class": [
            {
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/coverage-class",
                            "code": "group",
                        }
                    ]
                },
                "value": {"system": "http://payer.org/group", "value": "GRP-100"},
            }
        ],
        "paymentBy": [{"party": {"reference": "Patient/pat-1"}}],
    }


@pytest.fixture
def minimal_coverage() -> Dict[str, Any]:
    return {
        "resourceType": "Coverage",
        "id": "cov-min",
        "status": "active",
        "beneficiary": {"reference": "Patient/pat-min"},
        "type": {"coding": [{"code": "EHCPOL"}]},
    }


class TestCoverageReferenceParameters:
    def test_beneficiary(self, converter):
        q = converter.convert("Coverage", "beneficiary=pat-1")
        assert "pat-1" in str(q)

    def test_patient_alias(self, converter):
        q = converter.convert("Coverage", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_insurer(self, converter):
        q = converter.convert("Coverage", "insurer=org-ins")
        assert "_search.insurerId" in str(q)

    def test_policy_holder(self, converter):
        q = converter.convert("Coverage", "policy-holder=p-holder")
        assert "_search.policyHolderId" in str(q)

    def test_subscriber(self, converter):
        q = converter.convert("Coverage", "subscriber=p-sub")
        assert "_search.subscriberRefId" in str(q)

    def test_paymentby_party(self, converter):
        q = converter.convert("Coverage", "paymentby-party=pat-1")
        assert "_search.paymentByPartyIds" in str(q)


class TestCoverageTokenParameters:
    def test_status(self, converter):
        assert converter.convert("Coverage", "status=active") == {"status": "active"}

    def test_type(self, converter):
        q = converter.convert("Coverage", "type=EHCPOL")
        assert "EHCPOL" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Coverage", "identifier=COV-001")
        assert "COV-001" in str(q)

    def test_subscriberid(self, converter):
        q = converter.convert("Coverage", "subscriberid=SUB-001")
        assert "SUB-001" in str(q)

    def test_class_type(self, converter):
        q = converter.convert("Coverage", "class-type=group")
        assert "group" in str(q)

    def test_class_value(self, converter):
        q = converter.convert("Coverage", "class-value=GRP-100")
        assert "GRP-100" in str(q)


class TestCoverageStringParameters:
    def test_dependent(self, converter):
        q = converter.convert("Coverage", "dependent=01")
        assert "_search.dependent_lower" in str(q) or "dependent" in str(q)

    def test_dependent_exact(self, converter):
        q = converter.convert("Coverage", "dependent:exact=01")
        assert "_search.dependent" in str(q)


class TestCoverageCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Coverage", "_id=cov-rich")
        assert "cov-rich" in str(q)

    def test_last_updated(self, converter):
        q = converter.convert("Coverage", "_lastUpdated=ge2024-08-01")
        assert "meta.lastUpdated" in str(q)


class TestCoverageDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_coverage):
        out = denormalizer.denormalize(minimal_coverage)
        s = out.get("_search", {})
        assert s["beneficiaryId"] == "pat-min"
        assert s["patientId"] == "pat-min"
        assert "EHCPOL" in s["type_codes"]

    def test_rich_fields(self, denormalizer, rich_coverage):
        out = denormalizer.denormalize(rich_coverage)
        s = out["_search"]
        assert s["beneficiaryId"] == "pat-1"
        assert s["patientId"] == "pat-1"
        assert s["insurerId"] == "org-ins"
        assert s["policyHolderId"] == "p-holder"
        assert s["subscriberRefId"] == "p-sub"
        assert "pat-1" in s["paymentByPartyIds"]
        assert "EHCPOL" in s["type_codes"]
        assert "COV-001" in s["identifier_values"]
        assert "SUB-001" in s["subscriberId_values"]
        assert "group" in s["classType_codes"]
        assert "GRP-100" in s["classValue_values"]
        assert s["dependent"] == "01"
        assert s["dependent_lower"] == "01"

    def test_input_not_mutated(self, denormalizer, rich_coverage):
        original = copy.deepcopy(rich_coverage)
        denormalizer.denormalize(rich_coverage)
        assert rich_coverage == original


class TestCoveragePrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_coverage):
        out = denormalizer.denormalize(rich_coverage)
        patient_ids = out["_compartments"]["Patient"]
        assert "pat-1" in patient_ids
        assert "p-sub" in patient_ids
        assert "p-holder" in patient_ids

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Coverage", "status=active"
        )
        assert "_compartments.Patient" in str(q)


def _mongo_available() -> bool:
    try:
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=2000)
        client.server_info()
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.mongodb
@pytest.mark.skipif(not _mongo_available(), reason="MongoDB not running")
class TestCoverageMongoDB:
    def test_query_against_seeded_collection(self, converter, denormalizer):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_mql_test"]
        coll = db["Coverage_integration"]
        coll.drop()

        doc = {
            "resourceType": "Coverage",
            "id": "cov-mongo",
            "status": "active",
            "beneficiary": {"reference": "Patient/p-mongo"},
            "type": {"coding": [{"code": "EHCPOL"}]},
        }
        coll.insert_one(denormalizer.denormalize(doc))

        conv = converter.convert("Coverage", "status=active")
        results = list(coll.find(conv))
        assert len(results) >= 1
        client.close()
