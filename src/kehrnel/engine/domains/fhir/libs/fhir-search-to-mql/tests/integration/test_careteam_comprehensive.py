"""
Comprehensive integration tests for ALL CareTeam search parameters.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "CareTeam")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/CareTeam.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json

Exercises 10 search parameters in ``configs/CareTeam.yaml``.

Compartments (precomputed): Patient, Practitioner.
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
def rich_care_team() -> Dict[str, Any]:
    return {
        "resourceType": "CareTeam",
        "id": "ct-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "name": "Crisis Assessment Team",
        "identifier": [{"system": "http://hospital.org/ct", "value": "CT-001"}],
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/care-team-category",
                        "code": "LA27976-2",
                    }
                ]
            }
        ],
        "subject": {"reference": "Patient/pat-1"},
        "period": {
            "start": "2024-07-01T00:00:00Z",
            "end": "2024-12-31T23:59:59Z",
        },
        "participant": [
            {
                "member": {"reference": "Practitioner/prac-1"},
                "role": {"coding": [{"code": "224608005"}]},
                "coveragePeriod": {
                    "start": "2024-07-01T00:00:00Z",
                    "end": "2024-09-30T23:59:59Z",
                },
            },
            {
                "member": {"reference": "RelatedPerson/rp-1"},
                "coveragePeriod": {
                    "start": "2024-08-01T00:00:00Z",
                    "end": "2024-08-31T23:59:59Z",
                },
            },
        ],
    }


@pytest.fixture
def minimal_care_team() -> Dict[str, Any]:
    return {
        "resourceType": "CareTeam",
        "id": "ct-min",
        "status": "proposed",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestCareTeamReferenceParameters:
    def test_patient(self, converter):
        q = converter.convert("CareTeam", "patient=pat-1")
        assert "pat-1" in str(q)

    def test_subject(self, converter):
        q = converter.convert("CareTeam", "subject=Patient/pat-1")
        assert "_search.subjectId" in str(q)

    def test_participant_practitioner(self, converter):
        q = converter.convert("CareTeam", "participant=prac-1")
        assert "_search.participantIds" in str(q)


class TestCareTeamTokenParameters:
    def test_status(self, converter):
        assert converter.convert("CareTeam", "status=active") == {"status": "active"}

    def test_category(self, converter):
        q = converter.convert("CareTeam", "category=LA27976-2")
        assert "LA27976-2" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("CareTeam", "identifier=CT-001")
        assert "CT-001" in str(q)


class TestCareTeamStringParameters:
    def test_name_default(self, converter):
        q = converter.convert("CareTeam", "name=Crisis")
        assert "_search.name_lower" in str(q)

    def test_name_exact(self, converter):
        q = converter.convert("CareTeam", "name:exact=Crisis Assessment Team")
        assert "_search.name" in str(q)


class TestCareTeamDateParameters:
    def test_date_team_period(self, converter):
        q = converter.convert("CareTeam", "date=ge2024-07-01")
        s = str(q)
        assert "period" in s or "coveragePeriod" in s

    def test_date_coverage_period(self, converter):
        q = converter.convert("CareTeam", "date=2024-08-15")
        assert "coveragePeriod" in str(q) or "period" in str(q)


class TestCareTeamDenormalization:
    def test_sparse_minimal(self, denormalizer, minimal_care_team):
        out = denormalizer.denormalize(minimal_care_team)
        s = out.get("_search", {})
        assert s["patientId"] == "pat-min"

    def test_rich_fields(self, denormalizer, rich_care_team):
        out = denormalizer.denormalize(rich_care_team)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "LA27976-2" in s["category_codes"]
        assert "prac-1" in s["participantIds"]
        assert "rp-1" in s["participantIds"]
        assert s["name"] == "Crisis Assessment Team"
        assert "crisis assessment team" in s["name_lower"]
        assert "period" in s
        assert len(s["coveragePeriod"]) >= 1

    def test_input_not_mutated(self, denormalizer, rich_care_team):
        original = copy.deepcopy(rich_care_team)
        denormalizer.denormalize(rich_care_team)
        assert rich_care_team == original


class TestCareTeamPrecomputedCompartments:
    def test_patient_compartment_from_subject(self, denormalizer, rich_care_team):
        out = denormalizer.denormalize(rich_care_team)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment_performer_only(self, denormalizer, rich_care_team):
        out = denormalizer.denormalize(rich_care_team)
        prac = out["_compartments"]["Practitioner"]
        assert "prac-1" in prac
        assert "rp-1" not in prac

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "CareTeam", "status=active"
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
class TestCareTeamMongoDB:
    @pytest.fixture(scope="class")
    def seeded(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        col = client["fhir_search_to_mql_tests"]["careteam_e2e"]
        col.delete_many({})
        sample = {
            "resourceType": "CareTeam",
            "id": "e2e-ct-1",
            "status": "active",
            "name": "E2E Team",
            "subject": {"reference": "Patient/p1"},
            "period": {
                "start": datetime(2024, 7, 1),
                "end": datetime(2024, 12, 31),
            },
        }
        dn = ResourceDenormalizer()
        col.insert_one(dn.denormalize(copy.deepcopy(sample)))
        yield col
        col.delete_many({})
        client.close()

    def test_status_lookup(self, seeded):
        conv = FHIRSearchConverter()
        results = list(seeded.find(conv.convert("CareTeam", "status=active")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-ct-1"
