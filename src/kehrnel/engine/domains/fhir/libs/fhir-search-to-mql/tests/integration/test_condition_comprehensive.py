"""
Comprehensive integration tests for ALL Condition search parameters per FHIR R5.

References:
- https://www.hl7.org/fhir/condition-search.html
- https://www.hl7.org/fhir/condition-definitions.html
- https://www.hl7.org/fhir/compartmentdefinition-patient.html
- https://www.hl7.org/fhir/compartmentdefinition-practitioner.html
- https://www.hl7.org/fhir/compartmentdefinition-device.html
- https://www.hl7.org/fhir/compartmentdefinition-encounter.html

Exercises all 24 search parameters in ``configs/Condition.yaml``:

  References (5): encounter, evidence-detail, participant-actor, patient, subject
  Tokens (9): body-site, category, clinical-status, code, evidence, identifier,
              participant-function, severity, stage, verification-status
  Strings (2): abatement-string, onset-info
  Dates (3): abatement-date, onset-date, recorded-date
  Quantities (2): abatement-age, onset-age
  Common (2): _id, _lastUpdated

Compartments: Patient, Practitioner, Device precomputed; Encounter and
RelatedPerson use dynamic resolver fallback.
"""
from __future__ import annotations

import copy
import datetime
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
def rich_condition() -> Dict[str, Any]:
    return {
        "resourceType": "Condition",
        "id": "cond-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "identifier": [{"system": "http://hospital.org/cond", "value": "C-001"}],
        "clinicalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                    "code": "active",
                }
            ]
        },
        "verificationStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                    "code": "confirmed",
                }
            ]
        },
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                        "code": "problem-list-item",
                    }
                ]
            }
        ],
        "severity": {
            "coding": [{"system": "http://snomed.info/sct", "code": "6736007"}]
        },
        "code": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "44054006",
                    "display": "Diabetes mellitus type 2",
                }
            ],
            "text": "Type 2 diabetes",
        },
        "bodySite": [{"coding": [{"system": "http://snomed.info/sct", "code": "181414000"}]}],
        "subject": {"reference": "Patient/pat-1"},
        "encounter": {"reference": "Encounter/enc-1"},
        "onsetDateTime": "2020-01-15T08:00:00Z",
        "onsetString": "January 2020",
        "onsetAge": {"value": 45, "unit": "years", "system": "http://unitsofmeasure.org", "code": "a"},
        "abatementPeriod": {"start": "2024-01-01", "end": "2024-06-01"},
        "abatementString": "In remission since June",
        "recordedDate": "2024-06-01T10:00:00Z",
        "participant": [
            {
                "function": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/provenance-participant-type",
                            "code": "author",
                        }
                    ]
                },
                "actor": {"reference": "Practitioner/prac-1"},
            },
            {"actor": {"reference": "Device/dev-1"}},
        ],
        "stage": [
            {
                "summary": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "258219007",
                            "display": "Stage II",
                        }
                    ]
                }
            }
        ],
        "evidence": [
            {
                "concept": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "386661006",
                            "display": "Fever",
                        }
                    ]
                },
                "reference": {"reference": "Observation/obs-1"},
            }
        ],
    }


@pytest.fixture
def minimal_condition() -> Dict[str, Any]:
    return {
        "resourceType": "Condition",
        "id": "cond-min",
        "subject": {"reference": "Patient/p-min"},
    }


class TestConditionTokenParameters:
    def test_code_bare(self, converter):
        q = converter.convert("Condition", "code=44054006")
        s = str(q)
        assert "_search.code_codes" in s or "code_systemCode" in s

    def test_code_system_pipe(self, converter):
        q = converter.convert("Condition", "code=http://snomed.info/sct|44054006")
        assert "44054006" in str(q)

    def test_code_text_modifier(self, converter):
        q = converter.convert("Condition", "code:text=diabetes")
        assert "code_displays_lower" in str(q) or "code_text_lower" in str(q)

    def test_clinical_status(self, converter):
        q = converter.convert("Condition", "clinical-status=active")
        assert "clinicalStatus" in str(q)

    def test_verification_status(self, converter):
        q = converter.convert("Condition", "verification-status=confirmed")
        assert "verificationStatus" in str(q)

    def test_category(self, converter):
        q = converter.convert("Condition", "category=problem-list-item")
        assert "category" in str(q)

    def test_severity(self, converter):
        q = converter.convert("Condition", "severity=6736007")
        assert "severity" in str(q)

    def test_body_site(self, converter):
        q = converter.convert("Condition", "body-site=181414000")
        assert "bodySite" in str(q)

    def test_identifier(self, converter):
        q = converter.convert("Condition", "identifier=C-001")
        assert "C-001" in str(q)

    def test_evidence(self, converter):
        q = converter.convert("Condition", "evidence=386661006")
        assert "evidence" in str(q)

    def test_stage(self, converter):
        q = converter.convert("Condition", "stage=258219007")
        assert "stage" in str(q)

    def test_participant_function(self, converter):
        q = converter.convert("Condition", "participant-function=author")
        assert "participantFunction" in str(q)

    def test_code_not_modifier(self, converter):
        q = converter.convert("Condition", "code:not=999")
        s = str(q)
        assert "$nor" in s or "$ne" in s

    def test_identifier_missing(self, converter):
        q = converter.convert("Condition", "identifier:missing=true")
        assert "$exists" in str(q)


class TestConditionReferenceParameters:
    def test_patient_id(self, converter):
        q = converter.convert("Condition", "patient=pat-1")
        assert "patientId" in str(q) or "subjectId" in str(q)

    def test_subject_full_ref(self, converter):
        q = converter.convert("Condition", "subject=Patient/pat-1")
        assert "subjectId" in str(q)

    def test_subject_typed_modifier(self, converter):
        q = converter.convert("Condition", "subject:Patient=pat-1")
        assert "pat-1" in str(q)

    def test_encounter(self, converter):
        q = converter.convert("Condition", "encounter=enc-1")
        assert "encounterId" in str(q)

    def test_participant_actor(self, converter):
        q = converter.convert("Condition", "participant-actor=Practitioner/prac-1")
        assert "participantActorIds" in str(q)

    def test_evidence_detail(self, converter):
        q = converter.convert("Condition", "evidence-detail=Observation/obs-1")
        assert "evidenceDetailIds" in str(q)

    def test_encounter_missing(self, converter):
        q = converter.convert("Condition", "encounter:missing=true")
        assert "$exists" in str(q)


class TestConditionStringParameters:
    def test_onset_info_default(self, converter):
        q = converter.convert("Condition", "onset-info=January")
        assert "onsetString_lower" in str(q)

    def test_onset_info_exact(self, converter):
        q = converter.convert("Condition", "onset-info:exact=January 2020")
        assert "_search.onsetString" in str(q) or "onsetString" in str(q)

    def test_abatement_string_default(self, converter):
        q = converter.convert("Condition", "abatement-string=remission")
        assert "abatementString_lower" in str(q)


class TestConditionDateParameters:
    def test_onset_date_ge(self, converter):
        q = converter.convert("Condition", "onset-date=ge2020-01-01")
        s = str(q)
        assert "onsetDateTime" in s or "onsetPeriod" in s

    def test_abatement_date_ge(self, converter):
        q = converter.convert("Condition", "abatement-date=ge2024-01-01")
        s = str(q)
        assert "abatementDateTime" in s or "abatementPeriod" in s

    def test_recorded_date_ge(self, converter):
        q = converter.convert("Condition", "recorded-date=ge2024-01-01")
        assert "recordedDate" in str(q)

    def test_onset_date_lt(self, converter):
        q = converter.convert("Condition", "onset-date=lt2021-01-01")
        assert "$lt" in str(q) or "$lte" in str(q)


class TestConditionQuantityParameters:
    def test_onset_age_gt(self, converter):
        q = converter.convert("Condition", "onset-age=gt40")
        assert "onsetAge" in str(q)

    def test_abatement_age_ge(self, converter):
        q = converter.convert("Condition", "abatement-age=ge30")
        assert "abatementAge" in str(q)


class TestConditionCommonParameters:
    def test_id(self, converter):
        q = converter.convert("Condition", "_id=cond-rich")
        assert q == {"id": "cond-rich"}

    def test_last_updated_ge(self, converter):
        q = converter.convert("Condition", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestConditionCombinations:
    def test_code_and_clinical_status(self, converter):
        q = converter.convert("Condition", "code=44054006&clinical-status=active")
        assert "$and" in str(q)

    def test_patient_and_onset_date(self, converter):
        q = converter.convert("Condition", "patient=pat-1&onset-date=ge2020-01-01")
        assert "$and" in str(q)


class TestConditionDenormalization:
    def test_code_fields(self, denormalizer, rich_condition):
        s = denormalizer.denormalize(rich_condition)["_search"]
        assert "44054006" in s["code_codes"]
        assert "diabetes" in s["code_text_lower"]

    def test_subject_and_patient_ids(self, denormalizer, rich_condition):
        s = denormalizer.denormalize(rich_condition)["_search"]
        assert s["subjectId"] == "pat-1"
        assert s["patientId"] == "pat-1"
        assert s["subjectType"] == "Patient"

    def test_participant_actors(self, denormalizer, rich_condition):
        s = denormalizer.denormalize(rich_condition)["_search"]
        assert "prac-1" in s["participantActorIds"]
        assert "dev-1" in s["participantActorIds"]
        assert "Practitioner" in s["participantActorTypes"]

    def test_evidence_and_stage(self, denormalizer, rich_condition):
        s = denormalizer.denormalize(rich_condition)["_search"]
        assert "386661006" in s["evidence_codes"]
        assert "obs-1" in s["evidenceDetailIds"]
        assert "258219007" in s["stage_codes"]

    def test_onset_and_abatement_projections(self, denormalizer, rich_condition):
        s = denormalizer.denormalize(rich_condition)["_search"]
        assert s["onsetString_lower"] == "january 2020"
        assert s["abatementPeriod"]["start"] == "2024-01-01"
        assert "remission" in s["abatementString_lower"]

    def test_sparse_minimal(self, denormalizer, minimal_condition):
        out = denormalizer.denormalize(minimal_condition)
        s = out["_search"]
        assert "patientId" in s
        assert "code_codes" not in s
        assert "evidence_codes" not in s


class TestConditionResourcePurity:
    def test_only_buckets_added(self, denormalizer, rich_condition):
        original = set(rich_condition.keys())
        out = denormalizer.denormalize(rich_condition)
        added = set(out.keys()) - original
        assert added.issubset({"_search", "_compartments"})

    def test_fhir_fields_unchanged(self, denormalizer, rich_condition):
        out = denormalizer.denormalize(rich_condition)
        for field in ("id", "code", "subject", "participant", "evidence", "stage"):
            assert out[field] == rich_condition[field]

    def test_input_not_mutated(self, denormalizer, rich_condition):
        original = copy.deepcopy(rich_condition)
        denormalizer.denormalize(rich_condition)
        assert rich_condition == original


class TestConditionPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_condition):
        out = denormalizer.denormalize(rich_condition)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_condition):
        out = denormalizer.denormalize(rich_condition)
        assert "prac-1" in out["_compartments"]["Practitioner"]

    def test_device_compartment(self, denormalizer, rich_condition):
        out = denormalizer.denormalize(rich_condition)
        assert "dev-1" in out["_compartments"]["Device"]

    def test_patient_compartment_fast_path_query(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Condition", "code=44054006"
        )
        s = str(q)
        assert "_compartments.Patient" in s
        assert "pat-1" in s

    def test_practitioner_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Practitioner", "prac-1", "Condition", ""
        )
        assert q == {"_compartments.Practitioner": "prac-1"}


class TestConditionDynamicCompartments:
    def test_encounter_compartment_uses_encounter_id(self, converter):
        q = converter.convert_with_compartment(
            "Encounter", "enc-1", "Condition", "clinical-status=active"
        )
        s = str(q)
        assert "enc-1" in s
        assert "_compartments" not in s


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
class TestConditionMongoDB:
    _CONDITIONS = [
        {
            "resourceType": "Condition",
            "id": "e2e-cond-1",
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": "44054006"}]},
            "clinicalStatus": {
                "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]
            },
            "subject": {"reference": "Patient/p1"},
            "encounter": {"reference": "Encounter/enc-1"},
            "onsetDateTime": datetime.datetime(2020, 1, 15, 8, 0, 0),
            "recordedDate": datetime.datetime(2024, 6, 1, 10, 0, 0),
            "participant": [{"actor": {"reference": "Practitioner/dr1"}}],
        },
        {
            "resourceType": "Condition",
            "id": "e2e-cond-2",
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": "195967001"}]},
            "clinicalStatus": {
                "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "resolved"}]
            },
            "subject": {"reference": "Patient/p2"},
            "onsetDateTime": datetime.datetime(2018, 5, 1, 0, 0, 0),
            "recordedDate": datetime.datetime(2023, 1, 1, 0, 0, 0),
        },
    ]

    @pytest.fixture(scope="class")
    def mongo_col(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        col = db["condition_e2e"]
        col.delete_many({})
        yield col
        col.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_col):
        dn = ResourceDenormalizer()
        docs = [dn.denormalize(copy.deepcopy(s)) for s in self._CONDITIONS]
        mongo_col.insert_many(docs)
        return mongo_col

    @pytest.fixture(scope="class")
    def conv(self):
        return FHIRSearchConverter()

    def test_code_lookup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Condition", "code=44054006")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-cond-1"

    def test_patient_compartment_e2e(self, seeded, conv):
        mql = conv.convert_with_compartment("Patient", "p1", "Condition", "clinical-status=active")
        results = list(seeded.find(mql))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-cond-1"

    def test_encounter_and_code_combo(self, seeded, conv):
        mql = conv.convert("Condition", "encounter=enc-1&code=44054006")
        results = list(seeded.find(mql))
        assert len(results) == 1
