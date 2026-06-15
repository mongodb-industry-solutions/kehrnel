"""
Comprehensive integration tests for ALL Encounter search parameters per FHIR R5.

Local spec sources (do not link to external URLs in this docstring):
- schema/indexes/search-parameters.r5.json  (resource "Encounter")
- schema/indexes/resources.r5.json
- src/fhir_search_to_mql/configs/Encounter.yaml
- src/fhir_search_to_mql/compartments/definitions/
  patient.json, practitioner.json, device.json, encounter.json

This suite exercises the 29 search parameters declared in
``configs/Encounter.yaml`` (composite ``location-period`` deferred):

  References (14): account, appointment, based-on, careteam,
    diagnosis-reference, episode-of-care, location, part-of, participant,
    patient, practitioner, reason-reference, service-provider, subject
  Tokens (9): class, diagnosis-code, identifier, participant-type,
    reason-code, special-arrangement, status, subject-status, type
  Dates (3): date, date-start, end-date
  Quantity (1): length
  Common (2): _id, _lastUpdated

R5 structural notes:
  * ``actualPeriod`` — Period overlap for ``date``; scalar start/end for
    ``date-start`` / ``end-date``.
  * ``diagnosis[].condition`` and ``reason[].value`` are CodeableReference.
  * ``patient`` is a Patient/* shortcut on ``subject``.

Compartments (precomputed): Patient, Practitioner, Device, Encounter (self).
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
def rich_encounter() -> Dict[str, Any]:
    """Encounter with all denormalized fields populated."""
    return {
        "resourceType": "Encounter",
        "id": "enc-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "in-progress",
        "identifier": [{"system": "http://hospital.org/enc", "value": "ENC-001"}],
        "class": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                        "code": "AMB",
                    }
                ]
            }
        ],
        "type": [{"coding": [{"system": "http://snomed.info/sct", "code": "185349003"}]}],
        "subjectStatus": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/encounter-subject-status",
                    "code": "arrived",
                }
            ]
        },
        "specialArrangement": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/encounter-special-arrangements",
                        "code": "wheel",
                    }
                ]
            }
        ],
        "subject": {"reference": "Patient/pat-1"},
        "actualPeriod": {
            "start": "2024-07-01T08:00:00Z",
            "end": "2024-07-01T12:00:00Z",
        },
        "length": {
            "value": 1,
            "unit": "d",
            "system": "http://unitsofmeasure.org",
            "code": "d",
        },
        "participant": [
            {
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                                "code": "PPRF",
                            }
                        ]
                    }
                ],
                "actor": {"reference": "Practitioner/prac-1"},
            },
            {"actor": {"reference": "Device/dev-1"}},
        ],
        "location": [{"location": {"reference": "Location/loc-1"}}],
        "diagnosis": [
            {
                "condition": {
                    "concept": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "44054006"}]
                    },
                    "reference": {"reference": "Condition/cond-1"},
                }
            }
        ],
        "reason": [
            {
                "value": {
                    "concept": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "185345009"}]
                    },
                    "reference": {"reference": "Observation/obs-1"},
                }
            }
        ],
        "account": [{"reference": "Account/acct-1"}],
        "appointment": [{"reference": "Appointment/appt-1"}],
        "basedOn": [{"reference": "ServiceRequest/sr-1"}],
        "careTeam": [{"reference": "CareTeam/ct-1"}],
        "episodeOfCare": [{"reference": "EpisodeOfCare/eoc-1"}],
        "partOf": {"reference": "Encounter/enc-parent"},
        "serviceProvider": {"reference": "Organization/org-1"},
    }


@pytest.fixture
def minimal_encounter() -> Dict[str, Any]:
    return {
        "resourceType": "Encounter",
        "id": "enc-min",
        "status": "planned",
        "subject": {"reference": "Patient/pat-min"},
    }


class TestEncounterReferenceParameters:
    def test_patient_bare_id(self, converter):
        q = converter.convert("Encounter", "patient=pat-1")
        s = str(q)
        assert "$or" in s
        assert "pat-1" in s
        assert "_search.patientId" in s or "patientId" in s

    def test_patient_typed_reference(self, converter):
        q = converter.convert("Encounter", "patient=Patient/pat-1")
        assert "pat-1" in str(q)

    def test_subject_bare_id(self, converter):
        q = converter.convert("Encounter", "subject=pat-1")
        assert "_search.subjectId" in str(q)

    def test_practitioner_bare_id(self, converter):
        q = converter.convert("Encounter", "practitioner=prac-1")
        assert "_search.practitionerId" in str(q)
        assert "prac-1" in str(q)

    def test_participant_bare_id(self, converter):
        q = converter.convert("Encounter", "participant=dev-1")
        assert "_search.participantIds" in str(q)

    def test_location_bare_id(self, converter):
        q = converter.convert("Encounter", "location=loc-1")
        assert "_search.locationIds" in str(q)

    def test_part_of_encounter(self, converter):
        q = converter.convert("Encounter", "part-of=enc-parent")
        s = str(q)
        assert "_search.partOfId" in s
        assert "enc-parent" in s

    def test_account_reference(self, converter):
        q = converter.convert("Encounter", "account=acct-1")
        assert "_search.accountIds" in str(q)

    def test_appointment_reference(self, converter):
        q = converter.convert("Encounter", "appointment=appt-1")
        assert "_search.appointmentIds" in str(q)

    def test_based_on_reference(self, converter):
        q = converter.convert("Encounter", "based-on=sr-1")
        assert "_search.basedOnIds" in str(q)

    def test_careteam_reference(self, converter):
        q = converter.convert("Encounter", "careteam=ct-1")
        assert "_search.careTeamIds" in str(q)

    def test_episode_of_care_reference(self, converter):
        q = converter.convert("Encounter", "episode-of-care=eoc-1")
        assert "_search.episodeOfCareIds" in str(q)

    def test_diagnosis_reference(self, converter):
        q = converter.convert("Encounter", "diagnosis-reference=cond-1")
        assert "_search.diagnosisReferenceIds" in str(q)

    def test_reason_reference(self, converter):
        q = converter.convert("Encounter", "reason-reference=obs-1")
        assert "_search.reasonReferenceIds" in str(q)

    def test_service_provider_reference(self, converter):
        q = converter.convert("Encounter", "service-provider=org-1")
        assert "_search.serviceProviderId" in str(q)


class TestEncounterTokenParameters:
    def test_status_bare_code(self, converter):
        q = converter.convert("Encounter", "status=in-progress")
        assert q == {"status": "in-progress"}

    def test_class_bare_code(self, converter):
        q = converter.convert("Encounter", "class=AMB")
        assert "AMB" in str(q)

    def test_class_system_pipe_code(self, converter):
        q = converter.convert(
            "Encounter",
            "class=http://terminology.hl7.org/CodeSystem/v3-ActCode|AMB",
        )
        s = str(q)
        assert "AMB" in s
        assert "v3-ActCode" in s

    def test_type_bare_code(self, converter):
        q = converter.convert("Encounter", "type=185349003")
        assert "185349003" in str(q)

    def test_identifier_bare_value(self, converter):
        q = converter.convert("Encounter", "identifier=ENC-001")
        assert "ENC-001" in str(q)

    def test_diagnosis_code(self, converter):
        q = converter.convert("Encounter", "diagnosis-code=44054006")
        assert "44054006" in str(q)

    def test_reason_code(self, converter):
        q = converter.convert("Encounter", "reason-code=185345009")
        assert "185345009" in str(q)

    def test_participant_type(self, converter):
        q = converter.convert("Encounter", "participant-type=PPRF")
        assert "PPRF" in str(q)

    def test_special_arrangement(self, converter):
        q = converter.convert("Encounter", "special-arrangement=wheel")
        assert "wheel" in str(q)

    def test_subject_status(self, converter):
        q = converter.convert("Encounter", "subject-status=arrived")
        assert "arrived" in str(q)

    def test_id_search_parameter(self, converter):
        q = converter.convert("Encounter", "_id=enc-rich")
        assert "enc-rich" in str(q)


class TestEncounterDateParameters:
    def test_date_ge_overlap(self, converter):
        q = converter.convert("Encounter", "date=ge2024-07-01")
        s = str(q)
        assert "actualPeriod" in s

    def test_date_le(self, converter):
        q = converter.convert("Encounter", "date=le2024-12-31")
        assert "actualPeriod" in str(q)

    def test_date_start_ge(self, converter):
        q = converter.convert("Encounter", "date-start=ge2024-07-01")
        s = str(q)
        assert "actualPeriod.start" in s

    def test_end_date_le(self, converter):
        q = converter.convert("Encounter", "end-date=le2024-07-01")
        s = str(q)
        assert "actualPeriod.end" in s

    def test_last_updated(self, converter):
        q = converter.convert("Encounter", "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in str(q)


class TestEncounterQuantityParameters:
    def test_length_eq(self, converter):
        q = converter.convert("Encounter", "length=1")
        assert "length" in str(q)


class TestEncounterComplexQueries:
    def test_status_and_patient_and(self, converter):
        q = converter.convert("Encounter", "status=in-progress&patient=pat-1")
        s = str(q)
        assert "$and" in s or ("status" in s and "pat-1" in s)

    def test_date_range_and_status(self, converter):
        q = converter.convert(
            "Encounter",
            "date=ge2024-01-01&date=le2024-12-31&status=completed",
        )
        s = str(q)
        assert "actualPeriod" in s
        assert "completed" in s


class TestEncounterDenormalization:
    def test_sparse_output_contract(self, denormalizer, minimal_encounter):
        out = denormalizer.denormalize(minimal_encounter)
        search = out.get("_search", {})
        assert "subjectId" in search
        assert "patientId" in search
        assert "class_codes" not in search

    def test_rich_search_fields(self, denormalizer, rich_encounter):
        out = denormalizer.denormalize(rich_encounter)
        s = out["_search"]
        assert s["patientId"] == "pat-1"
        assert "prac-1" in s["practitionerId"]
        assert "dev-1" in s["participantIds"]
        assert "loc-1" in s["locationIds"]
        assert "44054006" in s["diagnosisCode_codes"]
        assert "AMB" in s["class_codes"]
        assert s["partOfId"] == "enc-parent"
        assert "actualPeriod" in s

    def test_only_search_and_compartments_added(self, denormalizer, rich_encounter):
        original = set(rich_encounter.keys())
        out = denormalizer.denormalize(rich_encounter)
        added = set(out.keys()) - original
        assert added.issubset({"_search", "_compartments"})

    def test_input_not_mutated(self, denormalizer, rich_encounter):
        original = copy.deepcopy(rich_encounter)
        denormalizer.denormalize(rich_encounter)
        assert rich_encounter == original


class TestEncounterPrecomputedCompartments:
    def test_patient_compartment(self, denormalizer, rich_encounter):
        out = denormalizer.denormalize(rich_encounter)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment(self, denormalizer, rich_encounter):
        out = denormalizer.denormalize(rich_encounter)
        assert "prac-1" in out["_compartments"]["Practitioner"]

    def test_device_compartment(self, denormalizer, rich_encounter):
        out = denormalizer.denormalize(rich_encounter)
        assert "dev-1" in out["_compartments"]["Device"]

    def test_encounter_compartment_self_and_parent(
        self, denormalizer, rich_encounter
    ):
        out = denormalizer.denormalize(rich_encounter)
        enc_ids = out["_compartments"]["Encounter"]
        assert "enc-rich" in enc_ids
        assert "enc-parent" in enc_ids

    def test_patient_not_in_practitioner_compartment(
        self, denormalizer, rich_encounter
    ):
        out = denormalizer.denormalize(rich_encounter)
        assert "pat-1" not in out["_compartments"]["Practitioner"]

    def test_patient_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Patient", "pat-1", "Encounter", "status=in-progress"
        )
        s = str(q)
        assert "_compartments.Patient" in s
        assert "pat-1" in s

    def test_practitioner_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Practitioner", "prac-1", "Encounter")
        assert q == {"_compartments.Practitioner": "prac-1"}

    def test_device_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Device", "dev-1", "Encounter")
        assert q == {"_compartments.Device": "dev-1"}

    def test_encounter_compartment_fast_path(self, converter):
        q = converter.convert_with_compartment("Encounter", "enc-rich", "Encounter")
        assert q == {"_compartments.Encounter": "enc-rich"}


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
class TestEncounterMongoDB:
    _ENCOUNTERS = [
        {
            "resourceType": "Encounter",
            "id": "e2e-enc-1",
            "status": "in-progress",
            "class": [{"coding": [{"code": "AMB"}]}],
            "subject": {"reference": "Patient/p1"},
            "actualPeriod": {
                "start": datetime(2024, 7, 1, 8, 0, 0),
                "end": datetime(2024, 7, 1, 12, 0, 0),
            },
            "participant": [{"actor": {"reference": "Practitioner/dr1"}}],
        },
        {
            "resourceType": "Encounter",
            "id": "e2e-enc-2",
            "status": "completed",
            "class": [{"coding": [{"code": "IMP"}]}],
            "subject": {"reference": "Patient/p2"},
            "actualPeriod": {
                "start": datetime(2024, 1, 1, 0, 0, 0),
                "end": datetime(2024, 1, 5, 0, 0, 0),
            },
        },
    ]

    @pytest.fixture(scope="class")
    def mongo_col(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        col = db["encounter_e2e"]
        col.delete_many({})
        yield col
        col.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_col):
        dn = ResourceDenormalizer()
        docs = [dn.denormalize(copy.deepcopy(s)) for s in self._ENCOUNTERS]
        mongo_col.insert_many(docs)
        return mongo_col

    @pytest.fixture(scope="class")
    def conv(self):
        return FHIRSearchConverter()

    def test_status_lookup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Encounter", "status=in-progress")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-enc-1"

    def test_patient_compartment_e2e(self, seeded, conv):
        mql = conv.convert_with_compartment("Patient", "p1", "Encounter", "status=in-progress")
        results = list(seeded.find(mql))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-enc-1"
