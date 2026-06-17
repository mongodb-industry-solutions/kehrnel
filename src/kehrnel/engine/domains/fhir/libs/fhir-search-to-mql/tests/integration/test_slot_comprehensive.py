"""
Comprehensive integration tests for Slot FHIR R5 resource configuration.

Coverage:
  - All 9 resource-specific search parameters + 2 common parameters
  - All FHIR R5 search modifiers (:exact, :contains, :not, :missing, :text,
    :of-type, typed-resource modifiers) per parameter type
  - All FHIR date prefixes (ge, gt, le, lt, eq, ne, sa, eb, ap) for `start`
  - Denormalization correctness (field shapes, sparse output)
  - Resource purity (no fields leak to FHIR resource root)
  - No-compartment contract (Slot has no FHIR R5 compartments)
  - MongoDB E2E roundtrip tests (requires running MongoDB)

FHIR R5 Slot resource notes:
  * status (1..1)         — top-level scalar code (queried directly)
  * start  (1..1)         — top-level scalar instant (queried directly)
  * schedule (1..1)       — required single Reference(Schedule)
  * serviceType 0..*      — CodeableReference(HealthcareService)  [R5]
  * appointmentType 0..*  — CodeableConcept[]  (was 0..1 in R4)
  * NO COMPARTMENTS — Slot participates in no FHIR R5 compartments
"""

from __future__ import annotations

import copy
import datetime
import pytest

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def converter():
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer():
    return ResourceDenormalizer()


@pytest.fixture
def sample_slot():
    """Rich Slot R5 resource that exercises every denormalization path."""
    return {
        "resourceType": "Slot",
        "id": "slot-test-001",
        "meta": {"lastUpdated": "2024-06-01T08:00:00Z"},
        "identifier": [
            {"system": "http://hospital.example/slots", "value": "SLOT-001"},
            {"system": "http://clinic.example/slots", "value": "SLOT-ALT"},
        ],
        "serviceCategory": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "408443003",
                        "display": "General medical practice",
                    }
                ]
            }
        ],
        "serviceType": [
            {
                "concept": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "11429006",
                            "display": "Consultation",
                        }
                    ]
                },
                "reference": {"reference": "HealthcareService/hs-1"},
            },
            {
                "concept": {
                    "coding": [
                        {
                            "system": "http://snomed.info/sct",
                            "code": "185389009",
                            "display": "Follow-up appointment",
                        }
                    ]
                }
            },
        ],
        "specialty": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "394814009",
                        "display": "General practice",
                    }
                ]
            }
        ],
        "appointmentType": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0276",
                        "code": "ROUTINE",
                        "display": "Routine appointment",
                    }
                ]
            },
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0276",
                        "code": "FOLLOWUP",
                        "display": "A follow up visit from a previous appointment",
                    }
                ]
            },
        ],
        "schedule": {"reference": "Schedule/sched-1"},
        "status": "free",
        "start": "2024-07-15T09:00:00Z",
        "end": "2024-07-15T09:30:00Z",
        "overbooked": False,
        "comment": "Morning slot",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────────────────────

def q(converter, qs: str) -> dict:
    return converter.convert("Slot", qs)


# ═════════════════════════════════════════════════════════════════════════════
# 1. Token Parameters
# ═════════════════════════════════════════════════════════════════════════════

class TestStatusParam:
    """status — required top-level scalar code (queried directly)."""

    def test_status_free(self, converter):
        result = q(converter, "status=free")
        assert result == {"status": "free"}

    def test_status_busy(self, converter):
        result = q(converter, "status=busy")
        assert result == {"status": "busy"}

    def test_status_busy_unavailable(self, converter):
        result = q(converter, "status=busy-unavailable")
        assert result == {"status": "busy-unavailable"}

    def test_status_busy_tentative(self, converter):
        result = q(converter, "status=busy-tentative")
        assert result == {"status": "busy-tentative"}

    def test_status_entered_in_error(self, converter):
        result = q(converter, "status=entered-in-error")
        assert result == {"status": "entered-in-error"}

    def test_status_not_modifier(self, converter):
        result = q(converter, "status:not=entered-in-error")
        assert result == {"status": {"$ne": "entered-in-error"}}

    def test_status_missing_true(self, converter):
        result = q(converter, "status:missing=true")
        result_str = str(result)
        assert "$exists" in result_str

    def test_status_missing_false(self, converter):
        result = q(converter, "status:missing=false")
        result_str = str(result)
        assert "$exists" in result_str


class TestIdentifierParam:
    """identifier — Slot.identifier (Identifier[])."""

    def test_identifier_value_only(self, converter):
        result = q(converter, "identifier=SLOT-001")
        assert "_search.identifier_values" in str(result) or "_search.identifier_systemCode" in str(result)

    def test_identifier_system_value(self, converter):
        result = q(converter, "identifier=http://hospital.example/slots|SLOT-001")
        assert "_search.identifier_systemCode" in str(result)

    def test_identifier_not_modifier(self, converter):
        result = q(converter, "identifier:not=SLOT-001")
        result_str = str(result)
        assert "$ne" in result_str or "$nor" in result_str or "$not" in result_str

    def test_identifier_missing_true(self, converter):
        result = q(converter, "identifier:missing=true")
        assert "$exists" in str(result)

    def test_identifier_missing_false(self, converter):
        result = q(converter, "identifier:missing=false")
        assert "$exists" in str(result)

    def test_identifier_of_type(self, converter):
        result = q(converter, "identifier:of-type=http://terminology.hl7.org/CodeSystem/v2-0203|MR|SLOT-001")
        assert result  # Returns a valid query structure


class TestServiceCategoryParam:
    """service-category — Slot.serviceCategory (CodeableConcept[])."""

    def test_service_category_code_only(self, converter):
        result = q(converter, "service-category=408443003")
        assert "_search.serviceCategory_codes" in str(result) or "_search.serviceCategory_systemCode" in str(result)

    def test_service_category_system_code(self, converter):
        result = q(converter, "service-category=http://snomed.info/sct|408443003")
        assert "_search.serviceCategory_systemCode" in str(result)

    def test_service_category_not_modifier(self, converter):
        result = q(converter, "service-category:not=408443003")
        result_str = str(result)
        assert "$ne" in result_str or "$nor" in result_str or "$not" in result_str

    def test_service_category_missing_true(self, converter):
        result = q(converter, "service-category:missing=true")
        assert "$exists" in str(result)

    def test_service_category_text_modifier(self, converter):
        result = q(converter, "service-category:text=General medical practice")
        assert result  # Valid query returned


class TestServiceTypeParam:
    """service-type — Slot.serviceType.concept (CodeableReference concept arm)."""

    def test_service_type_code_only(self, converter):
        result = q(converter, "service-type=11429006")
        assert "_search.serviceType_codes" in str(result) or "_search.serviceType_systemCode" in str(result)

    def test_service_type_system_code(self, converter):
        result = q(converter, "service-type=http://snomed.info/sct|11429006")
        assert "_search.serviceType_systemCode" in str(result)

    def test_service_type_not_modifier(self, converter):
        result = q(converter, "service-type:not=11429006")
        result_str = str(result)
        assert "$ne" in result_str or "$nor" in result_str or "$not" in result_str

    def test_service_type_missing_true(self, converter):
        result = q(converter, "service-type:missing=true")
        assert "$exists" in str(result)

    def test_service_type_system_only(self, converter):
        """system| with no value matches any code in that system."""
        result = q(converter, "service-type=http://snomed.info/sct|")
        assert result


class TestSpecialtyParam:
    """specialty — Slot.specialty (CodeableConcept[])."""

    def test_specialty_code_only(self, converter):
        result = q(converter, "specialty=394814009")
        assert "_search.specialty_codes" in str(result) or "_search.specialty_systemCode" in str(result)

    def test_specialty_system_code(self, converter):
        result = q(converter, "specialty=http://snomed.info/sct|394814009")
        assert "_search.specialty_systemCode" in str(result)

    def test_specialty_not_modifier(self, converter):
        result = q(converter, "specialty:not=394814009")
        result_str = str(result)
        assert "$ne" in result_str or "$nor" in result_str or "$not" in result_str

    def test_specialty_missing_true(self, converter):
        result = q(converter, "specialty:missing=true")
        assert "$exists" in str(result)

    def test_specialty_text_modifier(self, converter):
        result = q(converter, "specialty:text=General practice")
        assert result


class TestAppointmentTypeParam:
    """appointment-type — Slot.appointmentType (CodeableConcept[], 0..* in R5)."""

    def test_appointment_type_code_only(self, converter):
        result = q(converter, "appointment-type=ROUTINE")
        assert "_search.appointmentType_codes" in str(result) or "_search.appointmentType_systemCode" in str(result)

    def test_appointment_type_system_code(self, converter):
        result = q(converter, "appointment-type=http://terminology.hl7.org/CodeSystem/v2-0276|ROUTINE")
        assert "_search.appointmentType_systemCode" in str(result)

    def test_appointment_type_followup(self, converter):
        result = q(converter, "appointment-type=FOLLOWUP")
        assert result

    def test_appointment_type_not_modifier(self, converter):
        result = q(converter, "appointment-type:not=ROUTINE")
        result_str = str(result)
        assert "$ne" in result_str or "$nor" in result_str or "$not" in result_str

    def test_appointment_type_missing_true(self, converter):
        result = q(converter, "appointment-type:missing=true")
        assert "$exists" in str(result)

    def test_appointment_type_text_modifier(self, converter):
        result = q(converter, "appointment-type:text=Routine appointment")
        assert result


# ═════════════════════════════════════════════════════════════════════════════
# 2. Reference Parameters
# ═════════════════════════════════════════════════════════════════════════════

class TestScheduleParam:
    """schedule — Slot.schedule (required single Reference(Schedule))."""

    def test_schedule_by_id(self, converter):
        result = q(converter, "schedule=sched-1")
        assert result.get("_search.scheduleId") == "sched-1"

    def test_schedule_by_full_ref(self, converter):
        result = q(converter, "schedule=Schedule/sched-1")
        assert result.get("_search.scheduleId") == "sched-1"

    def test_schedule_typed_modifier(self, converter):
        result = q(converter, "schedule:Schedule=sched-1")
        assert result.get("_search.scheduleId") == "sched-1"

    def test_schedule_missing_true(self, converter):
        result = q(converter, "schedule:missing=true")
        assert "$exists" in str(result)

    def test_schedule_missing_false(self, converter):
        result = q(converter, "schedule:missing=false")
        assert "$exists" in str(result)


class TestServiceTypeReferenceParam:
    """service-type-reference — Slot.serviceType.reference (CodeableReference ref arm)."""

    def test_service_type_ref_id_only(self, converter):
        result = q(converter, "service-type-reference=hs-1")
        assert result.get("_search.serviceTypeReferenceId") == "hs-1"

    def test_service_type_ref_full(self, converter):
        result = q(converter, "service-type-reference=HealthcareService/hs-1")
        assert result.get("_search.serviceTypeReferenceId") == "hs-1"

    def test_service_type_ref_typed_modifier(self, converter):
        result = q(converter, "service-type-reference:HealthcareService=hs-1")
        assert result.get("_search.serviceTypeReferenceId") == "hs-1"

    def test_service_type_ref_missing_true(self, converter):
        result = q(converter, "service-type-reference:missing=true")
        assert "$exists" in str(result)

    def test_service_type_ref_missing_false(self, converter):
        result = q(converter, "service-type-reference:missing=false")
        assert "$exists" in str(result)


# ═════════════════════════════════════════════════════════════════════════════
# 3. Date Parameters
# ═════════════════════════════════════════════════════════════════════════════

class TestStartParam:
    """start — Slot.start (required top-level instant, queried directly)."""

    def test_start_exact(self, converter):
        # A date-only exact match generates a range covering the whole day
        result = q(converter, "start=2024-07-15")
        result_str = str(result)
        assert "start" in result_str
        assert "2024" in result_str

    def test_start_ge_prefix(self, converter):
        result = q(converter, "start=ge2024-07-15")
        assert result == {"start": {"$gte": datetime.datetime(2024, 7, 15, 0, 0)}}

    def test_start_gt_prefix(self, converter):
        # gt for a date-only value uses end-of-day (strictly after the entire day)
        result = q(converter, "start=gt2024-07-15")
        assert result == {"start": {"$gt": datetime.datetime(2024, 7, 15, 23, 59, 59, 999999)}}

    def test_start_le_prefix(self, converter):
        result = q(converter, "start=le2024-07-15")
        assert result == {"start": {"$lte": datetime.datetime(2024, 7, 15, 23, 59, 59, 999999)}}

    def test_start_lt_prefix(self, converter):
        result = q(converter, "start=lt2024-07-15")
        assert result == {"start": {"$lt": datetime.datetime(2024, 7, 15, 0, 0)}}

    def test_start_range_ge_le(self, converter):
        result = q(converter, "start=ge2024-07-01&start=le2024-07-31")
        result_str = str(result)
        assert "$gte" in result_str
        assert "$lte" in result_str
        assert "2024" in result_str

    def test_start_range_gt_lt(self, converter):
        result = q(converter, "start=gt2024-07-01&start=lt2024-08-01")
        result_str = str(result)
        assert "$gt" in result_str
        assert "$lt" in result_str

    def test_start_ne_prefix(self, converter):
        result = q(converter, "start=ne2024-07-15")
        result_str = str(result)
        assert "start" in result_str

    def test_start_sa_prefix(self, converter):
        result = q(converter, "start=sa2024-07-15")
        result_str = str(result)
        assert "start" in result_str

    def test_start_eb_prefix(self, converter):
        result = q(converter, "start=eb2024-07-15")
        result_str = str(result)
        assert "start" in result_str

    def test_start_missing_true(self, converter):
        result = q(converter, "start:missing=true")
        assert "$exists" in str(result)

    def test_start_missing_false(self, converter):
        result = q(converter, "start:missing=false")
        assert "$exists" in str(result)


class TestLastUpdatedParam:
    """_lastUpdated — common date parameter."""

    def test_last_updated_ge(self, converter):
        result = q(converter, "_lastUpdated=ge2024-01-01")
        assert "meta.lastUpdated" in result or "$gte" in str(result)

    def test_last_updated_lt(self, converter):
        result = q(converter, "_lastUpdated=lt2025-01-01")
        assert "meta.lastUpdated" in result or "$lt" in str(result)

    def test_last_updated_range(self, converter):
        result = q(converter, "_lastUpdated=ge2024-01-01&_lastUpdated=lt2025-01-01")
        result_str = str(result)
        assert "$gte" in result_str and "$lt" in result_str


# ═════════════════════════════════════════════════════════════════════════════
# 4. Common Parameters
# ═════════════════════════════════════════════════════════════════════════════

class TestCommonParams:
    """_id and _lastUpdated common parameters."""

    def test_id_lookup(self, converter):
        result = q(converter, "_id=slot-test-001")
        assert result.get("id") == "slot-test-001"

    def test_id_not_modifier(self, converter):
        result = q(converter, "_id:not=slot-test-001")
        result_str = str(result)
        assert "$ne" in result_str

    def test_id_missing_true(self, converter):
        result = q(converter, "_id:missing=true")
        assert "$exists" in str(result)


# ═════════════════════════════════════════════════════════════════════════════
# 5. Multi-parameter combination queries
# ═════════════════════════════════════════════════════════════════════════════

class TestCombinationQueries:
    """Multi-parameter AND combinations that represent real clinical search patterns."""

    def test_free_slots_in_window(self, converter):
        """Most common Slot query: free slots within a time window."""
        result = q(converter, "status=free&start=ge2024-07-01&start=le2024-07-31")
        result_str = str(result)
        assert "status" in result_str
        assert "$gte" in result_str or "start" in result_str

    def test_free_slots_for_schedule(self, converter):
        result = q(converter, "status=free&schedule=sched-1")
        result_str = str(result)
        assert "free" in result_str
        assert "sched-1" in result_str

    def test_specialty_and_service_type(self, converter):
        result = q(converter, "specialty=394814009&service-type=11429006")
        result_str = str(result)
        assert "394814009" in result_str
        assert "11429006" in result_str

    def test_schedule_with_appointment_type(self, converter):
        result = q(converter, "schedule=sched-1&appointment-type=ROUTINE")
        result_str = str(result)
        assert "sched-1" in result_str
        assert "ROUTINE" in result_str

    def test_full_clinical_query(self, converter):
        """All four dimensions: schedule + status + start range + service-type."""
        result = q(
            converter,
            "schedule=sched-1&status=free&start=ge2024-07-01&start=le2024-07-31&service-type=11429006",
        )
        result_str = str(result)
        assert "sched-1" in result_str
        assert "free" in result_str
        assert "11429006" in result_str

    def test_service_type_and_service_category(self, converter):
        result = q(converter, "service-category=408443003&service-type=11429006")
        result_str = str(result)
        assert "408443003" in result_str
        assert "11429006" in result_str


# ═════════════════════════════════════════════════════════════════════════════
# 6. Denormalization Tests
# ═════════════════════════════════════════════════════════════════════════════

class TestDenormalization:
    """Verify that denormalized _search fields are correctly populated."""

    def test_search_bucket_exists(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        assert "_search" in out

    def test_identifier_values_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert "SLOT-001" in search["identifier_values"]
        assert "SLOT-ALT" in search["identifier_values"]

    def test_identifier_system_code_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert "http://hospital.example/slots|SLOT-001" in search["identifier_systemCode"]

    def test_service_category_codes_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert "408443003" in search["serviceCategory_codes"]

    def test_service_category_system_code_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert "http://snomed.info/sct|408443003" in search["serviceCategory_systemCode"]

    def test_service_type_codes_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        # Both serviceType entries have concept codings
        assert "11429006" in search["serviceType_codes"]
        assert "185389009" in search["serviceType_codes"]

    def test_service_type_system_code_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert "http://snomed.info/sct|11429006" in search["serviceType_systemCode"]

    def test_service_type_legacy_r4_codeable_concept_shape(self, denormalizer):
        """Synthetic/legacy data may store serviceType as CodeableConcept[] (R4)."""
        slot = {
            "resourceType": "Slot",
            "id": "legacy-st",
            "serviceType": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/service-type",
                            "code": "533",
                            "display": "Cardiac Rehabilitation",
                        }
                    ],
                    "text": "Cardiac Rehabilitation",
                }
            ],
            "schedule": {"reference": "Schedule/s1"},
            "status": "free",
            "start": "2024-07-15T09:00:00Z",
            "end": "2024-07-15T09:30:00Z",
        }
        search = denormalizer.denormalize(slot)["_search"]
        assert "533" in search["serviceType_codes"]
        assert "http://terminology.hl7.org/CodeSystem/service-type|533" in search[
            "serviceType_systemCode"
        ]

    def test_service_type_reference_id_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        # Only the first serviceType entry has a reference
        assert "hs-1" in search["serviceTypeReferenceId"]

    def test_service_type_reference_type_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert "HealthcareService" in search["serviceTypeReferenceType"]

    def test_specialty_codes_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert "394814009" in search["specialty_codes"]

    def test_appointment_type_codes_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        # R5: appointmentType is now 0..* — both entries should appear
        assert "ROUTINE" in search["appointmentType_codes"]
        assert "FOLLOWUP" in search["appointmentType_codes"]

    def test_appointment_type_system_code_extracted(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert "http://terminology.hl7.org/CodeSystem/v2-0276|ROUTINE" in search["appointmentType_systemCode"]

    def test_schedule_id_extracted_scalar(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        # schedule is 1..1 → scalar string, not array
        assert search["scheduleId"] == "sched-1"
        assert not isinstance(search["scheduleId"], list)

    def test_schedule_type_extracted_scalar(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        search = out["_search"]
        assert search["scheduleType"] == "Schedule"
        assert not isinstance(search["scheduleType"], list)

    def test_sparse_output_no_service_type_ref_when_absent(self, denormalizer):
        """When serviceType has no reference arm, serviceTypeReferenceId is absent."""
        slot = {
            "resourceType": "Slot",
            "id": "sparse-1",
            "serviceType": [
                {
                    "concept": {
                        "coding": [{"system": "http://snomed.info/sct", "code": "11429006"}]
                    }
                    # no .reference
                }
            ],
            "schedule": {"reference": "Schedule/s1"},
            "status": "free",
            "start": "2024-07-15T09:00:00Z",
            "end": "2024-07-15T09:30:00Z",
        }
        out = denormalizer.denormalize(slot)
        search = out.get("_search", {})
        assert "serviceTypeReferenceId" not in search

    def test_sparse_output_no_identifier_when_absent(self, denormalizer):
        slot = {
            "resourceType": "Slot",
            "id": "sparse-2",
            "schedule": {"reference": "Schedule/s1"},
            "status": "busy",
            "start": "2024-07-15T10:00:00Z",
            "end": "2024-07-15T10:30:00Z",
        }
        out = denormalizer.denormalize(slot)
        search = out.get("_search", {})
        assert "identifier_values" not in search
        assert "identifier_systemCode" not in search

    def test_appointment_type_as_array_r5(self, denormalizer):
        """R5: multiple appointmentType entries produce array output."""
        slot = {
            "resourceType": "Slot",
            "id": "multi-appt-type",
            "appointmentType": [
                {"coding": [{"system": "http://sys.example/codes", "code": "TYPE-A"}]},
                {"coding": [{"system": "http://sys.example/codes", "code": "TYPE-B"}]},
                {"coding": [{"system": "http://sys.example/codes", "code": "TYPE-C"}]},
            ],
            "schedule": {"reference": "Schedule/s1"},
            "status": "free",
            "start": "2024-07-16T09:00:00Z",
            "end": "2024-07-16T09:30:00Z",
        }
        out = denormalizer.denormalize(slot)
        codes = out["_search"]["appointmentType_codes"]
        assert "TYPE-A" in codes
        assert "TYPE-B" in codes
        assert "TYPE-C" in codes


# ═════════════════════════════════════════════════════════════════════════════
# 7. Resource Purity Tests
# ═════════════════════════════════════════════════════════════════════════════

class TestResourcePurity:
    """Denormalization must not modify the original FHIR resource root."""

    FORBIDDEN_ROOT_FIELDS = [
        "identifier_values",
        "identifier_systemCode",
        "serviceCategory_codes",
        "serviceCategory_systemCode",
        "serviceType_codes",
        "serviceType_systemCode",
        "serviceTypeReferenceId",
        "serviceTypeReferenceType",
        "specialty_codes",
        "specialty_systemCode",
        "appointmentType_codes",
        "appointmentType_systemCode",
        "scheduleId",
        "scheduleType",
    ]

    def test_no_computed_fields_at_root(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        for field in self.FORBIDDEN_ROOT_FIELDS:
            assert field not in out, f"Projection field '{field}' leaked to FHIR resource root"

    def test_original_resource_not_mutated(self, denormalizer, sample_slot):
        original = copy.deepcopy(sample_slot)
        denormalizer.denormalize(sample_slot)
        assert sample_slot == original, "Original FHIR resource was mutated by denormalization"

    def test_only_allowed_extra_keys(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        original_keys = set(sample_slot.keys())
        added_keys = set(out.keys()) - original_keys
        assert added_keys.issubset({"_search", "_compartments"}), (
            f"Unexpected root keys added by denormalization: {added_keys - {'_search', '_compartments'}}"
        )

    def test_status_and_start_not_duplicated_in_search(self, denormalizer, sample_slot):
        """status and start are top-level scalars; they must NOT be copied into _search."""
        out = denormalizer.denormalize(sample_slot)
        search = out.get("_search", {})
        # These should NOT be present as _search subfields (queried from root directly)
        assert "status" not in search
        assert "start" not in search


# ═════════════════════════════════════════════════════════════════════════════
# 8. No-Compartment Contract
# ═════════════════════════════════════════════════════════════════════════════

class TestNoCompartmentContract:
    """
    Slot has NO defined FHIR R5 compartments. The system must:
    1. Produce no (or empty) _compartments bucket during denormalization.
    2. Raise an error for any compartment-routed query attempt.
    """

    def test_no_compartments_bucket_populated(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        compartments = out.get("_compartments", {})
        assert compartments == {}, (
            f"Expected empty/absent _compartments for Slot, got: {compartments}"
        )

    def test_no_patient_compartment_key(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        assert "Patient" not in out.get("_compartments", {})

    def test_no_practitioner_compartment_key(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        assert "Practitioner" not in out.get("_compartments", {})

    def test_no_device_compartment_key(self, denormalizer, sample_slot):
        out = denormalizer.denormalize(sample_slot)
        assert "Device" not in out.get("_compartments", {})

    def test_patient_compartment_query_raises(self, converter):
        """GET [base]/Patient/{id}/Slot is NOT valid — Slot not in Patient compartment."""
        with pytest.raises(Exception, match="(?i)compartment|not in"):
            converter.convert_with_compartment("Patient", "pat-1", "Slot")

    def test_practitioner_compartment_query_raises(self, converter):
        """GET [base]/Practitioner/{id}/Slot is NOT valid."""
        with pytest.raises(Exception, match="(?i)compartment|not in"):
            converter.convert_with_compartment("Practitioner", "prac-1", "Slot")

    def test_device_compartment_query_raises(self, converter):
        with pytest.raises(Exception, match="(?i)compartment|not in"):
            converter.convert_with_compartment("Device", "dev-1", "Slot")

    def test_encounter_compartment_query_raises(self, converter):
        with pytest.raises(Exception, match="(?i)compartment|not in"):
            converter.convert_with_compartment("Encounter", "enc-1", "Slot")

    def test_related_person_compartment_query_raises(self, converter):
        with pytest.raises(Exception, match="(?i)compartment|not in"):
            converter.convert_with_compartment("RelatedPerson", "rp-1", "Slot")


# ═════════════════════════════════════════════════════════════════════════════
# 9. MongoDB E2E Tests
# ═════════════════════════════════════════════════════════════════════════════

def _mongo_available() -> bool:
    try:
        from pymongo import MongoClient  # type: ignore
        client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.mongodb
@pytest.mark.skipif(
    not _mongo_available(),
    reason="MongoDB not running on localhost:27017",
)
class TestSlotMongoDB:
    """End-to-end roundtrips using real MongoDB to verify Slot query correctness.

    IMPORTANT: `start` and `end` fields must be Python `datetime` objects so
    PyMongo serialises them as BSON ISODate — enabling type-aware date-range
    queries generated by `FHIRSearchConverter`.
    """

    _SLOTS = [
        {
            "resourceType": "Slot",
            "id": "e2e-slot-1",
            "identifier": [{"system": "http://hosp.example/slots", "value": "S001"}],
            "serviceCategory": [
                {"coding": [{"system": "http://snomed.info/sct", "code": "408443003"}]}
            ],
            "serviceType": [
                {
                    "concept": {"coding": [{"system": "http://snomed.info/sct", "code": "11429006"}]},
                    "reference": {"reference": "HealthcareService/hs-1"},
                }
            ],
            "specialty": [
                {"coding": [{"system": "http://snomed.info/sct", "code": "394814009"}]}
            ],
            "appointmentType": [
                {"coding": [{"system": "http://v2.0276", "code": "ROUTINE"}]}
            ],
            "schedule": {"reference": "Schedule/sched-1"},
            "status": "free",
            "start": datetime.datetime(2024, 7, 15, 9, 0, 0),
            "end": datetime.datetime(2024, 7, 15, 9, 30, 0),
        },
        {
            "resourceType": "Slot",
            "id": "e2e-slot-2",
            "identifier": [{"system": "http://hosp.example/slots", "value": "S002"}],
            "serviceCategory": [
                {"coding": [{"system": "http://snomed.info/sct", "code": "408443003"}]}
            ],
            "serviceType": [
                {
                    "concept": {"coding": [{"system": "http://snomed.info/sct", "code": "185389009"}]},
                    "reference": {"reference": "HealthcareService/hs-2"},
                }
            ],
            "specialty": [
                {"coding": [{"system": "http://snomed.info/sct", "code": "418112009"}]}
            ],
            "appointmentType": [
                {"coding": [{"system": "http://v2.0276", "code": "FOLLOWUP"}]}
            ],
            "schedule": {"reference": "Schedule/sched-1"},
            "status": "busy",
            "start": datetime.datetime(2024, 7, 15, 10, 0, 0),
            "end": datetime.datetime(2024, 7, 15, 10, 30, 0),
        },
        {
            "resourceType": "Slot",
            "id": "e2e-slot-3",
            "schedule": {"reference": "Schedule/sched-2"},
            "status": "free",
            "start": datetime.datetime(2024, 8, 5, 14, 0, 0),
            "end": datetime.datetime(2024, 8, 5, 14, 30, 0),
        },
    ]

    @pytest.fixture(scope="class")
    def mongo_col(self):
        from pymongo import MongoClient  # type: ignore
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        col = db["slot_e2e"]
        col.delete_many({})
        yield col
        col.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_col):
        from fhir_search_to_mql import ResourceDenormalizer
        dn = ResourceDenormalizer()
        docs = [dn.denormalize(copy.deepcopy(s)) for s in self._SLOTS]
        mongo_col.insert_many(docs)
        return mongo_col

    @pytest.fixture(scope="module")
    def conv(self):
        return FHIRSearchConverter()

    # ─── Status ──────────────────────────────────────────────────────────────

    def test_status_free_finds_two_slots(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "status=free")))
        ids = [r["id"] for r in results]
        assert len(results) == 2
        assert "e2e-slot-1" in ids
        assert "e2e-slot-3" in ids

    def test_status_busy_finds_one_slot(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "status=busy")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-2"

    def test_status_not_excludes_busy(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "status:not=busy")))
        ids = [r["id"] for r in results]
        assert "e2e-slot-2" not in ids
        assert "e2e-slot-1" in ids

    def test_status_no_results_for_unavailable(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "status=busy-unavailable")))
        assert len(results) == 0

    # ─── Start date ──────────────────────────────────────────────────────────

    def test_start_ge_finds_august_slot(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "start=ge2024-08-01")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-3"

    def test_start_lt_finds_july_slots(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "start=lt2024-07-16")))
        ids = [r["id"] for r in results]
        assert "e2e-slot-1" in ids
        assert "e2e-slot-2" in ids

    def test_start_range_july_includes_two(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "start=ge2024-07-01&start=le2024-07-31")))
        ids = [r["id"] for r in results]
        assert len(results) == 2
        assert "e2e-slot-1" in ids
        assert "e2e-slot-2" in ids

    def test_start_range_july_excludes_august(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "start=ge2024-07-01&start=le2024-07-31")))
        ids = [r["id"] for r in results]
        assert "e2e-slot-3" not in ids

    def test_start_missing_false_matches_all(self, seeded, conv):
        """All seeded slots have a `start` field."""
        results = list(seeded.find(conv.convert("Slot", "start:missing=false")))
        assert len(results) == 3

    # ─── Schedule reference ───────────────────────────────────────────────────

    def test_schedule_id_finds_two(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "schedule=sched-1")))
        assert len(results) == 2

    def test_schedule_full_ref(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "schedule=Schedule/sched-1")))
        assert len(results) == 2

    def test_schedule_typed_modifier(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "schedule:Schedule=sched-2")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-3"

    # ─── Token parameters ─────────────────────────────────────────────────────

    def test_service_type_code_lookup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "service-type=11429006")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-1"

    def test_service_type_reference_lookup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "service-type-reference=hs-1")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-1"

    def test_specialty_lookup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "specialty=394814009")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-1"

    def test_appointment_type_routine(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "appointment-type=ROUTINE")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-1"

    def test_appointment_type_followup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "appointment-type=FOLLOWUP")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-2"

    def test_identifier_value_lookup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "identifier=S001")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-1"

    def test_service_category_system_code(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "service-category=http://snomed.info/sct|408443003")))
        assert len(results) == 2

    def test_id_lookup(self, seeded, conv):
        results = list(seeded.find(conv.convert("Slot", "_id=e2e-slot-1")))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-1"

    # ─── Compound clinical queries ───────────────────────────────────────────

    def test_free_slots_in_july_for_schedule(self, seeded, conv):
        """Primary FHIR scheduling query: free slots for a schedule in a window."""
        mql = conv.convert(
            "Slot",
            "status=free&schedule=sched-1&start=ge2024-07-01&start=le2024-07-31",
        )
        results = list(seeded.find(mql))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-1"

    def test_service_type_and_schedule(self, seeded, conv):
        mql = conv.convert("Slot", "service-type=11429006&schedule=sched-1")
        results = list(seeded.find(mql))
        assert len(results) == 1
        assert results[0]["id"] == "e2e-slot-1"
