"""
Comprehensive integration tests for ALL Schedule search parameters per FHIR R5.

References:
- https://www.hl7.org/fhir/schedule-search.html
- https://www.hl7.org/fhir/schedule-definitions.html
- https://www.hl7.org/fhir/compartmentdefinition-patient.html
- https://www.hl7.org/fhir/compartmentdefinition-practitioner.html
- https://www.hl7.org/fhir/compartmentdefinition-device.html
- https://www.hl7.org/fhir/compartmentdefinition-relatedperson.html

This suite exercises all 11 search parameters declared in
``configs/Schedule.yaml``:

  Strings  (1): name
  Tokens   (5): active, identifier, service-category, service-type, specialty
  References(2): actor, service-type-reference
  Dates    (1): date (planningHorizon Period)
  Common   (2): _id, _lastUpdated

R5 structural notes:
  * ``Schedule.active``      — boolean is-modifier; top-level scalar, no _search.
  * ``Schedule.serviceType`` — CodeableReference(HealthcareService)[]; two
    separate search params cover concept arm vs reference arm.
  * ``Schedule.actor``       — Reference[] with 8 target types; all 3 precomputed
    compartments (Patient, Practitioner, Device) plus the dynamic RelatedPerson
    compartment use this single source path.
  * ``Schedule.planningHorizon`` — native Period; date search performs
    interval-overlap queries against _search.planningHorizon.

Compartments (FHIR R5):
  Patient, Practitioner, Device — PRECOMPUTED via _compartments.*
  RelatedPerson — DYNAMIC (live reference query at query time)
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, Dict, List

import pytest

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer

pytestmark = pytest.mark.integration


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def converter() -> FHIRSearchConverter:
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.fixture
def rich_schedule() -> Dict[str, Any]:
    """Schedule with all R5 fields populated, covering every denorm rule."""
    return {
        "resourceType": "Schedule",
        "id": "sched-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "identifier": [
            {"system": "http://hospital.example/schedules", "value": "SCHED-001"},
            {"system": "http://nhs.uk/ids", "value": "NHS-SCHED-99"},
        ],
        "active": True,
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
                            "system": "http://terminology.hl7.org/CodeSystem/service-type",
                            "code": "57",
                            "display": "Immunology",
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
        "name": "Morning Cardiology Clinic",
        "actor": [
            {"reference": "Practitioner/prac-1", "display": "Dr Jane Smith"},
            {"reference": "Patient/pat-1", "display": "John Doe"},
            {"reference": "Location/loc-1", "display": "Room 3B"},
            {"reference": "Device/dev-1", "display": "Ultrasound Machine"},
            {"reference": "RelatedPerson/rp-1", "display": "Carer"},
            {"reference": "HealthcareService/hs-2"},
            {"reference": "PractitionerRole/pr-role-1"},
            {"reference": "CareTeam/ct-1"},
        ],
        "planningHorizon": {
            "start": "2024-07-01T00:00:00Z",
            "end": "2024-12-31T23:59:59Z",
        },
        "comment": "Morning slots 08:00-12:00",
    }


@pytest.fixture
def minimal_schedule() -> Dict[str, Any]:
    """Minimal valid R5 Schedule (only resourceType + id + required actor)."""
    return {
        "resourceType": "Schedule",
        "id": "sched-min",
        "actor": [{"reference": "Practitioner/prac-min"}],
    }


# ─────────────────────────────────────────────────────────────────────────────
# 1. String parameter: name
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleStringParameters:
    """FHIR R5 §3.1.1.5.3 — name (Schedule.name, 0..1 scalar)."""

    def test_name_default_starts_with(self, converter):
        q = converter.convert("Schedule", "name=Morning")
        s = str(q)
        assert "_search.name_lower" in s
        assert "morning" in s

    def test_name_default_case_insensitive(self, converter):
        q_lower = converter.convert("Schedule", "name=morning cardiology")
        q_upper = converter.convert("Schedule", "name=MORNING CARDIOLOGY")
        # Both should target the lowercased field
        assert "_search.name_lower" in str(q_lower)
        assert "_search.name_lower" in str(q_upper)

    def test_name_exact_modifier(self, converter):
        q = converter.convert("Schedule", "name:exact=Morning Cardiology Clinic")
        s = str(q)
        assert "_search.name" in s
        assert "Morning Cardiology Clinic" in s

    def test_name_exact_case_sensitive(self, converter):
        q = converter.convert("Schedule", "name:exact=Morning Cardiology Clinic")
        assert "_search.name_lower" not in str(q)

    def test_name_contains_modifier(self, converter):
        q = converter.convert("Schedule", "name:contains=Cardio")
        s = str(q)
        assert "_search.name_lower" in s
        assert "cardio" in s.lower()

    def test_name_missing_true(self, converter):
        q = converter.convert("Schedule", "name:missing=true")
        s = str(q)
        assert "$exists" in s

    def test_name_missing_false(self, converter):
        q = converter.convert("Schedule", "name:missing=false")
        s = str(q)
        assert "$exists" in s


# ─────────────────────────────────────────────────────────────────────────────
# 2. Token parameters: active, identifier, service-category,
#                      service-type, specialty
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleTokenParameters:
    """FHIR R5 §3.1.1.5.4 — token parameters."""

    # ── active ─────────────────────────────────────────────────────────────
    def test_active_true_coerces_to_bool(self, converter):
        """Crucial: must emit Python True, not string 'true' (BSON type check)."""
        q = converter.convert("Schedule", "active=true")
        assert q == {"active": True}

    def test_active_false_coerces_to_bool(self, converter):
        q = converter.convert("Schedule", "active=false")
        assert q == {"active": False}

    def test_active_targets_root_field_not_search(self, converter):
        """active is queried directly on root, not under _search.*"""
        q = converter.convert("Schedule", "active=true")
        assert "_search" not in str(q)

    def test_active_missing(self, converter):
        q = converter.convert("Schedule", "active:missing=true")
        s = str(q)
        assert "$exists" in s

    # ── identifier ─────────────────────────────────────────────────────────
    def test_identifier_bare_value(self, converter):
        q = converter.convert("Schedule", "identifier=SCHED-001")
        assert "SCHED-001" in str(q)

    def test_identifier_system_pipe_code(self, converter):
        q = converter.convert(
            "Schedule",
            "identifier=http://hospital.example/schedules|SCHED-001",
        )
        s = str(q)
        assert "http://hospital.example/schedules" in s
        assert "SCHED-001" in s

    def test_identifier_system_only(self, converter):
        q = converter.convert(
            "Schedule",
            "identifier=http://hospital.example/schedules|",
        )
        assert "http://hospital.example/schedules" in str(q)

    def test_identifier_not_modifier(self, converter):
        q = converter.convert("Schedule", "identifier:not=SCHED-OLD")
        s = str(q)
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s

    def test_identifier_missing(self, converter):
        q = converter.convert("Schedule", "identifier:missing=true")
        s = str(q)
        assert "$exists" in s

    # ── service-category ───────────────────────────────────────────────────
    def test_service_category_bare_code(self, converter):
        q = converter.convert("Schedule", "service-category=408443003")
        assert "408443003" in str(q)

    def test_service_category_system_pipe_code(self, converter):
        q = converter.convert(
            "Schedule",
            "service-category=http://snomed.info/sct|408443003",
        )
        s = str(q)
        assert "408443003" in s
        assert "snomed" in s.lower()

    def test_service_category_not_modifier(self, converter):
        q = converter.convert("Schedule", "service-category:not=999")
        s = str(q)
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s

    def test_service_category_missing(self, converter):
        q = converter.convert("Schedule", "service-category:missing=true")
        assert "$exists" in str(q)

    # ── service-type (CodeableReference concept arm) ───────────────────────
    def test_service_type_bare_code(self, converter):
        q = converter.convert("Schedule", "service-type=11429006")
        s = str(q)
        assert "serviceType" in s or "service_type" in s.lower() or "11429006" in s

    def test_service_type_system_pipe_code(self, converter):
        q = converter.convert(
            "Schedule",
            "service-type=http://snomed.info/sct|11429006",
        )
        s = str(q)
        assert "11429006" in s
        assert "snomed" in s.lower()

    def test_service_type_not_modifier(self, converter):
        q = converter.convert("Schedule", "service-type:not=57")
        s = str(q)
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s

    # ── specialty ──────────────────────────────────────────────────────────
    def test_specialty_bare_code(self, converter):
        q = converter.convert("Schedule", "specialty=394814009")
        assert "394814009" in str(q)

    def test_specialty_system_pipe_code(self, converter):
        q = converter.convert(
            "Schedule",
            "specialty=http://snomed.info/sct|394814009",
        )
        s = str(q)
        assert "394814009" in s

    def test_specialty_not_modifier(self, converter):
        q = converter.convert("Schedule", "specialty:not=CARDIO")
        s = str(q)
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s

    def test_specialty_missing(self, converter):
        q = converter.convert("Schedule", "specialty:missing=true")
        assert "$exists" in str(q)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Reference parameters: actor, service-type-reference
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleReferenceParameters:
    """FHIR R5 reference search parameters."""

    # ── actor ──────────────────────────────────────────────────────────────
    def test_actor_bare_id(self, converter):
        q = converter.convert("Schedule", "actor=prac-1")
        assert "prac-1" in str(q)

    def test_actor_full_reference_practitioner(self, converter):
        q = converter.convert("Schedule", "actor=Practitioner/prac-1")
        assert "prac-1" in str(q)

    def test_actor_full_reference_patient(self, converter):
        q = converter.convert("Schedule", "actor=Patient/pat-1")
        assert "pat-1" in str(q)

    def test_actor_full_reference_device(self, converter):
        q = converter.convert("Schedule", "actor=Device/dev-1")
        assert "dev-1" in str(q)

    def test_actor_full_reference_location(self, converter):
        q = converter.convert("Schedule", "actor=Location/loc-1")
        assert "loc-1" in str(q)

    def test_actor_full_reference_healthcare_service(self, converter):
        q = converter.convert("Schedule", "actor=HealthcareService/hs-2")
        assert "hs-2" in str(q)

    def test_actor_full_reference_related_person(self, converter):
        q = converter.convert("Schedule", "actor=RelatedPerson/rp-1")
        assert "rp-1" in str(q)

    def test_actor_typed_resource_modifier_practitioner(self, converter):
        """Typed-resource modifier filters by type — valid R5 reference modifier."""
        q = converter.convert("Schedule", "actor:Practitioner=prac-1")
        assert "prac-1" in str(q)

    def test_actor_typed_resource_modifier_patient(self, converter):
        q = converter.convert("Schedule", "actor:Patient=pat-1")
        assert "pat-1" in str(q)

    def test_actor_typed_resource_modifier_device(self, converter):
        q = converter.convert("Schedule", "actor:Device=dev-1")
        assert "dev-1" in str(q)

    def test_actor_missing(self, converter):
        q = converter.convert("Schedule", "actor:missing=true")
        assert "$exists" in str(q)

    # ── service-type-reference (CodeableReference reference arm) ───────────
    def test_service_type_reference_bare_id(self, converter):
        q = converter.convert("Schedule", "service-type-reference=hs-1")
        assert "hs-1" in str(q)

    def test_service_type_reference_full(self, converter):
        q = converter.convert(
            "Schedule", "service-type-reference=HealthcareService/hs-1"
        )
        assert "hs-1" in str(q)

    def test_service_type_reference_missing(self, converter):
        q = converter.convert("Schedule", "service-type-reference:missing=true")
        assert "$exists" in str(q)


# ─────────────────────────────────────────────────────────────────────────────
# 4. Date parameter: date (Schedule.planningHorizon Period)
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleDateParameters:
    """FHIR R5 date search against planningHorizon Period."""

    def test_date_eq(self, converter):
        q = converter.convert("Schedule", "date=2024-08-15")
        s = str(q)
        assert "planningHorizon" in s

    def test_date_ge(self, converter):
        q = converter.convert("Schedule", "date=ge2024-07-01")
        s = str(q)
        assert "$gte" in s
        assert "planningHorizon" in s

    def test_date_le(self, converter):
        q = converter.convert("Schedule", "date=le2024-12-31")
        s = str(q)
        assert "$lte" in s

    def test_date_lt(self, converter):
        q = converter.convert("Schedule", "date=lt2025-01-01")
        s = str(q)
        assert "$lt" in s

    def test_date_gt(self, converter):
        q = converter.convert("Schedule", "date=gt2024-06-30")
        s = str(q)
        assert "$gt" in s

    def test_date_ne(self, converter):
        # For a period field, `ne` generates an $or of the boundary
        # conditions (date NOT overlapping the period), which appears
        # as $lte/$gte rather than a literal $ne.
        q = converter.convert("Schedule", "date=ne2024-01-01")
        s = str(q)
        assert "planningHorizon" in s

    def test_date_range_overlap(self, converter):
        q = converter.convert(
            "Schedule", "date=ge2024-07-01&date=le2024-12-31"
        )
        s = str(q)
        assert "$gte" in s
        assert "$lte" in s

    def test_date_missing(self, converter):
        q = converter.convert("Schedule", "date:missing=true")
        assert "$exists" in str(q)

    def test_last_updated_ge(self, converter):
        q = converter.convert("Schedule", "_lastUpdated=ge2024-01-01")
        s = str(q)
        assert "lastUpdated" in s
        assert "$gte" in s


# ─────────────────────────────────────────────────────────────────────────────
# 5. Common parameters
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleCommonParameters:
    def test_id_search(self, converter):
        q = converter.convert("Schedule", "_id=sched-rich")
        assert "sched-rich" in str(q)

    def test_id_targets_root_id(self, converter):
        q = converter.convert("Schedule", "_id=sched-rich")
        assert q.get("id") == "sched-rich"


# ─────────────────────────────────────────────────────────────────────────────
# 6. Modifiers
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleModifiers:
    def test_name_text_modifier(self, converter):
        """`:text` is valid for token params; not applicable to string but
        verifying standard string contains-like behaviour."""
        q = converter.convert("Schedule", "name:contains=Clinic")
        assert "clinic" in str(q).lower()

    def test_identifier_not_modifier(self, converter):
        q = converter.convert("Schedule", "identifier:not=OLD-ID")
        s = str(q)
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s

    def test_service_category_not_modifier(self, converter):
        q = converter.convert("Schedule", "service-category:not=999")
        s = str(q)
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s

    def test_actor_missing_true(self, converter):
        q = converter.convert("Schedule", "actor:missing=true")
        assert "$exists" in str(q)

    def test_date_missing_true(self, converter):
        q = converter.convert("Schedule", "date:missing=true")
        assert "$exists" in str(q)

    def test_service_type_not_modifier(self, converter):
        q = converter.convert("Schedule", "service-type:not=OLD")
        s = str(q)
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s


# ─────────────────────────────────────────────────────────────────────────────
# 7. Combined queries
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleCombinations:
    def test_active_and_actor(self, converter):
        q = converter.convert("Schedule", "active=true&actor=Practitioner/prac-1")
        s = str(q)
        assert "True" in s
        assert "prac-1" in s

    def test_service_type_and_specialty(self, converter):
        q = converter.convert(
            "Schedule",
            "service-type=11429006&specialty=394814009",
        )
        s = str(q)
        assert "11429006" in s
        assert "394814009" in s

    def test_name_and_date(self, converter):
        q = converter.convert(
            "Schedule",
            "name=Cardiology&date=ge2024-07-01",
        )
        s = str(q)
        assert "cardiology" in s.lower()
        assert "$gte" in s

    def test_actor_and_date_range(self, converter):
        q = converter.convert(
            "Schedule",
            "actor=prac-1&date=ge2024-07-01&date=le2024-12-31",
        )
        s = str(q)
        assert "prac-1" in s
        assert "$gte" in s
        assert "$lte" in s

    def test_identifier_and_service_category(self, converter):
        q = converter.convert(
            "Schedule",
            "identifier=SCHED-001&service-category=408443003",
        )
        s = str(q)
        assert "SCHED-001" in s
        assert "408443003" in s


# ─────────────────────────────────────────────────────────────────────────────
# 8. Denormalization correctness
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleDenormalization:
    def test_identifier_values_present(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert "SCHED-001" in out["_search"]["identifier_values"]
        assert "NHS-SCHED-99" in out["_search"]["identifier_values"]

    def test_identifier_system_code_present(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert (
            "http://hospital.example/schedules|SCHED-001"
            in out["_search"]["identifier_systemCode"]
        )

    def test_service_category_codes(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert "408443003" in out["_search"]["serviceCategory_codes"]

    def test_service_category_system_code(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert (
            "http://snomed.info/sct|408443003"
            in out["_search"]["serviceCategory_systemCode"]
        )

    def test_service_type_codes_both_entries(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        codes = out["_search"]["serviceType_codes"]
        assert "11429006" in codes
        assert "57" in codes

    def test_service_type_reference_id(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert "hs-1" in out["_search"]["serviceTypeReferenceId"]

    def test_service_type_reference_type(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert "HealthcareService" in out["_search"]["serviceTypeReferenceType"]

    def test_specialty_codes(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert "394814009" in out["_search"]["specialty_codes"]

    def test_name_case_preserved(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert out["_search"]["name"] == "Morning Cardiology Clinic"

    def test_name_lower_is_lowercased(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert out["_search"]["name_lower"] == "morning cardiology clinic"

    def test_actor_ids_all_eight_types(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        ids = out["_search"]["actorIds"]
        assert "prac-1" in ids
        assert "pat-1" in ids
        assert "loc-1" in ids
        assert "dev-1" in ids
        assert "rp-1" in ids
        assert "hs-2" in ids
        assert "pr-role-1" in ids
        assert "ct-1" in ids

    def test_actor_types_all_eight_types(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        types = out["_search"]["actorTypes"]
        assert "Practitioner" in types
        assert "Patient" in types
        assert "Location" in types
        assert "Device" in types
        assert "RelatedPerson" in types
        assert "HealthcareService" in types
        assert "PractitionerRole" in types
        assert "CareTeam" in types

    def test_planning_horizon_start_end(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        ph = out["_search"]["planningHorizon"]
        assert "start" in ph
        assert "end" in ph
        assert "2024-07-01" in ph["start"]
        assert "2024-12-31" in ph["end"]

    def test_minimal_schedule_sparse_output(self, denormalizer, minimal_schedule):
        """Sparse-output contract: fields absent in resource must be absent from _search."""
        out = denormalizer.denormalize(minimal_schedule)
        search = out.get("_search", {})
        for f in (
            "identifier_values", "identifier_systemCode",
            "serviceCategory_codes", "serviceCategory_systemCode",
            "serviceType_codes", "serviceType_systemCode",
            "serviceTypeReferenceId", "serviceTypeReferenceType",
            "specialty_codes", "specialty_systemCode",
            "name", "name_lower",
            "planningHorizon",
        ):
            assert f not in search, f"Sparse-output violation: {f!r} present in minimal Schedule"

    def test_minimal_schedule_actor_denormalized(self, denormalizer, minimal_schedule):
        """actor is required (1..*) so it must always be present."""
        out = denormalizer.denormalize(minimal_schedule)
        assert "prac-min" in out["_search"]["actorIds"]


# ─────────────────────────────────────────────────────────────────────────────
# 9. Resource purity
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleResourcePurity:
    def test_root_keys_bounded_to_resource_plus_buckets(
        self, denormalizer, rich_schedule
    ):
        original_keys = set(rich_schedule.keys())
        out = denormalizer.denormalize(rich_schedule)
        added = set(out.keys()) - original_keys
        assert added.issubset({"_search", "_compartments"}), (
            f"Unexpected root-level keys added by denormalization: {added}"
        )

    def test_no_search_projections_at_root(self, denormalizer, rich_schedule):
        """_search projection fields (suffixed or with _ ) must NOT appear at root."""
        out = denormalizer.denormalize(rich_schedule)
        # Projection-only field names that have no corresponding top-level FHIR path
        forbidden = {
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
            "name_lower",
            "actorIds",
            "actorTypes",
            "planningHorizon",   # FHIR root field "planningHorizon" is the Period
                                 # _search.planningHorizon is ALSO "planningHorizon".
                                 # The _search bucket captures it correctly; the root
                                 # FHIR field is still present and unchanged.
        }
        # Check projection-only suffixed names are not at root
        for f in (
            "identifier_values", "identifier_systemCode",
            "serviceCategory_codes", "serviceCategory_systemCode",
            "serviceType_codes", "serviceType_systemCode",
            "serviceTypeReferenceId", "serviceTypeReferenceType",
            "specialty_codes", "specialty_systemCode",
            "name_lower", "actorIds", "actorTypes",
        ):
            assert f not in out, (
                f"Denormalization leaked projection field {f!r} to resource root"
            )

    def test_fhir_root_fields_preserved_unchanged(
        self, denormalizer, rich_schedule
    ):
        out = denormalizer.denormalize(rich_schedule)
        for field in ("id", "identifier", "active", "serviceType", "specialty",
                      "name", "actor", "planningHorizon", "comment"):
            if field in rich_schedule:
                assert out[field] == rich_schedule[field], (
                    f"Field {field!r} was mutated by denormalization"
                )

    def test_input_not_mutated_in_place(self, denormalizer, rich_schedule):
        original = copy.deepcopy(rich_schedule)
        denormalizer.denormalize(rich_schedule)
        assert rich_schedule == original


# ─────────────────────────────────────────────────────────────────────────────
# 10. Compartment routing — precomputed (Patient, Practitioner, Device)
# ─────────────────────────────────────────────────────────────────────────────

class TestSchedulePrecomputedCompartments:
    """All three precomputed compartments source from Schedule.actor."""

    def test_patient_compartment_populated(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert "pat-1" in out["_compartments"]["Patient"]

    def test_practitioner_compartment_populated(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert "prac-1" in out["_compartments"]["Practitioner"]

    def test_device_compartment_populated(self, denormalizer, rich_schedule):
        out = denormalizer.denormalize(rich_schedule)
        assert "dev-1" in out["_compartments"]["Device"]

    def test_related_person_not_in_precomputed(self, denormalizer, rich_schedule):
        """RelatedPerson is DYNAMIC — must NOT appear in _compartments."""
        out = denormalizer.denormalize(rich_schedule)
        assert "RelatedPerson" not in out.get("_compartments", {})

    def test_non_patient_actors_not_in_patient_compartment(
        self, denormalizer, rich_schedule
    ):
        """Other actor types (Location, Device, etc.) must not pollute _compartments.Patient."""
        out = denormalizer.denormalize(rich_schedule)
        patient_ids = out["_compartments"]["Patient"]
        assert "loc-1" not in patient_ids
        assert "dev-1" not in patient_ids
        assert "prac-1" not in patient_ids

    def test_non_device_actors_not_in_device_compartment(
        self, denormalizer, rich_schedule
    ):
        out = denormalizer.denormalize(rich_schedule)
        device_ids = out["_compartments"]["Device"]
        assert "pat-1" not in device_ids
        assert "prac-1" not in device_ids
        assert "loc-1" not in device_ids

    def test_patient_query_uses_fast_path(self, converter):
        q = converter.convert_with_compartment("Patient", "pat-1", "Schedule")
        assert q == {"_compartments.Patient": "pat-1"}

    def test_practitioner_query_uses_fast_path(self, converter):
        q = converter.convert_with_compartment("Practitioner", "prac-1", "Schedule")
        assert q == {"_compartments.Practitioner": "prac-1"}

    def test_device_query_uses_fast_path(self, converter):
        q = converter.convert_with_compartment("Device", "dev-1", "Schedule")
        assert q == {"_compartments.Device": "dev-1"}

    def test_multiple_patients_in_actor_all_in_compartment(
        self, denormalizer
    ):
        """All Patient actors should be captured in _compartments.Patient."""
        schedule = {
            "resourceType": "Schedule",
            "id": "sched-multi-pat",
            "actor": [
                {"reference": "Patient/pat-A"},
                {"reference": "Patient/pat-B"},
                {"reference": "Practitioner/prac-X"},
            ],
        }
        out = denormalizer.denormalize(schedule)
        assert "pat-A" in out["_compartments"]["Patient"]
        assert "pat-B" in out["_compartments"]["Patient"]
        assert "prac-X" not in out["_compartments"]["Patient"]

    def test_minimal_schedule_actor_populates_practitioner_compartment(
        self, denormalizer, minimal_schedule
    ):
        out = denormalizer.denormalize(minimal_schedule)
        assert "prac-min" in out["_compartments"]["Practitioner"]


# ─────────────────────────────────────────────────────────────────────────────
# 11. Compartment routing — dynamic (RelatedPerson)
# ─────────────────────────────────────────────────────────────────────────────

class TestScheduleDynamicCompartment:
    """RelatedPerson compartment uses the dynamic strategy (no precomputation)."""

    def test_related_person_query_is_dynamic(self, converter):
        """The converter should fall back to a dynamic reference query
        since RelatedPerson is NOT in compartments.precomputed."""
        q = converter.convert_with_compartment("RelatedPerson", "rp-1", "Schedule")
        q_str = str(q)
        # Dynamic query hits the actor reference field directly
        assert "_compartments.RelatedPerson" not in q_str
        # It should reference the actor path
        assert "rp-1" in q_str


# ─────────────────────────────────────────────────────────────────────────────
# 12. MongoDB end-to-end
# ─────────────────────────────────────────────────────────────────────────────

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
class TestScheduleMongoDB:
    """Live end-to-end roundtrips using real MongoDB."""

    @pytest.fixture(scope="class")
    def mongo_collection(self):
        from pymongo import MongoClient  # type: ignore

        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["schedule_e2e"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_collection):
        from fhir_search_to_mql import ResourceDenormalizer

        dn = ResourceDenormalizer()
        records: List[Dict[str, Any]] = [
            # sched-A  — active, Patient + Practitioner actors, planningHorizon 2024
            {
                "resourceType": "Schedule",
                "id": "sched-A",
                "identifier": [{"system": "http://example.org", "value": "ID-A"}],
                "active": True,
                "serviceCategory": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "408443003"}]}
                ],
                "serviceType": [
                    {
                        "concept": {
                            "coding": [
                                {"system": "http://snomed.info/sct", "code": "11429006"}
                            ]
                        },
                        "reference": {"reference": "HealthcareService/hs-A"},
                    }
                ],
                "specialty": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "394814009"}]}
                ],
                "name": "Cardiology Clinic",
                "actor": [
                    {"reference": "Practitioner/prac-A"},
                    {"reference": "Patient/pat-A"},
                    {"reference": "Device/dev-A"},
                ],
                # IMPORTANT: seed dates as Python datetime objects so MongoDB
                # stores BSON ISODate matching the converter's datetime operands.
                "planningHorizon": {
                    "start": datetime(2024, 7, 1),
                    "end": datetime(2024, 12, 31),
                },
                "meta": {"lastUpdated": datetime(2024, 8, 22, 10, 30)},
            },
            # sched-B  — inactive, Device + HealthcareService actors, planningHorizon 2025
            {
                "resourceType": "Schedule",
                "id": "sched-B",
                "identifier": [{"system": "http://example.org", "value": "ID-B"}],
                "active": False,
                "serviceCategory": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "999999"}]}
                ],
                "serviceType": [
                    {
                        "concept": {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/service-type",
                                    "code": "57",
                                }
                            ]
                        },
                        "reference": {"reference": "HealthcareService/hs-B"},
                    }
                ],
                "name": "Radiology Suite",
                "actor": [
                    {"reference": "Device/dev-B"},
                    {"reference": "HealthcareService/hs-B"},
                    {"reference": "Location/loc-B"},
                ],
                "planningHorizon": {
                    "start": datetime(2025, 1, 1),
                    "end": datetime(2025, 6, 30),
                },
                "meta": {"lastUpdated": datetime(2025, 2, 1)},
            },
            # sched-C  — active, RelatedPerson actor, no planningHorizon
            {
                "resourceType": "Schedule",
                "id": "sched-C",
                "active": True,
                "name": "Home Carer Schedule",
                "actor": [
                    {"reference": "RelatedPerson/rp-C"},
                    {"reference": "Patient/pat-C"},
                ],
                "meta": {"lastUpdated": datetime(2024, 3, 15)},
            },
        ]
        denorm = [dn.denormalize(r) for r in records]
        mongo_collection.insert_many(denorm)
        return mongo_collection

    # ── active ──────────────────────────────────────────────────────────────
    def test_active_true_returns_active_schedules(self, seeded, converter):
        q = converter.convert("Schedule", "active=true")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-C" in ids
        assert "sched-B" not in ids

    def test_active_false_returns_inactive(self, seeded, converter):
        q = converter.convert("Schedule", "active=false")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-B" in ids
        assert "sched-A" not in ids

    # ── name ────────────────────────────────────────────────────────────────
    def test_name_default_starts_with(self, seeded, converter):
        q = converter.convert("Schedule", "name=Cardio")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    def test_name_case_insensitive(self, seeded, converter):
        q = converter.convert("Schedule", "name=CARDIO")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids

    def test_name_exact(self, seeded, converter):
        q = converter.convert("Schedule", "name:exact=Cardiology Clinic")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    # ── identifier ──────────────────────────────────────────────────────────
    def test_identifier_bare_value(self, seeded, converter):
        q = converter.convert("Schedule", "identifier=ID-A")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    def test_identifier_system_pipe_code(self, seeded, converter):
        q = converter.convert("Schedule", "identifier=http://example.org|ID-B")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-B" in ids

    # ── service-category ────────────────────────────────────────────────────
    def test_service_category_code(self, seeded, converter):
        q = converter.convert("Schedule", "service-category=408443003")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    # ── service-type ────────────────────────────────────────────────────────
    def test_service_type_code(self, seeded, converter):
        q = converter.convert("Schedule", "service-type=11429006")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    # ── service-type-reference ──────────────────────────────────────────────
    def test_service_type_reference_id(self, seeded, converter):
        q = converter.convert("Schedule", "service-type-reference=hs-A")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    # ── actor ───────────────────────────────────────────────────────────────
    def test_actor_practitioner(self, seeded, converter):
        q = converter.convert("Schedule", "actor=Practitioner/prac-A")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    def test_actor_device(self, seeded, converter):
        q = converter.convert("Schedule", "actor=Device/dev-B")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-B" in ids
        assert "sched-A" not in ids

    def test_actor_location(self, seeded, converter):
        q = converter.convert("Schedule", "actor=Location/loc-B")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-B" in ids

    # ── date (planningHorizon Period) ────────────────────────────────────────
    def test_date_ge_returns_2024_schedule(self, seeded, converter):
        q = converter.convert("Schedule", "date=ge2024-07-01")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids

    def test_date_ge_2025_returns_only_sched_b(self, seeded, converter):
        q = converter.convert("Schedule", "date=ge2025-01-01")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-B" in ids
        assert "sched-A" not in ids

    def test_date_le_2024_returns_only_sched_a(self, seeded, converter):
        q = converter.convert("Schedule", "date=le2024-12-31")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    # ── compartment fast-path ────────────────────────────────────────────────
    def test_patient_compartment_fast_path(self, seeded, converter):
        q = converter.convert_with_compartment("Patient", "pat-A", "Schedule")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    def test_practitioner_compartment_fast_path(self, seeded, converter):
        q = converter.convert_with_compartment("Practitioner", "prac-A", "Schedule")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-C" not in ids

    def test_device_compartment_fast_path(self, seeded, converter):
        q = converter.convert_with_compartment("Device", "dev-A", "Schedule")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-A" in ids
        assert "sched-B" not in ids

    def test_patient_c_compartment_via_fast_path(self, seeded, converter):
        q = converter.convert_with_compartment("Patient", "pat-C", "Schedule")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-C" in ids
        assert "sched-A" not in ids

    # ── _lastUpdated ─────────────────────────────────────────────────────────
    def test_last_updated_ge_2025(self, seeded, converter):
        q = converter.convert("Schedule", "_lastUpdated=ge2025-01-01")
        ids = {r["id"] for r in seeded.find(q)}
        assert "sched-B" in ids
        assert "sched-A" not in ids


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
