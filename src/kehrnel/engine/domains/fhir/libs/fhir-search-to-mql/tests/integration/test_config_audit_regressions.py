"""
End-to-end regression tests for the bundled-config audit pass.

These tests pin the SHAPE of `_search.*` produced by the bundled
configs against realistic FHIR R5 sample resources. Each `test_*`
method targets a specific historical bug surfaced by the
`_audit_configs.py` smoke harness:

  - Patient.deceased[x] — DirectFieldExtractor + transform: presence
    bridge for the polymorphic choice.
  - Patient.communication.language — `source: $resource` so the path
    resolver descends through the BackboneElement wrapper.
  - Appointment.reason / serviceType (FHIR R5 CodeableReference) —
    `source: $resource` so the extractor resolves
    `reason[*].concept.coding[*]` AND `reason[*].reference` correctly
    (pre-resolved mode silently dropped both).
  - Appointment.requestedPeriod — PeriodExtractor `array[object]`
    branch produces `[{start, end}, ...]` instead of a flat array of
    start strings (the legacy shape lost the `end` boundary entirely).
  - Observation.effectiveTimingBounds — TimingExtractor falls back to
    `repeat.boundsPeriod` when `event[]` is absent.
  - CodeableConceptExtractor — array-typed targets are SPARSE (no
    empty-array pollution when the input has no codings).

The whole suite runs against ResourceDenormalizer directly (no
MongoDB), so it is fast and order-independent.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from fhir_search_to_mql import ResourceDenormalizer


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


# ---------------------------------------------------------------------------
# Patient.deceased[x] polymorphic CHOICE
# ---------------------------------------------------------------------------


class TestPatientDeceasedPolymorphism:
    """
    `Patient.deceased[x]` is a CHOICE — it appears as either
    `deceasedBoolean` or `deceasedDateTime`. The previous YAML used
    `source: deceased` (which never matched a real resource because
    FHIR's polymorphic siblings carry the type suffix) AND named a
    nonexistent `DirectFieldExtractor`. The fix introduces the real
    DirectFieldExtractor, switches the rule to `source: $resource`,
    and adds `transform: presence` to coerce a deceasedDateTime into
    `deceased=true` for the FHIR R5 token search parameter.
    """

    def test_deceasedBoolean_writes_deceased_token(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        resource = {
            "resourceType": "Patient",
            "id": "p1",
            "deceasedBoolean": True,
        }
        result = denormalizer.denormalize(resource)
        assert result["_search"]["deceased"] is True
        assert "deathDate" not in result["_search"], (
            "deceasedBoolean=true must NOT synthesize a phantom deathDate"
        )

    def test_deceasedDateTime_writes_both_token_and_date(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        resource = {
            "resourceType": "Patient",
            "id": "p2",
            "deceasedDateTime": "2024-03-15T12:00:00Z",
        }
        result = denormalizer.denormalize(resource)
        assert result["_search"]["deceased"] is True, (
            "deceasedDateTime presence must imply deceased=true per the "
            "R5 `deceased` token search parameter"
        )
        assert result["_search"]["deathDate"] == "2024-03-15T12:00:00Z"

    def test_no_deceased_field_omits_both_targets(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        resource = {"resourceType": "Patient", "id": "p3"}
        result = denormalizer.denormalize(resource)
        # _search may not even be present when no rule fires; either
        # way, neither deceased nor deathDate must surface.
        search = result.get("_search", {})
        assert "deceased" not in search
        assert "deathDate" not in search


# ---------------------------------------------------------------------------
# Patient.communication[].language
# ---------------------------------------------------------------------------


class TestPatientCommunicationLanguage:
    """Verify `_search.language` is populated from the BackboneElement."""

    def test_communication_language_is_extracted(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        resource = {
            "resourceType": "Patient",
            "id": "p1",
            "communication": [
                {"language": {"coding": [{"system": "urn:ietf:bcp:47", "code": "en-US"}]}},
                {"language": {"coding": [{"system": "urn:ietf:bcp:47", "code": "es"}]}},
            ],
        }
        result = denormalizer.denormalize(resource)
        assert result["_search"]["language"] == ["en-US", "es"]

    def test_no_communication_omits_language(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        resource = {"resourceType": "Patient", "id": "p1"}
        result = denormalizer.denormalize(resource)
        assert "language" not in result.get("_search", {})


# ---------------------------------------------------------------------------
# Appointment.reason / serviceType (FHIR R5 CodeableReference)
# ---------------------------------------------------------------------------


class TestAppointmentCodeableReferenceR5:
    """
    In FHIR R5 `Appointment.reason` and `Appointment.serviceType` are
    `CodeableReference[]` (`{concept: CodeableConcept, reference:
    Reference}`). Pre-resolved extractor mode crashed on the inner
    Reference dict (no `.startswith`) for `reason[*].reference` and
    silently dropped codings for `reason[*].concept.coding[*]`. The
    fix routes both rules through `source: $resource` so the path
    resolver descends through the CodeableReference wrapper.
    """

    @pytest.fixture
    def appointment(self) -> Dict[str, Any]:
        return {
            "resourceType": "Appointment",
            "id": "appt-001",
            "status": "booked",
            # CodeableReference[] entries can carry concept, reference,
            # or both. We supply one of each shape so both projection
            # rules have realistic data.
            "reason": [
                {"concept": {"coding": [{"system": "http://snomed.info/sct", "code": "162673000"}]}},
                {"reference": {"reference": "Condition/cond-1"}},
            ],
            "serviceType": [
                {
                    "concept": {
                        "coding": [{"system": "http://terminology.hl7.org/CodeSystem/service-type", "code": "124"}]
                    },
                    "reference": {"reference": "HealthcareService/hs-1"},
                }
            ],
            "participant": [
                {"actor": {"reference": "Patient/pat-001"}, "status": "accepted"},
            ],
        }

    def test_reason_codes_extracted_from_concept_branch(
        self, denormalizer: ResourceDenormalizer, appointment: Dict[str, Any]
    ) -> None:
        result = denormalizer.denormalize(appointment)
        assert result["_search"]["reasonCode_codes"] == ["162673000"]
        assert result["_search"]["reasonCode_systemCode"] == [
            "http://snomed.info/sct|162673000"
        ]

    def test_reason_reference_extracted_without_crashing(
        self, denormalizer: ResourceDenormalizer, appointment: Dict[str, Any]
    ) -> None:
        warnings: List[str] = []
        result = denormalizer.denormalize(appointment, warnings=warnings)
        assert warnings == [], f"unexpected warnings: {warnings!r}"
        assert result["_search"]["reasonReferenceId"] == ["cond-1"]
        assert result["_search"]["reasonReferenceType"] == ["Condition"]

    def test_serviceType_codes_extracted_from_concept_branch(
        self, denormalizer: ResourceDenormalizer, appointment: Dict[str, Any]
    ) -> None:
        result = denormalizer.denormalize(appointment)
        assert result["_search"]["serviceType_codes"] == ["124"]
        assert result["_search"]["serviceType_systemCode"] == [
            "http://terminology.hl7.org/CodeSystem/service-type|124"
        ]

    def test_serviceType_reference_extracted(
        self, denormalizer: ResourceDenormalizer, appointment: Dict[str, Any]
    ) -> None:
        result = denormalizer.denormalize(appointment)
        assert result["_search"]["serviceTypeReferenceId"] == ["hs-1"]

    def test_codeable_concept_array_is_sparse_when_no_codings(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        """A reason with ONLY a reference (no concept) must NOT leak `[]`."""
        appointment = {
            "resourceType": "Appointment",
            "id": "appt-002",
            "status": "booked",
            "reason": [{"reference": {"reference": "Condition/cond-1"}}],
            "participant": [{"actor": {"reference": "Patient/pat-001"}, "status": "accepted"}],
        }
        result = denormalizer.denormalize(appointment)
        assert "reasonCode_codes" not in result["_search"], (
            "Empty arrays must be omitted from _search to keep indexes clean"
        )
        assert "reasonCode_systemCode" not in result["_search"]
        # The reference branch should still populate.
        assert result["_search"]["reasonReferenceId"] == ["cond-1"]


# ---------------------------------------------------------------------------
# Appointment.requestedPeriod (Period[])
# ---------------------------------------------------------------------------


class TestAppointmentRequestedPeriodShape:
    """
    `_search.requestedPeriod` must be `[{start, end}, ...]` so date
    range queries work after a $unwind. The legacy shape was a flat
    array of start strings, which lost the end boundary entirely.
    """

    def test_requested_period_shape(self, denormalizer: ResourceDenormalizer) -> None:
        appointment = {
            "resourceType": "Appointment",
            "id": "appt-001",
            "status": "booked",
            "requestedPeriod": [
                {"start": "2024-06-25T00:00:00Z", "end": "2024-07-15T00:00:00Z"},
                {"start": "2024-08-01T00:00:00Z", "end": "2024-08-15T00:00:00Z"},
            ],
            "participant": [{"actor": {"reference": "Patient/pat-001"}, "status": "accepted"}],
        }
        result = denormalizer.denormalize(appointment)
        assert result["_search"]["requestedPeriod"] == [
            {"start": "2024-06-25T00:00:00Z", "end": "2024-07-15T00:00:00Z"},
            {"start": "2024-08-01T00:00:00Z", "end": "2024-08-15T00:00:00Z"},
        ]


# ---------------------------------------------------------------------------
# Observation.effectiveTimingBounds
# ---------------------------------------------------------------------------


class TestObservationEffectiveTimingBoundsFallback:
    """
    The `date` search parameter for Observation reads
    `_search.effectiveTimingBounds`. The legacy TimingExtractor only
    populated this from `effectiveTiming.event[]`. An Observation
    using `effectiveTiming.repeat.boundsPeriod` (the recurring-schedule
    shape) silently produced no bounds and the date search returned
    nothing. The fallback now writes the boundsPeriod directly when
    `event[]` is absent.
    """

    def _obs(self, **timing_extra: Any) -> Dict[str, Any]:
        timing: Dict[str, Any] = dict(timing_extra)
        return {
            "resourceType": "Observation",
            "id": "obs-001",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6"}]},
            "subject": {"reference": "Patient/pat-001"},
            "effectiveTiming": timing,
        }

    def test_falls_back_to_repeat_boundsPeriod(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        obs = self._obs(repeat={"boundsPeriod": {"start": "2024-06-15T08:00:00Z", "end": "2024-06-15T08:30:00Z"}})
        result = denormalizer.denormalize(obs)
        assert result["_search"]["effectiveTimingBounds"] == {
            "start": "2024-06-15T08:00:00Z",
            "end": "2024-06-15T08:30:00Z",
        }

    def test_event_takes_priority_over_repeat_bounds(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        obs = self._obs(
            event=["2024-06-15T08:00:00Z", "2024-06-15T08:30:00Z"],
            repeat={"boundsPeriod": {"start": "2030-01-01T00:00:00Z", "end": "2030-12-31T00:00:00Z"}},
        )
        result = denormalizer.denormalize(obs)
        assert result["_search"]["effectiveTimingBounds"] == {
            "start": "2024-06-15T08:00:00Z",
            "end": "2024-06-15T08:30:00Z",
        }

    def test_omitted_when_no_bounds_data(self, denormalizer: ResourceDenormalizer) -> None:
        obs = self._obs(repeat={"frequency": 2})
        result = denormalizer.denormalize(obs)
        assert "effectiveTimingBounds" not in result["_search"]


# ---------------------------------------------------------------------------
# Smoke: every bundled config denormalizes a realistic resource without
# warnings. This catches future regressions where a rule starts emitting
# field-level warnings (the same class of bug the audit harness found).
# ---------------------------------------------------------------------------


class TestNoFieldLevelWarningsAcrossAllConfigs:
    """
    Drive every bundled config with a realistic FHIR R5 resource and
    assert ResourceDenormalizer emits ZERO field-level warnings. This
    is the end-to-end pin for the audit pass — adding a rule that
    silently fails (e.g. the historical `'dict' has no .startswith`
    crash on Appointment.reason) immediately fails this test.
    """

    def _patient(self) -> Dict[str, Any]:
        return {
            "resourceType": "Patient",
            "id": "p1",
            "active": True,
            "name": [{"family": "Smith", "given": ["John"]}],
            "telecom": [{"system": "phone", "value": "555-1234"}],
            "gender": "male",
            "birthDate": "1980-01-01",
            "deceasedDateTime": "2024-03-15T12:00:00Z",
            "address": [{"use": "home", "city": "Springfield", "country": "US"}],
            "communication": [{"language": {"coding": [{"system": "urn:ietf:bcp:47", "code": "en-US"}]}}],
            "identifier": [{"system": "http://hospital.example/mrn", "value": "MRN-1"}],
            "generalPractitioner": [{"reference": "Practitioner/dr-jones"}],
            "managingOrganization": {"reference": "Organization/h1"},
            "link": [{"other": {"reference": "RelatedPerson/rp-1"}, "type": "seealso"}],
        }

    def _observation(self) -> Dict[str, Any]:
        return {
            "resourceType": "Observation",
            "id": "obs-1",
            "status": "final",
            "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "vital-signs"}]}],
            "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6"}]},
            "subject": {"reference": "Patient/p1"},
            "effectivePeriod": {"start": "2024-06-15T08:00:00Z", "end": "2024-06-15T08:30:00Z"},
            "effectiveTiming": {"repeat": {"boundsPeriod": {"start": "2024-06-15T08:00:00Z", "end": "2024-06-15T08:30:00Z"}}},
            "valueQuantity": {"value": 120, "unit": "mm[Hg]"},
            "component": [{"code": {"coding": [{"system": "http://loinc.org", "code": "8462-4"}]}, "valueQuantity": {"value": 80}}],
        }

    def _appointment(self) -> Dict[str, Any]:
        return {
            "resourceType": "Appointment",
            "id": "appt-1",
            "status": "booked",
            "reason": [
                {"concept": {"coding": [{"system": "http://snomed.info/sct", "code": "162673000"}]}},
                {"reference": {"reference": "Condition/cond-1"}},
            ],
            "serviceType": [
                {
                    "concept": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/service-type", "code": "124"}]},
                    "reference": {"reference": "HealthcareService/hs-1"},
                }
            ],
            "start": "2024-07-01T09:00:00Z",
            "end": "2024-07-01T10:00:00Z",
            "requestedPeriod": [{"start": "2024-06-25T00:00:00Z", "end": "2024-07-15T00:00:00Z"}],
            "participant": [
                {"actor": {"reference": "Patient/pat-001"}, "status": "accepted"},
                {"actor": {"reference": "Practitioner/dr-jones"}, "status": "accepted"},
                {"actor": {"reference": "Location/loc-1"}, "status": "accepted"},
            ],
        }

    def _organization(self) -> Dict[str, Any]:
        return {
            "resourceType": "Organization",
            "id": "org-1",
            "active": True,
            "name": "SGH",
            "contact": [
                {"address": {"use": "work", "city": "Springfield", "country": "US"}},
            ],
            "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"}],
        }

    def _location(self) -> Dict[str, Any]:
        return {
            "resourceType": "Location",
            "id": "loc-1",
            "status": "active",
            "name": "ER",
            "address": {"use": "work", "city": "Springfield", "country": "US"},
            "position": {"longitude": -89.0, "latitude": 39.0},
            "managingOrganization": {"reference": "Organization/org-1"},
        }

    @pytest.mark.parametrize(
        "factory_name",
        ["_patient", "_observation", "_appointment", "_organization", "_location"],
    )
    def test_bundled_config_emits_no_warnings(
        self, denormalizer: ResourceDenormalizer, factory_name: str
    ) -> None:
        resource = getattr(self, factory_name)()
        warnings: List[str] = []
        denormalizer.denormalize(resource, warnings=warnings)
        assert warnings == [], (
            f"{resource['resourceType']} produced field-level warnings:\n  "
            + "\n  ".join(warnings)
        )


# ---------------------------------------------------------------------------
# Resource-purity contract: denormalization must never mutate the FHIR
# resource. Every projected field MUST land in `_search` or
# `_compartments`. No rule may write a synthetic key (e.g. `deathDate`)
# at the resource root, and no input field may be dropped or rewritten.
# ---------------------------------------------------------------------------


class TestResourcePurity:
    """
    The user contract is: "we should not create any extra field in
    the actual FHIR resource". This class pins that contract with two
    layers of assertion:

      1. Static — every denormalization rule across every bundled
         config declares `target: _search` or `target: _compartments`.
      2. Dynamic — round-trip a realistic FHIR R5 resource through
         the denormalizer and assert:
           a. every input top-level key is preserved byte-identically,
           b. only `_search` / `_compartments` may be added,
           c. the input dict is NOT mutated in place.

    Together these guarantee that a denormalized document is the
    original FHIR resource + two sibling buckets, nothing else.
    """

    ALLOWED_NEW_TOPLEVEL_KEYS = {"_search", "_compartments"}

    # ---- Static audit ------------------------------------------------

    def test_every_rule_writes_to_allowed_bucket(self) -> None:
        from fhir_search_to_mql import ConfigLoader

        cl = ConfigLoader()
        offenders: List[str] = []
        for resource in sorted(cl.list_resources()):
            cfg = cl.get_config(resource)
            rules = cfg.get("denormalization", {}) or {}
            for rule_name, rule in rules.items():
                target = rule.get("target")
                if target not in self.ALLOWED_NEW_TOPLEVEL_KEYS:
                    offenders.append(
                        f"{resource}.{rule_name}: target={target!r}"
                    )
        assert not offenders, (
            "Denormalization rule(s) write to a target other than "
            "`_search` / `_compartments`, polluting the FHIR resource:\n  "
            + "\n  ".join(offenders)
        )

    # ---- Dynamic audit -----------------------------------------------

    @pytest.fixture
    def samples(self) -> Dict[str, Dict[str, Any]]:
        return {
            "Patient": {
                "resourceType": "Patient",
                "id": "p1",
                "active": True,
                "name": [{"family": "Smith", "given": ["John"]}],
                "deceasedDateTime": "2024-03-15T12:00:00Z",
                "address": [{"city": "Springfield"}],
                "communication": [{"language": {"coding": [{"system": "urn:ietf:bcp:47", "code": "en-US"}]}}],
                "identifier": [{"system": "http://x/mrn", "value": "MRN-1"}],
                "managingOrganization": {"reference": "Organization/h1"},
            },
            "Observation": {
                "resourceType": "Observation",
                "id": "o1",
                "status": "final",
                "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6"}]},
                "subject": {"reference": "Patient/p1"},
                "effectiveDateTime": "2024-06-15T08:30:00Z",
                "valueQuantity": {"value": 120, "unit": "mm[Hg]"},
            },
            "Appointment": {
                "resourceType": "Appointment",
                "id": "a1",
                "status": "booked",
                "start": "2024-07-01T09:00:00Z",
                "end": "2024-07-01T10:00:00Z",
                "requestedPeriod": [{"start": "2024-06-25T00:00:00Z", "end": "2024-07-15T00:00:00Z"}],
                "participant": [{"actor": {"reference": "Patient/p1"}, "status": "accepted"}],
            },
            "Organization": {
                "resourceType": "Organization",
                "id": "org1",
                "name": "SGH",
                "identifier": [{"system": "http://x", "value": "1"}],
            },
            "Location": {
                "resourceType": "Location",
                "id": "loc1",
                "status": "active",
                "name": "ER",
                "address": {"city": "Springfield"},
                "managingOrganization": {"reference": "Organization/org1"},
            },
            "Practitioner": {
                "resourceType": "Practitioner",
                "id": "pr1",
                "active": True,
                "name": [{"family": "Jones", "given": ["Alice"]}],
                "identifier": [{"system": "http://npi", "value": "NPI-1"}],
                "telecom": [{"system": "email", "value": "alice@example.org"}],
            },
            "PractitionerRole": {
                "resourceType": "PractitionerRole",
                "id": "prr1",
                "active": True,
                "practitioner": {"reference": "Practitioner/pr1"},
                "organization": {"reference": "Organization/org1"},
                "period": {"start": "2024-01-01", "end": "2026-12-31"},
            },
            "Device": {
                "resourceType": "Device",
                "id": "dev1",
                "status": "active",
                "manufacturer": "Acme",
                "modelNumber": "Pump-3000",
                "serialNumber": "SN-1",
                "identifier": [{"system": "http://hospital.org/dev", "value": "DEV-1"}],
                "type": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "182722004"}]}
                ],
                "owner": {"reference": "Organization/org1"},
            },
            "Group": {
                "resourceType": "Group",
                "id": "grp1",
                "active": True,
                "type": "person",
                "membership": "enumerated",
                "name": "Cohort 1",
                "identifier": [{"system": "http://x", "value": "G-1"}],
                "code": {"coding": [{"system": "http://snomed.info/sct", "code": "73211009"}]},
                "managingEntity": {"reference": "Organization/org1"},
                "characteristic": [
                    {"code": {"coding": [{"code": "trait-x"}]}, "exclude": False}
                ],
                "member": [
                    {"entity": {"reference": "Patient/p1"}},
                    {"entity": {"reference": "Practitioner/pr1"}},
                ],
            },
            "Schedule": {
                "resourceType": "Schedule",
                "id": "sched1",
                "identifier": [{"system": "http://hospital.example/schedules", "value": "SCHED-001"}],
                "active": True,
                "serviceCategory": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "408443003"}]}
                ],
                "serviceType": [
                    {
                        "concept": {
                            "coding": [{"system": "http://snomed.info/sct", "code": "11429006"}]
                        },
                        "reference": {"reference": "HealthcareService/hs-1"},
                    }
                ],
                "specialty": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "394814009"}]}
                ],
                "name": "Morning Cardiology Clinic",
                "actor": [
                    {"reference": "Practitioner/pr1"},
                    {"reference": "Patient/p1"},
                    {"reference": "Device/dev1"},
                ],
                "planningHorizon": {
                    "start": "2024-07-01T00:00:00Z",
                    "end": "2024-12-31T23:59:59Z",
                },
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Condition": {
                "resourceType": "Condition",
                "id": "cond-audit-1",
                "identifier": [{"system": "http://hospital.org/cond", "value": "C-AUDIT"}],
                "clinicalStatus": {
                    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]
                },
                "verificationStatus": {
                    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status", "code": "confirmed"}]
                },
                "category": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-category", "code": "problem-list-item"}]}],
                "code": {"coding": [{"system": "http://snomed.info/sct", "code": "44054006"}]},
                "bodySite": [{"coding": [{"system": "http://snomed.info/sct", "code": "181414000"}]}],
                "subject": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-1"},
                "onsetDateTime": "2020-01-15",
                "onsetString": "January 2020",
                "abatementPeriod": {"start": "2024-01-01", "end": "2024-06-01"},
                "recordedDate": "2024-06-01T10:00:00Z",
                "participant": [
                    {"function": {"coding": [{"code": "author"}]}, "actor": {"reference": "Practitioner/pr1"}},
                ],
                "stage": [{"summary": {"coding": [{"code": "258219007"}]}}],
                "evidence": [
                    {
                        "concept": {"coding": [{"code": "386661006"}]},
                        "reference": {"reference": "Observation/o1"},
                    }
                ],
            },
            "Encounter": {
                "resourceType": "Encounter",
                "id": "enc-audit-1",
                "status": "in-progress",
                "identifier": [{"system": "http://hospital.org/enc", "value": "ENC-AUDIT"}],
                "class": [
                    {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": "AMB"}]}
                ],
                "type": [{"coding": [{"system": "http://snomed.info/sct", "code": "185349003"}]}],
                "subject": {"reference": "Patient/p1"},
                "actualPeriod": {"start": "2024-07-01T08:00:00Z", "end": "2024-07-01T12:00:00Z"},
                "participant": [
                    {
                        "type": [{"coding": [{"code": "PPRF"}]}],
                        "actor": {"reference": "Practitioner/pr1"},
                    },
                    {"actor": {"reference": "Device/dev1"}},
                ],
                "location": [{"location": {"reference": "Location/loc1"}}],
                "diagnosis": [
                    {
                        "condition": {
                            "concept": {"coding": [{"code": "44054006"}]},
                            "reference": {"reference": "Condition/cond-audit-1"},
                        }
                    }
                ],
                "reason": [
                    {
                        "value": {
                            "concept": {"coding": [{"code": "185345009"}]},
                            "reference": {"reference": "Observation/o1"},
                        }
                    }
                ],
                "partOf": {"reference": "Encounter/enc-parent"},
                "serviceProvider": {"reference": "Organization/org1"},
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ServiceRequest": {
                "resourceType": "ServiceRequest",
                "id": "sr-audit-1",
                "status": "active",
                "intent": "order",
                "priority": "routine",
                "identifier": [{"system": "http://hospital.org/sr", "value": "SR-AUDIT"}],
                "requisition": {"system": "http://hospital.org/req", "value": "REQ-AUDIT"},
                "category": [{"coding": [{"code": "108252007"}]}],
                "code": {
                    "concept": {"coding": [{"code": "103693007"}]},
                    "reference": {"reference": "ActivityDefinition/lab-audit"},
                },
                "subject": {"reference": "Patient/p1"},
                "requester": {"reference": "Practitioner/pr1"},
                "performer": [{"reference": "Practitioner/pr1"}],
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "authoredOn": "2024-07-01T10:00:00Z",
                "occurrenceDateTime": "2024-07-15T09:00:00Z",
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Procedure": {
                "resourceType": "Procedure",
                "id": "proc-audit-1",
                "status": "completed",
                "identifier": [
                    {"system": "http://hospital.org/proc", "value": "PROC-AUDIT"}
                ],
                "category": [{"coding": [{"code": "103693007"}]}],
                "code": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "80146002"}]
                },
                "subject": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "performer": [
                    {"actor": {"reference": "Practitioner/pr1"}},
                    {"actor": {"reference": "Organization/org1"}},
                ],
                "reason": [
                    {
                        "concept": {"coding": [{"code": "109006"}]},
                        "reference": {"reference": "Condition/cond-audit-1"},
                    }
                ],
                "basedOn": [{"reference": "ServiceRequest/sr-audit-1"}],
                "partOf": [{"reference": "Observation/obs-audit-1"}],
                "location": {"reference": "Location/loc1"},
                "report": [{"reference": "DiagnosticReport/dr-audit-1"}],
                "occurrenceDateTime": "2024-07-15T09:00:00Z",
                "instantiatesCanonical": [
                    "http://example.org/fhir/ActivityDefinition/appendectomy"
                ],
                "instantiatesUri": ["http://example.org/protocols/appendectomy-v1"],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Medication": {
                "resourceType": "Medication",
                "id": "med-audit-1",
                "status": "active",
                "identifier": [
                    {"system": "http://hospital.org/med", "value": "MED-AUDIT"},
                    {"type": {"coding": [{"code": "SNO"}]}, "value": "SN-AUDIT"},
                ],
                "code": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "319785009"}]
                },
                "doseForm": {
                    "coding": [{"system": "http://snomed.info/sct", "code": "385055001"}]
                },
                "batch": {
                    "lotNumber": "LOT-AUDIT",
                    "expirationDate": "2026-12-31T00:00:00Z",
                },
                "ingredient": [
                    {
                        "item": {
                            "concept": {"coding": [{"code": "387517004"}]},
                            "reference": {"reference": "Substance/sub-audit"},
                        }
                    }
                ],
                "marketingAuthorizationHolder": {"reference": "Organization/org1"},
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "MedicationRequest": {
                "resourceType": "MedicationRequest",
                "id": "mr-audit-1",
                "status": "active",
                "intent": "order",
                "priority": "routine",
                "identifier": [{"system": "http://hospital.org/mr", "value": "MR-AUDIT"}],
                "groupIdentifier": {"system": "http://hospital.org/grp", "value": "GRP-AUDIT"},
                "category": [{"coding": [{"code": "outpatient"}]}],
                "medication": {
                    "concept": {"coding": [{"code": "319785009"}]},
                    "reference": {"reference": "Medication/med-audit"},
                },
                "subject": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "authoredOn": "2024-07-01T10:00:00Z",
                "requester": {"reference": "Practitioner/pr1"},
                "performer": [{"reference": "Practitioner/pr2"}],
                "performerType": {"coding": [{"code": "pharmacist"}]},
                "dispenseRequest": {
                    "dispenser": {"reference": "Organization/org-dispense"},
                },
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "MedicationAdministration": {
                "resourceType": "MedicationAdministration",
                "id": "ma-audit-1",
                "status": "completed",
                "identifier": [{"system": "http://hospital.org/ma", "value": "MA-AUDIT"}],
                "medication": {
                    "concept": {"coding": [{"code": "319785009"}]},
                    "reference": {"reference": "Medication/med-audit"},
                },
                "subject": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "request": {"reference": "MedicationRequest/mr-audit-1"},
                "occurenceDateTime": "2024-07-15T09:00:00Z",
                "device": [{"reference": {"reference": "Device/pump-audit"}}],
                "performer": [
                    {
                        "actor": {
                            "concept": {"coding": [{"code": "706699008"}]},
                            "reference": {"reference": "Practitioner/pr1"},
                        }
                    }
                ],
                "reason": [
                    {
                        "concept": {"coding": [{"code": "386661006"}]},
                        "reference": {"reference": "Condition/cond-audit-1"},
                    }
                ],
                "statusReason": [{"coding": [{"code": "182849000"}]}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "MedicationDispense": {
                "resourceType": "MedicationDispense",
                "id": "md-audit-1",
                "status": "completed",
                "type": {"coding": [{"code": "FF"}]},
                "identifier": [{"system": "http://hospital.org/md", "value": "MD-AUDIT"}],
                "medication": {
                    "concept": {"coding": [{"code": "319785009"}]},
                    "reference": {"reference": "Medication/med-audit"},
                },
                "subject": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "location": {"reference": "Location/loc1"},
                "destination": {"reference": "Location/loc-dest"},
                "whenPrepared": "2024-07-14T08:00:00Z",
                "whenHandedOver": "2024-07-15T09:00:00Z",
                "recorded": "2024-07-14T07:00:00Z",
                "authorizingPrescription": [{"reference": "MedicationRequest/mr-audit-1"}],
                "performer": [{"actor": {"reference": "Practitioner/pr1"}}],
                "receiver": [{"reference": "Patient/p1"}],
                "substitution": {
                    "responsibleParty": {"reference": "Practitioner/pr2"},
                },
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "MedicationStatement": {
                "resourceType": "MedicationStatement",
                "id": "ms-audit-1",
                "status": "recorded",
                "category": [{"coding": [{"code": "inpatient"}]}],
                "medication": {
                    "concept": {"coding": [{"code": "313782"}]},
                    "reference": {"reference": "Medication/med-audit"},
                },
                "subject": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "effectiveDateTime": "2024-06-15T08:00:00Z",
                "identifier": [{"system": "http://hospital.org/ms", "value": "MS-AUDIT"}],
                "informationSource": [{"reference": "Practitioner/pr1"}],
                "adherence": {"code": {"coding": [{"code": "taking"}]}},
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "AllergyIntolerance": {
                "resourceType": "AllergyIntolerance",
                "id": "ai-audit-1",
                "clinicalStatus": {"coding": [{"code": "active"}]},
                "verificationStatus": {"coding": [{"code": "confirmed"}]},
                "type": {"coding": [{"code": "allergy"}]},
                "category": ["food"],
                "criticality": "high",
                "code": {"coding": [{"code": "91935009"}]},
                "patient": {"reference": "Patient/p1"},
                "recordedDate": "2024-07-01T10:00:00Z",
                "lastOccurrence": "2024-06-15T08:00:00Z",
                "identifier": [{"system": "http://hospital.org/ai", "value": "AI-AUDIT"}],
                "participant": [{"actor": {"reference": "Practitioner/pr1"}}],
                "reaction": [
                    {
                        "substance": {"coding": [{"code": "227493005"}]},
                        "manifestation": [
                            {
                                "concept": {"coding": [{"code": "39579001"}]},
                                "reference": {"reference": "Observation/obs-audit"},
                            }
                        ],
                        "severity": "severe",
                        "exposureRoute": {"coding": [{"code": "26643006"}]},
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "DiagnosticReport": {
                "resourceType": "DiagnosticReport",
                "id": "dr-audit-1",
                "status": "final",
                "identifier": [{"system": "http://hospital.org/dr", "value": "DR-AUDIT"}],
                "category": [{"coding": [{"code": "LAB"}]}],
                "code": {"coding": [{"code": "11502-2"}]},
                "subject": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "effectiveDateTime": "2024-07-10T08:00:00Z",
                "issued": "2024-07-10T10:00:00Z",
                "performer": [{"reference": "Practitioner/pr1"}],
                "resultsInterpreter": [{"reference": "Practitioner/pr2"}],
                "basedOn": [{"reference": "ServiceRequest/sr-audit-1"}],
                "specimen": [{"reference": "Specimen/spec-audit"}],
                "result": [{"reference": "Observation/obs-audit"}],
                "study": [{"reference": "ImagingStudy/img-audit"}],
                "media": [{"link": {"reference": "DocumentReference/doc-audit"}}],
                "conclusionCode": [{"coding": [{"code": "10828004"}]}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "CareTeam": {
                "resourceType": "CareTeam",
                "id": "ct-audit-1",
                "status": "active",
                "name": "Audit Care Team",
                "identifier": [{"system": "http://hospital.org/ct", "value": "CT-AUDIT"}],
                "category": [{"coding": [{"code": "LA27976-2"}]}],
                "subject": {"reference": "Patient/p1"},
                "period": {
                    "start": "2024-07-01T00:00:00Z",
                    "end": "2024-12-31T23:59:59Z",
                },
                "participant": [
                    {
                        "member": {"reference": "Practitioner/pr1"},
                        "coveragePeriod": {
                            "start": "2024-07-01T00:00:00Z",
                            "end": "2024-09-30T23:59:59Z",
                        },
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Goal": {
                "resourceType": "Goal",
                "id": "goal-audit-1",
                "lifecycleStatus": "active",
                "achievementStatus": {"coding": [{"code": "in-progress"}]},
                "identifier": [{"system": "http://hospital.org/goal", "value": "GOAL-AUDIT"}],
                "category": [{"coding": [{"code": "dietary"}]}],
                "description": {
                    "coding": [{"code": "406156006"}],
                    "text": "Reduce body weight",
                },
                "subject": {"reference": "Patient/p1"},
                "startDate": "2024-07-01",
                "addresses": [{"reference": "Condition/cond-audit-1"}],
                "target": [
                    {
                        "measure": {"coding": [{"code": "29463-7"}]},
                        "dueDate": "2024-12-31",
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "CarePlan": {
                "resourceType": "CarePlan",
                "id": "cp-audit-1",
                "status": "active",
                "intent": "plan",
                "identifier": [{"system": "http://hospital.org/cp", "value": "CP-AUDIT"}],
                "category": [{"coding": [{"code": "assess-plan"}]}],
                "subject": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "period": {
                    "start": "2024-07-01T00:00:00Z",
                    "end": "2024-12-31T23:59:59Z",
                },
                "custodian": {"reference": "Practitioner/pr-cust"},
                "contributor": [{"reference": "Practitioner/pr1"}],
                "careTeam": [{"reference": "CareTeam/ct-audit"}],
                "goal": [{"reference": "Goal/goal-audit"}],
                "addresses": [{"reference": {"reference": "Condition/cond-audit-1"}}],
                "basedOn": [{"reference": "ServiceRequest/sr-audit-1"}],
                "activity": [
                    {
                        "plannedActivityReference": {
                            "reference": "ServiceRequest/sr-act-audit"
                        }
                    }
                ],
                "instantiatesCanonical": [
                    "http://example.org/fhir/PlanDefinition/plan-audit"
                ],
                "instantiatesUri": ["http://example.org/protocols/plan-audit-v1"],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Immunization": {
                "resourceType": "Immunization",
                "id": "imm-audit-1",
                "status": "completed",
                "vaccineCode": {
                    "coding": [{"code": "140"}],
                    "text": "Influenza vaccine",
                },
                "patient": {"reference": "Patient/p1"},
                "encounter": {"reference": "Encounter/enc-audit-1"},
                "occurrenceDateTime": "2024-07-15T10:00:00Z",
                "lotNumber": "LOT-AUDIT",
                "location": {"reference": "Location/loc-audit"},
                "manufacturer": {"reference": "Organization/org-audit"},
                "identifier": [{"system": "http://hospital.org/imm", "value": "IMM-AUDIT"}],
                "performer": [{"actor": {"reference": "Practitioner/pr1"}}],
                "reason": [
                    {
                        "concept": {"coding": [{"code": "429060002"}]},
                        "reference": {"reference": "Condition/cond-audit-1"},
                    }
                ],
                "statusReason": {"coding": [{"code": "immunity"}]},
                "protocolApplied": [
                    {
                        "series": "Standard 2024",
                        "targetDisease": {"coding": [{"code": "6142004"}]},
                    }
                ],
                "reaction": [
                    {
                        "date": "2024-07-16T08:00:00Z",
                        "manifestation": {
                            "reference": {"reference": "Observation/obs-audit-rx"}
                        },
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Coverage": {
                "resourceType": "Coverage",
                "id": "cov-audit-1",
                "status": "active",
                "beneficiary": {"reference": "Patient/p1"},
                "insurer": {"reference": "Organization/org-audit-ins"},
                "subscriber": {"reference": "Patient/p-sub-audit"},
                "policyHolder": {"reference": "Patient/p-holder-audit"},
                "dependent": "01",
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                            "code": "EHCPOL",
                        }
                    ]
                },
                "identifier": [
                    {"system": "http://hospital.org/cov", "value": "COV-AUDIT"}
                ],
                "subscriberId": [
                    {"system": "http://payer.org/sub", "value": "SUB-AUDIT"}
                ],
                "class": [
                    {
                        "type": {"coding": [{"code": "group"}]},
                        "value": {
                            "system": "http://payer.org/group",
                            "value": "GRP-AUDIT",
                        },
                    }
                ],
                "paymentBy": [{"party": {"reference": "Patient/p1"}}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Claim": {
                "resourceType": "Claim",
                "id": "claim-audit-1",
                "status": "active",
                "use": "claim",
                "type": {"coding": [{"code": "professional"}]},
                "patient": {"reference": "Patient/p1"},
                "created": "2024-07-15",
                "enterer": {"reference": "Practitioner/pr1"},
                "provider": {"reference": "Practitioner/pr2"},
                "insurer": {"reference": "Organization/org-audit-ins"},
                "facility": {"reference": "Location/loc-audit"},
                "priority": {"coding": [{"code": "normal"}]},
                "identifier": [
                    {"system": "http://hospital.org/claim", "value": "CLM-AUDIT"}
                ],
                "payee": {"party": {"reference": "Practitioner/pr-payee-audit"}},
                "careTeam": [{"provider": {"reference": "Practitioner/pr-ct-audit"}}],
                "item": [
                    {
                        "encounter": [{"reference": "Encounter/enc-audit-1"}],
                        "udi": [{"reference": "Device/dev-audit-item"}],
                        "detail": [
                            {
                                "udi": [{"reference": "Device/dev-audit-detail"}],
                                "subDetail": [
                                    {
                                        "udi": [
                                            {"reference": "Device/dev-audit-sub"}
                                        ]
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "procedure": [{"udi": [{"reference": "Device/dev-audit-proc"}]}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ClaimResponse": {
                "resourceType": "ClaimResponse",
                "id": "cr-audit-1",
                "status": "active",
                "use": "claim",
                "outcome": "complete",
                "type": {"coding": [{"code": "professional"}]},
                "patient": {"reference": "Patient/p1"},
                "created": "2024-07-15",
                "insurer": {"reference": "Organization/org-audit-ins"},
                "requestor": {"reference": "Practitioner/pr-req-audit"},
                "request": {"reference": "Claim/claim-audit-1"},
                "identifier": [
                    {
                        "system": "http://hospital.org/claimresponse",
                        "value": "CR-AUDIT",
                    }
                ],
                "disposition": "Processed",
                "payment": {"date": "2024-08-01"},
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "DocumentReference": {
                "resourceType": "DocumentReference",
                "id": "doc-audit-1",
                "status": "current",
                "docStatus": "final",
                "type": {"coding": [{"code": "34117-2"}]},
                "subject": {"reference": "Patient/p1"},
                "date": "2024-07-15T10:00:00Z",
                "author": [{"reference": "Practitioner/pr1"}],
                "attester": [{"party": {"reference": "Practitioner/pr-attest-audit"}}],
                "custodian": {"reference": "Organization/org-cust-audit"},
                "context": [{"reference": "Encounter/enc-audit-1"}],
                "identifier": [
                    {"system": "http://hospital.org/docs", "value": "DOC-AUDIT"}
                ],
                "category": [{"coding": [{"code": "clinical-note"}]}],
                "content": [
                    {
                        "attachment": {
                            "contentType": "application/pdf",
                            "language": "en",
                            "url": "https://example.org/docs/doc-audit.pdf",
                            "creation": "2024-07-14",
                        },
                        "profile": [
                            {
                                "valueCoding": {
                                    "system": "http://terminology.hl7.org/CodeSystem/formatcodes",
                                    "code": "urn:hl7-org:sdwg:ccda-structuredBody:1.1",
                                }
                            }
                        ],
                    }
                ],
                "relatesTo": [
                    {
                        "code": {"coding": [{"code": "replaces"}]},
                        "target": {"reference": "DocumentReference/doc-audit-old"},
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Substance": {
                "resourceType": "Substance",
                "id": "sub-audit-1",
                "status": "active",
                "code": {
                    "concept": {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "387517004",
                            }
                        ]
                    },
                    "reference": {"reference": "SubstanceDefinition/sd-audit"},
                },
                "category": [{"coding": [{"code": "chemical"}]}],
                "identifier": [
                    {"system": "http://hospital.org/substance", "value": "SUB-AUDIT"}
                ],
                "expiry": "2025-12-31",
                "quantity": {"value": 100, "unit": "mg", "code": "mg"},
                "ingredient": [
                    {
                        "substanceCodeableConcept": {"coding": [{"code": "387207008"}]},
                        "substanceReference": {"reference": "Substance/sub-ing-audit"},
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "EpisodeOfCare": {
                "resourceType": "EpisodeOfCare",
                "id": "eoc-audit-1",
                "status": "active",
                "patient": {"reference": "Patient/pat-audit"},
                "managingOrganization": {"reference": "Organization/org-audit"},
                "careManager": {"reference": "Practitioner/prac-audit"},
                "period": {"start": "2024-01-01", "end": "2024-12-31"},
                "type": [{"coding": [{"code": "hacc"}]}],
                "identifier": [
                    {"system": "http://hospital.org/eoc", "value": "EOC-AUDIT"}
                ],
                "diagnosis": [
                    {
                        "condition": {
                            "concept": {"coding": [{"code": "44054006"}]},
                            "reference": {"reference": "Condition/cond-audit"},
                        }
                    }
                ],
                "referralRequest": [{"reference": "ServiceRequest/sr-audit"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ResearchSubject": {
                "resourceType": "ResearchSubject",
                "id": "rsub-audit-1",
                "status": "active",
                "study": {"reference": "ResearchStudy/rs-audit"},
                "subject": {"reference": "Patient/pat-audit"},
                "period": {"start": "2024-01-15", "end": "2024-12-31"},
                "identifier": [
                    {"system": "http://hospital.org/rsub", "value": "RSUB-AUDIT"}
                ],
                "progress": [
                    {
                        "subjectState": {
                            "coding": [{"code": "on-study"}],
                        },
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Composition": {
                "resourceType": "Composition",
                "id": "comp-audit-1",
                "status": "final",
                "version": "1",
                "url": "http://example.org/Composition/comp-audit-1",
                "title": "Audit Note",
                "date": "2024-07-15",
                "type": {"coding": [{"code": "18842-5"}]},
                "category": [{"coding": [{"code": "clinical-note"}]}],
                "subject": [{"reference": "Patient/pat-audit"}],
                "author": [{"reference": "Practitioner/prac-audit"}],
                "encounter": {"reference": "Encounter/enc-audit"},
                "identifier": [
                    {"system": "http://hospital.org/comp", "value": "COMP-AUDIT"}
                ],
                "attester": [{"party": {"reference": "Practitioner/prac-audit"}}],
                "event": [
                    {
                        "period": {"start": "2024-07-01", "end": "2024-07-14"},
                        "detail": [
                            {
                                "concept": {"coding": [{"code": "admission"}]},
                                "reference": {"reference": "Encounter/enc-audit"},
                            }
                        ],
                    }
                ],
                "relatesTo": [
                    {
                        "resourceReference": {
                            "reference": "Composition/comp-prior-audit"
                        }
                    }
                ],
                "section": [
                    {
                        "code": {"coding": [{"code": "48767-8"}]},
                        "entry": [{"reference": "Observation/obs-audit"}],
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Questionnaire": {
                "resourceType": "Questionnaire",
                "id": "quest-audit-1",
                "status": "active",
                "version": "1.0",
                "url": "http://example.org/Questionnaire/quest-audit",
                "name": "audit-form",
                "title": "Audit Form",
                "publisher": "Audit Publisher",
                "description": "Audit questionnaire",
                "date": "2024-06-01",
                "subjectType": ["Patient"],
                "code": [{"code": "44249-1"}],
                "jurisdiction": [{"coding": [{"code": "US"}]}],
                "effectivePeriod": {"start": "2024-01-01", "end": "2025-12-31"},
                "identifier": [
                    {"system": "http://hospital.org/quest", "value": "Q-AUDIT"}
                ],
                "useContext": [
                    {
                        "code": {"code": "venue"},
                        "valueCodeableConcept": {"coding": [{"code": "ambulatory"}]},
                    }
                ],
                "item": [
                    {
                        "linkId": "1",
                        "code": [{"code": "item-audit"}],
                        "definition": "http://example.org/item/audit",
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ExplanationOfBenefit": {
                "resourceType": "ExplanationOfBenefit",
                "id": "eob-audit-1",
                "status": "active",
                "type": {"coding": [{"code": "professional"}]},
                "patient": {"reference": "Patient/pat-audit"},
                "created": "2024-07-15",
                "disposition": "Processed",
                "claim": {"reference": "Claim/clm-audit"},
                "enterer": {"reference": "Practitioner/prac-audit"},
                "provider": {"reference": "Practitioner/prac-audit"},
                "facility": {"reference": "Location/loc-audit"},
                "identifier": [
                    {"system": "http://hospital.org/eob", "value": "EOB-AUDIT"}
                ],
                "insurance": [{"coverage": {"reference": "Coverage/cov-audit"}}],
                "payee": {"party": {"reference": "Practitioner/prac-audit"}},
                "careTeam": [{"provider": {"reference": "Practitioner/prac-audit"}}],
                "item": [
                    {
                        "encounter": [{"reference": "Encounter/enc-audit"}],
                        "udi": [{"reference": "Device/dev-audit"}],
                        "detail": [
                            {
                                "udi": [{"reference": "Device/dev-detail-audit"}],
                                "subDetail": [
                                    {
                                        "udi": [
                                            {"reference": "Device/dev-sub-audit"}
                                        ]
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "procedure": [{"udi": [{"reference": "Device/dev-proc-audit"}]}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "CoverageEligibilityRequest": {
                "resourceType": "CoverageEligibilityRequest",
                "id": "cer-audit-1",
                "status": "active",
                "patient": {"reference": "Patient/pat-audit"},
                "insurer": {"reference": "Organization/org-audit"},
                "enterer": {"reference": "Practitioner/prac-audit"},
                "provider": {"reference": "Practitioner/prac-audit"},
                "facility": {"reference": "Location/loc-audit"},
                "created": "2024-07-15",
                "identifier": [
                    {"system": "http://hospital.org/cer", "value": "CER-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "CoverageEligibilityResponse": {
                "resourceType": "CoverageEligibilityResponse",
                "id": "ceres-audit-1",
                "status": "active",
                "outcome": "complete",
                "patient": {"reference": "Patient/pat-audit"},
                "insurer": {"reference": "Organization/org-audit"},
                "request": {"reference": "CoverageEligibilityRequest/cer-audit"},
                "requestor": {"reference": "Practitioner/prac-audit"},
                "created": "2024-07-15",
                "disposition": "Eligible",
                "identifier": [
                    {"system": "http://hospital.org/ceres", "value": "CERES-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ResearchStudy": {
                "resourceType": "ResearchStudy",
                "id": "rs-audit-1",
                "status": "active",
                "title": "Audit Trial",
                "name": "RS-AUDIT",
                "description": "Audit study description",
                "phase": {"coding": [{"code": "phase-2"}]},
                "period": {"start": "2024-01-01", "end": "2025-12-31"},
                "identifier": [
                    {"system": "http://hospital.org/rs", "value": "RS-AUDIT"}
                ],
                "condition": [{"coding": [{"code": "38341003"}]}],
                "keyword": [{"coding": [{"code": "hypertension"}]}],
                "studyDesign": [{"coding": [{"code": "interventional"}]}],
                "focus": [
                    {
                        "concept": {"coding": [{"code": "med-focus"}]},
                        "reference": {"reference": "Medication/med-audit"},
                    }
                ],
                "objective": [
                    {
                        "description": "Primary objective audit",
                        "type": {"coding": [{"code": "primary"}]},
                    }
                ],
                "progressStatus": [
                    {
                        "state": {"coding": [{"code": "recruiting"}]},
                        "actual": True,
                        "period": {"start": "2024-06-01"},
                    }
                ],
                "recruitment": {
                    "targetNumber": 100,
                    "actualNumber": 42,
                    "eligibility": {"reference": "Group/grp-audit"},
                },
                "protocol": [{"reference": "PlanDefinition/pd-audit"}],
                "site": [{"reference": "Location/loc-audit"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Invoice": {
                "resourceType": "Invoice",
                "id": "inv-audit-1",
                "status": "issued",
                "type": {"coding": [{"code": "invoice"}]},
                "subject": {"reference": "Patient/pat-audit"},
                "recipient": {"reference": "RelatedPerson/rp-audit"},
                "account": {"reference": "Account/acct-audit"},
                "issuer": {"reference": "Organization/org-audit"},
                "date": "2024-07-15T10:00:00Z",
                "identifier": [
                    {"system": "http://hospital.org/inv", "value": "INV-AUDIT"}
                ],
                "participant": [
                    {
                        "actor": {"reference": "Practitioner/prac-audit"},
                        "role": {"coding": [{"code": "author"}]},
                    }
                ],
                "totalGross": {"value": 500, "currency": "USD"},
                "totalNet": {"value": 450, "currency": "USD"},
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ChargeItem": {
                "resourceType": "ChargeItem",
                "id": "ci-audit-1",
                "status": "billable",
                "code": {"coding": [{"code": "99213"}]},
                "subject": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "enterer": {"reference": "Practitioner/prac-audit"},
                "occurrenceDateTime": "2024-07-15T10:00:00Z",
                "enteredDate": "2024-07-14T09:00:00Z",
                "identifier": [
                    {"system": "http://hospital.org/ci", "value": "CI-AUDIT"}
                ],
                "account": [{"reference": "Account/acct-audit"}],
                "performingOrganization": {
                    "reference": "Organization/org-perf-audit"
                },
                "requestingOrganization": {
                    "reference": "Organization/org-req-audit"
                },
                "performer": [
                    {
                        "actor": {"reference": "Practitioner/prac-perf-audit"},
                        "function": {"coding": [{"code": "performer"}]},
                    }
                ],
                "service": [
                    {"reference": {"reference": "Procedure/proc-audit"}}
                ],
                "quantity": {"value": 1},
                "totalPriceComponent": {
                    "factor": 1.5,
                    "amount": {"value": 120, "currency": "USD"},
                },
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Account": {
                "resourceType": "Account",
                "id": "acct-audit-1",
                "status": "active",
                "name": "Audit Account",
                "type": {"coding": [{"code": "PBILLACCT"}]},
                "subject": [{"reference": "Patient/pat-audit"}],
                "owner": {"reference": "Organization/org-audit"},
                "servicePeriod": {"start": "2024-07-01", "end": "2024-12-31"},
                "identifier": [
                    {"system": "http://hospital.org/acct", "value": "ACCT-AUDIT"}
                ],
                "guarantor": [
                    {"party": {"reference": "RelatedPerson/rp-audit"}}
                ],
                "relatedAccount": [
                    {"account": {"reference": "Account/acct-parent-audit"}}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "PaymentReconciliation": {
                "resourceType": "PaymentReconciliation",
                "id": "pr-audit-1",
                "status": "active",
                "outcome": "complete",
                "type": {"coding": [{"code": "payment"}]},
                "amount": {"value": 500, "currency": "USD"},
                "created": "2024-07-15T10:00:00Z",
                "disposition": "Payment processed audit",
                "requestor": {"reference": "Practitioner/prac-audit"},
                "request": {"reference": "Task/task-audit"},
                "paymentIssuer": {"reference": "Organization/org-audit"},
                "identifier": [
                    {"system": "http://hospital.org/pr", "value": "PR-AUDIT"}
                ],
                "allocation": [
                    {
                        "account": {"reference": "Account/acct-audit"},
                        "encounter": {"reference": "Encounter/enc-audit"},
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "PaymentNotice": {
                "resourceType": "PaymentNotice",
                "id": "pn-audit-1",
                "status": "active",
                "amount": {"value": 100, "currency": "USD"},
                "recipient": {"reference": "Organization/org-audit"},
                "created": "2024-07-15T10:00:00Z",
                "reporter": {"reference": "Practitioner/prac-audit"},
                "request": {"reference": "Claim/claim-audit"},
                "response": {"reference": "ClaimResponse/cr-audit"},
                "identifier": [
                    {"system": "http://hospital.org/pn", "value": "PN-AUDIT"}
                ],
                "paymentStatus": {"coding": [{"code": "paid"}]},
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "QuestionnaireResponse": {
                "resourceType": "QuestionnaireResponse",
                "id": "qr-audit-1",
                "status": "completed",
                "questionnaire": "Questionnaire/quest-audit",
                "authored": "2024-07-15T10:00:00Z",
                "subject": {"reference": "Patient/pat-audit"},
                "author": {"reference": "Practitioner/prac-audit"},
                "source": {"reference": "Practitioner/prac-source"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "identifier": [
                    {"system": "http://hospital.org/qr", "value": "QR-AUDIT"}
                ],
                "basedOn": [{"reference": "CarePlan/cp-audit"}],
                "partOf": [{"reference": "Observation/obs-audit"}],
                "item": [
                    {
                        "linkId": "subject-item",
                        "extension": [
                            {
                                "url": "http://hl7.org/fhir/StructureDefinition/questionnaireresponse-isSubject",
                                "valueBoolean": True,
                            }
                        ],
                        "answer": [
                            {
                                "valueReference": {
                                    "reference": "Patient/pat-item-audit"
                                }
                            }
                        ],
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "DetectedIssue": {
                "resourceType": "DetectedIssue",
                "id": "di-audit-1",
                "status": "final",
                "code": {"coding": [{"code": "DRG"}]},
                "category": [{"coding": [{"code": "drug-drug"}]}],
                "subject": {"reference": "Patient/pat-audit"},
                "author": {"reference": "Practitioner/prac-audit"},
                "identifiedDateTime": "2024-07-15T10:00:00Z",
                "identifier": [
                    {"system": "http://hospital.org/di", "value": "DI-AUDIT"}
                ],
                "implicated": [{"reference": "MedicationRequest/mr-audit"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ClinicalImpression": {
                "resourceType": "ClinicalImpression",
                "id": "ci-audit-1",
                "status": "completed",
                "date": "2024-07-15T10:00:00Z",
                "subject": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "performer": {"reference": "Practitioner/prac-audit"},
                "previous": {"reference": "ClinicalImpression/ci-prev-audit"},
                "identifier": [
                    {"system": "http://hospital.org/ci", "value": "CI-AUDIT"}
                ],
                "problem": [{"reference": "Condition/cond-audit"}],
                "supportingInfo": [{"reference": "Observation/obs-audit"}],
                "finding": [
                    {
                        "item": {
                            "concept": {"coding": [{"code": "386661006"}]},
                            "reference": {"reference": "Observation/obs-finding-audit"},
                        }
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "FamilyMemberHistory": {
                "resourceType": "FamilyMemberHistory",
                "id": "fmh-audit-1",
                "status": "completed",
                "date": "2024-07-15",
                "patient": {"reference": "Patient/pat-audit"},
                "relationship": {"coding": [{"code": "FTH"}]},
                "sex": {"coding": [{"code": "male"}]},
                "identifier": [
                    {"system": "http://hospital.org/fmh", "value": "FMH-AUDIT"}
                ],
                "condition": [{"code": {"coding": [{"code": "44054006"}]}}],
                "instantiatesCanonical": [
                    "http://example.org/PlanDefinition/fmh-audit"
                ],
                "instantiatesUri": ["http://example.org/protocols/fmh-audit"],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ImagingStudy": {
                "resourceType": "ImagingStudy",
                "id": "imaging-study-audit-1",
                "status": "available",
                "started": "2024-07-15T10:00:00Z",
                "subject": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "referrer": {"reference": "Practitioner/prac-audit"},
                "identifier": [{"value": "1.2.3.audit"}],
                "basedOn": [{"reference": "ServiceRequest/sr-audit"}],
                "endpoint": [{"reference": "Endpoint/ep-audit"}],
                "reason": [{"concept": {"coding": [{"code": "reason-audit"}]}}],
                "series": [
                    {
                        "uid": "1.2.3.4.audit",
                        "modality": {"coding": [{"code": "MR"}]},
                        "bodySite": {
                            "concept": {"coding": [{"code": "bs-audit"}]},
                            "reference": {"reference": "BodyStructure/bs-audit"},
                        },
                        "performer": [{"actor": {"reference": "Device/dev-audit"}}],
                        "instance": [
                            {
                                "uid": "1.2.3.4.5.audit",
                                "sopClass": {"code": "1.2.840.10008.5.1.4.1.1.4"},
                            }
                        ],
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Specimen": {
                "resourceType": "Specimen",
                "id": "specimen-audit-1",
                "status": "available",
                "type": {"coding": [{"code": "119297000"}]},
                "subject": {"reference": "Patient/pat-audit"},
                "accessionIdentifier": {
                    "system": "http://hospital.org/accession",
                    "value": "ACC-AUDIT",
                },
                "identifier": [
                    {"system": "http://hospital.org/specimen", "value": "SP-AUDIT"}
                ],
                "parent": [{"reference": "Specimen/spec-parent-audit"}],
                "collection": {
                    "collectedDateTime": "2024-07-15T08:00:00Z",
                    "collector": {"reference": "Practitioner/prac-audit"},
                    "procedure": {"reference": "Procedure/proc-audit"},
                    "bodySite": {
                        "reference": {"reference": "BodyStructure/bs-audit"},
                    },
                },
                "container": [{"device": {"reference": "Device/dev-audit"}}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "NutritionOrder": {
                "resourceType": "NutritionOrder",
                "id": "nutrition-order-audit-1",
                "status": "active",
                "dateTime": "2024-07-15T10:00:00Z",
                "subject": {"reference": "Patient/pat-audit"},
                "orderer": {"reference": "Practitioner/prac-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "groupIdentifier": {
                    "system": "http://hospital.org/group",
                    "value": "GRP-NO-AUDIT",
                },
                "identifier": [
                    {"system": "http://hospital.org/nutrition", "value": "NO-AUDIT"}
                ],
                "oralDiet": {
                    "type": [{"coding": [{"code": "226211001"}]}]
                },
                "enteralFormula": {
                    "baseFormulaType": {
                        "concept": {"coding": [{"code": "226783000"}]}
                    },
                    "additive": [
                        {
                            "type": {
                                "concept": {"coding": [{"code": "226789001"}]}
                            }
                        }
                    ],
                },
                "supplement": [
                    {"type": {"concept": {"coding": [{"code": "226352002"}]}}}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Contract": {
                "resourceType": "Contract",
                "id": "contract-audit-1",
                "status": "executed",
                "issued": "2024-07-15T10:00:00Z",
                "url": "http://example.org/contracts/contract-audit",
                "instantiatesUri": "http://example.org/contract-templates/audit",
                "subject": [{"reference": "Patient/pat-audit"}],
                "signer": [{"party": {"reference": "Practitioner/prac-audit"}}],
                "authority": [{"reference": "Organization/org-audit"}],
                "domain": [{"reference": "Location/loc-audit"}],
                "identifier": [
                    {"system": "http://hospital.org/contract", "value": "CTR-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Consent": {
                "resourceType": "Consent",
                "id": "consent-audit-1",
                "status": "active",
                "date": "2024-07-15",
                "subject": {"reference": "Patient/pat-audit"},
                "grantee": [{"reference": "Practitioner/prac-audit"}],
                "controller": [{"reference": "Organization/org-audit"}],
                "category": [{"coding": [{"code": "idscl"}]}],
                "identifier": [
                    {"system": "http://hospital.org/consent", "value": "CONSENT-AUDIT"}
                ],
                "provision": [
                    {
                        "period": {
                            "start": "2024-07-01T00:00:00Z",
                            "end": "2025-06-30T23:59:59Z",
                        },
                        "purpose": [{"code": "PATRQT"}],
                        "action": [{"coding": [{"code": "access"}]}],
                    }
                ],
                "verification": [
                    {
                        "verified": True,
                        "verificationDate": ["2024-07-16T10:00:00Z"],
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "AuditEvent": {
                "resourceType": "AuditEvent",
                "id": "ae-audit-1",
                "action": "R",
                "recorded": "2024-07-15T10:00:00Z",
                "code": {"coding": [{"code": "110100"}]},
                "category": [{"coding": [{"code": "rest"}]}],
                "patient": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "agent": [
                    {
                        "who": {"reference": "Practitioner/prac-audit"},
                        "role": [{"coding": [{"code": "implementer"}]}],
                        "policy": ["http://example.org/policy/audit"],
                    }
                ],
                "source": {"observer": {"reference": "Device/dev-audit"}},
                "entity": [
                    {
                        "what": {"reference": "Patient/pat-audit"},
                        "role": {"coding": [{"code": "1"}]},
                    }
                ],
                "outcome": {"code": {"code": "0"}},
                "authorization": [{"coding": [{"code": "PATADMIN"}]}],
                "basedOn": [{"reference": "ServiceRequest/sr-audit"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Flag": {
                "resourceType": "Flag",
                "id": "flag-audit-1",
                "status": "active",
                "code": {"coding": [{"code": "304379003"}]},
                "subject": {"reference": "Patient/pat-audit"},
                "author": {"reference": "Practitioner/prac-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "period": {
                    "start": "2024-07-01T00:00:00Z",
                    "end": "2024-12-31T23:59:59Z",
                },
                "category": [{"coding": [{"code": "safety"}]}],
                "identifier": [
                    {"system": "http://hospital.org/flag", "value": "FLAG-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Communication": {
                "resourceType": "Communication",
                "id": "comm-audit-1",
                "status": "completed",
                "subject": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "sender": {"reference": "Practitioner/prac-audit"},
                "recipient": [{"reference": "Practitioner/prac-recip"}],
                "sent": "2024-07-15T09:00:00Z",
                "received": "2024-07-15T09:05:00Z",
                "category": [{"coding": [{"code": "notification"}]}],
                "medium": [{"coding": [{"code": "WRITTEN"}]}],
                "topic": {"coding": [{"code": "371535009"}]},
                "identifier": [
                    {"system": "http://hospital.org/comm", "value": "COMM-AUDIT"}
                ],
                "basedOn": [{"reference": "ServiceRequest/sr-audit"}],
                "partOf": [{"reference": "Communication/parent-audit"}],
                "instantiatesCanonical": ["http://example.org/PlanDefinition/pd-audit"],
                "instantiatesUri": ["http://example.org/protocols/alert-audit"],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Task": {
                "resourceType": "Task",
                "id": "task-audit-1",
                "status": "in-progress",
                "intent": "order",
                "priority": "routine",
                "for": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "focus": {"reference": "ServiceRequest/sr-audit"},
                "owner": {"reference": "Practitioner/prac-audit"},
                "requester": {"reference": "Practitioner/prac-req"},
                "authoredOn": "2024-07-15T09:00:00Z",
                "lastModified": "2024-07-16T10:00:00Z",
                "executionPeriod": {
                    "start": "2024-07-15T09:00:00Z",
                    "end": "2024-07-20T17:00:00Z",
                },
                "code": {"coding": [{"code": "103693007"}]},
                "businessStatus": {"coding": [{"code": "in-progress"}]},
                "identifier": [
                    {"system": "http://hospital.org/task", "value": "TASK-AUDIT"}
                ],
                "groupIdentifier": {
                    "system": "http://hospital.org/group",
                    "value": "GRP-AUDIT",
                },
                "basedOn": [{"reference": "CarePlan/cp-audit"}],
                "partOf": [{"reference": "Task/parent-audit"}],
                "performer": [{"actor": {"reference": "Practitioner/prac-perf"}}],
                "requestedPerformer": [
                    {
                        "concept": {"coding": [{"code": "performer"}]},
                        "reference": {"reference": "Practitioner/prac-rp"},
                    }
                ],
                "output": [
                    {
                        "type": {"text": "result"},
                        "valueReference": {"reference": "Observation/obs-audit"},
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "RiskAssessment": {
                "resourceType": "RiskAssessment",
                "id": "ra-audit-1",
                "status": "final",
                "subject": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "condition": {"reference": "Condition/cond-audit"},
                "performer": {"reference": "Practitioner/prac-audit"},
                "occurrenceDateTime": "2024-07-15T10:00:00Z",
                "method": {"coding": [{"code": "clinical"}]},
                "identifier": [
                    {"system": "http://hospital.org/ra", "value": "RA-AUDIT"}
                ],
                "prediction": [
                    {
                        "probabilityDecimal": 0.5,
                        "qualitativeRisk": {"coding": [{"code": "moderate"}]},
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "HealthcareService": {
                "resourceType": "HealthcareService",
                "id": "hs-audit-1",
                "active": True,
                "name": "Audit Cardiology",
                "providedBy": {"reference": "Organization/org-audit"},
                "category": [{"coding": [{"code": "17"}]}],
                "type": [{"coding": [{"code": "11429006"}]}],
                "specialty": [{"coding": [{"code": "394579002"}]}],
                "location": [{"reference": "Location/loc-audit"}],
                "identifier": [
                    {"system": "http://hospital.org/hs", "value": "HS-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "RelatedPerson": {
                "resourceType": "RelatedPerson",
                "id": "rp-audit-1",
                "active": True,
                "patient": {"reference": "Patient/pat-audit"},
                "relationship": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                                "code": "WIFE",
                            }
                        ]
                    }
                ],
                "name": [{"family": "Audit", "given": ["Spouse"]}],
                "telecom": [{"system": "phone", "value": "555-AUDIT"}],
                "gender": "female",
                "birthDate": "1980-01-01",
                "identifier": [
                    {"system": "http://hospital.org/rp", "value": "RP-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "DeviceRequest": {
                "resourceType": "DeviceRequest",
                "id": "dr-audit-1",
                "status": "active",
                "intent": "order",
                "code": {
                    "concept": {"coding": [{"code": "dev-req-audit"}]},
                    "reference": {"reference": "Device/dev-audit"},
                },
                "subject": {"reference": "Patient/pat-audit"},
                "requester": {"reference": "Practitioner/prac-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "authoredOn": "2024-07-15T10:00:00Z",
                "occurrenceDateTime": "2024-07-20T10:00:00Z",
                "identifier": [
                    {"system": "http://hospital.org/dr", "value": "DR-AUDIT"}
                ],
                "groupIdentifier": {
                    "system": "http://hospital.org/grp",
                    "value": "GRP-AUDIT",
                },
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "AdverseEvent": {
                "resourceType": "AdverseEvent",
                "id": "ae-audit-1",
                "status": "completed",
                "actuality": "actual",
                "code": {"coding": [{"code": "ae-audit"}]},
                "category": [{"coding": [{"code": "medication-mishap"}]}],
                "subject": {"reference": "Patient/pat-audit"},
                "recorder": {"reference": "Practitioner/prac-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "occurrenceDateTime": "2024-07-15T10:00:00Z",
                "identifier": [
                    {"system": "http://hospital.org/ae", "value": "AE-AUDIT"}
                ],
                "suspectEntity": [
                    {
                        "instanceReference": {
                            "reference": "Medication/med-audit"
                        }
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ImmunizationRecommendation": {
                "resourceType": "ImmunizationRecommendation",
                "id": "ir-audit-1",
                "patient": {"reference": "Patient/pat-audit"},
                "date": "2024-07-15T10:00:00Z",
                "identifier": [
                    {"system": "http://hospital.org/ir", "value": "IR-AUDIT"}
                ],
                "recommendation": [
                    {
                        "forecastStatus": {"coding": [{"code": "due"}]},
                        "vaccineCode": [{"coding": [{"code": "flu-audit"}]}],
                        "targetDisease": [{"coding": [{"code": "6142004"}]}],
                        "supportingImmunization": [
                            {"reference": "Immunization/imm-audit"}
                        ],
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Person": {
                "resourceType": "Person",
                "id": "person-audit-1",
                "gender": "male",
                "birthDate": "1975-06-01",
                "name": [{"family": "Audit", "given": ["Person"]}],
                "telecom": [{"system": "email", "value": "audit@example.org"}],
                "address": [{"city": "Boston", "state": "MA", "use": "home"}],
                "link": [{"target": {"reference": "Patient/pat-audit"}}],
                "managingOrganization": {"reference": "Organization/org-audit"},
                "identifier": [
                    {"system": "http://hospital.org/person", "value": "PERSON-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "BodyStructure": {
                "resourceType": "BodyStructure",
                "id": "bs-audit-1",
                "patient": {"reference": "Patient/pat-audit"},
                "morphology": {"coding": [{"code": "morph-audit"}]},
                "includedStructure": [
                    {"structure": {"coding": [{"code": "arm-audit"}]}}
                ],
                "excludedStructure": [
                    {"structure": {"coding": [{"code": "finger-audit"}]}}
                ],
                "identifier": [
                    {"system": "http://hospital.org/bs", "value": "BS-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "OrganizationAffiliation": {
                "resourceType": "OrganizationAffiliation",
                "id": "oaf-audit-1",
                "active": True,
                "organization": {"reference": "Organization/org-audit"},
                "participatingOrganization": {"reference": "Organization/org-part-audit"},
                "code": [{"coding": [{"code": "provider-audit"}]}],
                "specialty": [{"coding": [{"code": "cardio-audit"}]}],
                "period": {"start": "2024-01-01", "end": "2025-12-31"},
                "identifier": [
                    {"system": "http://hospital.org/oaf", "value": "OAF-AUDIT"}
                ],
                "contact": [
                    {"telecom": [{"system": "email", "value": "oaf@audit.org"}]}
                ],
                "endpoint": [{"reference": "Endpoint/ep-audit"}],
                "location": [{"reference": "Location/loc-audit"}],
                "network": [{"reference": "Organization/net-audit"}],
                "healthcareService": [{"reference": "HealthcareService/hs-audit"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Endpoint": {
                "resourceType": "Endpoint",
                "id": "ep-audit-1",
                "status": "active",
                "name": "Audit FHIR Endpoint",
                "connectionType": [{"coding": [{"code": "hl7-fhir-rest"}]}],
                "managingOrganization": {"reference": "Organization/org-audit"},
                "identifier": [{"value": "EP-AUDIT"}],
                "payload": [{"type": [{"coding": [{"code": "application/fhir+json"}]}]}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Provenance": {
                "resourceType": "Provenance",
                "id": "prov-audit-1",
                "target": [{"reference": "Observation/obs-audit"}],
                "recorded": "2024-07-15T10:00:00Z",
                "occurredDateTime": "2024-07-14T08:00:00Z",
                "activity": {"coding": [{"code": "CREATE"}]},
                "patient": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "agent": [
                    {
                        "who": {"reference": "Practitioner/prac-audit"},
                        "role": [{"coding": [{"code": "author"}]}],
                        "type": {"coding": [{"code": "practitioner"}]},
                    }
                ],
                "entity": [{"what": {"reference": "Device/dev-audit"}}],
                "basedOn": [{"reference": "ServiceRequest/sr-audit"}],
                "signature": [{"type": [{"code": "ProofOfOrigin"}]}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "EnrollmentRequest": {
                "resourceType": "EnrollmentRequest",
                "id": "enr-audit-1",
                "status": "active",
                "candidate": {"reference": "Patient/pat-audit"},
                "identifier": [{"value": "ENR-AUDIT"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "EnrollmentResponse": {
                "resourceType": "EnrollmentResponse",
                "id": "enres-audit-1",
                "status": "active",
                "request": {"reference": "EnrollmentRequest/enr-audit-1"},
                "identifier": [{"value": "ENRES-AUDIT"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "InsurancePlan": {
                "resourceType": "InsurancePlan",
                "id": "ip-audit-1",
                "status": "active",
                "name": "Audit Gold Plan",
                "alias": ["AGP-AUDIT"],
                "type": [{"coding": [{"code": "medical-audit"}]}],
                "ownedBy": {"reference": "Organization/org-audit"},
                "administeredBy": {"reference": "Organization/org-admin-audit"},
                "identifier": [{"value": "IP-AUDIT"}],
                "contact": [
                    {
                        "address": {
                            "city": "Boston",
                            "state": "MA",
                            "postalCode": "02101",
                            "country": "US",
                            "use": "work",
                        }
                    }
                ],
                "endpoint": [{"reference": "Endpoint/ep-audit"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "ChargeItemDefinition": {
                "resourceType": "ChargeItemDefinition",
                "id": "cid-audit-1",
                "status": "active",
                "url": "http://example.org/ChargeItemDefinition/audit-panel",
                "version": "1.0",
                "title": "Audit Lab Panel",
                "publisher": "Audit Billing",
                "description": "Audit charge definition",
                "date": "2024-06-01",
                "jurisdiction": [{"coding": [{"code": "US"}]}],
                "identifier": [{"value": "CID-AUDIT"}],
                "useContext": [
                    {
                        "code": {"code": "focus"},
                        "valueCodeableConcept": {"coding": [{"code": "ambulatory-audit"}]},
                    }
                ],
                "applicability": [
                    {"effectivePeriod": {"start": "2024-01-01", "end": "2025-12-31"}}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Basic": {
                "resourceType": "Basic",
                "id": "basic-audit-1",
                "code": {"coding": [{"code": "referral-audit"}]},
                "subject": {"reference": "Patient/pat-audit"},
                "author": {"reference": "Practitioner/prac-audit"},
                "created": "2024-07-15",
                "identifier": [{"value": "BASIC-AUDIT"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "BiologicallyDerivedProduct": {
                "resourceType": "BiologicallyDerivedProduct",
                "id": "bdp-audit-1",
                "productCode": {"coding": [{"code": "E0398"}]},
                "productCategory": {"code": "organ"},
                "productStatus": {"code": "available"},
                "biologicalSourceEvent": {
                    "system": "http://hospital.org/bse",
                    "value": "BSE-AUDIT",
                },
                "identifier": [
                    {"system": "http://hospital.org/bdp", "value": "SN-AUDIT"}
                ],
                "collection": {
                    "collector": {"reference": "Practitioner/prac-audit"},
                },
                "request": [{"reference": "ServiceRequest/sr-audit"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "DeviceDispense": {
                "resourceType": "DeviceDispense",
                "id": "dd-audit-1",
                "status": "completed",
                "subject": {"reference": "Patient/pat-audit"},
                "device": {"concept": {"coding": [{"code": "pump-audit"}]}},
                "identifier": [
                    {"system": "http://hospital.org/dd", "value": "DD-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "DeviceUsage": {
                "resourceType": "DeviceUsage",
                "id": "du-audit-1",
                "status": "active",
                "patient": {"reference": "Patient/pat-audit"},
                "device": {
                    "concept": {"coding": [{"code": "monitor-audit"}]},
                    "reference": {"reference": "Device/dev-audit"},
                },
                "identifier": [
                    {"system": "http://hospital.org/du", "value": "DU-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "SupplyDelivery": {
                "resourceType": "SupplyDelivery",
                "id": "sd-audit-1",
                "status": "completed",
                "patient": {"reference": "Patient/pat-audit"},
                "supplier": {"reference": "Practitioner/prac-audit"},
                "receiver": [{"reference": "PractitionerRole/pr-audit"}],
                "identifier": [
                    {"system": "http://hospital.org/sd", "value": "SD-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "SupplyRequest": {
                "resourceType": "SupplyRequest",
                "id": "sr-audit-1",
                "status": "active",
                "category": {"coding": [{"code": "central-audit"}]},
                "authoredOn": "2024-07-15T10:00:00Z",
                "deliverFor": {"reference": "Patient/pat-audit"},
                "deliverTo": {"reference": "Patient/pat-audit"},
                "requester": {"reference": "Practitioner/prac-audit"},
                "supplier": [{"reference": "Organization/org-audit"}],
                "item": {"concept": {"coding": [{"code": "gloves-audit"}]}},
                "quantity": {"value": 10},
                "identifier": [
                    {"system": "http://hospital.org/sr", "value": "SR-AUDIT"}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "VisionPrescription": {
                "resourceType": "VisionPrescription",
                "id": "vp-audit-1",
                "status": "active",
                "dateWritten": "2024-07-15T10:00:00Z",
                "patient": {"reference": "Patient/pat-audit"},
                "prescriber": {"reference": "PractitionerRole/pr-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "identifier": [{"value": "VP-AUDIT"}],
                "lensSpecification": [{"eye": "right", "sphere": -1.0}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "NutritionIntake": {
                "resourceType": "NutritionIntake",
                "id": "ni-audit-1",
                "status": "completed",
                "subject": {"reference": "Patient/pat-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "reportedReference": {"reference": "Practitioner/prac-audit"},
                "occurrenceDateTime": "2024-07-15T12:00:00Z",
                "code": {"coding": [{"code": "meal-audit"}]},
                "identifier": [{"value": "NI-AUDIT"}],
                "consumedItem": [
                    {
                        "nutritionProduct": {
                            "concept": {"coding": [{"code": "apple-audit"}]}
                        }
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "RequestOrchestration": {
                "resourceType": "RequestOrchestration",
                "id": "ro-audit-1",
                "status": "active",
                "intent": "order",
                "subject": {"reference": "Patient/pat-audit"},
                "author": {"reference": "Practitioner/prac-audit"},
                "encounter": {"reference": "Encounter/enc-audit"},
                "authoredOn": "2024-07-15T09:00:00Z",
                "code": {"coding": [{"code": "protocol-audit"}]},
                "identifier": [{"value": "RO-AUDIT"}],
                "groupIdentifier": {"value": "GRP-RO-AUDIT"},
                "basedOn": [{"reference": "CarePlan/cp-audit"}],
                "action": [
                    {
                        "participant": [
                            {
                                "actorReference": {
                                    "reference": "Practitioner/part-audit"
                                }
                            }
                        ]
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "GenomicStudy": {
                "resourceType": "GenomicStudy",
                "id": "gs-audit-1",
                "status": "registered",
                "subject": {"reference": "Patient/pat-audit"},
                "identifier": [{"value": "GS-AUDIT"}],
                "analysis": [
                    {"focus": [{"reference": "Condition/cond-audit"}]}
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Measure": {
                "resourceType": "Measure",
                "id": "measure-audit-1",
                "status": "active",
                "name": "audit-measure",
                "title": "Audit Measure",
                "publisher": "Acme Audit",
                "url": "http://example.org/Measure/audit",
                "version": "1.0",
                "date": "2024-01-01",
                "identifier": [{"value": "MEAS-AUDIT"}],
                "jurisdiction": [{"coding": [{"code": "US"}]}],
                "topic": [{"coding": [{"code": "audit-topic"}]}],
                "useContext": [
                    {
                        "code": {"code": "focus"},
                        "valueCodeableConcept": {
                            "coding": [{"code": "ambulatory-audit"}]
                        },
                    }
                ],
                "effectivePeriod": {
                    "start": "2024-01-01",
                    "end": "2024-12-31",
                },
                "library": ["http://example.org/Library/lib-audit"],
                "relatedArtifact": [
                    {
                        "type": "depends-on",
                        "resource": {"reference": "Library/lib-dep-audit"},
                    }
                ],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "MeasureReport": {
                "resourceType": "MeasureReport",
                "id": "mr-audit-1",
                "status": "complete",
                "date": "2024-07-15T10:00:00Z",
                "subject": {"reference": "Patient/pat-audit"},
                "reporter": {"reference": "Practitioner/prac-audit"},
                "location": {"reference": "Location/loc-audit"},
                "measure": "http://example.org/Measure/audit",
                "period": {"start": "2024-07-01", "end": "2024-07-31"},
                "identifier": [{"value": "MR-AUDIT"}],
                "evaluatedResource": [{"reference": "Observation/obs-audit"}],
                "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
            },
            "Slot": {
                "resourceType": "Slot",
                "id": "slot-audit-1",
                "identifier": [
                    {"system": "http://hospital.example/slots", "value": "SLOT-AUDIT-001"}
                ],
                "serviceCategory": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "408443003"}]}
                ],
                "serviceType": [
                    {
                        "concept": {
                            "coding": [{"system": "http://snomed.info/sct", "code": "11429006"}]
                        },
                        "reference": {"reference": "HealthcareService/hs-1"},
                    }
                ],
                "specialty": [
                    {"coding": [{"system": "http://snomed.info/sct", "code": "394814009"}]}
                ],
                "appointmentType": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0276",
                                "code": "ROUTINE",
                            }
                        ]
                    }
                ],
                "schedule": {"reference": "Schedule/sched-1"},
                "status": "free",
                "start": "2024-07-15T09:00:00Z",
                "end": "2024-07-15T09:30:00Z",
                "meta": {"lastUpdated": "2024-06-01T08:00:00Z"},
            },
        }

    # Resources covered by the cross-config audit harness. Keep this in
    # lock-step with the `samples` fixture above and with `ConfigLoader`
    # — every shipped resource config MUST have a sample here so its
    # purity / orphan / phantom guarantees are enforced uniformly.
    ALL_AUDITED_RESOURCES = [
        "Account",
        "ChargeItem",
        "Invoice",
        "ResearchStudy",
        "ResearchSubject",
        "Composition",
        "Questionnaire",
        "ExplanationOfBenefit",
        "CoverageEligibilityRequest",
        "CoverageEligibilityResponse",
        "Patient", "Observation", "Appointment", "Organization", "Location",
        "Practitioner", "PractitionerRole", "Device", "Group", "Schedule",
        "Slot", "Condition", "Encounter", "ServiceRequest", "Procedure",
        "Medication", "MedicationRequest", "MedicationAdministration",
        "MedicationDispense", "MedicationStatement", "AllergyIntolerance",
        "DiagnosticReport",
        "CareTeam", "Goal", "CarePlan", "Immunization", "Coverage", "Claim",
        "ClaimResponse", "DocumentReference", "Substance", "RelatedPerson",
        "EpisodeOfCare", "HealthcareService", "RiskAssessment", "Task",
        "Communication", "Flag", "AuditEvent", "Consent", "Contract",
        "NutritionOrder", "Specimen", "ImagingStudy", "FamilyMemberHistory",
        "ClinicalImpression", "DetectedIssue", "QuestionnaireResponse",
        "PaymentNotice", "PaymentReconciliation",
        "DeviceRequest", "AdverseEvent", "ImmunizationRecommendation",
        "Person", "BodyStructure",
        "OrganizationAffiliation", "Endpoint", "Provenance",
        "EnrollmentRequest", "EnrollmentResponse", "InsurancePlan",
        "ChargeItemDefinition", "Basic",
        "BiologicallyDerivedProduct", "DeviceDispense", "DeviceUsage",
        "SupplyDelivery", "SupplyRequest",
        "VisionPrescription", "NutritionIntake", "RequestOrchestration",
        "GenomicStudy", "Measure", "MeasureReport",
    ]

    @pytest.mark.parametrize(
        "resource_type",
        ALL_AUDITED_RESOURCES,
    )
    def test_round_trip_preserves_resource(
        self,
        denormalizer: ResourceDenormalizer,
        samples: Dict[str, Dict[str, Any]],
        resource_type: str,
    ) -> None:
        import copy

        sample = samples[resource_type]
        original_input = copy.deepcopy(sample)
        result = denormalizer.denormalize(sample)

        # 1. Input dict is NOT mutated in place.
        assert sample == original_input, (
            f"{resource_type}: denormalize() mutated the input dict in place"
        )

        # 2. Every input top-level key is preserved byte-identically.
        for k, v in original_input.items():
            assert k in result, f"{resource_type}: input key {k!r} dropped"
            assert result[k] == v, (
                f"{resource_type}: input key {k!r} rewritten — "
                f"was {v!r}, now {result[k]!r}"
            )

        # 3. Only `_search` / `_compartments` may be added.
        added = set(result.keys()) - set(original_input.keys())
        unexpected = added - self.ALLOWED_NEW_TOPLEVEL_KEYS
        assert not unexpected, (
            f"{resource_type}: unexpected synthetic top-level key(s) on the "
            f"FHIR resource: {sorted(unexpected)}. Every projected field "
            "must live under `_search` or `_compartments`."
        )


# ---------------------------------------------------------------------------
# Query-coverage contract: every denormalized `_search.*` field MUST be
# consumed by at least one search parameter or index, and every search
# parameter `_search.*` reference MUST be backed by a denormalization
# rule. Catches both dead denormalization (storage waste) and phantom
# query routing (silently empty result sets).
# ---------------------------------------------------------------------------


class TestQueryCoverage:
    """
    Pins the audit guarantees from `analysis_documents/FHIR_SEARCH_QUERY.md`
    against the bundled configs:

      1. Every `_search.<x>` produced by a denormalization rule is
         referenced by at least one search parameter `fields` entry or
         an `indexes` entry — otherwise the field is dead weight that
         pollutes documents and indexes for no query benefit.
      2. Every `_search.<x>` referenced by a search parameter is
         actually produced by some rule — otherwise the parameter would
         silently match nothing.
      3. Every `_compartments.<Type>` produced has a backing index.

    Regression frame for the `:exact` / `:text` modifier-routing fix
    that wired the case-preserved `_search.<name>` and the
    `code_displays_lower` / `code_text_lower` projections to FHIR's
    case-sensitive `:exact` and token `:text` modifiers respectively.
    """

    @pytest.fixture(scope="class")
    def fields_per_resource(self) -> Dict[str, Dict[str, Any]]:
        from fhir_search_to_mql import ConfigLoader

        cl = ConfigLoader()
        out: Dict[str, Dict[str, Any]] = {}
        for resource in cl.list_resources():
            cfg = cl.get_config(resource)
            sp = cfg.get("search_parameters", {}) or {}
            rules = cfg.get("denormalization", {}) or {}
            indexes = cfg.get("indexes", []) or []

            produced_search: set = set()
            produced_compartments: set = set()
            for rule in rules.values():
                bucket = rule.get("target", "_search")
                for m in rule.get("field_mappings", []) or []:
                    tf = m.get("target_field")
                    if not isinstance(tf, str) or not tf:
                        continue
                    top = tf.split(".", 1)[0]
                    if bucket == "_search":
                        produced_search.add(f"_search.{top}")
                    elif bucket == "_compartments":
                        produced_compartments.add(f"_compartments.{top}")

            param_refs: set = set()
            for meta in sp.values():
                fields = meta.get("fields")
                entries: List[Dict[str, Any]] = []
                if isinstance(fields, list):
                    entries = [e for e in fields if isinstance(e, dict)]
                elif isinstance(fields, dict):
                    for v in fields.values():
                        if isinstance(v, list):
                            entries.extend(e for e in v if isinstance(e, dict))
                for e in entries:
                    f = e.get("field")
                    if isinstance(f, str) and f:
                        param_refs.add(f)
                for comp in meta.get("components") or []:
                    if not isinstance(comp, dict):
                        continue
                    for e in comp.get("fields") or []:
                        if not isinstance(e, dict):
                            continue
                        f = e.get("field")
                        if isinstance(f, str) and f:
                            param_refs.add(f)

            index_refs: set = set()
            for idx in indexes:
                if not isinstance(idx, dict):
                    continue
                for entry in idx.get("fields") or []:
                    if isinstance(entry, dict):
                        for k in entry.keys():
                            if isinstance(k, str):
                                index_refs.add(k)
                    elif isinstance(entry, str):
                        index_refs.add(entry)

            out[resource] = {
                "produced_search": produced_search,
                "produced_compartments": produced_compartments,
                "param_refs": param_refs,
                "index_refs": index_refs,
            }
        return out

    # Resources covered by the cross-config query-coverage audit. Same
    # scope as `TestRoundTripPreservesResource.ALL_AUDITED_RESOURCES`
    # — duplicated locally instead of shared because the two test
    # classes are otherwise independent and merging them would muddle
    # what each one is asserting.
    ALL_AUDITED_RESOURCES = [
        "Account",
        "ChargeItem",
        "Invoice",
        "ResearchStudy",
        "ResearchSubject",
        "Composition",
        "Questionnaire",
        "ExplanationOfBenefit",
        "CoverageEligibilityRequest",
        "CoverageEligibilityResponse",
        "Patient", "Observation", "Appointment", "Organization", "Location",
        "Practitioner", "PractitionerRole", "Device", "Group", "Schedule",
        "Slot", "Condition", "Encounter", "ServiceRequest", "Procedure",
        "Medication", "MedicationRequest", "MedicationAdministration",
        "MedicationDispense", "MedicationStatement", "AllergyIntolerance",
        "DiagnosticReport",
        "CareTeam", "Goal", "CarePlan", "Immunization", "Coverage", "Claim",
        "ClaimResponse", "DocumentReference", "Substance", "RelatedPerson",
        "EpisodeOfCare", "HealthcareService", "RiskAssessment", "Task",
        "Communication", "Flag", "AuditEvent", "Consent", "Contract",
        "NutritionOrder", "Specimen", "ImagingStudy", "FamilyMemberHistory",
        "ClinicalImpression", "DetectedIssue", "QuestionnaireResponse",
        "PaymentNotice", "PaymentReconciliation",
        "DeviceRequest", "AdverseEvent", "ImmunizationRecommendation",
        "Person", "BodyStructure",
        "OrganizationAffiliation", "Endpoint", "Provenance",
        "EnrollmentRequest", "EnrollmentResponse", "InsurancePlan",
        "ChargeItemDefinition", "Basic",
        "BiologicallyDerivedProduct", "DeviceDispense", "DeviceUsage",
        "SupplyDelivery", "SupplyRequest",
        "VisionPrescription", "NutritionIntake", "RequestOrchestration",
        "GenomicStudy", "Measure", "MeasureReport",
    ]

    @pytest.mark.parametrize(
        "resource_type",
        ALL_AUDITED_RESOURCES,
    )
    def test_every_denormalized_search_field_is_consumed(
        self,
        fields_per_resource: Dict[str, Dict[str, Any]],
        resource_type: str,
    ) -> None:
        bag = fields_per_resource[resource_type]
        consumed = (bag["param_refs"] | bag["index_refs"]) & bag["produced_search"]
        orphans = sorted(bag["produced_search"] - consumed)
        assert not orphans, (
            f"{resource_type}: denormalization writes _search field(s) that "
            f"no search parameter or index references — dead weight that "
            f"pollutes every document and burns index slots:\n  "
            + "\n  ".join(orphans)
        )

    @pytest.mark.parametrize(
        "resource_type",
        ALL_AUDITED_RESOURCES,
    )
    def test_no_search_parameter_references_phantom_field(
        self,
        fields_per_resource: Dict[str, Dict[str, Any]],
        resource_type: str,
    ) -> None:
        bag = fields_per_resource[resource_type]
        phantoms = sorted(
            ref
            for ref in bag["param_refs"]
            if ref.startswith("_search.") and ref not in bag["produced_search"]
        )
        assert not phantoms, (
            f"{resource_type}: search parameter(s) reference _search.* "
            f"field(s) that no denormalization rule produces — queries "
            f"against these would silently match nothing:\n  "
            + "\n  ".join(phantoms)
        )

    @pytest.mark.parametrize(
        "resource_type",
        ALL_AUDITED_RESOURCES,
    )
    def test_every_compartments_field_has_backing_index(
        self,
        fields_per_resource: Dict[str, Dict[str, Any]],
        resource_type: str,
    ) -> None:
        bag = fields_per_resource[resource_type]
        unindexed = sorted(bag["produced_compartments"] - bag["index_refs"])
        assert not unindexed, (
            f"{resource_type}: _compartments field(s) lack a backing "
            f"index — the CompartmentResolver fast-path would devolve "
            f"into a collection scan:\n  "
            + "\n  ".join(unindexed)
        )


# ---------------------------------------------------------------------------
# Default-modifier FHIR conformance — pins case-insensitive starts-with
# semantics for ALL string parameters with a `default`/`exact` split.
# This is the contract the `_lower` denormalized fields exist to serve,
# and is the FHIR R5 spec requirement for "no modifier" string search.
# ---------------------------------------------------------------------------


class TestDefaultStringModifierConformance:
    """
    FHIR R5 §3.1.1.5.3 (string parameter type) requires the default
    (no-modifier) string search to be a case-insensitive,
    accent-insensitive **starts-with** match. The contract:

        family=SMITH   -> matches "Smith"     (case-insensitive)
        family=Smi     -> matches "Smith"     (prefix)
        family=ith     -> does NOT match      (prefix-only, not contains)

    The converter implements this by lowercasing the input and
    emitting a range query `[value, value\\uffff)` against the
    `_search.<name>_lower` field, while the denormalizer pre-folds
    the stored value. Both sides being case-folded is what makes the
    comparison case-insensitive REGARDLESS of input casing — without
    this contract, `family=SMITH` would silently miss every patient
    whose family name starts with a capital letter.

    These parametrized tests pin the contract end-to-end against
    every string parameter that has a `default`/`exact` split in the
    bundled configs (Patient/Organization/Location/Observation).

    NOTE: accent-insensitivity is NOT yet implemented (would require
    an NFD-folded companion field). That gap is intentional and
    tracked separately; this class only pins case-insensitivity.
    """

    @pytest.fixture(scope="class")
    def converter(self):
        from fhir_search_to_mql import FHIRSearchConverter
        return FHIRSearchConverter()

    @pytest.fixture(scope="class")
    def patient_doc(self, denormalizer: ResourceDenormalizer) -> Dict[str, Any]:
        return denormalizer.denormalize(
            {
                "resourceType": "Patient",
                "id": "p1",
                "name": [{"family": "Smith", "given": ["John"]}],
                "address": [
                    {"city": "Springfield", "state": "IL", "country": "US"}
                ],
            }
        )

    @pytest.mark.parametrize(
        "query,expected_match,rationale",
        [
            ("family=SMITH",  True,  "all-caps input must match (case-insensitive)"),
            ("family=Smith",  True,  "mixed-case input must match"),
            ("family=smith",  True,  "all-lower input must match"),
            ("family=Smi",    True,  "prefix match (FHIR starts-with)"),
            ("family=S",      True,  "single-char prefix match"),
            ("family=SMI",    True,  "uppercase prefix must match"),
            ("family=ith",    False, "FHIR is starts-with, NOT contains"),
            ("family=Jones",  False, "different value must NOT match"),
            ("given=JOHN",    True,  "given name case-insensitive across casings"),
            ("given=jo",      True,  "given name prefix"),
            ("address-city=SPRINGFIELD", True, "address-city case-insensitive"),
            ("address-city=spring",      True, "address-city prefix"),
            ("address-state=il",         True, "address-state case-insensitive"),
            ("address-country=us",       True, "address-country case-insensitive"),
        ],
    )
    def test_patient_default_modifier_is_case_insensitive_prefix(
        self,
        converter,
        patient_doc: Dict[str, Any],
        query: str,
        expected_match: bool,
        rationale: str,
    ) -> None:
        mql = converter.convert("Patient", query)
        actual = TestExactModifierRouting._evaluate(mql, patient_doc)
        assert actual is expected_match, (
            f"{query!r} -> {mql} did not behave as a case-insensitive "
            f"starts-with match (expected={expected_match}, got={actual}); "
            f"FHIR R5 §3.1.1.5.3 requires {rationale}."
        )

    def test_default_modifier_lowercases_input_in_mql(
        self, converter
    ) -> None:
        """The MQL produced for any default string query must target a
        `_lower` field with a lowercased value — the structural
        guarantee that makes case-insensitivity work."""
        for q in ("family=SMITH", "family=Smith", "given=JOHN"):
            mql = converter.convert("Patient", q)
            assert any(k.endswith("_lower") for k in mql.keys()), (
                f"Default string MQL must target a `_lower` field for "
                f"FHIR §3.1.1.5.3 case-insensitivity. {q!r} -> {mql}"
            )
            for k, v in mql.items():
                if isinstance(v, dict) and "$gte" in v:
                    assert v["$gte"] == v["$gte"].lower(), (
                        f"Default string MQL value must be lowercased: "
                        f"{q!r} -> {mql}"
                    )


# ---------------------------------------------------------------------------
# `:exact` and `:text` modifier semantics — the fix lands real-world
# correctness for two FHIR R5 modifiers that were previously silently
# broken (querying `_lower` fields with case-sensitive input, or
# matching coded `system|code` tuples instead of human-readable display).
# ---------------------------------------------------------------------------


class TestExactModifierRouting:
    """
    Per FHIR R5 search §3.1.1.5.4, the `:exact` modifier on a string
    parameter is CASE- and ACCENT-sensitive equality. Before the fix,
    `family:exact=Smith` resolved to
    `{_search.familyName_lower: "Smith"}` — but the stored value was
    lowercased to `"smith"`, so the query silently matched nothing.

    The fix adds modifier-keyed `fields` blocks routing `:exact` to
    the case-preserved `_search.<name>` field. These tests run the
    converter end-to-end and evaluate the resulting MQL against a
    denormalized sample to prove the modifier picks up real hits.
    """

    @pytest.fixture(scope="class")
    def converter(self):
        from fhir_search_to_mql import FHIRSearchConverter
        return FHIRSearchConverter()

    @staticmethod
    def _evaluate(mql: Any, doc: Dict[str, Any]) -> bool:
        """Tiny in-memory MQL evaluator for the subset we generate."""
        if "$or" in mql:
            return any(
                TestExactModifierRouting._evaluate(sub, doc) for sub in mql["$or"]
            )
        for path, expected in mql.items():
            if path.startswith("$"):
                continue
            actual = doc
            for part in path.split("."):
                if isinstance(actual, dict):
                    actual = actual.get(part)
                else:
                    actual = None
                    break
            if isinstance(expected, dict):
                vals = actual if isinstance(actual, list) else [actual]
                vals = [v for v in vals if isinstance(v, str)]
                gte = expected.get("$gte")
                lt = expected.get("$lt")
                if not any(
                    (gte is None or v >= gte) and (lt is None or v < lt)
                    for v in vals
                ):
                    return False
            else:
                if isinstance(actual, list):
                    if expected not in actual:
                        return False
                elif actual != expected:
                    return False
        return True

    @pytest.fixture(scope="class")
    def patient_doc(self, denormalizer: ResourceDenormalizer) -> Dict[str, Any]:
        return denormalizer.denormalize(
            {
                "resourceType": "Patient",
                "id": "p1",
                "name": [{"family": "Smith", "given": ["John"], "text": "John Smith"}],
                "address": [
                    {
                        "city": "Springfield",
                        "state": "IL",
                        "country": "US",
                        "text": "1 Main St, Springfield, IL",
                    }
                ],
            }
        )

    @pytest.mark.parametrize(
        "query,expected_match",
        [
            ("family:exact=Smith", True),
            # Wrong case must NOT match (FHIR R5: `:exact` is
            # case-sensitive). Before the fix this would either
            # silently match-all-Smiths or always miss because the
            # converter pointed `:exact` at the `_lower` field.
            ("family:exact=smith", False),
            ("given:exact=John", True),
            ("given:exact=john", False),
            ("name:exact=Smith", True),
            ("name:exact=John Smith", True),
            ("address-city:exact=Springfield", True),
            ("address-city:exact=springfield", False),
            ("address-state:exact=IL", True),
            ("address-country:exact=US", True),
            ("address:exact=1 Main St, Springfield, IL", True),
        ],
    )
    def test_patient_exact_modifier(
        self,
        converter,
        patient_doc: Dict[str, Any],
        query: str,
        expected_match: bool,
    ) -> None:
        mql = converter.convert("Patient", query)
        assert self._evaluate(mql, patient_doc) is expected_match, (
            f"Patient `{query}` -> {mql} did not behave as expected "
            f"(expected match={expected_match}); modifier routing for "
            f"`:exact` may have regressed back to the `_lower` field."
        )

    @pytest.fixture(scope="class")
    def org_doc(self, denormalizer: ResourceDenormalizer) -> Dict[str, Any]:
        return denormalizer.denormalize(
            {
                "resourceType": "Organization",
                "id": "org-1",
                "name": "Springfield General Hospital",
                "alias": ["SGH"],
                "contact": [
                    {
                        "address": {
                            "city": "Springfield",
                            "state": "IL",
                            "country": "US",
                        }
                    }
                ],
            }
        )

    @pytest.mark.parametrize(
        "query,expected_match",
        [
            ("name:exact=Springfield General Hospital", True),
            # Aliases are part of FHIR's `name` parameter expression
            # (`Organization.name | Organization.alias`), so the
            # `:exact` routing must include `_search.aliases`.
            ("name:exact=SGH", True),
            ("name:exact=sgh", False),
            ("address-city:exact=Springfield", True),
            ("address-state:exact=IL", True),
        ],
    )
    def test_organization_exact_modifier(
        self, converter, org_doc: Dict[str, Any], query: str, expected_match: bool
    ) -> None:
        mql = converter.convert("Organization", query)
        assert self._evaluate(mql, org_doc) is expected_match, (
            f"Organization `{query}` -> {mql} did not behave as expected "
            f"(expected match={expected_match})."
        )

    @pytest.fixture(scope="class")
    def loc_doc(self, denormalizer: ResourceDenormalizer) -> Dict[str, Any]:
        return denormalizer.denormalize(
            {
                "resourceType": "Location",
                "id": "loc-1",
                "name": "ER",
                "alias": ["Emergency Room"],
                "address": {"city": "Springfield", "state": "IL", "country": "US"},
            }
        )

    @pytest.mark.parametrize(
        "query,expected_match",
        [
            ("name:exact=ER", True),
            ("name:exact=Emergency Room", True),
            ("name:exact=emergency room", False),
            ("address-city:exact=Springfield", True),
            ("address-state:exact=IL", True),
        ],
    )
    def test_location_exact_modifier(
        self, converter, loc_doc: Dict[str, Any], query: str, expected_match: bool
    ) -> None:
        mql = converter.convert("Location", query)
        assert self._evaluate(mql, loc_doc) is expected_match, (
            f"Location `{query}` -> {mql} did not behave as expected "
            f"(expected match={expected_match})."
        )


class TestTextModifierRouting:
    """
    Per FHIR R5 search §3.1.1.5.12, the `:text` modifier on a token
    parameter searches the human-readable display values
    (CodeableConcept.coding[*].display and CodeableConcept.text), NOT
    the coded `system|code` tuples. Before the fix, `code:text=blood`
    routed to `_search.code_systemCode_lower` and
    `_search.code_codes_lower` — neither of which contain display
    text, so the modifier was silently broken on real data.

    The fix adds a `text:` modifier-keyed `fields` block routing to
    `_search.code_displays_lower` / `_search.code_text_lower`, with
    matching denormalization rules that emit the lowercased display
    + text projections.
    """

    @pytest.fixture(scope="class")
    def converter(self):
        from fhir_search_to_mql import FHIRSearchConverter
        return FHIRSearchConverter()

    @pytest.fixture(scope="class")
    def obs_doc(self, denormalizer: ResourceDenormalizer) -> Dict[str, Any]:
        return denormalizer.denormalize(
            {
                "resourceType": "Observation",
                "id": "o1",
                "status": "final",
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": "85354-9",
                            "display": "Blood pressure panel",
                        }
                    ],
                    "text": "Blood pressure measurement",
                },
                "subject": {"reference": "Patient/p1"},
            }
        )

    @pytest.mark.parametrize(
        "query,expected_match",
        [
            # Prefix match against display, case-insensitive.
            ("code:text=blood pressure", True),
            ("code:text=BLOOD", True),
            ("code:text=Blood", True),
            # `:text` is prefix match (FHIR-spec range) — substring
            # in the middle of the display must NOT match.
            ("code:text=measurement", False),
            # Coded path — lookups by system|code still work
            # alongside `:text`, proving the per-modifier `fields`
            # routing didn't break the default token path.
            ("code=http://loinc.org|85354-9", True),
        ],
    )
    def test_observation_code_text_modifier(
        self, converter, obs_doc: Dict[str, Any], query: str, expected_match: bool
    ) -> None:
        mql = converter.convert("Observation", query)
        assert TestExactModifierRouting._evaluate(mql, obs_doc) is expected_match, (
            f"Observation `{query}` -> {mql} did not behave as expected "
            f"(expected match={expected_match}); `:text` routing may "
            f"have regressed back to the `system|code` fields."
        )

    def test_code_displays_lower_is_populated(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        """`:text` queries depend on `_search.code_displays_lower` being
        produced by the denormalization layer. Pin the exact projection
        so a future refactor that reverts to case-preserved fields
        breaks this test instead of silently breaking `:text`."""
        result = denormalizer.denormalize(
            {
                "resourceType": "Observation",
                "id": "o2",
                "status": "final",
                "code": {
                    "coding": [
                        {"display": "Systolic blood pressure"},
                        {"display": "Diastolic blood pressure"},
                    ],
                    "text": "BP Panel",
                },
                "subject": {"reference": "Patient/p1"},
            }
        )
        s = result["_search"]
        assert s["code_displays_lower"] == [
            "systolic blood pressure",
            "diastolic blood pressure",
        ]
        assert s["code_text_lower"] == "bp panel"
        assert "code_displays" not in s, (
            "Case-preserved `code_displays` is dead weight — only the "
            "`_lower` variant is queried by `:text`."
        )
        assert "code_text" not in s


# ---------------------------------------------------------------------------
# HumanNameExtractor sparsity — `nameText: []` empty-array leak
# ---------------------------------------------------------------------------


class TestHumanNameExtractorSparsity:
    """
    HumanNameExtractor previously wrote `nameText: []` whenever the
    HumanName had no `.text` sub-field — leaking an empty array into
    `_search` for every Patient name without a literal text rendering.
    Empty arrays pollute indexes and break `:missing=true` semantics
    (Mongo `{$exists: false}` would not match `[]`). This pins the
    sparse-output contract on parity with CodeableConceptExtractor and
    ReferenceExtractor.
    """

    def test_no_nameText_leak_when_text_is_absent(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        result = denormalizer.denormalize(
            {
                "resourceType": "Patient",
                "id": "p-no-text",
                "name": [{"family": "Smith", "given": ["John"]}],
            }
        )
        s = result.get("_search", {})
        assert "nameText" not in s, (
            f"HumanNameExtractor leaked an empty/None `nameText` field "
            f"into _search: {s.get('nameText')!r}"
        )

    def test_nameText_present_when_text_is_provided(
        self, denormalizer: ResourceDenormalizer
    ) -> None:
        result = denormalizer.denormalize(
            {
                "resourceType": "Patient",
                "id": "p-with-text",
                "name": [
                    {"family": "Smith", "given": ["John"], "text": "John Q. Smith"}
                ],
            }
        )
        s = result["_search"]
        assert s.get("nameText") == ["John Q. Smith"]


# ---------------------------------------------------------------------------
# Hybrid Approach 3 contract: every (resource, compartment_type) pair that
# opts into `compartments.precomputed: [<Type>]` MUST cover every linking
# parameter listed in `compartments/definitions/<type>.json` for that
# resource. This is the audit that would have caught the two Device-
# compartment gaps (Observation, Appointment) at PR time:
#   - Observation in Device compartment links via subject ∪ performer ∪
#     device. The `_compartments.Device` precompute MUST source from all
#     three; missing any would leak Device-compartment members from the
#     fast-path query result.
#   - Appointment in Device compartment links via `actor` (R5 widened
#     participant.actor cardinality to include Device). The
#     `_compartments.Device` precompute MUST source from
#     participant[*].actor (filtered to Device/* by reference_type).
# Pinning this contract here means the next config we add can never
# regress the precompute<->spec coupling without a loud test failure.
# ---------------------------------------------------------------------------


class TestPrecomputedCompartmentMatchesDefinition:
    """
    For every YAML config that declares ``compartments.precomputed: [X]``,
    the resource's ``compartment_membership.field_mappings`` entry with
    ``target_field == X`` must reference a FHIR path corresponding to
    every linking parameter the FHIR R5 CompartmentDefinition for X
    lists for this resource.

    The match heuristic is deliberately permissive: linking-parameter
    names from the R5 spec (kebab-case, e.g. ``managing-entity``) are
    normalized to the camelCase token convention the YAMLs use
    (``managingEntity``), then we check that each normalized token
    appears (case-insensitively) as a substring of at least one source
    path in the membership rule. The implicit ``[base]`` linking
    parameter is satisfied by ``include_self: true`` instead of a
    source path. False positives (a path matching by accident) are
    benign — denormalization still produces the correct bucket. The
    test catches the genuinely dangerous case: a linking parameter
    that has NO matching source path, meaning the precomputed bucket
    silently misses members.
    """

    @pytest.fixture(scope="class")
    def precomputed_audit_data(self) -> List[Dict[str, Any]]:
        """Build the audit dataset once: every (resource, compartment)
        precomputed pair, with the linking params from the spec and
        the source paths from the YAML. Pure-data fixture so the
        actual assertion is trivial — easier to debug failures."""
        from fhir_search_to_mql import ConfigLoader
        from fhir_search_to_mql.compartments.compartment_loader import (
            CompartmentLoader,
        )

        cl = ConfigLoader()
        # CompartmentLoader requires an explicit `load_all()` to read
        # the JSON spec files — without it `get_linking_parameters`
        # returns an empty list for every query and the audit would
        # silently pass on every config (false-negative).
        cmp_loader = CompartmentLoader()
        cmp_loader.load_all()

        rows: List[Dict[str, Any]] = []
        for resource in sorted(cl.list_resources()):
            cfg = cl.get_config(resource)
            cmp_section = cfg.get("compartments") or {}
            precomputed = cmp_section.get("precomputed") or []
            if not isinstance(precomputed, list):
                continue

            # Locate the compartment_membership rule field_mappings.
            membership_rule = (cfg.get("denormalization") or {}).get(
                "compartment_membership"
            ) or {}
            membership_mappings = membership_rule.get("field_mappings") or []

            for compartment_type in precomputed:
                # Find the field_mapping whose target_field matches
                # this compartment.
                mapping = next(
                    (
                        m
                        for m in membership_mappings
                        if isinstance(m, dict)
                        and m.get("target_field") == compartment_type
                    ),
                    None,
                )

                # Linking parameters from the FHIR R5 CompartmentDefinition.
                # Returns [] if the resource isn't listed in that
                # compartment's resource[] array (e.g. self-membership
                # via the implicit [base] parameter — handled below).
                linking_params = list(
                    cmp_loader.get_linking_parameters(
                        compartment_type, resource
                    )
                )

                rows.append(
                    {
                        "resource": resource,
                        "compartment_type": compartment_type,
                        "linking_params": linking_params,
                        "is_self_compartment": resource == compartment_type,
                        "mapping": mapping,
                    }
                )
        return rows

    @staticmethod
    def _normalize_param(param_name: str) -> str:
        """FHIR linking-param names are kebab-case
        (``managing-entity``, ``general-practitioner``); the YAMLs use
        camelCase tokens (``managingEntity``, ``generalPractitioner``).
        Normalize to a single comparable form."""
        parts = param_name.split("-")
        return parts[0] + "".join(p.title() for p in parts[1:])

    def test_membership_mapping_exists_for_each_precomputed_pair(
        self, precomputed_audit_data: List[Dict[str, Any]]
    ) -> None:
        """For every precomputed (resource, compartment) pair, the
        compartment_membership rule MUST declare a field_mapping with
        target_field == compartment_type. A precompute opt-in without
        a matching field_mapping is a guaranteed silent miss — the
        fast-path query would target an empty bucket."""
        missing: List[str] = []
        for row in precomputed_audit_data:
            if row["mapping"] is None:
                missing.append(
                    f"{row['resource']}.compartments.precomputed lists "
                    f"'{row['compartment_type']}' but no compartment_membership "
                    f"field_mapping has target_field == '{row['compartment_type']}'"
                )
        assert not missing, (
            "Configs opt into precompute fast-path without populating the "
            "bucket:\n  " + "\n  ".join(missing)
        )

    def test_self_compartment_uses_include_self(
        self, precomputed_audit_data: List[Dict[str, Any]]
    ) -> None:
        """When the precomputed compartment is the resource's own
        compartment (Patient/Patient, Practitioner/Practitioner,
        Device/Device), the FHIR R5 ``[base]`` linking parameter is
        the only way the resource enters its own compartment. The
        membership rule MUST set ``include_self: true`` so the
        resource's own id lands in the bucket."""
        violations: List[str] = []
        for row in precomputed_audit_data:
            if not row["is_self_compartment"]:
                continue
            mapping = row["mapping"]
            if mapping is None:
                continue
            if not mapping.get("include_self"):
                violations.append(
                    f"{row['resource']}.compartment_membership[{row['compartment_type']}] "
                    f"is the self-compartment but does not set "
                    f"`include_self: true` — the resource will NEVER appear "
                    f"in its own compartment query."
                )
        assert not violations, "\n  ".join(violations)

    def test_every_linking_param_is_covered_by_a_source_path(
        self, precomputed_audit_data: List[Dict[str, Any]]
    ) -> None:
        """For every precomputed (resource, compartment) pair, every
        linking parameter declared in the FHIR R5 CompartmentDefinition
        for that resource MUST be reachable via at least one
        ``source_paths`` entry on the membership rule. Catches the
        regression where Observation precomputed Device but only
        sourced from `subject ∪ performer` (the FHIR R5 def lists
        `subject ∪ performer ∪ device`) — `Device/<id>/Observation`
        would silently miss Observations whose only Device-typed
        reference was on `device` rather than `subject`/`performer`."""
        violations: List[str] = []
        for row in precomputed_audit_data:
            mapping = row["mapping"]
            if mapping is None:
                # Already reported by the prior test; don't double-fail.
                continue

            linking_params = row["linking_params"]
            if not linking_params:
                # Self-compartment without an explicit resource entry
                # in the spec doc (Practitioner / Device — handled by
                # the include_self test above).
                continue

            source_paths = mapping.get("source_paths") or []
            # Combine into a single lowercased haystack for substring
            # matching — the YAML uses camelCase tokens like
            # `participant[*].actor`, `managingEntity`, etc.
            haystack = " ".join(str(p) for p in source_paths).lower()

            for param in linking_params:
                token = self._normalize_param(param).lower()
                if token not in haystack:
                    violations.append(
                        f"{row['resource']} precomputes "
                        f"`_compartments.{row['compartment_type']}` but "
                        f"no source_path covers the FHIR R5 linking "
                        f"parameter `{param}` (normalized token "
                        f"`{token}`). Source paths declared: "
                        f"{source_paths}. Linking params per "
                        f"compartments/definitions/"
                        f"{row['compartment_type'].lower()}.json: "
                        f"{linking_params}."
                    )
        assert not violations, (
            "Precomputed `_compartments.<Type>` buckets miss linking "
            "parameters declared in the FHIR R5 CompartmentDefinition. "
            "Queries against these compartments will silently omit "
            "members:\n  " + "\n  ".join(violations)
        )

    def test_precomputed_compartments_have_backing_index(
        self, precomputed_audit_data: List[Dict[str, Any]]
    ) -> None:
        """Every precomputed `_compartments.<Type>` field MUST have a
        backing MongoDB index — the whole point of Hybrid Approach 3 is
        the single-field indexed lookup. An unindexed precompute is
        slower than the dynamic-resolver fallback (the latter at least
        hits the per-linking-param `_search.*Id` indexes)."""
        from fhir_search_to_mql import ConfigLoader

        cl = ConfigLoader()
        violations: List[str] = []
        for row in precomputed_audit_data:
            cfg = cl.get_config(row["resource"])
            indexed_fields: set = set()
            for idx in cfg.get("indexes") or []:
                if not isinstance(idx, dict):
                    continue
                for entry in idx.get("fields") or []:
                    if isinstance(entry, dict):
                        indexed_fields.update(entry.keys())
                    elif isinstance(entry, str):
                        indexed_fields.add(entry)

            target = f"_compartments.{row['compartment_type']}"
            if target not in indexed_fields:
                violations.append(
                    f"{row['resource']} precomputes `{target}` but does "
                    f"NOT declare a corresponding index. Add it to the "
                    f"`indexes:` section of {row['resource']}.yaml."
                )
        assert not violations, "\n  ".join(violations)
