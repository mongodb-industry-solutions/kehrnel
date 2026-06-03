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
        "Patient", "Observation", "Appointment", "Organization", "Location",
        "Practitioner", "PractitionerRole", "Device", "Group", "Schedule",
        "Slot", "Condition", "Encounter", "ServiceRequest",
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
        "Patient", "Observation", "Appointment", "Organization", "Location",
        "Practitioner", "PractitionerRole", "Device", "Group", "Schedule",
        "Slot", "Condition", "Encounter", "ServiceRequest",
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
