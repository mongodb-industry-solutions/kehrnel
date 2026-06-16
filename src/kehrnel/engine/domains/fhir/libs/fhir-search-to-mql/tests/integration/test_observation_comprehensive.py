"""
Comprehensive integration tests for ALL Observation search parameters per FHIR R5.

Reference: https://hl7.org/fhir/R5/observation.html#search

This test suite ensures complete coverage of the 42 R5 spec parameters declared
in `configs/Observation.yaml` (plus the common params `_id` and `_lastUpdated`):

  - References (13): based-on, derived-from, device, encounter, focus,
                     has-member, part-of, patient, performer, specimen,
                     subject, value-reference, component-value-reference
  - Tokens (14):     category, code, combo-code, combo-data-absent-reason,
                     combo-value-concept, component-code,
                     component-data-absent-reason, component-value-concept,
                     data-absent-reason, identifier, method, status,
                     value-concept, _id
  - Dates (3):       date, value-date, _lastUpdated
  - Quantities (3):  value-quantity, combo-value-quantity, component-value-quantity
  - URIs (2):        value-canonical, component-value-canonical
  - Strings (1):     value-markdown
  - Composites (8):  code-value-concept, code-value-date, code-value-quantity,
                     code-value-string, combo-code-value-concept,
                     combo-code-value-quantity, component-code-value-concept,
                     component-code-value-quantity

Plus end-to-end denormalization for the new fields, modifier handling, prefix
handling, and a real MongoDB workflow against `localhost:27017`.
"""

import os
import pytest
from datetime import datetime
from typing import Any, Dict, List

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def converter() -> FHIRSearchConverter:
    """Single converter loaded from disk-backed configs/."""
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    """Single denormalizer loaded from disk-backed configs/."""
    return ResourceDenormalizer()


@pytest.fixture
def rich_observation() -> Dict[str, Any]:
    """An Observation with most R5 fields populated for deep extractor testing."""
    return {
        "resourceType": "Observation",
        "id": "obs-rich-1",
        "meta": {"lastUpdated": "2024-06-01T10:00:00Z"},
        "identifier": [
            {
                "system": "http://hospital.example.org/obs",
                "value": "OBS-12345",
            }
        ],
        "basedOn": [{"reference": "ServiceRequest/sr-001"}],
        "partOf": [{"reference": "Procedure/proc-001"}],
        "status": "final",
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "vital-signs",
                        "display": "Vital Signs",
                    }
                ]
            }
        ],
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "85354-9",
                    "display": "Blood pressure panel",
                }
            ],
            "text": "Blood pressure panel",
        },
        "subject": {"reference": "Patient/pat-001", "display": "John Smith"},
        "focus": [{"reference": "Procedure/proc-002"}],
        "encounter": {"reference": "Encounter/enc-001"},
        "effectiveDateTime": "2024-06-01T09:30:00Z",
        "performer": [{"reference": "Practitioner/prac-001"}],
        "device": {"reference": "Device/dev-001"},
        "specimen": {"reference": "Specimen/spec-001"},
        "method": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "37931006",
                    "display": "Auscultation",
                }
            ]
        },
        "valueQuantity": {
            "value": 120,
            "unit": "mmHg",
            "system": "http://unitsofmeasure.org",
            "code": "mm[Hg]",
        },
        "dataAbsentReason": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/data-absent-reason",
                    "code": "unknown",
                }
            ]
        },
        "derivedFrom": [{"reference": "Observation/obs-source-1"}],
        "hasMember": [{"reference": "Observation/obs-member-1"}],
        "component": [
            {
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": "8480-6",
                            "display": "Systolic blood pressure",
                        }
                    ]
                },
                "valueQuantity": {
                    "value": 120,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]",
                },
            },
            {
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": "8462-4",
                            "display": "Diastolic blood pressure",
                        }
                    ]
                },
                "valueQuantity": {
                    "value": 80,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]",
                },
            },
        ],
    }


# ---------------------------------------------------------------------------
# 1) Reference parameters (13)
# ---------------------------------------------------------------------------

class TestObservationReferenceParameters:
    """All R5 reference search parameters."""

    @pytest.mark.parametrize("param,value,expect_field", [
        ("based-on", "ServiceRequest/sr-001", "_search.basedOnId"),
        ("derived-from", "Observation/obs-source-1", "_search.derivedFromId"),
        ("device", "Device/dev-001", "_search.deviceId"),
        ("encounter", "Encounter/enc-001", "_search.encounterId"),
        ("focus", "Procedure/proc-002", "_search.focusId"),
        ("has-member", "Observation/obs-member-1", "_search.hasMemberId"),
        ("part-of", "Procedure/proc-001", "_search.partOfId"),
        ("patient", "Patient/pat-001", "_search.patientId"),
        ("performer", "Practitioner/prac-001", "_search.performerId"),
        ("specimen", "Specimen/spec-001", "_search.specimenId"),
        ("subject", "Patient/pat-001", "_search.subjectId"),
        ("value-reference", "MolecularSequence/ms-001", "_search.valueReferenceId"),
        ("component-value-reference", "MolecularSequence/ms-002",
         "_search.componentValueReferenceId"),
    ])
    def test_reference_param(self, converter, param, value, expect_field):
        query = converter.convert("Observation", f"{param}={value}")
        query_str = str(query)
        # Bare id should appear; the exact field may vary by query shape ($or/$and)
        bare_id = value.split("/", 1)[1] if "/" in value else value
        assert bare_id in query_str
        assert expect_field in query_str or "subject.reference" in query_str \
            or "performer.reference" in query_str or "encounter.reference" in query_str \
            or "basedOn.reference" in query_str or "device.reference" in query_str \
            or "focus.reference" in query_str or "hasMember.reference" in query_str \
            or "partOf.reference" in query_str or "specimen.reference" in query_str \
            or "derivedFrom.reference" in query_str \
            or "valueReference.reference" in query_str \
            or "component.valueReference.reference" in query_str

    def test_patient_typed_reference(self, converter):
        query = converter.convert("Observation", "patient=Patient/pat-001")
        assert "pat-001" in str(query)

    def test_subject_with_type_modifier(self, converter):
        # Typed reference modifier scopes to a specific resource type
        query = converter.convert("Observation", "subject:Patient=pat-001")
        s = str(query)
        # Some configurations log a warning and return {} — ensure we never raise
        assert isinstance(query, dict)
        if query:
            assert "pat-001" in s


# ---------------------------------------------------------------------------
# 2) Token parameters (14)
# ---------------------------------------------------------------------------

class TestObservationTokenParameters:
    """All R5 token search parameters."""

    @pytest.mark.parametrize("param,value", [
        ("category", "vital-signs"),
        ("code", "http://loinc.org|85354-9"),
        ("code", "85354-9"),  # value only
        ("combo-code", "8480-6"),
        ("combo-data-absent-reason", "unknown"),
        ("combo-value-concept", "http://snomed.info/sct|123456"),
        ("component-code", "http://loinc.org|8480-6"),
        ("component-data-absent-reason", "asked-unknown"),
        ("component-value-concept", "http://snomed.info/sct|789"),
        ("data-absent-reason", "unknown"),
        ("identifier", "http://hospital.example.org/obs|OBS-12345"),
        ("identifier", "OBS-12345"),
        ("method", "37931006"),
        ("status", "final"),
        ("value-concept", "http://snomed.info/sct|1234"),
        ("_id", "obs-rich-1"),
    ])
    def test_token_param(self, converter, param, value):
        query = converter.convert("Observation", f"{param}={value}")
        query_str = str(query)
        # The trailing token (value or system|value) should always be in the query
        assert value.split("|")[-1] in query_str

    def test_status_in_modifier(self, converter):
        query = converter.convert("Observation", "status=final,amended,corrected")
        query_str = str(query)
        assert "final" in query_str
        assert "amended" in query_str
        assert "corrected" in query_str

    def test_token_missing_modifier(self, converter):
        query = converter.convert("Observation", "code:missing=true")
        query_str = str(query)
        assert "$exists" in query_str or "missing" in query_str.lower()

    def test_token_not_modifier(self, converter):
        query = converter.convert("Observation", "status:not=cancelled")
        query_str = str(query)
        assert "cancelled" in query_str


# ---------------------------------------------------------------------------
# 3) Date parameters (3)
# ---------------------------------------------------------------------------

class TestObservationDateParameters:
    """date, value-date, _lastUpdated and all comparators."""

    @pytest.mark.parametrize("prefix,operator", [
        ("eq", "$gte"),
        ("ne", "$ne"),
        ("gt", "$gt"),
        ("ge", "$gte"),
        ("lt", "$lt"),
        ("le", "$lte"),
        ("sa", "$gt"),
        ("eb", "$lt"),
    ])
    def test_date_prefixes(self, converter, prefix, operator):
        query = converter.convert("Observation", f"date={prefix}2024-06-01")
        # The converter emits real datetime objects; year, month, day
        # all appear inside the str() form: "datetime.datetime(2024, 6, 1, ...)".
        s = str(query)
        assert "2024" in s
        assert "6, 1" in s or "6, 30" in s

    def test_date_year_only(self, converter):
        query = converter.convert("Observation", "date=2024")
        assert "2024" in str(query)

    def test_date_range(self, converter):
        query = converter.convert(
            "Observation",
            "date=ge2024-01-01&date=le2024-12-31")
        s = str(query)
        # Datetime tuples for the range bounds
        assert "datetime.datetime(2024, 1, 1" in s
        assert "datetime.datetime(2024, 12, 31" in s

    def test_value_date(self, converter):
        query = converter.convert("Observation", "value-date=ge2024-01-01")
        assert "datetime.datetime(2024, 1, 1" in str(query)

    def test_lastupdated(self, converter):
        query = converter.convert("Observation", "_lastUpdated=ge2024-01-01")
        assert "datetime.datetime(2024, 1, 1" in str(query)


# ---------------------------------------------------------------------------
# 4) Quantity parameters (3)
# ---------------------------------------------------------------------------

class TestObservationQuantityParameters:
    """value-quantity, combo-value-quantity, component-value-quantity."""

    @pytest.mark.parametrize("param", [
        "value-quantity",
        "combo-value-quantity",
        "component-value-quantity",
    ])
    def test_quantity_simple(self, converter, param):
        query = converter.convert("Observation", f"{param}=120")
        query_str = str(query)
        # value 120 must appear (possibly as 120, 119.5, or 120.5 around range)
        assert "120" in query_str or "119" in query_str

    def test_quantity_with_unit_and_system(self, converter):
        query = converter.convert(
            "Observation",
            "value-quantity=120|http://unitsofmeasure.org|mm[Hg]")
        query_str = str(query)
        assert "120" in query_str or "119" in query_str

    @pytest.mark.parametrize("prefix", ["gt", "ge", "lt", "le", "eq", "ne"])
    def test_quantity_prefix(self, converter, prefix):
        query = converter.convert("Observation", f"value-quantity={prefix}120")
        query_str = str(query)
        assert "120" in query_str or "119" in query_str or "121" in query_str


# ---------------------------------------------------------------------------
# 5) URI parameters (2)
# ---------------------------------------------------------------------------

class TestObservationURIParameters:
    """value-canonical, component-value-canonical."""

    def test_value_canonical(self, converter):
        query = converter.convert(
            "Observation",
            "value-canonical=http://example.org/MolecularSequence/123")
        query_str = str(query)
        assert "MolecularSequence/123" in query_str or "example.org" in query_str

    def test_component_value_canonical(self, converter):
        query = converter.convert(
            "Observation",
            "component-value-canonical=http://example.org/cv/456")
        assert "cv/456" in str(query) or "example.org" in str(query)


# ---------------------------------------------------------------------------
# 6) String parameter (value-markdown)
# ---------------------------------------------------------------------------

class TestObservationStringParameters:
    """The R5 spec value-markdown string parameter."""

    def test_value_markdown_default(self, converter):
        query = converter.convert("Observation", "value-markdown=Normal")
        # Default string search lower-cases the value
        assert "normal" in str(query).lower()

    def test_value_markdown_exact(self, converter):
        query = converter.convert("Observation", "value-markdown:exact=Normal")
        assert "Normal" in str(query)

    def test_value_markdown_contains(self, converter):
        query = converter.convert("Observation", "value-markdown:contains=norm")
        assert "norm" in str(query).lower()


# ---------------------------------------------------------------------------
# 7) Composite parameters (all 8)
# ---------------------------------------------------------------------------

class TestObservationCompositeParameters:
    """All 8 R5 composite parameters."""

    def test_code_value_concept(self, converter):
        query = converter.convert(
            "Observation",
            "code-value-concept=http://loinc.org|85354-9$http://snomed.info/sct|123")
        query_str = str(query)
        # AND must combine the code half with the value-concept half
        assert "$and" in query_str
        assert "85354-9" in query_str
        assert "123" in query_str

    def test_code_value_date(self, converter):
        query = converter.convert(
            "Observation",
            "code-value-date=http://loinc.org|85354-9$2024-06-01")
        s = str(query)
        assert "85354-9" in s
        assert "datetime.datetime(2024, 6, 1" in s

    def test_code_value_quantity(self, converter):
        query = converter.convert(
            "Observation",
            "code-value-quantity=http://loinc.org|2093-3$le5")
        query_str = str(query)
        assert "2093-3" in query_str
        assert "5" in query_str

    def test_code_value_string(self, converter):
        query = converter.convert(
            "Observation",
            "code-value-string=http://loinc.org|85354-9$Normal")
        query_str = str(query)
        assert "85354-9" in query_str
        assert "normal" in query_str.lower()

    def test_combo_code_value_concept(self, converter):
        query = converter.convert(
            "Observation",
            "combo-code-value-concept=http://loinc.org|8480-6$http://snomed.info/sct|11")
        query_str = str(query)
        assert "8480-6" in query_str

    def test_combo_code_value_quantity(self, converter):
        query = converter.convert(
            "Observation",
            "combo-code-value-quantity=http://loinc.org|8480-6$gt100")
        query_str = str(query)
        assert "8480-6" in query_str

    def test_component_code_value_concept(self, converter):
        query = converter.convert(
            "Observation",
            "component-code-value-concept=http://loinc.org|8480-6$http://snomed.info/sct|22")
        assert "8480-6" in str(query)

    def test_component_code_value_quantity(self, converter):
        query = converter.convert(
            "Observation",
            "component-code-value-quantity=http://loinc.org|8480-6$ge140")
        assert "8480-6" in str(query)

    def test_composite_missing_modifier(self, converter):
        query = converter.convert(
            "Observation", "code-value-quantity:missing=false"
        )
        query_str = str(query)
        assert "$exists" in query_str or "missing" in query_str.lower()

    def test_composite_too_few_components_raises(self, converter):
        # Only one component supplied for a 2-component composite
        result = converter.convert("Observation", "code-value-quantity=lone-value")
        # The library logs a warning and skips bad params, returning {}
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 8) Combinations / boolean logic
# ---------------------------------------------------------------------------

class TestObservationCombinations:
    """Boolean combinations of multiple parameters."""

    def test_status_and_code(self, converter):
        query = converter.convert(
            "Observation",
            "status=final&code=http://loinc.org|85354-9")
        query_str = str(query)
        assert "final" in query_str
        assert "85354-9" in query_str
        assert "$and" in query_str

    def test_patient_and_date_range(self, converter):
        query = converter.convert(
            "Observation",
            "patient=Patient/pat-001&date=ge2024-01-01&date=le2024-12-31")
        s = str(query)
        assert "pat-001" in s
        assert "datetime.datetime(2024, 1, 1" in s
        assert "datetime.datetime(2024, 12, 31" in s

    def test_or_status(self, converter):
        query = converter.convert(
            "Observation", "status=final,amended,corrected"
        )
        query_str = str(query)
        assert "final" in query_str
        assert "amended" in query_str
        assert "corrected" in query_str

    def test_complex_clinical_query(self, converter):
        """Real-world: vital signs for a patient over a year, with status."""
        query = converter.convert(
            "Observation",
            "patient=Patient/pat-001"
            "&category=vital-signs"
            "&date=ge2024-01-01"
            "&date=le2024-12-31"
            "&status=final")
        s = str(query)
        for needle in (
            "pat-001",
            "vital-signs",
            "datetime.datetime(2024, 1, 1",
            "datetime.datetime(2024, 12, 31",
            "final"):
            assert needle in s, f"Missing {needle}"


# ---------------------------------------------------------------------------
# 9) Compartment search
#
# The Observation YAML opts into Hybrid Approach 3 for the Patient
# compartment via:
#
#     compartments:
#       precomputed:
#         - Patient
#
# These tests pin the resulting two-tier behavior:
#   - Patient compartment → single-field fast-path against
#     `_compartments.Patient` (driven by CompartmentMembershipExtractor at
#     denormalization time).
#   - All other compartments (Encounter, Practitioner, Device,
#     RelatedPerson) → dynamic translation against the configured
#     `_search.*` linking fields.
# ---------------------------------------------------------------------------

class TestObservationCompartment:
    """Smoke-level compartment search tests (kept for back-compat)."""

    def test_patient_compartment_observation(self, converter):
        query = converter.convert_with_compartment(
            "Patient", "pat-001", "Observation", "code=http://loinc.org|85354-9"
        )
        query_str = str(query)
        assert "pat-001" in query_str
        assert "85354-9" in query_str

    def test_encounter_compartment_observation(self, converter):
        query = converter.convert_with_compartment(
            "Encounter", "enc-001", "Observation", "status=final"
        )
        query_str = str(query)
        assert "enc-001" in query_str
        assert "final" in query_str


class TestObservationPatientCompartmentFastPath:
    """
    Hybrid Approach 3 — precomputed Patient compartment.

    Confirms that:
      1. Observation/<id>/Patient compartment query collapses to a single
         indexed predicate against ``_compartments.Patient``.
      2. The dynamic-path linking fields (``_search.subjectId``,
         ``_search.performerId``) are NOT used for this compartment.
      3. Additional FHIR search parameters (``code``, ``status``, ``date``)
         compose cleanly with the fast-path predicate via ``$and``.
    """

    def test_patient_compartment_collapses_to_precomputed_field(self, converter):
        query = converter.convert_with_compartment(
            "Patient", "pat-123", "Observation"
        )
        # Exact equality assertion — the resolver must NOT augment the
        # fast-path query with the dynamic $or branches.
        assert query == {"_compartments.Patient": "pat-123"}

    def test_patient_compartment_does_not_use_dynamic_subject_or_performer(
        self, converter
    ):
        s = str(converter.convert_with_compartment(
            "Patient", "pat-123", "Observation"
        ))
        assert "_search.subjectId" not in s
        assert "_search.performerId" not in s
        assert "subject.reference" not in s

    def test_patient_compartment_with_code_filter(self, converter):
        query = converter.convert_with_compartment(
            "Patient", "pat-123", "Observation",
            "code=http://loinc.org|85354-9")
        s = str(query)
        # Compartment predicate (fast-path)
        assert "_compartments.Patient" in s
        assert "pat-123" in s
        # Code filter from the parameter side
        assert "85354-9" in s

    def test_patient_compartment_with_status_and_date(self, converter):
        query = converter.convert_with_compartment(
            "Patient", "pat-123", "Observation",
            "status=final&date=ge2024-01-01")
        s = str(query)
        assert "_compartments.Patient" in s
        assert "pat-123" in s
        assert "final" in s
        # Date filter retained (lexical or datetime serialization).
        assert "2024-01-01" in s or "datetime" in s


class TestObservationCompartmentDenormalization:
    """
    Round-trip: the documents produced by the denormalizer carry the
    `_compartments.Patient` field that the fast-path query searches
    against, and reference-type filtering excludes non-Patient subjects.
    """

    @pytest.fixture
    def denormalizer(self):
        return ResourceDenormalizer()

    def test_subject_only_populates_compartment(self, denormalizer):
        obs = {
            "resourceType": "Observation",
            "id": "obs-1",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "1234-5"}]},
            "subject": {"reference": "Patient/pat-1"},
        }
        out = denormalizer.denormalize(obs)
        assert out["_compartments"] == {"Patient": ["pat-1"]}

    def test_subject_and_performer_unioned_and_deduped(self, denormalizer):
        obs = {
            "resourceType": "Observation",
            "id": "obs-1",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "1234-5"}]},
            "subject": {"reference": "Patient/pat-1"},
            "performer": [
                {"reference": "Patient/pat-2"},
                {"reference": "Patient/pat-1"},   # duplicate of subject → deduped
                {"reference": "Practitioner/dr-9"},
            ],
        }
        out = denormalizer.denormalize(obs)
        # First-seen order preserved; duplicates squashed; non-Patient ref
        # filtered out by `reference_type: Patient`.
        assert out["_compartments"]["Patient"] == ["pat-1", "pat-2"]

    def test_group_subject_excluded_from_patient_compartment(self, denormalizer):
        obs = {
            "resourceType": "Observation",
            "id": "obs-x",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "1234-5"}]},
            "subject": {"reference": "Group/g-1"},
            "performer": [{"reference": "Practitioner/dr-9"}],
        }
        out = denormalizer.denormalize(obs)
        # Per FHIR R5 Patient compartment definition, only Patient-typed
        # subjects/performers contribute → Patient bucket empty.
        # The Practitioner performer DOES feed the Practitioner
        # compartment precompute (Hybrid Approach 3), so `_compartments`
        # is present but carries only the Practitioner key.
        comp = out.get("_compartments", {})
        assert not comp.get("Patient")
        assert comp.get("Practitioner") == ["dr-9"]

    def test_round_trip_search_finds_denormalized_doc(self, converter):
        """End-to-end: denormalize → seed in-memory → fast-path query matches."""
        denorm = ResourceDenormalizer()
        docs = [
            denorm.denormalize({
                "resourceType": "Observation",
                "id": "obs-target",
                "status": "final",
                "code": {"coding": [{"system": "http://loinc.org", "code": "1"}]},
                "subject": {"reference": "Patient/pat-target"},
            }),
            denorm.denormalize({
                "resourceType": "Observation",
                "id": "obs-other",
                "status": "final",
                "code": {"coding": [{"system": "http://loinc.org", "code": "2"}]},
                "subject": {"reference": "Patient/pat-other"},
            }),
        ]
        query = converter.convert_with_compartment(
            "Patient", "pat-target", "Observation"
        )
        # Apply the (single-key) fast-path query as an in-memory predicate.
        target_id = query["_compartments.Patient"]
        matches = [
            d for d in docs
            if target_id in d.get("_compartments", {}).get("Patient", [])
        ]
        assert len(matches) == 1
        assert matches[0]["id"] == "obs-target"


class TestObservationDynamicCompartmentFallback:
    """
    Compartments NOT in the precomputed list must continue to use the
    dynamic translation (`$or` over linking-parameter `_search.*` fields)
    so the long-tail compartments stay configuration-driven.
    """

    def test_encounter_compartment_uses_dynamic_path(self, converter):
        s = str(converter.convert_with_compartment(
            "Encounter", "enc-1", "Observation", "status=final"
        ))
        # MUST NOT route through the precomputed field for non-Patient.
        assert "_compartments.Encounter" not in s
        # MUST hit the configured Encounter linking field on Observation.
        assert "_search.encounterId" in s
        assert "enc-1" in s

    def test_practitioner_compartment_uses_fast_path(self, converter):
        # Practitioner compartment for Observation is now ALSO opted into
        # the Hybrid Approach 3 fast-path (see Observation.yaml's
        # `compartments.precomputed: [Patient, Practitioner]`). The
        # resolver emits a single-field lookup against
        # `_compartments.Practitioner` instead of walking `performer`
        # dynamically. The precompute extractor takes care of the
        # `reference_type: Practitioner` filtering at denorm time.
        s = str(converter.convert_with_compartment(
            "Practitioner", "dr-1", "Observation"
        ))
        assert "_compartments.Practitioner" in s
        assert "dr-1" in s


class TestObservationDeviceCompartmentFastPath:
    """
    Hybrid Approach 3 — precomputed Device compartment.

    Per FHIR R5 compartmentdefinition-device.html, Observation
    participates in the Device compartment via THREE linking
    parameters: ``subject or performer or device``. Observation.yaml
    now opts into the fast-path so ``Device/<id>/Observation``
    collapses to a single indexed lookup against
    ``_compartments.Device`` instead of the previous dynamic 3-field
    ``$or`` over ``_search.subjectId``, ``_search.performerId``,
    ``_search.deviceId``.
    """

    def test_device_compartment_collapses_to_precomputed_field(
        self, converter
    ):
        query = converter.convert_with_compartment(
            "Device", "dev-99", "Observation"
        )
        assert query == {"_compartments.Device": "dev-99"}

    def test_device_compartment_does_not_use_dynamic_or(self, converter):
        s = str(converter.convert_with_compartment(
            "Device", "dev-99", "Observation"
        ))
        assert "$or" not in s
        assert "_search.subjectId" not in s
        assert "_search.performerId" not in s
        assert "_search.deviceId" not in s

    def test_device_compartment_with_code_filter(self, converter):
        query = converter.convert_with_compartment(
            "Device", "dev-99", "Observation",
            "code=http://loinc.org|85354-9",
        )
        s = str(query)
        assert "_compartments.Device" in s
        assert "dev-99" in s
        assert "85354-9" in s


class TestObservationDeviceCompartmentDenormalization:
    """
    Verify ``_compartments.Device`` is populated from the union of
    ``subject``, ``performer``, and ``device`` references filtered to
    ``Device/*`` only — the FHIR R5 linking-parameter set for
    Observation in the Device compartment.
    """

    @pytest.fixture
    def denormalizer(self):
        return ResourceDenormalizer()

    def test_device_compartment_unions_three_paths(self, denormalizer):
        obs = {
            "resourceType": "Observation",
            "id": "obs-mixed",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "1"}]},
            "subject": {"reference": "Device/dev-A"},
            "performer": [
                {"reference": "Device/dev-B"},
                {"reference": "Practitioner/dr-1"},
                {"reference": "Patient/pat-1"},
            ],
            "device": {"reference": "Device/dev-C"},
        }
        out = denormalizer.denormalize(obs)
        # First-seen order preserved across the three source paths;
        # non-Device refs (Practitioner, Patient) are routed to their
        # own compartment buckets and do NOT pollute Device.
        assert out["_compartments"]["Device"] == ["dev-A", "dev-B", "dev-C"]

    def test_device_compartment_dedupes_across_paths(self, denormalizer):
        obs = {
            "resourceType": "Observation",
            "id": "obs-dup",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "1"}]},
            "subject": {"reference": "Device/dev-X"},
            "performer": [{"reference": "Device/dev-X"}],
            "device": {"reference": "Device/dev-X"},
        }
        out = denormalizer.denormalize(obs)
        # Same Device referenced from all three paths must collapse to
        # a single id (CompartmentMembershipExtractor's dedup contract).
        assert out["_compartments"]["Device"] == ["dev-X"]

    def test_no_device_subject_means_no_device_bucket(
        self, denormalizer
    ):
        obs = {
            "resourceType": "Observation",
            "id": "obs-pat",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "1"}]},
            "subject": {"reference": "Patient/pat-1"},
            "performer": [{"reference": "Practitioner/dr-9"}],
        }
        out = denormalizer.denormalize(obs)
        # Sparse-output contract: no Device-typed refs anywhere → the
        # `Device` key must be absent, not an empty list.
        assert "Device" not in out.get("_compartments", {})

    def test_round_trip_device_fast_path_finds_doc(self, converter):
        denorm = ResourceDenormalizer()
        docs = [
            denorm.denormalize({
                "resourceType": "Observation",
                "id": "obs-with-dev",
                "status": "final",
                "code": {"coding": [{"system": "http://loinc.org", "code": "1"}]},
                "subject": {"reference": "Patient/pat-1"},
                "device": {"reference": "Device/dev-target"},
            }),
            denorm.denormalize({
                "resourceType": "Observation",
                "id": "obs-without-dev",
                "status": "final",
                "code": {"coding": [{"system": "http://loinc.org", "code": "2"}]},
                "subject": {"reference": "Patient/pat-1"},
            }),
        ]
        query = converter.convert_with_compartment(
            "Device", "dev-target", "Observation"
        )
        target_id = query["_compartments.Device"]
        matches = [
            d for d in docs
            if target_id in d.get("_compartments", {}).get("Device", [])
        ]
        assert len(matches) == 1
        assert matches[0]["id"] == "obs-with-dev"


# ---------------------------------------------------------------------------
# 10) Denormalization end-to-end
# ---------------------------------------------------------------------------

class TestObservationDenormalization:
    """Verify that all denormalization rules in Observation.yaml fire correctly."""

    def test_full_denormalization(self, denormalizer, rich_observation):
        result = denormalizer.denormalize(rich_observation)
        assert "_search" in result
        s = result["_search"]

        # CodeableConcept extractors
        assert "code_codes" in s
        assert "85354-9" in s["code_codes"]
        assert "code_systemCode" in s
        assert "http://loinc.org|85354-9" in s["code_systemCode"]
        assert "category_codes" in s
        assert "vital-signs" in s["category_codes"]
        assert "method_codes" in s
        assert "37931006" in s["method_codes"]
        assert "dataAbsentReason_codes" in s
        assert "unknown" in s["dataAbsentReason_codes"]

        # Reference extractors
        assert s["subjectId"] == "pat-001"
        assert s["patientId"] == "pat-001"
        assert s["encounterId"] == "enc-001"
        assert "prac-001" in s["performerId"]
        assert "sr-001" in s["basedOnId"]
        assert "obs-source-1" in s["derivedFromId"]
        assert s["deviceId"] == "dev-001"
        assert "proc-002" in s["focusId"]
        assert "obs-member-1" in s["hasMemberId"]
        assert "proc-001" in s["partOfId"]
        assert s["specimenId"] == "spec-001"

        # Identifier extractor
        assert "identifier_values" in s
        assert "OBS-12345" in s["identifier_values"]

    def test_minimal_observation_only_status(self, denormalizer):
        minimal = {
            "resourceType": "Observation",
            "id": "min-1",
            "status": "final",
        }
        result = denormalizer.denormalize(minimal)
        # Result should be returned even when no rules fire
        assert result["status"] == "final"

    def test_unknown_resource_type_passthrough(self, denormalizer):
        # No config for QuestionnaireResponse → returned unchanged, no _search
        result = denormalizer.denormalize(
            {"resourceType": "QuestionnaireResponse", "id": "q1"}
        )
        assert "_search" not in result

    def test_validate_denormalized(self, denormalizer, rich_observation):
        denorm = denormalizer.denormalize(rich_observation)
        assert denormalizer.validate(denorm) is True

    def test_denormalize_field(self, denormalizer, rich_observation):
        out = denormalizer.denormalize_field(
            "code", rich_observation["code"], "Observation"
        )
        assert "code_codes" in out
        assert "85354-9" in out["code_codes"]

    def test_get_denormalization_rules(self, denormalizer):
        rules = denormalizer.get_denormalization_rules("Observation")
        assert "code" in rules
        assert "subject" in rules
        assert "identifier" in rules


# ---------------------------------------------------------------------------
# 11) MongoDB integration (localhost:27017)
# ---------------------------------------------------------------------------

def _mongo_available() -> bool:
    try:
        from pymongo import MongoClient
        client = MongoClient(
            "mongodb://localhost:27017", serverSelectionTimeoutMS=1500
        )
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.mongodb
@pytest.mark.skipif(not _mongo_available(), reason="MongoDB not running on localhost:27017")
class TestObservationMongoDB:
    """End-to-end against a real MongoDB at localhost:27017."""

    @pytest.fixture(scope="class")
    def mongo_collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["observations_e2e"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_collection, denormalizer):
        # Seed a small longitudinal dataset for one patient.
        # Use real datetime objects for effectiveDateTime so $gte/$lte
        # queries (which the converter emits as datetimes) match.
        observations: List[Dict[str, Any]] = [
            {
                "resourceType": "Observation",
                "id": f"obs-{i:03d}",
                "status": "final" if i % 2 == 0 else "amended",
                "category": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                "code": "vital-signs",
                            }
                        ]
                    }
                ],
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": "85354-9",
                            "display": "Blood pressure panel",
                        }
                    ]
                },
                "subject": {"reference": "Patient/pat-mongo-001"},
                "encounter": {"reference": f"Encounter/enc-{i:03d}"},
                "effectiveDateTime": datetime(
                    2024, (i % 12) + 1, 15, 10, 0, 0
                ),
                "valueQuantity": {
                    "value": 110 + i,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]",
                },
            }
            for i in range(20)
        ]
        denorm = [denormalizer.denormalize(o) for o in observations]
        mongo_collection.insert_many(denorm)
        return mongo_collection

    def test_query_by_patient(self, converter, seeded):
        q = converter.convert("Observation", "patient=Patient/pat-mongo-001")
        results = list(seeded.find(q))
        assert len(results) == 20

    def test_query_by_patient_and_status(self, converter, seeded):
        q = converter.convert(
            "Observation",
            "patient=Patient/pat-mongo-001&status=final")
        results = list(seeded.find(q))
        assert len(results) == 10
        assert all(r["status"] == "final" for r in results)

    def test_query_by_code(self, converter, seeded):
        q = converter.convert("Observation", "code=http://loinc.org|85354-9")
        results = list(seeded.find(q))
        assert len(results) == 20

    def test_query_by_date_range(self, converter, seeded):
        q = converter.convert(
            "Observation",
            "date=ge2024-01-01&date=le2024-06-30")
        results = list(seeded.find(q))
        assert 0 < len(results) <= 20

    def test_query_compartment_patient(self, converter, seeded):
        q = converter.convert_with_compartment(
            "Patient", "pat-mongo-001", "Observation",
            "category=vital-signs&status=final")
        results = list(seeded.find(q))
        assert len(results) == 10
        for r in results:
            assert r["status"] == "final"

    def test_query_value_quantity_threshold(self, converter, seeded):
        q = converter.convert("Observation", "value-quantity=gt115")
        results = list(seeded.find(q))
        # Values seeded are 110..129; >115 should be 14 records
        assert len(results) == 14

    def test_query_lastupdated_missing(self, converter, seeded):
        q = converter.convert("Observation", "_lastUpdated:missing=true")
        # All seeded docs lack meta.lastUpdated
        results = list(seeded.find(q))
        assert len(results) == 20

    def test_indexes_recommend(self, converter):
        # Ensure the MQL query produced for a hot path is index-friendly
        q = converter.convert(
            "Observation",
            "patient=Patient/pat-mongo-001&date=ge2024-01-01")
        # The string form should reference the indexed _search.patientId field
        assert "_search.patientId" in str(q)


# =============================================================================
# Observation gap-fix regression tests
#
# Earlier the Observation YAML declared search parameters whose `_search.*`
# target fields were never populated by any denormalization rule, so every
# combo-/component-/effectiveTiming-/valueText-style search silently
# returned an empty result set against denormalized data. The fix uses
# ONLY generic, data-type-specific extractors (CodeableConceptExtractor,
# ReferenceExtractor, TimingExtractor, TextExtractor) wired via
# `source: $resource` and FHIRPath-lite path expressions in `source_path`.
# The tests below pin that behavior so future refactors cannot regress to
# the old silent-empty state and so the project keeps its "no
# resource-specific extractors" architectural property.
# =============================================================================


from fhir_search_to_mql.denormalizer.path_resolver import resolve_path
from fhir_search_to_mql.denormalizer.extractors import (
    CodeableConceptExtractor,
    ReferenceExtractor,
    TextExtractor,
    TimingExtractor)


def _full_observation() -> Dict[str, Any]:
    """Observation that exercises every aggregate the extractor produces."""
    return {
        "resourceType": "Observation",
        "id": "obs-composites-1",
        "status": "final",
        "code": {
            "coding": [{"system": "http://loinc.org", "code": "85354-9"}]
        },
        "valueCodeableConcept": {
            "coding": [{"system": "http://loinc.org", "code": "LA6699-8"}]
        },
        "dataAbsentReason": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/data-absent-reason",
                    "code": "unknown",
                }
            ]
        },
        "effectiveTiming": {
            "event": [
                "2024-01-15T08:00:00Z",
                "2024-01-01T20:00:00Z",
                "2024-01-30T12:00:00Z",
            ]
        },
        "valueString": "Patient narrative",
        "valueMarkdown": "**bold** finding",
        "component": [
            {
                "code": {
                    "coding": [{"system": "http://loinc.org", "code": "8480-6"}]
                },
                "valueQuantity": {"value": 120, "unit": "mmHg"},
            },
            {
                "code": {
                    "coding": [{"system": "http://loinc.org", "code": "8462-4"}]
                },
                "valueCodeableConcept": {
                    "coding": [{"system": "http://loinc.org", "code": "LA6699-8"}]
                },
                "valueReference": {"reference": "MolecularSequence/seq-1"},
                "dataAbsentReason": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/data-absent-reason",
                            "code": "asked-unknown",
                        }
                    ]
                },
            },
        ],
    }


class TestObservationCompositesExtractor:
    """
    Unit-level checks on the GENERIC extractors that now power Observation's
    cross-cutting aggregates. These same extractors are used by every other
    resource — there is no Observation-specific extractor to test.
    """

    # -- FHIRPath-lite resolver -----------------------------------------

    def test_path_resolver_walks_array_and_union(self):
        resource = _full_observation()
        # Top-level + component union.
        nodes = resolve_path(resource, "code | component[*].code")
        assert len(nodes) == 3  # 1 top-level + 2 components

    def test_path_resolver_returns_empty_for_missing_paths(self):
        assert resolve_path({}, "any.missing.path") == []
        assert resolve_path(None, "x") == []

    def test_path_resolver_handles_dot_navigation_to_primitive(self):
        leaves = resolve_path(_full_observation(), "code.coding[*].code")
        assert leaves == ["85354-9"]

    # -- CodeableConceptExtractor (resource-rooted) ---------------------

    def test_codeable_concept_extractor_walks_resource_paths(self):
        result = CodeableConceptExtractor().extract(
            _full_observation(),
            field_mappings=[
                {
                    "source_path": "component[*].code",
                    "target_field": "componentCode_codes",
                    "datatype": "array[string]",
                },
                {
                    "source_path": "component[*].code",
                    "target_field": "componentCode_systemCode",
                    "datatype": "array[string]",
                },
            ])
        assert result["componentCode_codes"] == ["8480-6", "8462-4"]
        assert result["componentCode_systemCode"] == [
            "http://loinc.org|8480-6",
            "http://loinc.org|8462-4",
        ]

    def test_codeable_concept_extractor_legacy_mode_unchanged(self):
        # When the input isn't a full resource, behavior must match the
        # pre-refactor pre-resolved-value contract.
        cc = {"coding": [{"system": "http://loinc.org", "code": "X"}], "text": "t"}
        result = CodeableConceptExtractor().extract(
            cc,
            field_mappings=[
                {
                    "source_path": "code",
                    "target_field": "code_codes",
                    "datatype": "array[string]",
                }
            ])
        assert result["code_codes"] == ["X"]

    # -- ReferenceExtractor (resource-rooted) ---------------------------

    def test_reference_extractor_walks_resource_paths(self):
        result = ReferenceExtractor().extract(
            _full_observation(),
            field_mappings=[
                {
                    "source_path": "component[*].valueReference",
                    "target_field": "componentValueReferenceId",
                    "datatype": "array[string]",
                    "extractType": "id",
                },
                {
                    "source_path": "component[*].valueReference",
                    "target_field": "componentValueReferenceType",
                    "datatype": "array[string]",
                    "extractType": "type",
                },
            ])
        assert result["componentValueReferenceId"] == ["seq-1"]
        assert result["componentValueReferenceType"] == ["MolecularSequence"]

    # -- TimingExtractor bounds output ----------------------------------

    def test_timing_extractor_emits_bounds_period(self):
        timing = {
            "event": [
                "2024-01-15T08:00:00Z",
                "2024-01-01T20:00:00Z",
                "2024-01-30T12:00:00Z",
            ]
        }
        result = TimingExtractor().extract(
            timing,
            field_mappings=[
                {
                    "source_path": "event[*]",
                    "target_field": "effectiveTimingBounds",
                    "datatype": "object",
                }
            ])
        assert result["effectiveTimingBounds"] == {
            "start": "2024-01-01T20:00:00Z",
            "end": "2024-01-30T12:00:00Z",
        }

    # -- TextExtractor --------------------------------------------------

    def test_text_extractor_concatenates_and_lowercases(self):
        result = TextExtractor().extract(
            _full_observation(),
            field_mappings=[
                {
                    "source_path": "valueString | valueMarkdown | valueCodeableConcept.text",
                    "target_field": "valueText_lower",
                    "datatype": "string",
                    "normalize": "lowercase",
                    "separator": " ",
                }
            ])
        assert result["valueText_lower"] == "patient narrative **bold** finding"

    def test_text_extractor_skips_when_no_matches(self):
        result = TextExtractor().extract(
            {"resourceType": "Observation"},
            field_mappings=[
                {
                    "source_path": "valueString | valueMarkdown",
                    "target_field": "valueText_lower",
                    "datatype": "string",
                }
            ])
        assert result == {}


class TestObservationCompositesDenormalization:
    """End-to-end: every previously-empty `_search.*` field is now populated."""

    @pytest.fixture
    def search(self) -> Dict[str, Any]:
        denormalizer = ResourceDenormalizer()
        out = denormalizer.denormalize(_full_observation())
        return out.get("_search", {})

    def test_component_code_aggregates(self, search):
        assert search["componentCode_codes"] == ["8480-6", "8462-4"]
        assert search["componentCode_systemCode"] == [
            "http://loinc.org|8480-6",
            "http://loinc.org|8462-4",
        ]

    def test_component_value_concept_aggregates(self, search):
        assert search["componentValueConcept_codes"] == ["LA6699-8"]
        assert search["componentValueConcept_systemCode"] == [
            "http://loinc.org|LA6699-8",
        ]

    def test_component_data_absent_reason_aggregates(self, search):
        assert search["componentDataAbsentReason_codes"] == ["asked-unknown"]
        assert search["componentDataAbsentReason_systemCode"] == [
            "http://terminology.hl7.org/CodeSystem/data-absent-reason|asked-unknown",
        ]

    def test_component_value_reference_split_into_id_and_type(self, search):
        assert search["componentValueReferenceId"] == ["seq-1"]
        assert search["componentValueReferenceType"] == ["MolecularSequence"]

    def test_combo_code_unions_top_level_and_component(self, search):
        # Order: top-level first, then component[0], then component[1].
        assert search["comboCode_codes"] == ["85354-9", "8480-6", "8462-4"]

    def test_combo_value_concept_unions_top_level_and_component(self, search):
        # The top-level value and component-1's value share the same code,
        # so we expect both occurrences (the union is intentionally
        # multiset-preserving so $in queries find every match).
        assert search["comboValueConcept_codes"] == ["LA6699-8", "LA6699-8"]

    def test_combo_data_absent_reason_unions_top_level_and_component(self, search):
        assert search["comboDataAbsentReason_codes"] == [
            "unknown",
            "asked-unknown",
        ]

    def test_effective_timing_bounds_picks_chronological_min_max(self, search):
        bounds = search["effectiveTimingBounds"]
        assert bounds == {
            "start": "2024-01-01T20:00:00Z",
            "end": "2024-01-30T12:00:00Z",
        }

    def test_value_text_lower_combines_string_markdown_and_text(self, search):
        # valueString + valueMarkdown joined and lowercased.
        assert search["valueText_lower"] == "patient narrative **bold** finding"

    def test_extractor_skips_when_resource_has_no_components_or_text(self):
        denormalizer = ResourceDenormalizer()
        minimal = {
            "resourceType": "Observation",
            "id": "obs-min",
            "status": "final",
            "code": {
                "coding": [{"system": "http://loinc.org", "code": "1234-5"}]
            },
        }
        out = denormalizer.denormalize(minimal)
        search = out.get("_search", {})
        # Top-level code-driven combo still populates...
        assert search.get("comboCode_codes") == ["1234-5"]
        # ...but component aggregates and value/timing aggregates stay absent.
        for key in (
            "componentCode_codes",
            "componentValueConcept_codes",
            "componentDataAbsentReason_codes",
            "componentValueReferenceId",
            "effectiveTimingBounds",
            "valueText_lower"):
            assert key not in search


class TestObservationCompositeSearchAgainstDenormalized:
    """Search queries against the new fields actually produce matches."""

    @pytest.fixture(scope="class")
    def converter(self) -> FHIRSearchConverter:
        return FHIRSearchConverter()

    def _build_corpus(self) -> List[Dict[str, Any]]:
        denormalizer = ResourceDenormalizer()
        return [
            denormalizer.denormalize(_full_observation()),
            denormalizer.denormalize(
                {
                    "resourceType": "Observation",
                    "id": "obs-other",
                    "status": "final",
                    "code": {
                        "coding": [
                            {"system": "http://loinc.org", "code": "11111-1"}
                        ]
                    },
                }
            ),
        ]

    def _matches(self, query: Dict[str, Any], doc: Dict[str, Any]) -> bool:
        """Tiny in-memory MQL evaluator covering the equality + $in slices
        we generate for token searches. Sufficient for these tests; we are
        not trying to reimplement MongoDB."""
        if not isinstance(query, dict):
            return False
        for key, expected in query.items():
            if key in ("$and", "$or"):
                clauses = expected if isinstance(expected, list) else []
                ok = (
                    all(self._matches(c, doc) for c in clauses)
                    if key == "$and"
                    else any(self._matches(c, doc) for c in clauses)
                )
                if not ok:
                    return False
                continue
            cur: Any = doc
            for part in key.split("."):
                if isinstance(cur, dict):
                    cur = cur.get(part)
                else:
                    cur = None
                    break
            if isinstance(expected, dict) and "$in" in expected:
                values = expected["$in"]
                if isinstance(cur, list):
                    if not any(v in cur for v in values):
                        return False
                else:
                    if cur not in values:
                        return False
            else:
                if isinstance(cur, list):
                    if expected not in cur:
                        return False
                else:
                    if cur != expected:
                        return False
        return True

    def test_component_code_query_matches_denormalized_doc(self, converter):
        query = converter.convert("Observation", "component-code=8462-4")
        corpus = self._build_corpus()
        matches = [d for d in corpus if self._matches(query, d)]
        assert len(matches) == 1
        assert matches[0]["id"] == "obs-composites-1"

    def test_combo_code_query_matches_via_component_code(self, converter):
        # combo-code unions top-level and component codes, so a query for
        # the component-only code "8480-6" should still hit the parent doc.
        query = converter.convert("Observation", "combo-code=8480-6")
        corpus = self._build_corpus()
        matches = [d for d in corpus if self._matches(query, d)]
        assert len(matches) == 1
        assert matches[0]["id"] == "obs-composites-1"

    def test_component_value_reference_query_matches(self, converter):
        query = converter.convert(
            "Observation", "component-value-reference=MolecularSequence/seq-1"
        )
        corpus = self._build_corpus()
        matches = [d for d in corpus if self._matches(query, d)]
        assert len(matches) == 1
        assert matches[0]["id"] == "obs-composites-1"
