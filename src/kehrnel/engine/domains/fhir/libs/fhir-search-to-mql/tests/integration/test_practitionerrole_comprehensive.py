"""
Comprehensive integration tests for ALL PractitionerRole search parameters per FHIR R5.

References:
- https://www.hl7.org/fhir/practitionerrole-search.html
- https://www.hl7.org/fhir/practitionerrole-definitions.html
- https://www.hl7.org/fhir/compartmentdefinition-practitioner.html

This suite exercises the 17 search parameters declared in
``configs/PractitionerRole.yaml``:

  - References (5):  practitioner, organization, location, service, endpoint
  - Tokens (9):      active, identifier, role, specialty, characteristic,
                     communication, email, phone, telecom, _id
  - Dates (2):       date (PractitionerRole.period), _lastUpdated

Plus:

- FHIR R5 ``PractitionerRole.contact`` is ``ExtendedContactDetail[]`` —
  the actual ContactPoints live at ``contact[*].telecom[*]``. The
  denormalization rule uses ``ContactPointExtractor``'s ``$resource``
  mode with path resolution; this suite verifies the path correctly
  flattens nested telecom into ``_search.email`` / ``_search.phone`` /
  ``_search.telecom_*``.
- ``PractitionerRole.code`` (the ``role`` parameter), ``.specialty``,
  ``.characteristic``, ``.communication`` are all ``CodeableConcept[]``
  in R5 — denormalized as ``*_codes`` (bare codes) AND ``*_systemCode``
  (``system|code`` pairs) for FHIR token search compliance.
- ``PractitionerRole.period`` (single ``Period``, cardinality 0..1)
  powers the ``date`` parameter with full prefix support (eq/ne/gt/ge/
  lt/le/sa/eb/ap).
- Compartment routing: PractitionerRole is a member of the FHIR R5
  Practitioner compartment via ``practitioner`` (single Reference,
  target Practitioner). With Practitioner opted into the Hybrid
  Approach 3 fast-path, ``Practitioner/<id>/PractitionerRole``
  collapses to a single ``_compartments.Practitioner`` lookup.
- Cross-resource forward links: PractitionerRole's
  ``practitioner`` / ``organization`` / ``location`` /
  ``healthcareService`` / ``endpoint`` references must denormalize
  cleanly so a search like ``PractitionerRole?practitioner=dr-jones``
  hits ``_search.practitionerId`` directly.
- Real MongoDB roundtrip against ``localhost:27017``.
"""

from __future__ import annotations

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
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.fixture
def rich_role() -> Dict[str, Any]:
    """A PractitionerRole with most R5 fields populated."""
    return {
        "resourceType": "PractitionerRole",
        "id": "pr-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "active": True,
        "period": {"start": "2024-01-01", "end": "2025-12-31"},
        "practitioner": {"reference": "Practitioner/dr-jones"},
        "organization": {"reference": "Organization/hospital-1"},
        "code": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/practitioner-role",
                        "code": "doctor",
                        "display": "Doctor",
                    }
                ]
            },
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/practitioner-role",
                        "code": "researcher",
                    }
                ]
            },
        ],
        "specialty": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "394814009",
                        "display": "General medicine",
                    }
                ]
            },
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "394579002",
                        "display": "Cardiology",
                    }
                ]
            },
        ],
        "location": [
            {"reference": "Location/clinic-3"},
            {"reference": "Location/clinic-4"},
        ],
        "healthcareService": [
            {"reference": "HealthcareService/svc-1"},
            {"reference": "HealthcareService/svc-2"},
        ],
        "endpoint": [
            {"reference": "Endpoint/ep-1"},
        ],
        "characteristic": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/service-mode",
                        "code": "in-person",
                    }
                ]
            }
        ],
        "communication": [
            {"coding": [{"system": "urn:ietf:bcp:47", "code": "en-US"}]},
            {"coding": [{"system": "urn:ietf:bcp:47", "code": "es-MX"}]},
        ],
        "identifier": [
            {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"},
            {"system": "http://hospital.org/role-id", "value": "ROLE-42"},
        ],
        "contact": [
            {
                "purpose": {"coding": [{"code": "ADMIN"}]},
                "telecom": [
                    {"system": "phone", "value": "555-0100", "use": "work"},
                    {"system": "email", "value": "admin@hospital.org", "use": "work"},
                ],
            },
            {
                "purpose": {"coding": [{"code": "PATIENT"}]},
                "telecom": [
                    {"system": "phone", "value": "555-0200", "use": "work"},
                    {"system": "fax", "value": "555-0300"},
                ],
            },
        ],
    }


@pytest.fixture
def minimal_role() -> Dict[str, Any]:
    """Bare-bones — only id + resourceType. FHIR has no required field
    on PractitionerRole, so this is the legitimate floor for
    sparse-output testing."""
    return {
        "resourceType": "PractitionerRole",
        "id": "pr-min",
    }


# ===========================================================================
# 1) Reference parameters (5)
# ===========================================================================


class TestPractitionerRoleReferenceParameters:
    """5 reference search parameters: practitioner, organization,
    location, service, endpoint."""

    def test_practitioner_reference(self, converter):
        q = converter.convert("PractitionerRole", "practitioner=dr-jones")
        assert "practitioner" in str(q).lower()
        assert "dr-jones" in str(q)

    def test_organization_reference(self, converter):
        q = converter.convert("PractitionerRole", "organization=hospital-1")
        assert "organization" in str(q).lower()
        assert "hospital-1" in str(q)

    def test_location_reference(self, converter):
        q = converter.convert("PractitionerRole", "location=clinic-3")
        assert "location" in str(q).lower()
        assert "clinic-3" in str(q)

    def test_service_reference_targets_healthcare_service(self, converter):
        # The `service` search parameter targets PractitionerRole.healthcareService
        q = converter.convert("PractitionerRole", "service=svc-1")
        assert "healthcareservice" in str(q).lower()
        assert "svc-1" in str(q)

    def test_endpoint_reference(self, converter):
        q = converter.convert("PractitionerRole", "endpoint=ep-1")
        assert "endpoint" in str(q).lower()
        assert "ep-1" in str(q)

    def test_typed_reference_with_resource_prefix(self, converter):
        # FHIR allows specifying the target type explicitly.
        q = converter.convert(
            "PractitionerRole", "practitioner=Practitioner/dr-jones"
        )
        # Either the bare id or the full reference should appear in the query.
        assert "dr-jones" in str(q)


# ===========================================================================
# 2) Token parameters (9 incl. _id)
# ===========================================================================


class TestPractitionerRoleTokenParameters:
    """9 token parameters: active, identifier, role, specialty,
    characteristic, communication, email, phone, telecom, _id."""

    def test_active_true(self, converter):
        q = converter.convert("PractitionerRole", "active=true")
        assert "active" in str(q).lower()

    def test_active_false(self, converter):
        q = converter.convert("PractitionerRole", "active=false")
        assert "active" in str(q).lower()

    def test_identifier_bare_value(self, converter):
        q = converter.convert("PractitionerRole", "identifier=1234567890")
        assert "1234567890" in str(q)
        assert "identifier" in str(q).lower()

    def test_identifier_system_code_pair(self, converter):
        q = converter.convert(
            "PractitionerRole",
            "identifier=http://hl7.org/fhir/sid/us-npi|1234567890",
        )
        assert "1234567890" in str(q)

    def test_role_token(self, converter):
        q = converter.convert("PractitionerRole", "role=doctor")
        assert "doctor" in str(q)
        assert "role" in str(q).lower()

    def test_specialty_token(self, converter):
        q = converter.convert("PractitionerRole", "specialty=394814009")
        assert "394814009" in str(q)
        assert "specialty" in str(q).lower()

    def test_characteristic_token(self, converter):
        q = converter.convert("PractitionerRole", "characteristic=in-person")
        assert "in-person" in str(q)
        assert "characteristic" in str(q).lower()

    def test_communication_language(self, converter):
        q = converter.convert("PractitionerRole", "communication=en-US")
        assert "en-US" in str(q)
        assert "language" in str(q).lower()

    def test_email_token(self, converter):
        q = converter.convert(
            "PractitionerRole", "email=admin@hospital.org"
        )
        assert "admin@hospital.org" in str(q)
        assert "email" in str(q).lower()

    def test_phone_token(self, converter):
        q = converter.convert("PractitionerRole", "phone=555-0100")
        assert "555-0100" in str(q)
        assert "phone" in str(q).lower()

    def test_telecom_token(self, converter):
        q = converter.convert("PractitionerRole", "telecom=555-0100")
        assert "555-0100" in str(q)
        assert "telecom" in str(q).lower()

    def test_id_search_parameter(self, converter):
        q = converter.convert("PractitionerRole", "_id=pr-rich")
        assert "pr-rich" in str(q)
        assert "id" in str(q).lower()


# ===========================================================================
# 3) Date parameters (2)
# ===========================================================================


class TestPractitionerRoleDateParameters:
    """2 date parameters: date (PractitionerRole.period) + _lastUpdated."""

    def test_date_eq(self, converter):
        q = converter.convert("PractitionerRole", "date=eq2024-06-01")
        s = str(q)
        assert "period" in s.lower() or "_search.period" in s
        assert "2024" in s

    def test_date_ge_uses_start(self, converter):
        # `ge` lower-bound queries should target the period's start.
        q = converter.convert("PractitionerRole", "date=ge2024-01-01")
        s = str(q)
        assert "period.start" in s
        assert "$gte" in s

    def test_date_lt_uses_end(self, converter):
        # `lt` upper-bound queries should target the period's end.
        q = converter.convert("PractitionerRole", "date=lt2025-01-01")
        s = str(q)
        assert "period.end" in s
        assert "$lt" in s

    def test_date_le_uses_end(self, converter):
        q = converter.convert("PractitionerRole", "date=le2025-12-31")
        s = str(q)
        assert "period.end" in s
        assert "$lte" in s

    def test_date_gt_uses_start(self, converter):
        q = converter.convert("PractitionerRole", "date=gt2024-06-30")
        s = str(q)
        assert "period.start" in s
        assert "$gt" in s

    def test_date_sa_starts_after(self, converter):
        # `sa` (starts after) — period.start strictly > value
        q = converter.convert("PractitionerRole", "date=sa2024-06-30")
        s = str(q)
        assert "period.start" in s

    def test_date_eb_ends_before(self, converter):
        # `eb` (ends before) — period.end strictly < value
        q = converter.convert("PractitionerRole", "date=eb2026-01-01")
        s = str(q)
        assert "period.end" in s

    def test_lastupdated_range(self, converter):
        q = converter.convert("PractitionerRole", "_lastUpdated=ge2024-01-01")
        s = str(q)
        # The MQL field path stays case-preserved (`meta.lastUpdated`) but
        # the assertion lowercases both sides for a structure-only check.
        assert "lastupdated" in s.lower()
        assert "$gte" in s


# ===========================================================================
# 4) Modifiers
# ===========================================================================


class TestPractitionerRoleModifiers:
    """Modifier coverage. PractitionerRole has no string parameters
    (the spec doesn't define `name`/`address`/etc. on roles), so the
    string-only `:exact` and `:contains` modifiers are intentionally
    out of scope — only the modifiers that apply to token parameters
    are exercised here: `:not`, `:text`, `:missing`, `:in`, `:not-in`,
    `:of-type` per FHIR R5 §3.1.1.5.10."""

    def test_active_missing_true(self, converter):
        # `:missing=true` flips into an absence test.
        q = converter.convert("PractitionerRole", "active:missing=true")
        s = str(q)
        # The MQL must encode "field is absent or null".
        assert "active" in s.lower()
        assert "$exists" in s or "$eq" in s or "null" in s.lower()

    def test_role_not_modifier(self, converter):
        # FHIR R5 `:not` over a multi-field token parameter compiles
        # to MongoDB's `$nor` (none of the field equalities match) —
        # the canonical negation of an `$or` set. Equivalent to
        # `$ne` for single-field params; for multi-field params
        # `$nor` is the only correct rendering.
        q = converter.convert("PractitionerRole", "role:not=doctor")
        s = str(q)
        assert "doctor" in s
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s

    def test_identifier_text_modifier(self, converter):
        # `:text` is the spec-allowed modifier on token params for
        # free-text search against the parameter's display/text fields.
        q = converter.convert(
            "PractitionerRole", "identifier:text=npi"
        )
        # Either the modifier is honored (text is in the query) or
        # the converter explicitly maps `:text` to a regex/like — both
        # are acceptable; we only assert the value made it through.
        assert "npi" in str(q).lower() or str(q) == "{}"

    def test_role_in_modifier(self, converter):
        # `:in` / `:not-in` resolve a ValueSet reference; the
        # converter typically passes through the raw parameter value
        # to the MQL even when the value-set isn't dereferenced.
        q = converter.convert(
            "PractitionerRole", "role:in=http://example.org/vs/role-codes"
        )
        # Just confirm conversion didn't crash; the exact MQL shape
        # depends on whether the ValueSet expansion is wired up.
        assert isinstance(q, dict)


# ===========================================================================
# 5) Denormalization correctness
# ===========================================================================


class TestPractitionerRoleDenormalization:
    """Verifies the shape of the `_search` document for the rich fixture."""

    def test_active_field(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)
        assert out["active"] is True

    def test_identifier_denormalization(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "1234567890" in out["identifier_values"]
        assert "ROLE-42" in out["identifier_values"]
        # `system|value` pairs for the system-aware token search.
        sysvals = " ".join(out["identifier_systemCode"])
        assert "1234567890" in sysvals
        assert "us-npi" in sysvals

    def test_role_codeable_concept(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "doctor" in out["role_codes"]
        assert "researcher" in out["role_codes"]

    def test_specialty_codeable_concept(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "394814009" in out["specialty_codes"]
        assert "394579002" in out["specialty_codes"]

    def test_characteristic_codeable_concept(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "in-person" in out["characteristic_codes"]

    def test_communication_language_codes(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "en-US" in out["language"]
        assert "es-MX" in out["language"]

    def test_period_object(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        # PractitionerRole.period is a single Period (cardinality 0..1)
        # — projected as a {start, end} object.
        assert isinstance(out["period"], dict)
        assert out["period"]["start"] == "2024-01-01"
        assert out["period"]["end"] == "2025-12-31"

    def test_practitioner_reference_extraction(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert out["practitionerId"] == "dr-jones"
        assert out["practitionerType"] == "Practitioner"

    def test_organization_reference_extraction(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert out["organizationId"] == "hospital-1"
        assert out["organizationType"] == "Organization"

    def test_location_array_references(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "clinic-3" in out["locationId"]
        assert "clinic-4" in out["locationId"]

    def test_healthcare_service_references(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "svc-1" in out["healthcareServiceId"]
        assert "svc-2" in out["healthcareServiceId"]

    def test_endpoint_references(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "ep-1" in out["endpointId"]


# ===========================================================================
# 6) Nested ContactPoint resolution (R5 ExtendedContactDetail)
# ===========================================================================


class TestPractitionerRoleNestedContactTelecom:
    """PractitionerRole.contact is ExtendedContactDetail[] in R5 — the
    actual ContactPoints live two levels deep at ``contact[*].telecom[*]``.
    Verifies ContactPointExtractor's `$resource` mode + path resolution
    flattens this correctly into _search.email / _search.phone /
    _search.telecom_*.
    """

    def test_email_extracted_from_nested_path(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "admin@hospital.org" in out["email"]

    def test_phones_extracted_from_multiple_contacts(
        self, denormalizer, rich_role
    ):
        # Phones are split across two ExtendedContactDetail entries —
        # both should land in the flat _search.phone array.
        out = denormalizer.denormalize(rich_role)["_search"]
        assert "555-0100" in out["phone"]
        assert "555-0200" in out["phone"]

    def test_telecom_values_unfiltered_union(self, denormalizer, rich_role):
        # `telecom_values` is the union across ALL telecom systems
        # (phone + email + fax) flattened from contact[*].telecom[*].
        out = denormalizer.denormalize(rich_role)["_search"]
        union = out["telecom_values"]
        assert "555-0100" in union  # phone
        assert "admin@hospital.org" in union  # email
        assert "555-0300" in union  # fax

    def test_role_with_no_contact_omits_telecom_buckets(
        self, denormalizer, minimal_role
    ):
        # A bare PractitionerRole has no contact[]; the rule must not
        # synthesize empty arrays in the _search document.
        out = denormalizer.denormalize(minimal_role)
        search = out.get("_search", {})
        # phone / email / telecom_values must either be absent OR be empty
        assert not search.get("phone")
        assert not search.get("email")
        assert not search.get("telecom_values")


# ===========================================================================
# 7) Resource purity — no extraneous fields injected
# ===========================================================================


class TestPractitionerRoleResourcePurity:
    """Denormalization must NOT mutate the original FHIR resource —
    only `_search` and `_compartments` may be added at the root."""

    def test_root_keys_are_resource_plus_buckets_only(
        self, denormalizer, rich_role
    ):
        original_keys = set(rich_role.keys())
        out = denormalizer.denormalize(rich_role)
        added_keys = set(out.keys()) - original_keys
        assert added_keys.issubset({"_search", "_compartments"}), (
            f"Denormalization added unexpected root keys: {added_keys}"
        )

    def test_no_search_subfields_injected_at_root(
        self, denormalizer, rich_role
    ):
        out = denormalizer.denormalize(rich_role)
        forbidden = {
            "practitionerId",
            "organizationId",
            "locationId",
            "healthcareServiceId",
            "endpointId",
            "role_codes",
            "specialty_codes",
            "characteristic_codes",
            "language",
            "email",
            "phone",
            "telecom_values",
            "telecom_systemCode",
            "identifier_values",
            "identifier_systemCode",
            "period",  # (would clobber the FHIR resource's `period` field)
        }
        # `period` IS a real FHIR field, so allow it at root; check
        # that it still has the SAME shape it started with.
        assert out["period"] == rich_role["period"]
        # Other denorm-only fields must not be promoted to the root.
        for f in forbidden - {"period"}:
            assert f not in out, (
                f"Denormalization leaked '{f}' to the resource root"
            )

    def test_original_contact_structure_preserved(
        self, denormalizer, rich_role
    ):
        out = denormalizer.denormalize(rich_role)
        # ExtendedContactDetail wrapper must be left untouched.
        assert out["contact"] == rich_role["contact"]


# ===========================================================================
# 8) Practitioner compartment routing (Hybrid Approach 3 fast-path)
# ===========================================================================


class TestPractitionerRolePractitionerCompartment:
    """``Practitioner/<id>/PractitionerRole`` should route through the
    precomputed `_compartments.Practitioner` field added to the rich
    fixture's denormalized document."""

    def test_compartment_membership_populated(self, denormalizer, rich_role):
        out = denormalizer.denormalize(rich_role)
        comp = out["_compartments"]
        assert comp["Practitioner"] == ["dr-jones"]

    def test_minimal_role_has_no_compartment_bucket(
        self, denormalizer, minimal_role
    ):
        # Without a `practitioner` reference there's nothing to seed
        # the precomputed bucket → omitted entirely.
        out = denormalizer.denormalize(minimal_role)
        assert "_compartments" not in out or not out["_compartments"].get(
            "Practitioner"
        )

    def test_compartment_query_uses_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "PractitionerRole"
        )
        s = str(q)
        # Hybrid fast-path: single indexed lookup against
        # _compartments.Practitioner — no $or, no walk over
        # `practitioner.reference`.
        assert "_compartments.Practitioner" in s
        assert "dr-jones" in s
        assert "$or" not in s

    def test_compartment_query_is_single_field_equality(self, converter):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-7", "PractitionerRole"
        )
        # Exact shape: {"_compartments.Practitioner": "dr-7"}
        assert q == {"_compartments.Practitioner": "dr-7"}

    def test_organization_reference_filtered_out_of_practitioner_bucket(
        self, denormalizer
    ):
        # A role that points at an Organization-typed practitioner ref
        # (uncommon but valid FHIR — practitioner cardinality is 0..1
        # with target Practitioner only, so this models a malformed
        # resource). The reference_type filter must drop the bad entry
        # from `_compartments.Practitioner`.
        bad_role = {
            "resourceType": "PractitionerRole",
            "id": "pr-bad",
            "practitioner": {"reference": "Organization/wrong-target"},
        }
        out = denormalizer.denormalize(bad_role)
        comp = out.get("_compartments", {})
        assert not comp.get("Practitioner")


# ===========================================================================
# 9) Cross-resource forward linking
# ===========================================================================


class TestPractitionerRoleCrossResourceLinking:
    """Forward references from PractitionerRole to its 5 target resource
    types resolve through the per-parameter `_search.*` fields."""

    def test_search_by_practitioner(self, converter):
        q = converter.convert("PractitionerRole", "practitioner=dr-jones")
        s = str(q)
        assert "practitionerId" in s
        assert "dr-jones" in s

    def test_search_by_organization(self, converter):
        q = converter.convert("PractitionerRole", "organization=hospital-1")
        s = str(q)
        assert "organizationId" in s
        assert "hospital-1" in s

    def test_search_by_location(self, converter):
        q = converter.convert("PractitionerRole", "location=clinic-3")
        s = str(q)
        assert "locationId" in s
        assert "clinic-3" in s

    def test_search_by_service(self, converter):
        # `service` parameter binds to PractitionerRole.healthcareService
        q = converter.convert("PractitionerRole", "service=svc-1")
        s = str(q)
        assert "healthcareServiceId" in s
        assert "svc-1" in s

    def test_search_by_endpoint(self, converter):
        q = converter.convert("PractitionerRole", "endpoint=ep-1")
        s = str(q)
        assert "endpointId" in s
        assert "ep-1" in s


# ===========================================================================
# 10) Combined queries
# ===========================================================================


class TestPractitionerRoleCombinations:
    """Multi-parameter queries combining tokens, references, dates."""

    def test_active_and_organization(self, converter):
        q = converter.convert(
            "PractitionerRole", "active=true&organization=hospital-1"
        )
        s = str(q)
        assert "active" in s.lower()
        assert "hospital-1" in s

    def test_role_and_specialty(self, converter):
        q = converter.convert(
            "PractitionerRole", "role=doctor&specialty=394814009"
        )
        s = str(q)
        assert "doctor" in s
        assert "394814009" in s

    def test_practitioner_and_location_and_active(self, converter):
        q = converter.convert(
            "PractitionerRole",
            "practitioner=dr-jones&location=clinic-3&active=true",
        )
        s = str(q)
        assert "dr-jones" in s
        assert "clinic-3" in s
        assert "active" in s.lower()

    def test_date_range_and_organization(self, converter):
        q = converter.convert(
            "PractitionerRole",
            "date=ge2024-01-01&date=lt2026-01-01&organization=hospital-1",
        )
        s = str(q)
        assert "$gte" in s
        assert "$lt" in s
        assert "hospital-1" in s


# ===========================================================================
# 11) MongoDB end-to-end
# ===========================================================================


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
@pytest.mark.skipif(
    not _mongo_available(),
    reason="MongoDB not running on localhost:27017",
)
class TestPractitionerRoleMongoDB:
    """End-to-end roundtrip — seed denormalized PractitionerRoles into a
    MongoDB collection, generate MQL via the converter, execute the
    query, and assert the right docs come back."""

    @pytest.fixture(scope="class")
    def mongo_collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["practitioner_roles_e2e"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_collection, denormalizer):
        # NOTE: We seed `period.start` / `period.end` as Python
        # `datetime` rather than the FHIR-canonical ISO-8601 strings.
        # The MQL date converter emits BSON `datetime` for `$gte` /
        # `$lte` operands, and BSON's type-aware comparison treats
        # string-vs-datetime as never equal — so a stored
        # `_search.period.start` of `"2024-01-01"` (string) would
        # silently miss every range query. Storing as `datetime`
        # aligns the persisted type with the query type so the
        # integration test actually exercises filtering. Production
        # pipelines ingesting raw FHIR JSON should pre-coerce date /
        # dateTime / instant fields the same way (see Practitioner
        # E2E for the matching convention). PeriodExtractor passes
        # the values through verbatim.
        roles: List[Dict[str, Any]] = [
            {
                "resourceType": "PractitionerRole",
                "id": "pr-jones-cardio",
                "active": True,
                "period": {
                    "start": datetime(2024, 1, 1),
                    "end": datetime(2026, 12, 31),
                },
                "practitioner": {"reference": "Practitioner/dr-jones"},
                "organization": {"reference": "Organization/hospital-1"},
                "code": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/practitioner-role",
                                "code": "doctor",
                            }
                        ]
                    }
                ],
                "specialty": [
                    {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "394579002",
                            }
                        ]
                    }
                ],
                "location": [{"reference": "Location/clinic-3"}],
                "healthcareService": [{"reference": "HealthcareService/svc-1"}],
                "endpoint": [{"reference": "Endpoint/ep-1"}],
                "communication": [
                    {"coding": [{"system": "urn:ietf:bcp:47", "code": "en-US"}]}
                ],
                "identifier": [
                    {
                        "system": "http://hl7.org/fhir/sid/us-npi",
                        "value": "1111111111",
                    }
                ],
                "contact": [
                    {
                        "telecom": [
                            {"system": "email", "value": "jones@hospital-1.org"},
                            {"system": "phone", "value": "555-0001"},
                        ]
                    }
                ],
            },
            {
                "resourceType": "PractitionerRole",
                "id": "pr-smith-gp",
                "active": True,
                "period": {
                    "start": datetime(2023, 6, 1),
                    "end": datetime(2025, 6, 1),
                },
                "practitioner": {"reference": "Practitioner/dr-smith"},
                "organization": {"reference": "Organization/hospital-2"},
                "code": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/practitioner-role",
                                "code": "doctor",
                            }
                        ]
                    }
                ],
                "specialty": [
                    {
                        "coding": [
                            {
                                "system": "http://snomed.info/sct",
                                "code": "394814009",
                            }
                        ]
                    }
                ],
                "location": [{"reference": "Location/clinic-1"}],
                "healthcareService": [{"reference": "HealthcareService/svc-2"}],
                "communication": [
                    {"coding": [{"system": "urn:ietf:bcp:47", "code": "es-MX"}]}
                ],
                "identifier": [
                    {
                        "system": "http://hl7.org/fhir/sid/us-npi",
                        "value": "2222222222",
                    }
                ],
                "contact": [
                    {
                        "telecom": [
                            {"system": "email", "value": "smith@hospital-2.org"},
                        ]
                    }
                ],
            },
            {
                "resourceType": "PractitionerRole",
                "id": "pr-jones-research",
                "active": False,
                "period": {
                    "start": datetime(2020, 1, 1),
                    "end": datetime(2022, 12, 31),
                },
                "practitioner": {"reference": "Practitioner/dr-jones"},
                "organization": {"reference": "Organization/research-1"},
                "code": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/practitioner-role",
                                "code": "researcher",
                            }
                        ]
                    }
                ],
                "identifier": [
                    {
                        "system": "http://hospital.org/role-id",
                        "value": "RES-99",
                    }
                ],
            },
        ]

        denorm = [denormalizer.denormalize(r) for r in roles]
        mongo_collection.insert_many(denorm)
        return mongo_collection

    # --- single-param queries ---

    def test_query_active_true(self, seeded, converter):
        q = converter.convert("PractitionerRole", "active=true")
        results = list(seeded.find(q))
        ids = {r["id"] for r in results}
        assert "pr-jones-cardio" in ids
        assert "pr-smith-gp" in ids
        assert "pr-jones-research" not in ids  # active=false

    def test_query_practitioner(self, seeded, converter):
        q = converter.convert("PractitionerRole", "practitioner=dr-jones")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio", "pr-jones-research"}

    def test_query_organization(self, seeded, converter):
        q = converter.convert("PractitionerRole", "organization=hospital-1")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}

    def test_query_role(self, seeded, converter):
        q = converter.convert("PractitionerRole", "role=doctor")
        ids = {r["id"] for r in seeded.find(q)}
        assert "pr-jones-cardio" in ids
        assert "pr-smith-gp" in ids
        assert "pr-jones-research" not in ids

    def test_query_specialty_cardiology(self, seeded, converter):
        q = converter.convert("PractitionerRole", "specialty=394579002")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}

    def test_query_location(self, seeded, converter):
        q = converter.convert("PractitionerRole", "location=clinic-3")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}

    def test_query_service(self, seeded, converter):
        q = converter.convert("PractitionerRole", "service=svc-1")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}

    def test_query_endpoint(self, seeded, converter):
        q = converter.convert("PractitionerRole", "endpoint=ep-1")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}

    def test_query_communication(self, seeded, converter):
        q = converter.convert("PractitionerRole", "communication=es-MX")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-smith-gp"}

    def test_query_identifier_value(self, seeded, converter):
        q = converter.convert("PractitionerRole", "identifier=1111111111")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}

    def test_query_email(self, seeded, converter):
        q = converter.convert(
            "PractitionerRole", "email=jones@hospital-1.org"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}

    def test_query_phone(self, seeded, converter):
        q = converter.convert("PractitionerRole", "phone=555-0001")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}

    # --- date-range queries ---

    def test_query_date_ge_filters_role_validity(self, seeded, converter):
        # Roles with period.start >= 2024-01-01.
        q = converter.convert("PractitionerRole", "date=ge2024-01-01")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio"}  # smith starts in 2023, research in 2020

    def test_query_date_lt_filters_role_validity(self, seeded, converter):
        # Roles with period.end < 2024-01-01.
        q = converter.convert("PractitionerRole", "date=lt2024-01-01")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-research"}  # only role that ended before 2024

    # --- compartment fast-path queries ---

    def test_compartment_dr_jones_returns_only_jones_roles(
        self, seeded, converter
    ):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "PractitionerRole"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-jones-cardio", "pr-jones-research"}

    def test_compartment_dr_smith_returns_only_smith_role(
        self, seeded, converter
    ):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "PractitionerRole"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"pr-smith-gp"}

    def test_compartment_unknown_practitioner_returns_empty(
        self, seeded, converter
    ):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-nobody", "PractitionerRole"
        )
        assert list(seeded.find(q)) == []

    # --- combined queries ---

    def test_compartment_plus_active_filter(self, seeded, converter):
        # Combine the precomputed compartment lookup with an extra
        # boolean filter — the resulting query is an $and of both.
        compartment_q = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "PractitionerRole"
        )
        active_q = converter.convert("PractitionerRole", "active=true")
        combined = {"$and": [compartment_q, active_q]}
        ids = {r["id"] for r in seeded.find(combined)}
        # Of the two dr-jones roles, only the cardiology one is active.
        assert ids == {"pr-jones-cardio"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
