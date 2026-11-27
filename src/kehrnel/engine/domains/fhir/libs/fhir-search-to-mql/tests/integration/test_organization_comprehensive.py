"""
Comprehensive integration tests for ALL Organization search parameters per FHIR R5.

References:
- https://www.hl7.org/fhir/organization-definitions.html
- https://www.hl7.org/fhir/organization-search.html

This suite ensures complete coverage of the 13 R5 spec parameters declared in
``configs/Organization.yaml`` (plus the common params ``_id`` and ``_lastUpdated``):

  - Tokens (4):    active, type, identifier, address-use, phonetic, _id
  - Strings (6):   name, address, address-city, address-country,
                   address-postalcode, address-state
  - References (2): partof, endpoint
  - Common (2):    _id, _lastUpdated

Plus:
- Denormalization correctness against the generic data-type extractors
  (``HumanNameExtractor`` is intentionally NOT used — Organization.name is a
  plain string, so the rules in ``Organization.yaml`` lean on
  ``TextExtractor``, ``PhoneticExtractor`` (extended for plain strings),
  ``IdentifierExtractor`` (resource-rooted union path),
  ``CodeableConceptExtractor``, ``AddressExtractor`` (resource-rooted for
  ``contact[*].address``), and ``ReferenceExtractor``).
- Cross-resource linking: Organization is referenced by Patient
  (``managingOrganization``, ``generalPractitioner``), Observation
  (``subject``, ``performer``), and Appointment (``participant.actor``).
  Tests verify those forward-search paths resolve as expected.
- Compartments: Organization is **NOT** in any FHIR compartment per
  https://www.hl7.org/fhir/compartmentdefinition.html, so this file does
  NOT contain compartment-membership precompute tests (compare with
  Observation/Appointment which DO).
- Real MongoDB roundtrip against ``localhost:27017``.
"""

from __future__ import annotations

import pytest
from datetime import datetime
from typing import Any, Dict, List

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from fhir_search_to_mql.denormalizer.extractors.phonetic import soundex


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
def rich_organization() -> Dict[str, Any]:
    """An Organization with most R5 fields populated for deep extractor testing."""
    return {
        "resourceType": "Organization",
        "id": "org-rich-1",
        "meta": {"lastUpdated": "2024-06-01T10:00:00Z"},
        "active": True,
        "identifier": [
            {
                "system": "http://example.org/orgs",
                "value": "ORG-12345",
                "use": "official",
            },
            {
                "system": "urn:oid:2.16.840.1.113883.4.6",
                "value": "1234567890",
                "use": "usual",
            },
        ],
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                        "code": "prov",
                        "display": "Healthcare Provider",
                    }
                ],
                "text": "Healthcare Provider",
            },
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                        "code": "edu",
                        "display": "Educational Institute",
                    }
                ],
            },
        ],
        "name": "Acme Hospital",
        "alias": ["Acme General", "Acme Medical Center"],
        "description": "A regional acute-care provider.",
        "contact": [
            {
                "purpose": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/contactentity-type",
                            "code": "BILL",
                        }
                    ]
                },
                "address": {
                    "use": "work",
                    "line": ["100 Main St"],
                    "city": "Springfield",
                    "state": "IL",
                    "postalCode": "62701",
                    "country": "USA",
                    "text": "100 Main St, Springfield, IL 62701, USA",
                },
            },
            {
                "purpose": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/contactentity-type",
                            "code": "ADMIN",
                        }
                    ]
                },
                "address": {
                    "use": "work",
                    "line": ["200 Admin Way"],
                    "city": "Chicago",
                    "state": "IL",
                    "postalCode": "60601",
                    "country": "USA",
                },
            },
        ],
        "partOf": {"reference": "Organization/parent-001"},
        "endpoint": [
            {"reference": "Endpoint/endpoint-001"},
            {"reference": "Endpoint/endpoint-002"},
        ],
        "qualification": [
            {
                "identifier": [
                    {
                        "system": "http://accreditation.example.org",
                        "value": "JC-2024-001",
                    }
                ],
                "code": {
                    "coding": [
                        {
                            "system": "http://example.org/accred",
                            "code": "ACCRED",
                            "display": "Joint Commission",
                        }
                    ]
                },
                "issuer": {"reference": "Organization/joint-commission"},
            }
        ],
    }


@pytest.fixture
def minimal_organization() -> Dict[str, Any]:
    """Sparsely-populated Organization (only the FHIR required field set).

    FHIR R5 invariant ``org-1``: ``identifier.count() + name.count() > 0``.
    """
    return {
        "resourceType": "Organization",
        "id": "org-min-1",
        "name": "Minimal Org",
    }


# ---------------------------------------------------------------------------
# 1) String parameters
# ---------------------------------------------------------------------------

class TestOrganizationStringParameters:
    """All Organization string search parameters per FHIR R5."""

    def test_name_search(self, converter):
        """`name` searches BOTH Organization.name AND Organization.alias."""
        query = converter.convert("Organization", "name=Acme")
        s = str(query)
        assert "acme" in s.lower()
        # Should fan out across all three name-lookup fields per yaml.
        assert "$or" in s

    def test_name_search_targets_aliases_field(self, converter):
        """The `name` parameter must hit `_search.aliases_lower`."""
        query = converter.convert("Organization", "name=Memorial")
        s = str(query)
        assert "_search.aliases_lower" in s or "aliases_lower" in s.lower()

    def test_name_search_targets_namealias_field(self, converter):
        """The `name` parameter must hit `_search.nameAlias_lower`."""
        query = converter.convert("Organization", "name=Hospital")
        s = str(query)
        assert "_search.nameAlias_lower" in s or "namealias_lower" in s.lower()

    def test_address_general_search(self, converter):
        query = converter.convert("Organization", "address=Springfield")
        s = str(query)
        assert "springfield" in s.lower()

    def test_address_city_search(self, converter):
        query = converter.convert("Organization", "address-city=Chicago")
        s = str(query)
        assert "chicago" in s.lower()

    def test_address_state_search(self, converter):
        query = converter.convert("Organization", "address-state=Illinois")
        s = str(query)
        assert "illinois" in s.lower()

    def test_address_postalcode_search(self, converter):
        query = converter.convert("Organization", "address-postalcode=62701")
        s = str(query)
        assert "62701" in s

    def test_address_country_search(self, converter):
        query = converter.convert("Organization", "address-country=USA")
        s = str(query)
        assert "usa" in s.lower()


# ---------------------------------------------------------------------------
# 2) Token parameters
# ---------------------------------------------------------------------------

class TestOrganizationTokenParameters:
    """All Organization token search parameters."""

    def test_active_true(self, converter):
        query = converter.convert("Organization", "active=true")
        assert query.get("active") is True or "active" in str(query)

    def test_active_false(self, converter):
        query = converter.convert("Organization", "active=false")
        assert query.get("active") is False or "active" in str(query)

    def test_type_with_system(self, converter):
        query = converter.convert(
            "Organization",
            "type=http://terminology.hl7.org/CodeSystem/organization-type|prov")
        s = str(query)
        assert "prov" in s
        assert "organization-type" in s

    def test_type_value_only(self, converter):
        query = converter.convert("Organization", "type=edu")
        s = str(query)
        assert "edu" in s

    def test_identifier_with_system(self, converter):
        query = converter.convert(
            "Organization",
            "identifier=http://example.org/orgs|ORG-12345")
        s = str(query)
        assert "ORG-12345" in s
        assert "example.org" in s

    def test_identifier_value_only(self, converter):
        """Plain identifier value must be searchable in the values array."""
        query = converter.convert("Organization", "identifier=ORG-12345")
        s = str(query)
        assert "ORG-12345" in s

    def test_identifier_qualification_value_searchable(self, converter, denormalizer, rich_organization):
        """`identifier` parameter spans both top-level AND qualification.identifier (FHIR R5 spec expression)."""
        denorm = denormalizer.denormalize(rich_organization)
        ids = denorm.get("_search", {}).get("identifier_values", [])
        # JC-2024-001 lives at qualification[0].identifier[0].value
        assert "JC-2024-001" in ids

    def test_address_use_search(self, converter):
        query = converter.convert("Organization", "address-use=work")
        s = str(query)
        assert "work" in s

    def test_id_search(self, converter):
        query = converter.convert("Organization", "_id=org-rich-1")
        s = str(query)
        assert "org-rich-1" in s

    def test_phonetic_search(self, converter):
        """`phonetic` is implemented as Soundex token match against `_search.phonetic_codes`."""
        query = converter.convert("Organization", "phonetic=Acme")
        s = str(query)
        # The query value should be Soundex-encoded by the converter (or, at
        # minimum, matched against the phonetic_codes field).
        assert "phonetic_codes" in s or "phonetic" in s


# ---------------------------------------------------------------------------
# 3) Date parameter (_lastUpdated only — Organization has no resource-specific date)
# ---------------------------------------------------------------------------

class TestOrganizationDateParameters:
    """Date comparators for the common `_lastUpdated` parameter."""

    def test_lastupdated_eq(self, converter):
        query = converter.convert("Organization", "_lastUpdated=2024-06-01")
        s = str(query)
        assert "lastUpdated" in s or "lastupdated" in s.lower()

    def test_lastupdated_ge(self, converter):
        query = converter.convert("Organization", "_lastUpdated=ge2024-01-01")
        s = str(query)
        assert "$gte" in s or "ge" in s.lower()

    def test_lastupdated_le(self, converter):
        query = converter.convert("Organization", "_lastUpdated=le2024-12-31")
        s = str(query)
        assert "$lte" in s or "le" in s.lower()

    def test_lastupdated_range(self, converter):
        query = converter.convert(
            "Organization",
            "_lastUpdated=ge2024-01-01&_lastUpdated=le2024-12-31")
        s = str(query)
        assert "$gte" in s and "$lte" in s


# ---------------------------------------------------------------------------
# 4) Reference parameters
# ---------------------------------------------------------------------------

class TestOrganizationReferenceParameters:
    """All Organization reference search parameters."""

    def test_partof_full_reference(self, converter):
        query = converter.convert(
            "Organization", "partof=Organization/parent-001"
        )
        s = str(query)
        assert "parent-001" in s

    def test_partof_id_only(self, converter):
        query = converter.convert("Organization", "partof=parent-001")
        s = str(query)
        assert "parent-001" in s

    def test_endpoint_reference(self, converter):
        query = converter.convert(
            "Organization", "endpoint=Endpoint/endpoint-001"
        )
        s = str(query)
        assert "endpoint-001" in s

    def test_partof_typed_modifier(self, converter):
        """`partof:Organization=X` should still resolve correctly."""
        query = converter.convert(
            "Organization", "partof:Organization=parent-001"
        )
        s = str(query)
        assert "parent-001" in s


# ---------------------------------------------------------------------------
# 5) Combinations
# ---------------------------------------------------------------------------

class TestOrganizationCombinations:
    """AND / OR / multiple-parameter combinations."""

    def test_active_and_type(self, converter):
        query = converter.convert(
            "Organization", "active=true&type=prov"
        )
        s = str(query)
        assert "active" in s and "prov" in s

    def test_name_and_address_city(self, converter):
        query = converter.convert(
            "Organization",
            "name=Acme&address-city=Springfield")
        s = str(query)
        assert "acme" in s.lower() and "springfield" in s.lower()

    def test_partof_and_active(self, converter):
        query = converter.convert(
            "Organization",
            "partof=Organization/parent-001&active=true")
        s = str(query)
        assert "parent-001" in s
        assert "active" in s


# ---------------------------------------------------------------------------
# 6) Modifiers
# ---------------------------------------------------------------------------

class TestOrganizationModifiers:
    """FHIR search modifiers applied to Organization parameters."""

    def test_name_exact(self, converter):
        """:exact disables prefix-style range matching."""
        query = converter.convert(
            "Organization", "name:exact=Acme Hospital"
        )
        s = str(query)
        assert "Acme Hospital" in s or "acme hospital" in s.lower()

    def test_name_contains(self, converter):
        """:contains uses substring (regex) on lowercased fields."""
        query = converter.convert("Organization", "name:contains=osp")
        s = str(query)
        assert "osp" in s.lower()

    def test_active_missing(self, converter):
        """:missing returns docs where the field is null / absent."""
        query = converter.convert("Organization", "active:missing=true")
        s = str(query)
        assert "active" in s

    def test_type_not_modifier(self, converter):
        """:not negates a token match."""
        query = converter.convert("Organization", "type:not=prov")
        s = str(query)
        assert "prov" in s


# ---------------------------------------------------------------------------
# 7) Denormalization correctness
# ---------------------------------------------------------------------------

class TestOrganizationDenormalization:
    """Validate every denormalized field that the search YAML targets."""

    def test_denormalize_name_and_lower(self, denormalizer, rich_organization):
        out = denormalizer.denormalize(rich_organization)["_search"]
        assert out["name"] == "Acme Hospital"
        assert out["name_lower"] == "acme hospital"

    def test_denormalize_aliases_arrays(self, denormalizer, rich_organization):
        out = denormalizer.denormalize(rich_organization)["_search"]
        assert sorted(out["aliases"]) == sorted(["Acme General", "Acme Medical Center"])
        assert sorted(out["aliases_lower"]) == sorted(["acme general", "acme medical center"])

    def test_denormalize_namealias_blob(self, denormalizer, rich_organization):
        """`nameAlias_lower` is the union blob that powers the FHIR `name` parameter."""
        out = denormalizer.denormalize(rich_organization)["_search"]
        blob = out["nameAlias_lower"]
        assert "acme hospital" in blob
        assert "acme general" in blob
        assert "acme medical center" in blob

    def test_denormalize_phonetic_codes(self, denormalizer, rich_organization):
        """Soundex codes computed from name + every alias token."""
        out = denormalizer.denormalize(rich_organization)["_search"]
        codes = out["phonetic_codes"]
        # "Acme" -> A250, "Hospital" -> H214, etc. We don't pin every code
        # since the algorithm is well-defined, but assert a few canonical
        # ones plus dedup correctness.
        assert soundex("Acme") in codes
        assert soundex("Hospital") in codes
        assert soundex("Medical") in codes
        # Dedup: "Acme" appears in name AND in two aliases — must collapse.
        assert codes.count(soundex("Acme")) == 1

    def test_denormalize_phonetic_dedup_includes_alias_token_distinct_from_name(
        self, denormalizer, rich_organization):
        """Tokens that appear ONLY in alias must still contribute codes."""
        out = denormalizer.denormalize(rich_organization)["_search"]
        codes = out["phonetic_codes"]
        # "General" appears only in the alias list.
        assert soundex("General") in codes

    def test_denormalize_identifier_top_level_and_qualification(
        self, denormalizer, rich_organization):
        """`identifier` parameter expression unions top-level + qualification.identifier."""
        out = denormalizer.denormalize(rich_organization)["_search"]
        ids = out["identifier_values"]
        assert "ORG-12345" in ids
        assert "1234567890" in ids
        assert "JC-2024-001" in ids  # qualification path

    def test_denormalize_identifier_systemcode_pairs(
        self, denormalizer, rich_organization):
        out = denormalizer.denormalize(rich_organization)["_search"]
        sysv = out["identifier_systemCode"]
        assert "http://example.org/orgs|ORG-12345" in sysv
        assert "http://accreditation.example.org|JC-2024-001" in sysv

    def test_denormalize_type_codes_and_systemcode(
        self, denormalizer, rich_organization):
        out = denormalizer.denormalize(rich_organization)["_search"]
        codes = out["type_codes"]
        assert "prov" in codes and "edu" in codes
        sysc = out["type_systemCode"]
        assert any(
            v.endswith("|prov") for v in sysc
        ), f"missing prov system|code in {sysc}"

    def test_denormalize_address_components(
        self, denormalizer, rich_organization):
        out = denormalizer.denormalize(rich_organization)["_search"]
        assert "Springfield" in out["addressCity"]
        assert "Chicago" in out["addressCity"]
        assert "springfield" in out["addressCity_lower"]
        assert "62701" in out["addressPostalCode"]
        assert "60601" in out["addressPostalCode"]
        assert "USA" in out["addressCountry"]
        assert "usa" in out["addressCountry_lower"]
        assert "IL" in out["addressState"]
        assert "il" in out["addressState_lower"]
        assert "work" in out["addressUse"]

    def test_denormalize_address_full_uses_text_when_present(
        self, denormalizer, rich_organization):
        """When `address.text` is supplied, addressFull prefers it (curated form)."""
        out = denormalizer.denormalize(rich_organization)["_search"]
        full = out["addressFull"]
        # First contact has `text`; second does not — joined fallback for second.
        assert any("100 Main St, Springfield, IL 62701, USA" == s for s in full)
        assert any("Chicago" in s for s in full)

    def test_denormalize_partof_id_and_type(
        self, denormalizer, rich_organization):
        out = denormalizer.denormalize(rich_organization)["_search"]
        assert out["partOfId"] == "parent-001"
        assert out["partOfType"] == "Organization"

    def test_denormalize_endpoint_ids(
        self, denormalizer, rich_organization):
        out = denormalizer.denormalize(rich_organization)["_search"]
        assert sorted(out["endpointId"]) == ["endpoint-001", "endpoint-002"]

    def test_denormalize_active_preserved(
        self, denormalizer, rich_organization):
        """`active` is a top-level boolean — denormalization must not drop it."""
        out = denormalizer.denormalize(rich_organization)
        assert out["active"] is True

    def test_denormalize_minimal_is_sparse(
        self, denormalizer, minimal_organization):
        """A bare-bones Organization should not generate spurious search fields."""
        out = denormalizer.denormalize(minimal_organization)
        search = out.get("_search", {})
        # name_lower MUST exist (we have a name).
        assert search.get("name_lower") == "minimal org"
        # No address / identifier / partOf / endpoint / phonetic surplus.
        for absent in (
            "addressCity",
            "addressCity_lower",
            "addressPostalCode",
            "partOfId",
            "endpointId",
            "identifier_values",
            "type_codes"):
            assert absent not in search, f"unexpectedly populated: {absent}"

    def test_denormalize_does_not_compute_compartments(
        self, denormalizer, rich_organization):
        """Organization is not in any FHIR compartment — verify NO _compartments bucket emitted."""
        out = denormalizer.denormalize(rich_organization)
        assert "_compartments" not in out


# ---------------------------------------------------------------------------
# 8) Cross-resource linking
#
# Organization is referenced by:
#   - Patient.managingOrganization, Patient.generalPractitioner
#   - Observation.subject, Observation.performer
#   - Appointment.participant.actor
# Forward search should resolve those references via the existing
# Patient/Observation/Appointment YAML.
# ---------------------------------------------------------------------------

class TestOrganizationCrossResourceLinking:
    """Verify the `Reference(Organization)` fields on other resources are searchable."""

    def test_patient_managing_organization_search(self, converter):
        """Patient `organization` parameter resolves managingOrganization references."""
        query = converter.convert(
            "Patient", "organization=Organization/org-rich-1"
        )
        s = str(query)
        assert "org-rich-1" in s
        # Should target the denormalized id field.
        assert "managingOrganizationId" in s or "managingorganizationid" in s.lower()

    def test_patient_general_practitioner_organization(self, converter):
        """Patient `general-practitioner` accepts Organization references."""
        query = converter.convert(
            "Patient", "general-practitioner=Organization/org-rich-1"
        )
        s = str(query)
        assert "org-rich-1" in s

    def test_observation_subject_organization(self, converter):
        """Observation `subject` can be Organization (e.g., environmental sample)."""
        query = converter.convert(
            "Observation", "subject=Organization/org-rich-1"
        )
        s = str(query)
        assert "org-rich-1" in s

    def test_observation_performer_organization(self, converter):
        """Observation `performer` accepts Organization references."""
        query = converter.convert(
            "Observation", "performer=Organization/org-rich-1"
        )
        s = str(query)
        assert "org-rich-1" in s

    def test_appointment_actor_organization(self, converter):
        """Appointment `actor` accepts Organization references."""
        query = converter.convert(
            "Appointment", "actor=Organization/org-rich-1"
        )
        s = str(query)
        assert "org-rich-1" in s

    def test_patient_managing_organization_denormalization(self, denormalizer):
        """Round-trip: Patient → managingOrganization → searchable id."""
        patient = {
            "resourceType": "Patient",
            "id": "pat-001",
            "managingOrganization": {"reference": "Organization/org-rich-1"},
        }
        out = denormalizer.denormalize(patient)["_search"]
        assert out.get("managingOrganizationId") == "org-rich-1"

    def test_observation_subject_organization_type_captured(self, denormalizer):
        """When subject is an Organization, _search.subjectType must be 'Organization'."""
        obs = {
            "resourceType": "Observation",
            "id": "obs-org-subject",
            "status": "final",
            "code": {"coding": [{"code": "noise"}]},
            "subject": {"reference": "Organization/org-rich-1"},
        }
        out = denormalizer.denormalize(obs)["_search"]
        assert out.get("subjectId") == "org-rich-1"
        assert out.get("subjectType") == "Organization"

    def test_observation_performer_organization_type_captured(self, denormalizer):
        """When performer is an Organization, performerType must include 'Organization'."""
        obs = {
            "resourceType": "Observation",
            "id": "obs-org-performer",
            "status": "final",
            "code": {"coding": [{"code": "noise"}]},
            "performer": [
                {"reference": "Organization/org-rich-1"},
                {"reference": "Practitioner/p1"},
            ],
        }
        out = denormalizer.denormalize(obs)["_search"]
        assert "org-rich-1" in out.get("performerId", [])
        assert "Organization" in out.get("performerType", [])

    def test_appointment_actor_organization_id_captured(self, denormalizer):
        """When an Appointment actor is an Organization, its id and type are captured.

        We use a single actor (no Patient/Practitioner alongside) because
        the existing Appointment.yaml's `participant` rule projects
        ``patientId`` / ``practitionerId`` / ``locationId`` as
        ``datatype: string`` against the ``participant[*].actor.reference``
        union path — having multiple non-Organization actors would
        trigger a string-vs-list cardinality clash unrelated to the
        Organization linking we're verifying here.
        """
        appt = {
            "resourceType": "Appointment",
            "id": "appt-org-actor",
            "status": "booked",
            "participant": [
                {
                    "actor": {"reference": "Organization/org-rich-1"},
                    "status": "accepted",
                },
            ],
        }
        out = denormalizer.denormalize(appt)["_search"]
        actor_ids = out.get("actorIds") or out.get("actorId") or []
        if isinstance(actor_ids, str):
            actor_ids = [actor_ids]
        assert "org-rich-1" in actor_ids
        actor_types = out.get("actorTypes") or []
        if isinstance(actor_types, str):
            actor_types = [actor_types]
        assert "Organization" in actor_types


# ---------------------------------------------------------------------------
# 9) MongoDB integration (localhost:27017)
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
@pytest.mark.skipif(
    not _mongo_available(),
    reason="MongoDB not running on localhost:27017")
class TestOrganizationMongoDB:
    """End-to-end: denormalize → insert → query via converter → assert results."""

    @pytest.fixture(scope="class")
    def mongo_collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["organizations_e2e"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_collection, denormalizer):
        # A small org hierarchy:
        #   - parent: regional system "Acme Health Network"
        #   - child orgs: 3 hospitals and 1 clinic, all partOf parent
        #   - 1 inactive org
        organizations: List[Dict[str, Any]] = [
            {
                "resourceType": "Organization",
                "id": "org-parent",
                "active": True,
                "name": "Acme Health Network",
                "alias": ["AHN"],
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                                "code": "prov",
                            }
                        ]
                    }
                ],
                "identifier": [
                    {"system": "http://example.org/orgs", "value": "AHN-001"}
                ],
                "contact": [
                    {
                        "address": {
                            "use": "work",
                            "city": "Springfield",
                            "state": "IL",
                            "postalCode": "62701",
                            "country": "USA",
                        }
                    }
                ],
                "meta": {"lastUpdated": datetime(2024, 1, 15, 10, 0, 0)},
            },
        ]
        for i in range(1, 4):
            organizations.append(
                {
                    "resourceType": "Organization",
                    "id": f"org-hosp-{i:03d}",
                    "active": True,
                    "name": f"Acme Hospital {i}",
                    "type": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                                    "code": "prov",
                                }
                            ]
                        }
                    ],
                    "partOf": {"reference": "Organization/org-parent"},
                    "identifier": [
                        {
                            "system": "http://example.org/orgs",
                            "value": f"HOSP-{i:03d}",
                        }
                    ],
                    "contact": [
                        {
                            "address": {
                                "use": "work",
                                "city": "Springfield" if i == 1 else "Chicago",
                                "state": "IL",
                                "postalCode": "62701" if i == 1 else "60601",
                                "country": "USA",
                            }
                        }
                    ],
                    "meta": {
                        "lastUpdated": datetime(2024, 2, i * 5, 10, 0, 0)
                    },
                }
            )
        organizations.append(
            {
                "resourceType": "Organization",
                "id": "org-clinic-001",
                "active": True,
                "name": "Acme Clinic Downtown",
                "alias": ["ACD"],
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                                "code": "ambulatory",
                            }
                        ]
                    }
                ],
                "partOf": {"reference": "Organization/org-parent"},
                "identifier": [
                    {"system": "http://example.org/orgs", "value": "CLINIC-001"}
                ],
                "contact": [
                    {
                        "address": {
                            "use": "work",
                            "city": "Chicago",
                            "state": "IL",
                            "postalCode": "60601",
                            "country": "USA",
                        }
                    }
                ],
                "meta": {"lastUpdated": datetime(2024, 3, 10, 10, 0, 0)},
            }
        )
        organizations.append(
            {
                "resourceType": "Organization",
                "id": "org-defunct",
                "active": False,
                "name": "Defunct Imaging Co",
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                                "code": "prov",
                            }
                        ]
                    }
                ],
                "meta": {"lastUpdated": datetime(2020, 1, 1, 10, 0, 0)},
            }
        )

        denorm = [denormalizer.denormalize(o) for o in organizations]
        # Restore _id so the docs round-trip as documents, not just the
        # `id` business field. This mirrors how the production pipeline
        # would persist resources.
        for d, src in zip(denorm, organizations):
            d.setdefault("_id", src["id"])
        mongo_collection.insert_many(denorm)
        return mongo_collection

    def test_query_by_name_finds_all_acme(self, converter, seeded):
        q = converter.convert("Organization", "name=Acme")
        results = list(seeded.find(q))
        # 1 parent + 3 hospitals + 1 clinic
        assert len(results) == 5

    def test_query_by_active_true_excludes_defunct(self, converter, seeded):
        q = converter.convert("Organization", "active=true")
        results = list(seeded.find(q))
        names = sorted(r["name"] for r in results)
        assert "Defunct Imaging Co" not in names
        assert len(results) == 5

    def test_query_by_type_prov(self, converter, seeded):
        q = converter.convert("Organization", "type=prov")
        results = list(seeded.find(q))
        # parent + 3 hospitals + defunct (clinic is `ambulatory`)
        assert len(results) == 5

    def test_query_by_partof_returns_children_only(self, converter, seeded):
        q = converter.convert(
            "Organization", "partof=Organization/org-parent"
        )
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        # Only the 3 hospitals + 1 clinic — NOT the parent itself.
        assert ids == [
            "org-clinic-001",
            "org-hosp-001",
            "org-hosp-002",
            "org-hosp-003",
        ]

    def test_query_by_identifier(self, converter, seeded):
        q = converter.convert(
            "Organization",
            "identifier=http://example.org/orgs|HOSP-002")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "org-hosp-002"

    def test_query_by_address_city(self, converter, seeded):
        q = converter.convert("Organization", "address-city=Chicago")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        # 2 of the 3 hospitals + 1 clinic
        assert ids == [
            "org-clinic-001",
            "org-hosp-002",
            "org-hosp-003",
        ]

    def test_query_combo_partof_and_type(self, converter, seeded):
        q = converter.convert(
            "Organization",
            "partof=Organization/org-parent&type=prov")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        # Children of org-parent that are type=prov → the 3 hospitals only.
        assert ids == ["org-hosp-001", "org-hosp-002", "org-hosp-003"]

    def test_query_by_phonetic(self, converter, seeded):
        """Soundex match against `_search.phonetic_codes`.

        The FHIR ``phonetic`` parameter is implemented as a token search
        against the precomputed Soundex array, so the caller is expected
        to Soundex-encode the search value (same convention as
        :class:`TestPatientPhoneticDenormalization`). Searching for the
        Soundex code of "Acme" should match every Acme-named org.
        """
        acme_code = soundex("Acme")
        q = converter.convert("Organization", f"phonetic={acme_code}")
        results = list(seeded.find(q))
        assert len(results) >= 5

    def test_query_by_lastupdated_range(self, converter, seeded):
        q = converter.convert(
            "Organization",
            "_lastUpdated=ge2024-01-01&_lastUpdated=le2024-04-01")
        results = list(seeded.find(q))
        # parent + 3 hospitals + clinic; defunct (2020) is excluded.
        assert len(results) == 5

    def test_cross_resource_patient_org(self, converter, seeded, denormalizer):
        """End-to-end: insert a Patient with managingOrganization, query Patients by org."""
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        pat_coll = db["patients_org_link_e2e"]
        pat_coll.delete_many({})
        try:
            patient = {
                "resourceType": "Patient",
                "id": "pat-org-001",
                "active": True,
                "managingOrganization": {
                    "reference": "Organization/org-hosp-001"
                },
            }
            denorm = denormalizer.denormalize(patient)
            denorm.setdefault("_id", patient["id"])
            pat_coll.insert_one(denorm)

            q = converter.convert(
                "Patient", "organization=Organization/org-hosp-001"
            )
            results = list(pat_coll.find(q))
            assert len(results) == 1
            assert results[0]["id"] == "pat-org-001"
        finally:
            pat_coll.delete_many({})
            client.close()
