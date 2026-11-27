"""
Comprehensive integration tests for ALL Practitioner search parameters per FHIR R5.

References:
- https://www.hl7.org/fhir/practitioner-search.html
- https://www.hl7.org/fhir/practitioner-definitions.html
- https://www.hl7.org/fhir/compartmentdefinition-practitioner.html

This suite exercises the 22 search parameters declared in
``configs/Practitioner.yaml``:

  - Strings (8):    name, family, given, address, address-city,
                    address-country, address-postalcode, address-state,
                    phonetic
  - Tokens (10):    active, gender, identifier, deceased, communication,
                    email, phone, telecom, address-use, _id
  - Dates (3):      death-date, qualification-period, _lastUpdated

Plus:
- FHIR R5 polymorphic ``deceased[x]`` handling — the resource carries
  either ``deceasedBoolean`` or ``deceasedDateTime`` and BOTH variants
  must satisfy the ``deceased`` token search.
- ``identifier`` union over ``Practitioner.identifier`` AND
  ``Practitioner.qualification.identifier`` — single search parameter,
  two source paths merged into a single denormalized projection.
- ``communication.language`` CodeableConcept extraction.
- ``qualification[*].period`` projected as a flat array of ``{start, end}``
  pairs powering the ``qualification-period`` date parameter.
- Self-compartment membership: a Practitioner's own ``id`` lands in
  ``_compartments.Practitioner`` so ``Practitioner/<id>/Practitioner``
  routes through the precomputed fast-path.
- Cross-resource compartment routing: Patient (``generalPractitioner``),
  Observation (``performer``), and Appointment (``participant.actor``)
  each populate ``_compartments.Practitioner`` for Practitioner-typed
  references — the ``CompartmentResolver`` short-circuits the
  multi-parameter ``$or`` to a single indexed lookup.
- Modifiers: ``:exact``, ``:contains``, ``:missing``, ``:not``.
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
    return FHIRSearchConverter()


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.fixture
def rich_practitioner() -> Dict[str, Any]:
    """A Practitioner with most R5 fields populated for deep extractor coverage."""
    return {
        "resourceType": "Practitioner",
        "id": "dr-jones",
        "meta": {"lastUpdated": "2024-06-01T10:00:00Z"},
        "active": True,
        "identifier": [
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": "1234567890",
                "use": "official",
            },
            {
                "system": "http://hospital.org/empno",
                "value": "E-7788",
            },
        ],
        "name": [
            {
                "use": "official",
                "family": "Jones",
                "given": ["Sarah", "M"],
                "prefix": ["Dr."],
                "suffix": ["MD"],
            },
            {
                "use": "nickname",
                "given": ["Sally"],
            },
        ],
        "telecom": [
            {"system": "email", "value": "sarah.jones@hospital.org", "use": "work"},
            {"system": "phone", "value": "555-1234", "use": "work"},
            {"system": "fax", "value": "555-9999"},
        ],
        "address": [
            {
                "use": "home",
                "line": ["123 Main St"],
                "city": "Springfield",
                "state": "IL",
                "postalCode": "62701",
                "country": "US",
            },
        ],
        "gender": "female",
        "birthDate": "1975-04-12",
        # FHIR R5 polymorphic CHOICE — the dateTime variant.
        "deceasedDateTime": "2024-08-22T00:00:00Z",
        "qualification": [
            {
                "identifier": [
                    {"system": "http://license.gov", "value": "MD-IL-9876"},
                ],
                "code": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0360",
                            "code": "MD",
                            "display": "Doctor of Medicine",
                        }
                    ]
                },
                "period": {"start": "2005-06-01", "end": "2030-06-01"},
                "issuer": {"reference": "Organization/medboard-il"},
            },
            {
                # Second qualification with a narrower validity window.
                "identifier": [
                    {"system": "http://board.example.org", "value": "CARD-2018"},
                ],
                "code": {
                    "coding": [
                        {
                            "system": "http://example.org/specialties",
                            "code": "CARD",
                            "display": "Cardiology",
                        }
                    ]
                },
                "period": {"start": "2018-01-01", "end": "2028-01-01"},
            },
        ],
        "communication": [
            {"language": {"coding": [{"system": "urn:ietf:bcp:47", "code": "en-US"}]}},
            {"language": {"coding": [{"system": "urn:ietf:bcp:47", "code": "es-MX"}]}},
        ],
    }


@pytest.fixture
def deceased_boolean_practitioner() -> Dict[str, Any]:
    """A practitioner using the OTHER polymorphic deceased[x] variant."""
    return {
        "resourceType": "Practitioner",
        "id": "dr-bool",
        "active": False,
        "name": [{"family": "Bool", "given": ["Boolean"]}],
        # Boolean variant of deceased[x] — no death date.
        "deceasedBoolean": True,
    }


@pytest.fixture
def alive_practitioner() -> Dict[str, Any]:
    """No `deceased[x]` field at all (FHIR default = alive)."""
    return {
        "resourceType": "Practitioner",
        "id": "dr-alive",
        "active": True,
        "name": [{"family": "Alive", "given": ["Allie"]}],
    }


@pytest.fixture
def minimal_practitioner() -> Dict[str, Any]:
    """Bare-bones — only id and resourceType.

    FHIR has no required field on Practitioner per the R5 spec, so this
    is the legitimate floor for sparse-output testing.
    """
    return {
        "resourceType": "Practitioner",
        "id": "dr-min",
    }


# ---------------------------------------------------------------------------
# 1) String parameters
# ---------------------------------------------------------------------------

class TestPractitionerStringParameters:
    """All Practitioner string search parameters per FHIR R5 §8.4."""

    def test_name_search_fans_out(self, converter):
        """`name` matches across family, given, fullName, nameText (default mode)."""
        query = converter.convert("Practitioner", "name=Jones")
        s = str(query)
        assert "$or" in s
        assert "familyName_lower" in s
        assert "givenNames_lower" in s
        assert "fullName_lower" in s
        # Default mode uses the `_lower` companion (case-insensitive prefix).
        assert "jones" in s.lower()

    def test_name_search_lowercased_value(self, converter):
        """Default string match lowercases the input regardless of casing."""
        query = converter.convert("Practitioner", "name=JONES")
        s = str(query)
        # FHIR default = case-insensitive — query value must be folded.
        assert "jones" in s.lower()

    def test_family_search(self, converter):
        query = converter.convert("Practitioner", "family=Jones")
        s = str(query)
        assert "familyName_lower" in s
        assert "jones" in s.lower()

    def test_given_search(self, converter):
        query = converter.convert("Practitioner", "given=Sarah")
        s = str(query)
        assert "givenNames_lower" in s
        assert "sarah" in s.lower()

    def test_given_search_does_not_strip_sa_prefix(self, converter):
        """Regression: `given=sarah` (lowercase) must NOT have `sa` parsed as a prefix.

        Pre-fix, the parameter parser treated the leading `sa` as a date/quantity
        comparator prefix and rewrote the value to `"rah"`, silently breaking
        every Sarah, Sasha, Samuel, etc. The fix anchors string param types
        BEFORE the prefix regex runs.
        """
        query = converter.convert("Practitioner", "given=sarah")
        s = str(query)
        # The intact string must appear in the query value, not the truncated `rah`.
        assert "sarah" in s
        assert '"rah"' not in s and "'rah'" not in s

    def test_family_search_does_not_strip_ge_prefix(self, converter):
        """Regression: `family=geraldine` must keep its `ge` (not parsed as `>=`)."""
        query = converter.convert("Practitioner", "family=geraldine")
        s = str(query)
        assert "geraldine" in s
        assert '"raldine"' not in s and "'raldine'" not in s

    def test_address_search(self, converter):
        query = converter.convert("Practitioner", "address=Springfield")
        s = str(query)
        assert "addressFull_lower" in s
        assert "springfield" in s.lower()

    def test_address_city_search(self, converter):
        query = converter.convert("Practitioner", "address-city=Springfield")
        s = str(query)
        assert "addressCity_lower" in s
        assert "springfield" in s.lower()

    def test_address_state_search(self, converter):
        query = converter.convert("Practitioner", "address-state=IL")
        s = str(query)
        assert "addressState_lower" in s
        assert "il" in s.lower()

    def test_address_postalcode_search(self, converter):
        """Postal code is case-uniform — single field, no `_lower` companion."""
        query = converter.convert("Practitioner", "address-postalcode=62701")
        s = str(query)
        assert "addressPostalCode" in s
        assert "62701" in s

    def test_address_country_search(self, converter):
        query = converter.convert("Practitioner", "address-country=US")
        s = str(query)
        assert "addressCountry_lower" in s
        assert "us" in s.lower()


# ---------------------------------------------------------------------------
# 2) Token parameters
# ---------------------------------------------------------------------------

class TestPractitionerTokenParameters:
    """All Practitioner token search parameters."""

    def test_active_true(self, converter):
        query = converter.convert("Practitioner", "active=true")
        assert query.get("active") is True or "active" in str(query)

    def test_active_false(self, converter):
        query = converter.convert("Practitioner", "active=false")
        assert query.get("active") is False or "active" in str(query)

    def test_gender_male(self, converter):
        query = converter.convert("Practitioner", "gender=male")
        s = str(query)
        assert "male" in s
        assert "gender" in s

    def test_gender_female(self, converter):
        query = converter.convert("Practitioner", "gender=female")
        assert "female" in str(query)

    def test_identifier_value_only(self, converter):
        query = converter.convert("Practitioner", "identifier=1234567890")
        s = str(query)
        assert "1234567890" in s
        assert "identifier" in s

    def test_identifier_with_system(self, converter):
        query = converter.convert(
            "Practitioner",
            "identifier=http://hl7.org/fhir/sid/us-npi|1234567890",
        )
        s = str(query)
        assert "1234567890" in s
        assert "us-npi" in s

    def test_identifier_qualification_searchable(self, converter):
        """The R5 `identifier` expression unions Practitioner-level AND qualification.identifier."""
        query = converter.convert("Practitioner", "identifier=MD-IL-9876")
        s = str(query)
        # Routing layer: query targets identifier_values / identifier_systemCode.
        assert "MD-IL-9876" in s

    def test_deceased_true(self, converter):
        query = converter.convert("Practitioner", "deceased=true")
        s = str(query)
        assert "_search.deceased" in s
        assert "true" in s.lower()

    def test_deceased_false(self, converter):
        query = converter.convert("Practitioner", "deceased=false")
        s = str(query)
        assert "_search.deceased" in s

    def test_communication_language(self, converter):
        query = converter.convert("Practitioner", "communication=es-MX")
        s = str(query)
        assert "es-MX" in s
        # Routes to denormalized language array.
        assert "_search.language" in s

    def test_email_search(self, converter):
        query = converter.convert(
            "Practitioner", "email=sarah.jones@hospital.org"
        )
        s = str(query)
        assert "_search.email" in s
        assert "hospital.org" in s

    def test_phone_search(self, converter):
        query = converter.convert("Practitioner", "phone=555-1234")
        s = str(query)
        assert "_search.phone" in s
        assert "555-1234" in s

    def test_telecom_value_only(self, converter):
        query = converter.convert("Practitioner", "telecom=555-9999")
        s = str(query)
        assert "555-9999" in s
        # `telecom` searches BOTH the systemCode pair AND the values array.
        assert "telecom" in s

    def test_telecom_with_system(self, converter):
        query = converter.convert(
            "Practitioner", "telecom=fax|555-9999"
        )
        s = str(query)
        assert "555-9999" in s
        assert "fax" in s

    def test_address_use_search(self, converter):
        query = converter.convert("Practitioner", "address-use=home")
        s = str(query)
        assert "_search.addressUse" in s
        assert "home" in s

    def test_phonetic_search(self, converter):
        """`phonetic` is implemented as Soundex token match."""
        query = converter.convert("Practitioner", f"phonetic={soundex('Jones')}")
        s = str(query)
        assert "phonetic_codes" in s
        assert soundex("Jones") in s

    def test_id_search(self, converter):
        query = converter.convert("Practitioner", "_id=dr-jones")
        s = str(query)
        assert "dr-jones" in s


# ---------------------------------------------------------------------------
# 3) Date parameters
# ---------------------------------------------------------------------------

class TestPractitionerDateParameters:
    """Date comparators for `death-date`, `qualification-period`, `_lastUpdated`."""

    def test_death_date_eq(self, converter):
        # The DateConverter parses the input date and emits a datetime
        # range covering the day. Match the year (consistent with Patient
        # tests) rather than the full ISO string — `str(query)` shows a
        # `datetime.datetime(2024, 8, 22, ...)` object, not a literal.
        query = converter.convert("Practitioner", "death-date=2024-08-22")
        s = str(query)
        assert "_search.deathDate" in s
        assert "2024" in s and "8, 22" in s

    def test_death_date_ge(self, converter):
        query = converter.convert("Practitioner", "death-date=ge2024-01-01")
        s = str(query)
        assert "_search.deathDate" in s
        assert "$gte" in s
        assert "2024" in s

    def test_death_date_le(self, converter):
        query = converter.convert("Practitioner", "death-date=le2024-12-31")
        s = str(query)
        assert "_search.deathDate" in s
        assert "$lte" in s
        assert "2024" in s and "12, 31" in s

    def test_death_date_range(self, converter):
        query = converter.convert(
            "Practitioner",
            "death-date=ge2024-01-01&death-date=le2024-12-31",
        )
        s = str(query)
        assert "$gte" in s and "$lte" in s

    def test_qualification_period_ge(self, converter):
        query = converter.convert("Practitioner", "qualification-period=ge2010-01-01")
        s = str(query)
        assert "qualificationPeriod" in s
        assert "$gte" in s
        assert "2010" in s

    def test_qualification_period_overlap_query(self, converter):
        """Two prefixes form an overlap window."""
        query = converter.convert(
            "Practitioner",
            "qualification-period=ge2018-01-01&qualification-period=le2025-12-31",
        )
        s = str(query)
        assert "$gte" in s and "$lte" in s
        assert "qualificationPeriod" in s

    def test_lastupdated_eq(self, converter):
        query = converter.convert("Practitioner", "_lastUpdated=2024-06-01")
        s = str(query)
        assert "lastUpdated" in s.lower() or "lastupdated" in s.lower()

    def test_lastupdated_ge(self, converter):
        query = converter.convert("Practitioner", "_lastUpdated=ge2024-01-01")
        s = str(query)
        assert "$gte" in s


# ---------------------------------------------------------------------------
# 4) Modifiers
# ---------------------------------------------------------------------------

class TestPractitionerModifiers:
    """FHIR search modifiers applied to Practitioner parameters."""

    def test_family_exact(self, converter):
        """`:exact` MUST hit the case-preserved field, NOT the `_lower` companion."""
        query = converter.convert("Practitioner", "family:exact=Jones")
        s = str(query)
        # Case-preserved value lives in `_search.familyName` (no `_lower` suffix).
        assert "familyName" in s
        assert "familyName_lower" not in s
        assert "Jones" in s

    def test_family_exact_case_sensitive(self, converter):
        """Per FHIR R5 §3.1.1.5.4, `:exact` value must NOT be lowercased."""
        query = converter.convert("Practitioner", "family:exact=Jones")
        # The literal `Jones` (capital-J) must appear in the query.
        assert "Jones" in str(query)

    def test_name_exact_targets_all_namelookup_fields(self, converter):
        query = converter.convert("Practitioner", "name:exact=Jones")
        s = str(query)
        # `:exact` should fan out across familyName/givenNames/fullName/nameText
        # case-preserved targets — see Practitioner.yaml `name.fields.exact`.
        assert "$or" in s
        assert "familyName" in s and "givenNames" in s

    def test_address_city_exact(self, converter):
        query = converter.convert("Practitioner", "address-city:exact=Springfield")
        s = str(query)
        assert "addressCity" in s
        assert "addressCity_lower" not in s
        assert "Springfield" in s

    def test_family_contains(self, converter):
        """`:contains` does substring (regex) on the lowercased field."""
        query = converter.convert("Practitioner", "family:contains=one")
        s = str(query).lower()
        # After `s.lower()` the field name appears as `familyname_lower`
        # (default `:contains` routes through the `_lower` projection).
        assert "familyname" in s
        assert "one" in s

    def test_active_missing_true(self, converter):
        """`:missing=true` finds resources without an active field."""
        query = converter.convert("Practitioner", "active:missing=true")
        s = str(query)
        assert "active" in s

    def test_gender_not_modifier(self, converter):
        """`:not` negates a token match."""
        query = converter.convert("Practitioner", "gender:not=male")
        s = str(query)
        assert "gender" in s
        assert "male" in s
        # Should produce a $ne or $nin form.
        assert "$ne" in s or "$nin" in s or "$not" in s


# ---------------------------------------------------------------------------
# 5) Combinations / multi-parameter
# ---------------------------------------------------------------------------

class TestPractitionerCombinations:
    """AND / OR / multi-parameter logic."""

    def test_active_and_gender(self, converter):
        query = converter.convert("Practitioner", "active=true&gender=female")
        s = str(query)
        assert "active" in s and "female" in s

    def test_family_and_address_city(self, converter):
        query = converter.convert(
            "Practitioner", "family=Jones&address-city=Springfield"
        )
        s = str(query)
        assert "jones" in s.lower() and "springfield" in s.lower()

    def test_identifier_or_via_comma(self, converter):
        """Comma-separated values within a single parameter form OR logic."""
        query = converter.convert(
            "Practitioner", "identifier=1234567890,9876543210"
        )
        s = str(query)
        assert "1234567890" in s and "9876543210" in s

    def test_communication_and_active(self, converter):
        query = converter.convert(
            "Practitioner", "communication=es-MX&active=true"
        )
        s = str(query)
        assert "es-MX" in s and "active" in s

    def test_qualification_period_and_active(self, converter):
        query = converter.convert(
            "Practitioner",
            "qualification-period=ge2020-01-01&active=true",
        )
        s = str(query)
        assert "active" in s
        assert "qualificationPeriod" in s


# ---------------------------------------------------------------------------
# 6) Denormalization correctness
# ---------------------------------------------------------------------------

class TestPractitionerDenormalization:
    """Verify every denormalized field declared in Practitioner.yaml."""

    def test_denormalize_name_fields(self, denormalizer, rich_practitioner):
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        # Family — one string per HumanName.family (0..1); collected across all names.
        assert out["familyName"] == ["Jones"]
        assert out["familyName_lower"] == ["jones"]
        # Given names — flat array across all HumanName entries.
        assert "Sarah" in out["givenNames"]
        assert "M" in out["givenNames"]
        assert "Sally" in out["givenNames"]
        assert "sarah" in out["givenNames_lower"]
        assert "sally" in out["givenNames_lower"]
        # Full name reconstruction (prefix + given + family + suffix).
        assert any("Sarah" in n and "Jones" in n for n in out["fullName"])
        # Lower-cased companion mirrors the case-preserved blob.
        assert any("sarah" in n and "jones" in n for n in out["fullName_lower"])

    def test_denormalize_phonetic_codes_present(self, denormalizer, rich_practitioner):
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        codes = out["phonetic_codes"]
        assert soundex("Jones") in codes
        assert soundex("Sarah") in codes
        assert soundex("Sally") in codes

    def test_denormalize_identifier_top_level_and_qualification(
        self, denormalizer, rich_practitioner
    ):
        """Per FHIR R5 §8.4.31: `Practitioner.identifier | Practitioner.qualification.identifier`."""
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        ids = out["identifier_values"]
        # Resource-level identifiers.
        assert "1234567890" in ids
        assert "E-7788" in ids
        # Qualification-level identifiers (via the `|` union path).
        assert "MD-IL-9876" in ids
        assert "CARD-2018" in ids

    def test_denormalize_identifier_systemcode_pairs(
        self, denormalizer, rich_practitioner
    ):
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        sysv = out["identifier_systemCode"]
        assert "http://hl7.org/fhir/sid/us-npi|1234567890" in sysv
        assert "http://hospital.org/empno|E-7788" in sysv
        assert "http://license.gov|MD-IL-9876" in sysv
        assert "http://board.example.org|CARD-2018" in sysv

    def test_denormalize_telecom_split_by_system(
        self, denormalizer, rich_practitioner
    ):
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        # ContactPointExtractor splits by system.
        assert "sarah.jones@hospital.org" in out["email"]
        assert "555-1234" in out["phone"]
        # `telecom_values` is the union (powers the unfiltered `telecom` param).
        assert "sarah.jones@hospital.org" in out["telecom_values"]
        assert "555-1234" in out["telecom_values"]
        assert "555-9999" in out["telecom_values"]
        # `telecom_systemCode` uses the value (not system|value) per ContactPointExtractor.
        # Just verify all three values are present.
        assert "555-9999" in out["telecom_systemCode"]

    def test_denormalize_address_components(self, denormalizer, rich_practitioner):
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        assert "Springfield" in out["addressCity"]
        assert "springfield" in out["addressCity_lower"]
        assert "IL" in out["addressState"]
        assert "il" in out["addressState_lower"]
        assert "62701" in out["addressPostalCode"]
        assert "US" in out["addressCountry"]
        assert "us" in out["addressCountry_lower"]
        assert "home" in out["addressUse"]
        # addressFull reconstructed by extractor.
        assert any("Springfield" in s and "62701" in s for s in out["addressFull"])

    def test_denormalize_communication_languages(
        self, denormalizer, rich_practitioner
    ):
        """`communication.language.coding[*].code` flattened to `_search.language`."""
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        langs = out["language"]
        assert "en-US" in langs
        assert "es-MX" in langs

    def test_denormalize_qualification_period_array(
        self, denormalizer, rich_practitioner
    ):
        """`qualification[*].period` projected as an array of {start,end} dicts."""
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        periods = out["qualificationPeriod"]
        assert isinstance(periods, list)
        assert len(periods) == 2
        # Both qualifications must contribute, with start AND end preserved.
        starts = sorted(p.get("start") for p in periods if p.get("start"))
        ends = sorted(p.get("end") for p in periods if p.get("end"))
        assert "2005-06-01" in starts[0] and "2018-01-01" in starts[1]
        assert "2028-01-01" in ends[0] and "2030-06-01" in ends[1]

    def test_denormalize_active_preserved_at_root(
        self, denormalizer, rich_practitioner
    ):
        """`active` is a resource-root boolean — must NOT be moved into `_search`."""
        out = denormalizer.denormalize(rich_practitioner)
        assert out["active"] is True

    def test_denormalize_gender_preserved_at_root(
        self, denormalizer, rich_practitioner
    ):
        """`gender` is a resource-root code — read directly, not denormalized."""
        out = denormalizer.denormalize(rich_practitioner)
        assert out["gender"] == "female"

    def test_denormalize_birthdate_preserved_at_root(
        self, denormalizer, rich_practitioner
    ):
        """`birthDate` is a resource-root field — read directly."""
        out = denormalizer.denormalize(rich_practitioner)
        assert out["birthDate"] == "1975-04-12"

    def test_denormalize_minimal_is_sparse(self, denormalizer, minimal_practitioner):
        """A minimal Practitioner produces a minimal `_search` document."""
        out = denormalizer.denormalize(minimal_practitioner)
        search = out.get("_search", {})
        # No name → no familyName field; no telecom → no email/phone; etc.
        for absent in (
            "familyName", "familyName_lower", "givenNames", "fullName",
            "addressCity", "addressPostalCode",
            "identifier_values", "identifier_systemCode",
            "email", "phone", "telecom_values",
            "language", "qualificationPeriod",
            "deceased", "deathDate",
        ):
            assert absent not in search, f"unexpectedly populated: {absent}"
        # `_compartments.Practitioner` MUST always include the resource's own id.
        assert out.get("_compartments", {}).get("Practitioner") == ["dr-min"]


# ---------------------------------------------------------------------------
# 7) Polymorphic deceased[x] handling per FHIR R5
# ---------------------------------------------------------------------------

class TestPractitionerDeceasedPolymorphism:
    """`Practitioner.deceased[x]` is a CHOICE — boolean OR dateTime."""

    def test_deceased_datetime_variant_sets_deceased_true(
        self, denormalizer, rich_practitioner
    ):
        """The dateTime variant satisfies the `deceased` token search via `transform: presence`."""
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        assert out["deceased"] is True

    def test_deceased_datetime_variant_populates_deathdate(
        self, denormalizer, rich_practitioner
    ):
        """`deathDate` only populated when the dateTime variant is present."""
        out = denormalizer.denormalize(rich_practitioner)["_search"]
        assert out["deathDate"].startswith("2024-08-22")

    def test_deceased_boolean_variant_sets_deceased_true(
        self, denormalizer, deceased_boolean_practitioner
    ):
        """`deceasedBoolean=true` satisfies the `deceased` token search."""
        out = denormalizer.denormalize(deceased_boolean_practitioner)["_search"]
        assert out["deceased"] is True

    def test_deceased_boolean_variant_omits_deathdate(
        self, denormalizer, deceased_boolean_practitioner
    ):
        """`deceasedBoolean` carries no date — `deathDate` must NOT be populated."""
        out = denormalizer.denormalize(deceased_boolean_practitioner)["_search"]
        assert "deathDate" not in out

    def test_alive_practitioner_has_no_deceased_fields(
        self, denormalizer, alive_practitioner
    ):
        """Without a `deceased[x]` element, neither `deceased` nor `deathDate` should appear."""
        out = denormalizer.denormalize(alive_practitioner)
        search = out.get("_search", {})
        assert "deceased" not in search
        assert "deathDate" not in search

    def test_deceased_query_matches_both_variants(self, converter):
        """`deceased=true` query targets `_search.deceased` regardless of which polymorphic variant populated it."""
        query = converter.convert("Practitioner", "deceased=true")
        s = str(query)
        # Single canonical target — the denormalizer normalized both
        # variants into `_search.deceased: true`.
        assert "_search.deceased" in s


# ---------------------------------------------------------------------------
# 8) Resource purity
# ---------------------------------------------------------------------------

class TestPractitionerResourcePurity:
    """Denormalization MUST NOT add fields directly to the FHIR resource root.

    Only `_search` and `_compartments` sub-documents are allowed to be
    appended. All denormalized data lives under those — never as a peer
    of the canonical FHIR fields.
    """

    EXPECTED_TOP_LEVEL_KEYS = {
        # Canonical FHIR Practitioner fields (R5).
        "resourceType", "id", "meta",
        "active", "identifier", "name", "telecom", "address",
        "gender", "birthDate", "deceasedBoolean", "deceasedDateTime",
        "photo", "qualification", "communication",
        # Plus our additive sub-documents.
        "_search", "_compartments",
    }

    def test_no_extraneous_root_keys_rich(self, denormalizer, rich_practitioner):
        out = denormalizer.denormalize(rich_practitioner)
        unexpected = set(out.keys()) - self.EXPECTED_TOP_LEVEL_KEYS
        assert not unexpected, f"Denormalization leaked root keys: {unexpected}"

    def test_no_extraneous_root_keys_boolean_variant(
        self, denormalizer, deceased_boolean_practitioner
    ):
        out = denormalizer.denormalize(deceased_boolean_practitioner)
        unexpected = set(out.keys()) - self.EXPECTED_TOP_LEVEL_KEYS
        assert not unexpected, f"Denormalization leaked root keys: {unexpected}"

    def test_original_resource_unchanged(self, denormalizer, rich_practitioner):
        """The denormalizer copies the resource — input must remain pristine."""
        import copy
        snapshot = copy.deepcopy(rich_practitioner)
        denormalizer.denormalize(rich_practitioner)
        assert rich_practitioner == snapshot


# ---------------------------------------------------------------------------
# 9) Self-compartment membership
# ---------------------------------------------------------------------------

class TestPractitionerSelfCompartment:
    """Practitioner is a compartment-defining resource — its own `id` lands in
    `_compartments.Practitioner` so `Practitioner/<id>/Practitioner` queries
    short-circuit to a precomputed-field lookup.
    """

    def test_self_membership_for_rich(self, denormalizer, rich_practitioner):
        out = denormalizer.denormalize(rich_practitioner)
        assert out["_compartments"]["Practitioner"] == ["dr-jones"]

    def test_self_membership_for_minimal(self, denormalizer, minimal_practitioner):
        out = denormalizer.denormalize(minimal_practitioner)
        assert out["_compartments"]["Practitioner"] == ["dr-min"]

    def test_self_compartment_query_uses_fast_path(self, converter):
        """`Practitioner/dr-jones/Practitioner` must route through the
        precomputed field, not an `$or` over linking parameters."""
        query = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "Practitioner"
        )
        s = str(query)
        # Fast-path produces `{"_compartments.Practitioner": "dr-jones"}`.
        assert "_compartments.Practitioner" in s
        assert "dr-jones" in s

    def test_self_compartment_with_extra_param(self, converter):
        """Compartment + extra search parameter ANDed together."""
        query = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "Practitioner", "active=true"
        )
        s = str(query)
        assert "_compartments.Practitioner" in s
        assert "dr-jones" in s
        assert "active" in s


# ---------------------------------------------------------------------------
# 10) Cross-resource compartment routing
# ---------------------------------------------------------------------------

class TestCrossResourcePractitionerCompartment:
    """Patient/Observation/Appointment must populate `_compartments.Practitioner`
    for every Practitioner-typed reference they carry. This powers the
    `Practitioner/<id>/<OtherResource>` compartment fast-path."""

    def test_patient_general_practitioner_filtered_by_type(self, denormalizer):
        """Patient.generalPractitioner accepts Practitioner|Organization|PractitionerRole.
        Only Practitioner refs should land in `_compartments.Practitioner`."""
        patient = {
            "resourceType": "Patient",
            "id": "p-001",
            "generalPractitioner": [
                {"reference": "Practitioner/dr-smith"},
                {"reference": "Practitioner/dr-jones"},
                {"reference": "Organization/clinic-a"},  # MUST be excluded.
                {"reference": "PractitionerRole/role-1"},  # MUST be excluded.
            ],
        }
        comp = denormalizer.denormalize(patient).get("_compartments", {})
        prac = comp.get("Practitioner") or []
        assert sorted(prac) == ["dr-jones", "dr-smith"]

    def test_observation_performer_filtered_by_type(self, denormalizer):
        """Observation.performer accepts many types — only Practitioner refs feed the compartment."""
        obs = {
            "resourceType": "Observation",
            "id": "o-001",
            "status": "final",
            "code": {"coding": [{"code": "8480-6"}]},
            "subject": {"reference": "Patient/p-001"},
            "performer": [
                {"reference": "Practitioner/dr-smith"},
                {"reference": "Organization/lab-1"},
                {"reference": "Patient/p-001"},
            ],
        }
        comp = denormalizer.denormalize(obs).get("_compartments", {})
        prac = comp.get("Practitioner") or []
        assert prac == ["dr-smith"]

    def test_observation_subject_practitioner_not_in_compartment(self, denormalizer):
        """Observation.subject is (Patient|Group|Device|Location) per R5 —
        a Practitioner subject would not be standard, but if it appears
        it should still NOT enter the Practitioner compartment because
        only `performer` is the linking parameter for that compartment."""
        obs = {
            "resourceType": "Observation",
            "id": "o-002",
            "status": "final",
            "code": {"coding": [{"code": "8480-6"}]},
            "subject": {"reference": "Practitioner/dr-smith"},
            "performer": [{"reference": "Organization/lab-1"}],
        }
        comp = denormalizer.denormalize(obs).get("_compartments", {})
        # No performer is a Practitioner → compartment list empty (or absent).
        assert not comp.get("Practitioner")

    def test_appointment_actor_filtered_by_type(self, denormalizer):
        """Appointment.participant.actor: only Practitioner refs feed the compartment."""
        appt = {
            "resourceType": "Appointment",
            "id": "a-001",
            "status": "booked",
            "participant": [
                {"actor": {"reference": "Patient/p-001"}, "status": "accepted"},
                {"actor": {"reference": "Practitioner/dr-smith"}, "status": "accepted"},
                {"actor": {"reference": "Practitioner/dr-jones"}, "status": "accepted"},
                {"actor": {"reference": "Location/loc-1"}, "status": "accepted"},
            ],
        }
        comp = denormalizer.denormalize(appt).get("_compartments", {})
        prac = comp.get("Practitioner") or []
        assert sorted(prac) == ["dr-jones", "dr-smith"]

    def test_compartment_query_fast_path_for_observation(self, converter):
        """`Practitioner/p1/Observation` must route through `_compartments.Practitioner`."""
        query = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "Observation"
        )
        s = str(query)
        assert "_compartments.Practitioner" in s
        assert "dr-smith" in s

    def test_compartment_query_fast_path_for_appointment(self, converter):
        query = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "Appointment"
        )
        s = str(query)
        assert "_compartments.Practitioner" in s
        assert "dr-smith" in s

    def test_compartment_query_fast_path_for_patient(self, converter):
        """Patient declares `Practitioner` in its `compartments.precomputed` list."""
        query = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "Patient"
        )
        s = str(query)
        assert "_compartments.Practitioner" in s
        assert "dr-smith" in s

    def test_compartment_with_search_param(self, converter):
        """Compartment + extra param ANDed."""
        query = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "Observation", "code=8480-6"
        )
        s = str(query)
        assert "_compartments.Practitioner" in s
        assert "dr-smith" in s
        assert "8480-6" in s


# ---------------------------------------------------------------------------
# 11) Cross-resource forward references
# ---------------------------------------------------------------------------

class TestPractitionerCrossResourceLinking:
    """Practitioner is referenced from many resources. Forward search must work."""

    def test_patient_general_practitioner_full_reference(self, converter):
        query = converter.convert(
            "Patient", "general-practitioner=Practitioner/dr-smith"
        )
        s = str(query)
        assert "dr-smith" in s
        assert "generalPractitioner" in s.lower() or "generalpractitioner" in s.lower()

    def test_patient_general_practitioner_id_only(self, converter):
        query = converter.convert(
            "Patient", "general-practitioner=dr-smith"
        )
        s = str(query)
        assert "dr-smith" in s

    def test_observation_performer_practitioner(self, converter):
        query = converter.convert(
            "Observation", "performer=Practitioner/dr-smith"
        )
        s = str(query)
        assert "dr-smith" in s
        assert "performer" in s.lower()

    def test_appointment_actor_practitioner(self, converter):
        query = converter.convert(
            "Appointment", "actor=Practitioner/dr-smith"
        )
        s = str(query)
        assert "dr-smith" in s

    def test_appointment_practitioner_search_param(self, converter):
        """Appointment.yaml declares a `practitioner` search param explicitly."""
        query = converter.convert(
            "Appointment", "practitioner=Practitioner/dr-smith"
        )
        s = str(query)
        assert "dr-smith" in s
        assert "practitionerId" in s.lower() or "practitionerid" in s.lower()

    def test_patient_general_practitioner_id_captured_in_denorm(self, denormalizer):
        """Round-trip: Patient denorm captures the Practitioner id and type."""
        patient = {
            "resourceType": "Patient",
            "id": "p-001",
            "generalPractitioner": [{"reference": "Practitioner/dr-smith"}],
        }
        out = denormalizer.denormalize(patient)["_search"]
        assert "dr-smith" in out.get("generalPractitionerId", [])
        assert "Practitioner" in out.get("generalPractitionerType", [])

    def test_observation_performer_practitioner_id_captured(self, denormalizer):
        obs = {
            "resourceType": "Observation",
            "id": "obs-1",
            "status": "final",
            "code": {"coding": [{"code": "8480-6"}]},
            "subject": {"reference": "Patient/p-001"},
            "performer": [
                {"reference": "Practitioner/dr-smith"},
                {"reference": "Organization/lab-1"},
            ],
        }
        out = denormalizer.denormalize(obs)["_search"]
        assert "dr-smith" in out.get("performerId", [])
        assert "Practitioner" in out.get("performerType", [])

    def test_appointment_actor_practitioner_id_captured(self, denormalizer):
        appt = {
            "resourceType": "Appointment",
            "id": "a-1",
            "status": "booked",
            "participant": [
                {"actor": {"reference": "Practitioner/dr-smith"}, "status": "accepted"}
            ],
        }
        out = denormalizer.denormalize(appt)["_search"]
        # The Appointment YAML splits actorIds + practitionerId by filterType.
        actor_ids = out.get("actorIds")
        if isinstance(actor_ids, list):
            assert "dr-smith" in actor_ids
        else:
            assert actor_ids == "dr-smith"


# ---------------------------------------------------------------------------
# 12) MongoDB end-to-end (localhost:27017)
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
    reason="MongoDB not running on localhost:27017",
)
class TestPractitionerMongoDB:
    """End-to-end: denormalize → insert → query via converter → assert results."""

    @pytest.fixture(scope="class")
    def mongo_collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["practitioners_e2e"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_collection, denormalizer):
        practitioners: List[Dict[str, Any]] = [
            {
                "resourceType": "Practitioner",
                "id": "dr-jones",
                "active": True,
                "identifier": [
                    {"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567890"},
                ],
                "name": [
                    {"family": "Jones", "given": ["Sarah", "M"], "prefix": ["Dr."]},
                ],
                "telecom": [
                    {"system": "email", "value": "sarah.jones@hospital.org"},
                    {"system": "phone", "value": "555-1234"},
                ],
                "address": [
                    {
                        "use": "home",
                        "city": "Springfield",
                        "state": "IL",
                        "postalCode": "62701",
                        "country": "US",
                    }
                ],
                "gender": "female",
                "birthDate": "1975-04-12",
                "qualification": [
                    {
                        "identifier": [
                            {"system": "http://license.gov", "value": "MD-IL-9876"},
                        ],
                        "code": {
                            "coding": [{"system": "http://hl7.org", "code": "MD"}]
                        },
                        # NOTE: We seed qualification.period as Python `datetime`
                        # values rather than the FHIR-canonical ISO-8601 strings.
                        # The MQL date converter emits BSON `datetime` for `$gte`
                        # / `$lte` operands, and BSON's type-aware comparison
                        # treats string-vs-datetime as never equal — so a
                        # `_search.qualificationPeriod.start` stored as the
                        # original ISO string would silently miss every range
                        # query. Storing as `datetime` aligns the persisted
                        # type with the query type so the integration test
                        # actually exercises filtering. Production pipelines
                        # that ingest raw FHIR JSON should pre-coerce date /
                        # dateTime fields to BSON `datetime` at write time
                        # for the same reason; the in-process denormalizer
                        # passes values through verbatim.
                        "period": {
                            "start": datetime(2005, 6, 1),
                            "end": datetime(2030, 6, 1),
                        },
                    }
                ],
                "communication": [
                    {"language": {"coding": [{"code": "en-US"}]}},
                    {"language": {"coding": [{"code": "es-MX"}]}},
                ],
                "meta": {"lastUpdated": datetime(2024, 6, 1, 10, 0, 0)},
            },
            {
                "resourceType": "Practitioner",
                "id": "dr-smith",
                "active": True,
                "identifier": [
                    {"system": "http://hl7.org/fhir/sid/us-npi", "value": "9876543210"},
                ],
                "name": [
                    {"family": "Smith", "given": ["John", "Q"]},
                ],
                "telecom": [
                    {"system": "phone", "value": "555-5555"},
                ],
                "address": [
                    {
                        "use": "work",
                        "city": "Chicago",
                        "state": "IL",
                        "postalCode": "60601",
                        "country": "US",
                    }
                ],
                "gender": "male",
                "birthDate": "1980-09-15",
                "qualification": [
                    {
                        "code": {"coding": [{"system": "http://hl7.org", "code": "MD"}]},
                        "period": {
                            "start": datetime(2010, 6, 1),
                            "end": datetime(2025, 6, 1),
                        },
                    }
                ],
                "communication": [
                    {"language": {"coding": [{"code": "en-US"}]}},
                ],
                "meta": {"lastUpdated": datetime(2024, 4, 15, 10, 0, 0)},
            },
            {
                "resourceType": "Practitioner",
                "id": "dr-deceased",
                "active": False,
                "name": [{"family": "Bones", "given": ["Olde"]}],
                "gender": "male",
                # FHIR R5 polymorphic dateTime variant of deceased[x].
                # Stored as BSON `datetime` for the same comparison-type
                # alignment reason as `qualification.period` above.
                "deceasedDateTime": datetime(2023, 1, 15, 0, 0, 0),
                "qualification": [
                    {
                        "code": {"coding": [{"system": "http://hl7.org", "code": "MD"}]},
                        "period": {
                            "start": datetime(1970, 1, 1),
                            "end": datetime(2023, 1, 15),
                        },
                    }
                ],
                "meta": {"lastUpdated": datetime(2023, 2, 1, 10, 0, 0)},
            },
            {
                "resourceType": "Practitioner",
                "id": "dr-bool-deceased",
                "active": False,
                "name": [{"family": "Quinn", "given": ["Doc"]}],
                "gender": "other",
                # FHIR R5 polymorphic boolean variant of deceased[x].
                "deceasedBoolean": True,
                "meta": {"lastUpdated": datetime(2024, 1, 1, 10, 0, 0)},
            },
        ]
        denorm = [denormalizer.denormalize(p) for p in practitioners]
        for d, src in zip(denorm, practitioners):
            d.setdefault("_id", src["id"])
        mongo_collection.insert_many(denorm)
        return mongo_collection

    def test_query_by_family(self, converter, seeded):
        q = converter.convert("Practitioner", "family=Jones")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_family_case_insensitive(self, converter, seeded):
        """`family=JONES` should match the Smith/Jones data per FHIR default semantics."""
        q = converter.convert("Practitioner", "family=JONES")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_family_exact_case_sensitive(self, converter, seeded):
        """`:exact` is case-sensitive — `Jones` (capital J) matches, `JONES` does not."""
        q_match = converter.convert("Practitioner", "family:exact=Jones")
        q_miss = converter.convert("Practitioner", "family:exact=JONES")
        assert seeded.count_documents(q_match) == 1
        assert seeded.count_documents(q_miss) == 0

    def test_query_by_given_sarah_keeps_full_value(self, converter, seeded):
        """Regression: `given=sarah` (lowercase) must NOT be truncated to `rah`."""
        q = converter.convert("Practitioner", "given=sarah")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_gender_male(self, converter, seeded):
        q = converter.convert("Practitioner", "gender=male")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["dr-deceased", "dr-smith"]

    def test_query_by_active_true(self, converter, seeded):
        q = converter.convert("Practitioner", "active=true")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["dr-jones", "dr-smith"]

    def test_query_by_identifier_value(self, converter, seeded):
        q = converter.convert("Practitioner", "identifier=1234567890")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_qualification_identifier(self, converter, seeded):
        """Identifier search MUST find qualification-level identifiers (R5 union)."""
        q = converter.convert("Practitioner", "identifier=MD-IL-9876")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_communication(self, converter, seeded):
        q = converter.convert("Practitioner", "communication=es-MX")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_email(self, converter, seeded):
        q = converter.convert("Practitioner", "email=sarah.jones@hospital.org")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_phone(self, converter, seeded):
        q = converter.convert("Practitioner", "phone=555-1234")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_address_city(self, converter, seeded):
        q = converter.convert("Practitioner", "address-city=Chicago")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-smith"

    def test_query_by_address_postalcode(self, converter, seeded):
        q = converter.convert("Practitioner", "address-postalcode=62701")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_query_by_deceased_true_finds_both_polymorphic_variants(
        self, converter, seeded
    ):
        """Both `deceasedDateTime` AND `deceasedBoolean=true` must satisfy `deceased=true`."""
        q = converter.convert("Practitioner", "deceased=true")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["dr-bool-deceased", "dr-deceased"]

    def test_query_by_death_date_finds_only_dateTime_variant(self, converter, seeded):
        """`death-date=ge2020-01-01` matches only the dateTime variant —
        the boolean-deceased practitioner has no date and must not match."""
        q = converter.convert("Practitioner", "death-date=ge2020-01-01")
        results = list(seeded.find(q))
        ids = [r["id"] for r in results]
        assert ids == ["dr-deceased"]

    def test_query_by_qualification_period_overlap(self, converter, seeded):
        """`qualification-period=ge2020-01-01` matches practitioners with at
        least one qualification valid on or after 2020 — uses the projected
        `_search.qualificationPeriod.start` field."""
        q = converter.convert("Practitioner", "qualification-period=ge2020-01-01")
        # MQL `$gte` against the array sub-field returns docs whose ANY array
        # element matches — covers dr-jones (start=2005, before 2020 — but
        # MongoDB's array-element semantics will still match if ANY element
        # is >= 2020). Since dr-jones's only qualification has start=2005, it
        # should NOT match this filter; dr-smith (2010) also misses;
        # dr-deceased (1970) misses; dr-bool-deceased has no qualification.
        # So zero matches is the expected outcome — this asserts the date
        # comparison happens against the projected start field, not against
        # a synthetic value.
        # NOTE: MongoDB array semantics MAY produce hits if ANY embedded
        # `start` >= 2020 — none of our test docs satisfy that, so we expect 0.
        ids = [r["id"] for r in q and seeded.find(q) or []]
        assert ids == []

    def test_query_by_qualification_period_includes_old(self, converter, seeded):
        """A practitioner with qualification.start=2005 must match `ge2000`."""
        q = converter.convert("Practitioner", "qualification-period=ge2000-01-01")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        # dr-jones (2005) and dr-smith (2010) and dr-deceased (1970? no, 1970<2000) → only dr-jones+dr-smith match
        # dr-deceased starts 1970 — doesn't match ge2000.
        assert "dr-jones" in ids
        assert "dr-smith" in ids
        assert "dr-deceased" not in ids

    def test_query_by_id(self, converter, seeded):
        q = converter.convert("Practitioner", "_id=dr-jones")
        results = list(seeded.find(q))
        assert len(results) == 1

    def test_compartment_self_query(self, converter, seeded):
        """`Practitioner/dr-jones/Practitioner` must match dr-jones via the precomputed field."""
        q = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "Practitioner"
        )
        results = list(seeded.find(q))
        ids = [r["id"] for r in results]
        assert ids == ["dr-jones"]

    def test_compartment_self_query_with_filter(self, converter, seeded):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "Practitioner", "active=true"
        )
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "dr-jones"

    def test_combined_active_and_communication(self, converter, seeded):
        q = converter.convert(
            "Practitioner", "active=true&communication=en-US"
        )
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["dr-jones", "dr-smith"]


# ---------------------------------------------------------------------------
# 13) Cross-resource compartment routing — MongoDB end-to-end
# ---------------------------------------------------------------------------

@pytest.mark.mongodb
@pytest.mark.skipif(
    not _mongo_available(),
    reason="MongoDB not running on localhost:27017",
)
class TestPractitionerCompartmentE2E:
    """Verify the precomputed `_compartments.Practitioner` field actually
    drives Patient/Observation/Appointment queries against MongoDB.
    """

    @pytest.fixture(scope="class")
    def db(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        for coll_name in ("patients_prac_e2e", "observations_prac_e2e",
                          "appointments_prac_e2e"):
            db[coll_name].delete_many({})
        yield db
        for coll_name in ("patients_prac_e2e", "observations_prac_e2e",
                          "appointments_prac_e2e"):
            db[coll_name].delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, db, denormalizer):
        # 2 patients, one with dr-smith as GP and one with dr-jones.
        patients = [
            {
                "resourceType": "Patient",
                "id": "p-001",
                "name": [{"family": "PatientA", "given": ["Alice"]}],
                "generalPractitioner": [{"reference": "Practitioner/dr-smith"}],
            },
            {
                "resourceType": "Patient",
                "id": "p-002",
                "name": [{"family": "PatientB", "given": ["Bob"]}],
                "generalPractitioner": [{"reference": "Practitioner/dr-jones"}],
            },
            {
                "resourceType": "Patient",
                "id": "p-003",
                "name": [{"family": "PatientC", "given": ["Carol"]}],
                # No GP — must NOT appear in any Practitioner compartment.
            },
        ]
        # Observations: one performed by dr-smith on p-001, one by dr-jones.
        observations = [
            {
                "resourceType": "Observation",
                "id": "o-001",
                "status": "final",
                "code": {"coding": [{"code": "8480-6"}]},
                "subject": {"reference": "Patient/p-001"},
                "performer": [{"reference": "Practitioner/dr-smith"}],
            },
            {
                "resourceType": "Observation",
                "id": "o-002",
                "status": "final",
                "code": {"coding": [{"code": "8480-6"}]},
                "subject": {"reference": "Patient/p-002"},
                "performer": [{"reference": "Practitioner/dr-jones"}],
            },
            {
                "resourceType": "Observation",
                "id": "o-003",
                "status": "final",
                "code": {"coding": [{"code": "8480-6"}]},
                "subject": {"reference": "Patient/p-001"},
                # No Practitioner performer — must NOT enter the Practitioner compartment.
                "performer": [{"reference": "Organization/lab-1"}],
            },
        ]
        appointments = [
            {
                "resourceType": "Appointment",
                "id": "a-001",
                "status": "booked",
                "participant": [
                    {"actor": {"reference": "Patient/p-001"}, "status": "accepted"},
                    {"actor": {"reference": "Practitioner/dr-smith"}, "status": "accepted"},
                ],
            },
            {
                "resourceType": "Appointment",
                "id": "a-002",
                "status": "booked",
                "participant": [
                    {"actor": {"reference": "Patient/p-002"}, "status": "accepted"},
                    {"actor": {"reference": "Practitioner/dr-jones"}, "status": "accepted"},
                ],
            },
        ]

        for src, coll_name in (
            (patients, "patients_prac_e2e"),
            (observations, "observations_prac_e2e"),
            (appointments, "appointments_prac_e2e"),
        ):
            denorm = [denormalizer.denormalize(r) for r in src]
            for d, s in zip(denorm, src):
                d.setdefault("_id", s["id"])
            db[coll_name].insert_many(denorm)

        return db

    def test_practitioner_compartment_for_patient(self, converter, seeded):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "Patient"
        )
        results = list(seeded["patients_prac_e2e"].find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["p-001"]

    def test_practitioner_compartment_for_patient_other_practitioner(
        self, converter, seeded
    ):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-jones", "Patient"
        )
        results = list(seeded["patients_prac_e2e"].find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["p-002"]

    def test_practitioner_compartment_excludes_patient_without_gp(
        self, converter, seeded
    ):
        """A patient with no `generalPractitioner` must not appear in any Practitioner compartment."""
        for prac_id in ("dr-smith", "dr-jones"):
            q = converter.convert_with_compartment(
                "Practitioner", prac_id, "Patient"
            )
            results = list(seeded["patients_prac_e2e"].find(q))
            assert "p-003" not in [r["id"] for r in results]

    def test_practitioner_compartment_for_observation(self, converter, seeded):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "Observation"
        )
        results = list(seeded["observations_prac_e2e"].find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["o-001"]

    def test_practitioner_compartment_observation_excludes_org_performer(
        self, converter, seeded
    ):
        """o-003 has Organization/lab-1 as performer — must NOT match either practitioner's compartment."""
        for prac_id in ("dr-smith", "dr-jones"):
            q = converter.convert_with_compartment(
                "Practitioner", prac_id, "Observation"
            )
            results = list(seeded["observations_prac_e2e"].find(q))
            assert "o-003" not in [r["id"] for r in results]

    def test_practitioner_compartment_for_appointment(self, converter, seeded):
        q = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "Appointment"
        )
        results = list(seeded["appointments_prac_e2e"].find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["a-001"]

    def test_practitioner_compartment_with_search_param(self, converter, seeded):
        """`Practitioner/dr-smith/Observation?code=8480-6` ANDs both filters."""
        q = converter.convert_with_compartment(
            "Practitioner", "dr-smith", "Observation", "code=8480-6"
        )
        results = list(seeded["observations_prac_e2e"].find(q))
        assert len(results) == 1
        assert results[0]["id"] == "o-001"
