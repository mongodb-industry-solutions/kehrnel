"""
Comprehensive integration tests for ALL Location search parameters per FHIR R5.

References:
- https://www.hl7.org/fhir/location-definitions.html
- https://www.hl7.org/fhir/location-search.html

This suite ensures complete coverage of the 15 R5 spec parameters declared in
``configs/Location.yaml`` (plus the common params ``_id`` and ``_lastUpdated``):

  - Tokens (6):     status, operational-status, type, characteristic,
                    identifier, address-use, _id
  - Strings (6):    name, address, address-city, address-country,
                    address-postalcode, address-state
  - References (3): organization, partof, endpoint
  - Common (2):     _id, _lastUpdated

Two FHIR R5 Location parameters are intentionally OUT of scope of this
config (and therefore of this test suite):
  - ``near``     (geospatial, requires 2dsphere + GeoJSON projection)
  - ``contains`` (extension-based GeoJSON polygon)

Plus:
- Denormalization correctness against the generic data-type extractors
  (``TextExtractor`` for name/alias/text, ``IdentifierExtractor``,
  ``CodeableConceptExtractor`` for type/characteristic, ``CodingExtractor``
  for the bare-Coding ``operationalStatus``, ``AddressExtractor`` for the
  singleton address, ``ReferenceExtractor`` for managingOrganization /
  partOf / endpoint).
- Cross-resource linking: Location is referenced by Appointment
  (``participant.actor`` with type=Location) and Observation
  (``subject`` with type=Location). Tests verify those forward-search
  paths resolve as expected. Location itself references Organization
  (``managingOrganization``) and Location (``partOf``).
- Compartments: Location is **NOT** in any FHIR compartment per
  https://www.hl7.org/fhir/compartmentdefinition.html, so this file does
  NOT contain compartment-membership precompute tests.
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
def rich_location() -> Dict[str, Any]:
    """A Location with most R5 fields populated for deep extractor testing."""
    return {
        "resourceType": "Location",
        "id": "loc-rich-1",
        "meta": {"lastUpdated": "2024-06-01T10:00:00Z"},
        "status": "active",
        "identifier": [
            {
                "system": "http://example.org/loc",
                "value": "LOC-12345",
                "use": "official",
            }
        ],
        "operationalStatus": {
            "system": "http://terminology.hl7.org/CodeSystem/v2-0116",
            "code": "O",
            "display": "Occupied",
        },
        "name": "Acme General ICU Bed 12",
        "alias": ["ICU-12", "Bed 12"],
        "description": "Bed 12 in the ICU.",
        "mode": "instance",
        "type": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": "BD",
                        "display": "Bed",
                    }
                ]
            },
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": "ICU",
                    }
                ]
            },
        ],
        "characteristic": [
            {
                "coding": [
                    {
                        "system": "http://example.org/cs",
                        "code": "wheelchair-accessible",
                    }
                ]
            }
        ],
        "address": {
            "use": "work",
            "line": ["100 Main St"],
            "city": "Springfield",
            "state": "IL",
            "postalCode": "62701",
            "country": "USA",
        },
        "position": {
            "longitude": -89.65,
            "latitude": 39.78,
            "altitude": 0,
        },
        "managingOrganization": {"reference": "Organization/org-rich-1"},
        "partOf": {"reference": "Location/loc-parent"},
        "endpoint": [
            {"reference": "Endpoint/ep-1"},
            {"reference": "Endpoint/ep-2"},
        ],
    }


@pytest.fixture
def minimal_location() -> Dict[str, Any]:
    """Sparsely-populated Location (only minimal field set)."""
    return {
        "resourceType": "Location",
        "id": "loc-min-1",
        "name": "Empty Room",
    }


# ---------------------------------------------------------------------------
# 1) String parameters
# ---------------------------------------------------------------------------

class TestLocationStringParameters:
    """All Location string search parameters per FHIR R5."""

    def test_name_search(self, converter):
        """`name` searches BOTH Location.name AND Location.alias."""
        query = converter.convert("Location", "name=ICU")
        s = str(query)
        assert "icu" in s.lower()
        assert "$or" in s

    def test_name_search_targets_aliases_field(self, converter):
        query = converter.convert("Location", "name=Bed")
        s = str(query)
        assert "_search.aliases_lower" in s or "aliases_lower" in s.lower()

    def test_name_search_targets_namealias_field(self, converter):
        query = converter.convert("Location", "name=Acme")
        s = str(query)
        assert "_search.nameAlias_lower" in s or "namealias_lower" in s.lower()

    def test_address_general_search(self, converter):
        query = converter.convert("Location", "address=Springfield")
        s = str(query)
        assert "springfield" in s.lower()

    def test_address_city_search(self, converter):
        query = converter.convert("Location", "address-city=Springfield")
        s = str(query)
        assert "springfield" in s.lower()

    def test_address_state_search(self, converter):
        query = converter.convert("Location", "address-state=Illinois")
        s = str(query)
        assert "illinois" in s.lower()

    def test_address_postalcode_search(self, converter):
        query = converter.convert("Location", "address-postalcode=62701")
        s = str(query)
        assert "62701" in s

    def test_address_country_search(self, converter):
        query = converter.convert("Location", "address-country=USA")
        s = str(query)
        assert "usa" in s.lower()


# ---------------------------------------------------------------------------
# 2) Token parameters
# ---------------------------------------------------------------------------

class TestLocationTokenParameters:
    """All Location token search parameters."""

    def test_status_active(self, converter):
        query = converter.convert("Location", "status=active")
        s = str(query)
        assert "active" in s

    def test_status_inactive(self, converter):
        query = converter.convert("Location", "status=inactive")
        s = str(query)
        assert "inactive" in s

    def test_status_suspended(self, converter):
        query = converter.convert("Location", "status=suspended")
        s = str(query)
        assert "suspended" in s

    def test_operational_status_with_system(self, converter):
        query = converter.convert(
            "Location",
            "operational-status=http://terminology.hl7.org/CodeSystem/v2-0116|O")
        s = str(query)
        assert "v2-0116" in s
        assert "O" in s

    def test_operational_status_value_only(self, converter):
        query = converter.convert("Location", "operational-status=O")
        s = str(query)
        assert "O" in s
        assert "operationalStatus_codes" in s or "operationalstatus_codes" in s.lower()

    def test_type_with_system(self, converter):
        query = converter.convert(
            "Location",
            "type=http://terminology.hl7.org/CodeSystem/v3-RoleCode|BD")
        s = str(query)
        assert "BD" in s
        assert "RoleCode" in s

    def test_type_value_only(self, converter):
        query = converter.convert("Location", "type=ICU")
        s = str(query)
        assert "ICU" in s

    def test_characteristic(self, converter):
        query = converter.convert(
            "Location", "characteristic=wheelchair-accessible"
        )
        s = str(query)
        assert "wheelchair-accessible" in s

    def test_identifier_with_system(self, converter):
        query = converter.convert(
            "Location",
            "identifier=http://example.org/loc|LOC-12345")
        s = str(query)
        assert "LOC-12345" in s
        assert "example.org" in s

    def test_identifier_value_only(self, converter):
        query = converter.convert("Location", "identifier=LOC-12345")
        s = str(query)
        assert "LOC-12345" in s

    def test_address_use_search(self, converter):
        query = converter.convert("Location", "address-use=work")
        s = str(query)
        assert "work" in s

    def test_id_search(self, converter):
        query = converter.convert("Location", "_id=loc-rich-1")
        s = str(query)
        assert "loc-rich-1" in s


# ---------------------------------------------------------------------------
# 3) Date parameter (_lastUpdated only)
# ---------------------------------------------------------------------------

class TestLocationDateParameters:
    def test_lastupdated_eq(self, converter):
        query = converter.convert("Location", "_lastUpdated=2024-06-01")
        s = str(query)
        assert "lastUpdated" in s or "lastupdated" in s.lower()

    def test_lastupdated_ge(self, converter):
        query = converter.convert("Location", "_lastUpdated=ge2024-01-01")
        s = str(query)
        assert "$gte" in s

    def test_lastupdated_le(self, converter):
        query = converter.convert("Location", "_lastUpdated=le2024-12-31")
        s = str(query)
        assert "$lte" in s

    def test_lastupdated_range(self, converter):
        query = converter.convert(
            "Location",
            "_lastUpdated=ge2024-01-01&_lastUpdated=le2024-12-31")
        s = str(query)
        assert "$gte" in s and "$lte" in s


# ---------------------------------------------------------------------------
# 4) Reference parameters
# ---------------------------------------------------------------------------

class TestLocationReferenceParameters:
    def test_organization_full_reference(self, converter):
        query = converter.convert(
            "Location", "organization=Organization/org-rich-1"
        )
        s = str(query)
        assert "org-rich-1" in s
        assert "managingOrganizationId" in s or "managingorganizationid" in s.lower()

    def test_organization_id_only(self, converter):
        query = converter.convert("Location", "organization=org-rich-1")
        s = str(query)
        assert "org-rich-1" in s

    def test_partof_full_reference(self, converter):
        query = converter.convert(
            "Location", "partof=Location/loc-parent"
        )
        s = str(query)
        assert "loc-parent" in s

    def test_partof_id_only(self, converter):
        query = converter.convert("Location", "partof=loc-parent")
        s = str(query)
        assert "loc-parent" in s

    def test_partof_typed_modifier(self, converter):
        """`partof:Location=X` should still resolve correctly."""
        query = converter.convert(
            "Location", "partof:Location=loc-parent"
        )
        s = str(query)
        assert "loc-parent" in s

    def test_endpoint_reference(self, converter):
        query = converter.convert(
            "Location", "endpoint=Endpoint/ep-1"
        )
        s = str(query)
        assert "ep-1" in s


# ---------------------------------------------------------------------------
# 5) Combinations
# ---------------------------------------------------------------------------

class TestLocationCombinations:
    def test_status_and_type(self, converter):
        query = converter.convert(
            "Location", "status=active&type=BD"
        )
        s = str(query)
        assert "active" in s and "BD" in s

    def test_organization_and_status(self, converter):
        query = converter.convert(
            "Location",
            "organization=Organization/org-rich-1&status=active")
        s = str(query)
        assert "org-rich-1" in s and "active" in s

    def test_address_city_and_partof(self, converter):
        query = converter.convert(
            "Location",
            "address-city=Springfield&partof=Location/loc-parent")
        s = str(query)
        assert "springfield" in s.lower() and "loc-parent" in s


# ---------------------------------------------------------------------------
# 6) Modifiers
# ---------------------------------------------------------------------------

class TestLocationModifiers:
    def test_name_exact(self, converter):
        query = converter.convert(
            "Location", "name:exact=Acme General ICU Bed 12"
        )
        s = str(query)
        assert "Acme General" in s or "acme general" in s.lower()

    def test_name_contains(self, converter):
        query = converter.convert("Location", "name:contains=ICU")
        s = str(query)
        assert "icu" in s.lower()

    def test_status_missing(self, converter):
        query = converter.convert("Location", "status:missing=true")
        s = str(query)
        assert "status" in s

    def test_type_not_modifier(self, converter):
        query = converter.convert("Location", "type:not=BD")
        s = str(query)
        assert "BD" in s


# ---------------------------------------------------------------------------
# 6.5) Geospatial special parameters — `near` and `contains`
#
# Per FHIR R5 §8.7.24 (`contains`) and §8.7.28 (`near`), these two are
# typed `special` and require a geospatial query engine + GeoJSON
# projections this conversion layer does not yet implement. R5 §8.7.28
# explicitly permits the server to "report the unused parameter in a
# bundled OperationOutcome and still perform the search ignoring the
# near parameter" if it is unsupported. The converter implements that
# contract: it warns and drops the parameter while the rest of the
# query proceeds.
#
# The tests in this section pin that behavior so a future
# GeoPointExtractor + 2dsphere index implementation can find AND
# update them in one place.
# ---------------------------------------------------------------------------

class TestLocationGeospatialDegradation:
    """`near` and `contains`: spec-conformant graceful degradation."""

    def test_near_alone_yields_empty_query(self, converter, capsys):
        """Bare `near=...` with no other params -> empty MongoDB query.

        R5 §8.7.28 allows the server to ignore an unsupported `near`
        parameter and "still perform the search". With only `near`
        present, the remaining filter is empty, so the converter
        returns `{}` (a match-all query). Callers needing strict
        rejection should consume the underlying
        `UnsupportedParameterError`.
        """
        out = converter.convert("Location", "near=39.78|-89.65|10|km")
        assert out == {}
        captured = capsys.readouterr()
        assert "near" in captured.out and "not supported" in captured.out

    def test_contains_alone_yields_empty_query(self, converter, capsys):
        """Same graceful-degradation contract as `near`."""
        out = converter.convert("Location", "contains=39.78|-89.65")
        assert out == {}
        captured = capsys.readouterr()
        assert "contains" in captured.out and "not supported" in captured.out

    def test_near_combined_with_supported_param_drops_only_near(
        self, converter, capsys):
        """`status=active&near=...` -> the `status` clause is preserved.

        This is the exact behavior R5 §8.7.28 endorses: drop the
        unsupported geospatial term, honor everything else.
        """
        out = converter.convert(
            "Location", "status=active&near=39.78|-89.65|10|km"
        )
        # The status filter must survive.
        assert out.get("status") == "active"
        captured = capsys.readouterr()
        assert "near" in captured.out

    def test_contains_combined_with_supported_param_drops_only_contains(
        self, converter, capsys):
        out = converter.convert(
            "Location", "status=active&contains=39.78|-89.65"
        )
        assert out.get("status") == "active"
        captured = capsys.readouterr()
        assert "contains" in captured.out

    def test_position_is_preserved_on_document(self, denormalizer):
        """`Location.position` MUST stay on the denormalized document.

        This is the contract that lets a future GeoPointExtractor lift
        position into a `_search.position_geojson` GeoJSON Point and
        light up `near` without a schema migration.
        """
        loc = {
            "resourceType": "Location",
            "id": "loc-position-1",
            "name": "Pinned",
            "position": {
                "latitude": 39.78,
                "longitude": -89.65,
                "altitude": 0,
            },
        }
        out = denormalizer.denormalize(loc)
        # `position` survives at the top level...
        assert out.get("position") == {
            "latitude": 39.78,
            "longitude": -89.65,
            "altitude": 0,
        }
        # ...and we deliberately do NOT yet emit a GeoJSON projection.
        # When that ships, this assertion should be flipped to
        # `assert "position_geojson" in out["_search"]`.
        assert "position_geojson" not in out.get("_search", {})


# ---------------------------------------------------------------------------
# 7) Denormalization correctness
# ---------------------------------------------------------------------------

class TestLocationDenormalization:
    """Validate every denormalized field that the search YAML targets."""

    def test_denormalize_name_and_lower(self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert out["name"] == "Acme General ICU Bed 12"
        assert out["name_lower"] == "acme general icu bed 12"

    def test_denormalize_aliases_arrays(self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert sorted(out["aliases"]) == sorted(["ICU-12", "Bed 12"])
        assert sorted(out["aliases_lower"]) == sorted(["icu-12", "bed 12"])

    def test_denormalize_namealias_blob(self, denormalizer, rich_location):
        """`nameAlias_lower` powers the FHIR `name` parameter (name | alias)."""
        out = denormalizer.denormalize(rich_location)["_search"]
        blob = out["nameAlias_lower"]
        assert "acme general" in blob
        assert "icu" in blob
        assert "bed 12" in blob

    def test_denormalize_identifier_values_and_systemcode(
        self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert "LOC-12345" in out["identifier_values"]
        assert "http://example.org/loc|LOC-12345" in out["identifier_systemCode"]

    def test_denormalize_type_codes_and_systemcode(
        self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert "BD" in out["type_codes"]
        assert "ICU" in out["type_codes"]
        sysc = out["type_systemCode"]
        assert any(v.endswith("|BD") for v in sysc)
        assert any(v.endswith("|ICU") for v in sysc)

    def test_denormalize_characteristic_codes_and_systemcode(
        self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert "wheelchair-accessible" in out["characteristic_codes"]
        assert any(
            "|wheelchair-accessible" in v for v in out["characteristic_systemCode"]
        )

    def test_denormalize_operational_status(self, denormalizer, rich_location):
        """`operationalStatus` is a bare Coding, extracted via CodingExtractor."""
        out = denormalizer.denormalize(rich_location)["_search"]
        assert "O" in out["operationalStatus_codes"]
        assert any(
            v.endswith("|O") for v in out["operationalStatus_systemCode"]
        )

    def test_denormalize_address_components(
        self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert "Springfield" in out["addressCity"]
        assert "springfield" in out["addressCity_lower"]
        assert "62701" in out["addressPostalCode"]
        assert "USA" in out["addressCountry"]
        assert "usa" in out["addressCountry_lower"]
        assert "IL" in out["addressState"]
        assert "il" in out["addressState_lower"]
        assert "work" in out["addressUse"]

    def test_denormalize_address_full(self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        full = out["addressFull"]
        # Joined components: line, city, state, postalCode, country
        joined = full[0] if isinstance(full, list) else full
        assert "100 Main St" in joined
        assert "Springfield" in joined
        assert "62701" in joined

    def test_denormalize_address_text_when_present(self, denormalizer):
        """`Address.text` mirrors Patient's `addressText` projection."""
        loc = {
            "resourceType": "Location",
            "id": "loc-text-1",
            "name": "Curated Address Loc",
            "address": {
                "use": "work",
                "text": "100 Main St, Springfield IL 62701",
                "city": "Springfield",
            },
        }
        out = denormalizer.denormalize(loc)["_search"]
        assert "100 Main St, Springfield IL 62701" in out["addressText"]
        # And `addressFull` PREFERS the curated text over component join.
        assert out["addressFull"][0] == "100 Main St, Springfield IL 62701"

    def test_denormalize_address_text_absent_when_missing(
        self, denormalizer, rich_location):
        """If `Address.text` is not set, `addressText` must NOT leak as `[]`/None."""
        # rich_location has components but no `address.text`.
        out = denormalizer.denormalize(rich_location)["_search"]
        # Acceptable: field is absent entirely, OR present-but-empty list.
        # We allow either to keep this consistent with current
        # AddressExtractor sparse-output semantics, but explicitly
        # forbid a misleading non-empty value.
        if "addressText" in out:
            assert out["addressText"] == []

    def test_denormalize_managing_organization(
        self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert out["managingOrganizationId"] == "org-rich-1"
        assert out["managingOrganizationType"] == "Organization"

    def test_denormalize_partof(self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert out["partOfId"] == "loc-parent"
        assert out["partOfType"] == "Location"

    def test_denormalize_endpoint_ids(self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)["_search"]
        assert sorted(out["endpointId"]) == ["ep-1", "ep-2"]

    def test_denormalize_status_preserved(
        self, denormalizer, rich_location):
        out = denormalizer.denormalize(rich_location)
        assert out["status"] == "active"

    def test_denormalize_position_preserved(
        self, denormalizer, rich_location):
        """`position` stays on the document so a future `near` query can use it.

        The denormalizer must not strip top-level FHIR fields it
        doesn't have explicit rules for; it must augment with a
        `_search` bucket and leave the rest alone.
        """
        out = denormalizer.denormalize(rich_location)
        pos = out.get("position")
        assert pos is not None
        assert pos.get("latitude") == 39.78
        assert pos.get("longitude") == -89.65

    def test_denormalize_minimal_is_sparse(
        self, denormalizer, minimal_location):
        """A bare-bones Location should not generate spurious search fields."""
        out = denormalizer.denormalize(minimal_location)
        search = out.get("_search", {})
        # name_lower MUST exist (we have a name).
        assert search.get("name_lower") == "empty room"
        # No address / identifier / partOf / endpoint / type surplus.
        for absent in (
            "addressCity",
            "addressCity_lower",
            "addressPostalCode",
            "partOfId",
            "endpointId",
            "identifier_values",
            "type_codes",
            "characteristic_codes",
            "operationalStatus_codes",
            "managingOrganizationId"):
            assert absent not in search, f"unexpectedly populated: {absent}"

    def test_denormalize_does_not_compute_compartments(
        self, denormalizer, rich_location):
        """Location is not in any FHIR compartment — verify NO _compartments bucket emitted."""
        out = denormalizer.denormalize(rich_location)
        assert "_compartments" not in out


# ---------------------------------------------------------------------------
# 8) Cross-resource linking
#
# Location is referenced by:
#   - Appointment.participant.actor (when actor is Location/X)
#   - Observation.subject (when subject is Location/X)
# Location itself references:
#   - Organization (via managingOrganization)
#   - Location (via partOf)
#   - Endpoint (via endpoint)
# ---------------------------------------------------------------------------

class TestLocationCrossResourceLinking:
    """Verify the `Reference(Location)` fields on other resources are searchable."""

    def test_appointment_location_search(self, converter):
        """Appointment `location` parameter resolves participant.actor[type=Location]."""
        query = converter.convert(
            "Appointment", "location=Location/loc-rich-1"
        )
        s = str(query)
        assert "loc-rich-1" in s
        assert "locationId" in s or "locationid" in s.lower()

    def test_observation_subject_location(self, converter):
        """Observation `subject` can be a Location reference."""
        query = converter.convert(
            "Observation", "subject=Location/loc-rich-1"
        )
        s = str(query)
        assert "loc-rich-1" in s

    def test_location_organization_search(self, converter):
        """Forward search: find Locations managed by a given Organization."""
        query = converter.convert(
            "Location", "organization=Organization/org-rich-1"
        )
        s = str(query)
        assert "org-rich-1" in s

    def test_location_partof_search(self, converter):
        """Forward search: find Locations that are part of a given parent Location."""
        query = converter.convert(
            "Location", "partof=Location/loc-parent"
        )
        s = str(query)
        assert "loc-parent" in s

    def test_appointment_with_location_actor_denormalization(self, denormalizer):
        """Round-trip: Appointment with a Location actor → searchable locationId."""
        appt = {
            "resourceType": "Appointment",
            "id": "appt-loc-001",
            "status": "booked",
            "participant": [
                {
                    "actor": {"reference": "Location/loc-rich-1"},
                    "status": "accepted",
                },
            ],
        }
        out = denormalizer.denormalize(appt)["_search"]
        # Appointment YAML maps the participant.actor's id into BOTH
        # `actorIds` (array, type-agnostic) AND `locationId` (legacy,
        # single — populated even when the only participant is a
        # Location).
        actor_ids = out.get("actorIds") or []
        if isinstance(actor_ids, str):
            actor_ids = [actor_ids]
        assert "loc-rich-1" in actor_ids
        actor_types = out.get("actorTypes") or []
        if isinstance(actor_types, str):
            actor_types = [actor_types]
        assert "Location" in actor_types

    def test_observation_subject_location_denormalization(self, denormalizer):
        """Round-trip: Observation with Location subject → subjectType=Location."""
        obs = {
            "resourceType": "Observation",
            "id": "obs-loc-subject",
            "status": "final",
            "code": {"coding": [{"code": "noise"}]},
            "subject": {"reference": "Location/loc-rich-1"},
        }
        out = denormalizer.denormalize(obs)["_search"]
        assert out.get("subjectId") == "loc-rich-1"
        assert out.get("subjectType") == "Location"

    def test_location_managing_organization_denormalization(self, denormalizer):
        """Location → managingOrganization → searchable organization id+type."""
        loc = {
            "resourceType": "Location",
            "id": "loc-cross-1",
            "name": "X",
            "managingOrganization": {"reference": "Organization/org-99"},
        }
        out = denormalizer.denormalize(loc)["_search"]
        assert out["managingOrganizationId"] == "org-99"
        assert out["managingOrganizationType"] == "Organization"

    def test_location_partof_self_reference_denormalization(self, denormalizer):
        """Location → partOf → searchable parent location id+type."""
        loc = {
            "resourceType": "Location",
            "id": "loc-cross-2",
            "name": "Y",
            "partOf": {"reference": "Location/loc-parent-99"},
        }
        out = denormalizer.denormalize(loc)["_search"]
        assert out["partOfId"] == "loc-parent-99"
        assert out["partOfType"] == "Location"


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
class TestLocationMongoDB:
    """End-to-end: denormalize → insert → query via converter → assert results."""

    @pytest.fixture(scope="class")
    def mongo_collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["locations_e2e"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_collection, denormalizer):
        # A small location hierarchy:
        #   - parent: a hospital campus
        #   - child: 3 wards (ICU, ER, Surgery)
        #   - grandchild: 4 beds (2 in ICU, 1 ER, 1 Surgery)
        #   - 1 inactive bed
        organizations_org_id = "Organization/org-acme"
        organizations: List[Dict[str, Any]] = [
            {
                "resourceType": "Location",
                "id": "loc-campus",
                "status": "active",
                "name": "Acme General Hospital",
                "alias": ["AGH"],
                "type": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                                "code": "HOSP",
                            }
                        ]
                    }
                ],
                "managingOrganization": {"reference": organizations_org_id},
                "address": {
                    "use": "work",
                    "city": "Springfield",
                    "state": "IL",
                    "postalCode": "62701",
                    "country": "USA",
                },
                "meta": {"lastUpdated": datetime(2024, 1, 15, 10, 0, 0)},
            },
        ]
        for ward in ["icu", "er", "surgery"]:
            organizations.append(
                {
                    "resourceType": "Location",
                    "id": f"loc-ward-{ward}",
                    "status": "active",
                    "name": f"{ward.upper()} Ward",
                    "type": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                                    "code": "WARD",
                                }
                            ]
                        }
                    ],
                    "partOf": {"reference": "Location/loc-campus"},
                    "managingOrganization": {"reference": organizations_org_id},
                    "address": {
                        "use": "work",
                        "city": "Springfield",
                        "state": "IL",
                        "postalCode": "62701",
                        "country": "USA",
                    },
                    "meta": {
                        "lastUpdated": datetime(2024, 2, 1, 10, 0, 0)
                    },
                }
            )
        beds = [
            ("loc-bed-icu-1", "ICU Bed 1", "loc-ward-icu", "active", "O"),
            ("loc-bed-icu-2", "ICU Bed 2", "loc-ward-icu", "active", "U"),
            ("loc-bed-er-1", "ER Bed 1", "loc-ward-er", "active", "O"),
            ("loc-bed-surg-1", "Surgery Bed 1", "loc-ward-surgery", "active", "K"),
            ("loc-bed-defunct", "Defunct Bed", "loc-ward-icu", "inactive", "C"),
        ]
        for bed_id, bed_name, parent_id, bed_status, op_code in beds:
            organizations.append(
                {
                    "resourceType": "Location",
                    "id": bed_id,
                    "status": bed_status,
                    "name": bed_name,
                    "type": [
                        {
                            "coding": [
                                {
                                    "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                                    "code": "BD",
                                }
                            ]
                        }
                    ],
                    "operationalStatus": {
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0116",
                        "code": op_code,
                    },
                    "partOf": {"reference": f"Location/{parent_id}"},
                    "managingOrganization": {"reference": organizations_org_id},
                    "identifier": [
                        {
                            "system": "http://example.org/beds",
                            "value": bed_id.upper(),
                        }
                    ],
                    "address": {
                        "use": "work",
                        "city": "Springfield",
                        "state": "IL",
                        "postalCode": "62701",
                        "country": "USA",
                    },
                    "meta": {
                        "lastUpdated": datetime(2024, 3, 5, 10, 0, 0)
                    },
                }
            )

        denorm = [denormalizer.denormalize(o) for o in organizations]
        for d, src in zip(denorm, organizations):
            d.setdefault("_id", src["id"])
        mongo_collection.insert_many(denorm)
        return mongo_collection

    def test_query_by_status_active(self, converter, seeded):
        q = converter.convert("Location", "status=active")
        results = list(seeded.find(q))
        # 1 campus + 3 wards + 4 active beds = 8 (defunct is inactive)
        assert len(results) == 8

    def test_query_by_status_inactive(self, converter, seeded):
        q = converter.convert("Location", "status=inactive")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "loc-bed-defunct"

    def test_query_by_type_bed(self, converter, seeded):
        q = converter.convert("Location", "type=BD")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        # All 5 beds (including defunct).
        assert ids == [
            "loc-bed-defunct",
            "loc-bed-er-1",
            "loc-bed-icu-1",
            "loc-bed-icu-2",
            "loc-bed-surg-1",
        ]

    def test_query_by_partof_returns_children_only(self, converter, seeded):
        """Wards under campus."""
        q = converter.convert(
            "Location", "partof=Location/loc-campus"
        )
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == [
            "loc-ward-er",
            "loc-ward-icu",
            "loc-ward-surgery",
        ]

    def test_query_by_partof_grandchildren(self, converter, seeded):
        """Beds under ICU ward."""
        q = converter.convert(
            "Location", "partof=Location/loc-ward-icu"
        )
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        # 2 active ICU beds + 1 defunct.
        assert ids == [
            "loc-bed-defunct",
            "loc-bed-icu-1",
            "loc-bed-icu-2",
        ]

    def test_query_by_organization(self, converter, seeded):
        """Every seeded location is managed by org-acme."""
        q = converter.convert(
            "Location", "organization=Organization/org-acme"
        )
        results = list(seeded.find(q))
        assert len(results) == 9

    def test_query_by_operational_status_occupied(self, converter, seeded):
        """`O` = Occupied (only the beds carry operationalStatus)."""
        q = converter.convert(
            "Location",
            "operational-status=http://terminology.hl7.org/CodeSystem/v2-0116|O")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        assert ids == ["loc-bed-er-1", "loc-bed-icu-1"]

    def test_query_by_identifier(self, converter, seeded):
        q = converter.convert(
            "Location",
            "identifier=http://example.org/beds|LOC-BED-ICU-1")
        results = list(seeded.find(q))
        assert len(results) == 1
        assert results[0]["id"] == "loc-bed-icu-1"

    def test_query_by_address_city(self, converter, seeded):
        q = converter.convert("Location", "address-city=Springfield")
        results = list(seeded.find(q))
        # Every seeded location is in Springfield.
        assert len(results) == 9

    def test_query_combo_status_and_partof(self, converter, seeded):
        """Active children of a specific ward."""
        q = converter.convert(
            "Location",
            "status=active&partof=Location/loc-ward-icu")
        results = list(seeded.find(q))
        ids = sorted(r["id"] for r in results)
        # Defunct bed is inactive → excluded.
        assert ids == ["loc-bed-icu-1", "loc-bed-icu-2"]

    def test_query_by_lastupdated_range(self, converter, seeded):
        q = converter.convert(
            "Location",
            "_lastUpdated=ge2024-01-01&_lastUpdated=le2024-04-01")
        results = list(seeded.find(q))
        assert len(results) == 9

    def test_cross_resource_appointment_at_location(
        self, converter, seeded, denormalizer):
        """End-to-end: insert an Appointment with a Location actor, query Appointments by location."""
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        appt_coll = db["appointments_loc_link_e2e"]
        appt_coll.delete_many({})
        try:
            appt = {
                "resourceType": "Appointment",
                "id": "appt-loc-001",
                "status": "booked",
                "participant": [
                    {
                        "actor": {"reference": "Location/loc-bed-icu-1"},
                        "status": "accepted",
                    },
                ],
            }
            denorm = denormalizer.denormalize(appt)
            denorm.setdefault("_id", appt["id"])
            appt_coll.insert_one(denorm)

            q = converter.convert(
                "Appointment", "location=Location/loc-bed-icu-1"
            )
            results = list(appt_coll.find(q))
            assert len(results) == 1
            assert results[0]["id"] == "appt-loc-001"
        finally:
            appt_coll.delete_many({})
            client.close()
