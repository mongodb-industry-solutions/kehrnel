"""
Comprehensive integration tests for ALL Device search parameters per FHIR R5.

References:
- https://www.hl7.org/fhir/device-search.html
- https://www.hl7.org/fhir/device-definitions.html
- https://www.hl7.org/fhir/compartmentdefinition-device.html

This suite exercises the 25 search parameters declared in
``configs/Device.yaml``:

  - Strings (8):    device-name, lot-number, manufacturer, model,
                    serial-number, udi-carrier, udi-di, version
  - Tokens (7):     biological-source-event, code, identifier,
                    specification, status, type, _id
  - References (4): definition, location, organization, parent
  - Dates (3):      expiration-date, manufacture-date, _lastUpdated
  - URI (1):        url

Plus:

- FHIR R5 ``Device.definition`` is a CodeableReference combining a
  CodeableConcept and a Reference. ``code`` searches the
  CodeableConcept side; ``definition`` searches the Reference side.
- FHIR R5 ``Device.type`` is CodeableConcept[] (R5 widened cardinality
  from R4's 0..1) — the ``type`` parameter spans every coding.
- ``device-name`` unions ``Device.name.value``,
  ``Device.type.coding.display``, and ``Device.type.text`` per the
  FHIR R5 expression — TextExtractor + path-union ``|`` syntax.
- ``serial-number`` unions ``Device.serialNumber`` (top-level scalar)
  with ``Device.identifier.where(type='SNO')``. Our path resolver
  doesn't implement ``.where()``, so the practical compromise unions
  the scalar with all identifier values; production callers that care
  should pre-tag SNO identifiers downstream.
- ``Device.expirationDate`` / ``Device.manufactureDate`` are dateTime
  scalars (NOT Periods) — DirectFieldExtractor preserves Python
  ``datetime`` objects so MongoDB BSON-typed range comparisons line up.
- Self-compartment routing: Device opts into the precomputed fast-path
  for its own compartment so ``Device/<id>/Device`` collapses to a
  single ``_compartments.Device`` lookup. Cross-resource queries
  (``Device/<id>/Observation``, ``Device/<id>/Appointment``) go
  through the dynamic resolver against the linking parameters in
  ``compartments/definitions/device.json`` (Configuration 2 pattern
  from FHIR_TO_MQL_COMPARTMENT.md).
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
def rich_device() -> Dict[str, Any]:
    """A Device with most R5 fields populated."""
    return {
        "resourceType": "Device",
        "id": "dev-rich",
        "meta": {"lastUpdated": "2024-08-22T10:30:00Z"},
        "status": "active",
        "url": "https://devices.example.org/dev-rich",
        "manufacturer": "Acme Medical",
        "modelNumber": "Pump-3000",
        "lotNumber": "LOT-42-A",
        "serialNumber": "SN-9876",
        "manufactureDate": "2023-01-15",
        "expirationDate": "2028-01-15",
        "identifier": [
            {"system": "http://hospital.org/dev-id", "value": "DEV-12345"},
            {
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                            "code": "SNO",
                        }
                    ]
                },
                "system": "http://manufacturer.example.org/serial",
                "value": "SN-9876",
            },
        ],
        "biologicalSourceEvent": {
            "system": "http://example.org/biosource",
            "value": "BIO-EVT-1",
        },
        "name": [
            {"value": "Acme Infusion Pump", "type": "user-friendly-name"},
            {"value": "Pump-3000-IV", "type": "registered-name"},
        ],
        "type": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "182722004",
                        "display": "Infusion pump",
                    }
                ]
            },
            {
                "coding": [
                    {
                        "system": "http://example.org/types",
                        "code": "PUMP-IV",
                    }
                ]
            },
        ],
        "definition": {
            "concept": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "182722004",
                    }
                ]
            },
            "reference": {"reference": "DeviceDefinition/dd-pump-3000"},
        },
        "version": [
            {
                "type": {"coding": [{"code": "firmware"}]},
                "value": "v1.2.3",
            },
            {
                "type": {"coding": [{"code": "software"}]},
                "value": "OS-2024.08",
            },
        ],
        "udiCarrier": [
            {
                "deviceIdentifier": "01099876543210",
                "carrierHRF": "(01)01099876543210(11)240115(17)280115",
            }
        ],
        "conformsTo": [
            {
                "specification": {
                    "coding": [
                        {"system": "http://iso.org/spec", "code": "ISO-13485"}
                    ]
                },
                "version": "2016",
            }
        ],
        "owner": {"reference": "Organization/hospital-1"},
        "location": {"reference": "Location/clinic-3"},
        "parent": {"reference": "Device/dev-parent"},
    }


@pytest.fixture
def minimal_device() -> Dict[str, Any]:
    """Bare-bones: only id + resourceType. FHIR R5 Device has no
    required fields, so this models the legitimate floor for
    sparse-output testing."""
    return {
        "resourceType": "Device",
        "id": "dev-min",
    }


# ===========================================================================
# 1) String parameters (8)
# ===========================================================================


class TestDeviceStringParameters:
    """device-name, lot-number, manufacturer, model, serial-number,
    udi-carrier, udi-di, version. Default = lowercase starts-with;
    `:exact` = case-sensitive equality."""

    def test_device_name_default_starts_with(self, converter):
        q = converter.convert("Device", "device-name=acme")
        s = str(q)
        assert "deviceName_lower" in s
        assert "acme" in s

    def test_device_name_exact_modifier(self, converter):
        q = converter.convert("Device", "device-name:exact=Acme Infusion Pump")
        s = str(q)
        assert "deviceName" in s
        assert "Acme Infusion Pump" in s

    def test_lot_number(self, converter):
        q = converter.convert("Device", "lot-number=lot-42")
        s = str(q)
        assert "lotNumber_lower" in s
        assert "lot-42" in s

    def test_manufacturer(self, converter):
        q = converter.convert("Device", "manufacturer=acme")
        s = str(q)
        assert "manufacturer_lower" in s
        assert "acme" in s

    def test_manufacturer_exact_preserves_case(self, converter):
        q = converter.convert("Device", "manufacturer:exact=Acme Medical")
        s = str(q)
        assert "manufacturer" in s
        assert "Acme Medical" in s

    def test_model_targets_model_number(self, converter):
        # FHIR `model` parameter binds to Device.modelNumber per R5.
        q = converter.convert("Device", "model=Pump-3000")
        s = str(q)
        assert "modelNumber_lower" in s

    def test_serial_number(self, converter):
        q = converter.convert("Device", "serial-number=SN-9876")
        s = str(q)
        assert "serialNumber_lower" in s
        assert "sn-9876" in s

    def test_udi_carrier(self, converter):
        q = converter.convert("Device", "udi-carrier=(01)010998")
        s = str(q)
        assert "udiCarrier_lower" in s

    def test_udi_di(self, converter):
        q = converter.convert("Device", "udi-di=01099876543210")
        s = str(q)
        assert "udiDi_lower" in s
        assert "01099876543210" in s

    def test_version(self, converter):
        q = converter.convert("Device", "version=v1.2.3")
        s = str(q)
        assert "version_lower" in s


# ===========================================================================
# 2) Token parameters (7)
# ===========================================================================


class TestDeviceTokenParameters:
    """biological-source-event, code, identifier, specification, status,
    type, _id."""

    def test_status_active(self, converter):
        q = converter.convert("Device", "status=active")
        assert q == {"status": "active"} or "status" in str(q)

    def test_status_inactive(self, converter):
        q = converter.convert("Device", "status=inactive")
        assert "inactive" in str(q)

    def test_identifier_bare_value(self, converter):
        q = converter.convert("Device", "identifier=DEV-12345")
        s = str(q)
        assert "DEV-12345" in s
        assert "identifier" in s.lower()

    def test_identifier_system_pipe_code(self, converter):
        q = converter.convert(
            "Device",
            "identifier=http://hospital.org/dev-id|DEV-12345",
        )
        assert "DEV-12345" in str(q)

    def test_code_token(self, converter):
        # FHIR R5 `code` targets Device.definition.concept.
        q = converter.convert("Device", "code=182722004")
        s = str(q)
        assert "182722004" in s
        assert "code" in s.lower()

    def test_type_token(self, converter):
        # FHIR R5 `type` targets Device.type (CodeableConcept[]).
        q = converter.convert("Device", "type=182722004")
        s = str(q)
        assert "182722004" in s
        assert "type" in s.lower()

    def test_specification_token(self, converter):
        q = converter.convert("Device", "specification=ISO-13485")
        s = str(q)
        assert "ISO-13485" in s
        assert "specification" in s.lower()

    def test_biological_source_event_token(self, converter):
        q = converter.convert("Device", "biological-source-event=BIO-EVT-1")
        s = str(q)
        assert "BIO-EVT-1" in s
        assert "biological" in s.lower()

    def test_id_search_parameter(self, converter):
        q = converter.convert("Device", "_id=dev-rich")
        assert "dev-rich" in str(q)


# ===========================================================================
# 3) Reference parameters (4)
# ===========================================================================


class TestDeviceReferenceParameters:
    """definition, location, organization, parent."""

    def test_definition_reference(self, converter):
        q = converter.convert("Device", "definition=dd-pump-3000")
        s = str(q)
        assert "definition" in s.lower()
        assert "dd-pump-3000" in s

    def test_location_reference(self, converter):
        q = converter.convert("Device", "location=clinic-3")
        s = str(q)
        assert "location" in s.lower()
        assert "clinic-3" in s

    def test_organization_targets_owner(self, converter):
        # FHIR R5 `organization` parameter binds to Device.owner.
        q = converter.convert("Device", "organization=hospital-1")
        s = str(q)
        assert "ownerId" in s or "owner" in s.lower()
        assert "hospital-1" in s

    def test_parent_reference_recursive(self, converter):
        q = converter.convert("Device", "parent=dev-parent")
        s = str(q)
        assert "parent" in s.lower()
        assert "dev-parent" in s

    def test_typed_reference_with_resource_prefix(self, converter):
        q = converter.convert("Device", "location=Location/clinic-3")
        assert "clinic-3" in str(q)


# ===========================================================================
# 4) Date parameters (3)
# ===========================================================================


class TestDeviceDateParameters:
    """expiration-date, manufacture-date, _lastUpdated. All support the
    full FHIR R5 prefix set (eq/ne/gt/ge/lt/le/sa/eb/ap)."""

    def test_expiration_date_eq(self, converter):
        q = converter.convert("Device", "expiration-date=2028-01-15")
        s = str(q)
        assert "expirationDate" in s
        assert "2028" in s

    def test_expiration_date_ge(self, converter):
        q = converter.convert("Device", "expiration-date=ge2025-01-01")
        s = str(q)
        assert "expirationDate" in s
        assert "$gte" in s
        assert "2025" in s

    def test_expiration_date_lt(self, converter):
        q = converter.convert("Device", "expiration-date=lt2030-01-01")
        s = str(q)
        assert "$lt" in s
        assert "2030" in s

    def test_expiration_date_range(self, converter):
        q = converter.convert(
            "Device",
            "expiration-date=ge2025-01-01&expiration-date=le2030-12-31",
        )
        s = str(q)
        assert "$gte" in s and "$lte" in s

    def test_manufacture_date_eq(self, converter):
        q = converter.convert("Device", "manufacture-date=2023-01-15")
        s = str(q)
        assert "manufactureDate" in s

    def test_manufacture_date_lt(self, converter):
        q = converter.convert("Device", "manufacture-date=lt2024-01-01")
        s = str(q)
        assert "$lt" in s
        assert "2024" in s

    def test_lastupdated_ge(self, converter):
        q = converter.convert("Device", "_lastUpdated=ge2024-01-01")
        s = str(q)
        assert "lastupdated" in s.lower()
        assert "$gte" in s


# ===========================================================================
# 5) URI parameter (1)
# ===========================================================================


class TestDeviceUriParameter:
    """url — FHIR R5 §3.1.1.5.5 URI parameter (exact-match)."""

    def test_url_exact_match(self, converter):
        q = converter.convert(
            "Device", "url=https://devices.example.org/dev-rich"
        )
        s = str(q)
        assert "url" in s.lower()
        assert "devices.example.org" in s


# ===========================================================================
# 6) Modifiers
# ===========================================================================


class TestDeviceModifiers:
    """`:exact`, `:contains`, `:not`, `:missing` modifier coverage."""

    def test_manufacturer_contains(self, converter):
        # `:contains` should yield a regex / substring search.
        q = converter.convert("Device", "manufacturer:contains=cme")
        assert "cme" in str(q).lower()

    def test_status_missing_true(self, converter):
        q = converter.convert("Device", "status:missing=true")
        s = str(q)
        assert "status" in s.lower()
        assert "$exists" in s or "$eq" in s or "null" in s.lower()

    def test_type_not_modifier(self, converter):
        # FHIR `:not` over multi-field token compiles to `$nor`.
        q = converter.convert("Device", "type:not=182722004")
        s = str(q)
        assert "182722004" in s
        assert "$nor" in s or "$ne" in s or "$not" in s or "$nin" in s


# ===========================================================================
# 7) Combined queries
# ===========================================================================


class TestDeviceCombinations:
    """Multi-parameter queries combining strings, tokens, refs, dates."""

    def test_status_and_manufacturer(self, converter):
        q = converter.convert(
            "Device", "status=active&manufacturer=acme"
        )
        s = str(q)
        assert "acme" in s.lower()
        assert "active" in s.lower()

    def test_type_and_organization(self, converter):
        q = converter.convert(
            "Device", "type=182722004&organization=hospital-1"
        )
        s = str(q)
        assert "182722004" in s
        assert "hospital-1" in s

    def test_expiration_range_and_status(self, converter):
        q = converter.convert(
            "Device",
            "expiration-date=ge2025-01-01&expiration-date=le2030-12-31"
            "&status=active",
        )
        s = str(q)
        assert "$gte" in s and "$lte" in s
        assert "active" in s.lower()

    def test_serial_and_lot_and_status(self, converter):
        q = converter.convert(
            "Device",
            "serial-number=SN-9876&lot-number=lot-42&status=active",
        )
        s = str(q)
        assert "sn-9876" in s.lower()
        assert "lot-42" in s.lower()


# ===========================================================================
# 8) Denormalization correctness
# ===========================================================================


class TestDeviceDenormalization:
    """Verifies the shape of the `_search` document for the rich fixture."""

    def test_status_top_level(self, denormalizer, rich_device):
        out = denormalizer.denormalize(rich_device)
        assert out["status"] == "active"

    def test_url_top_level(self, denormalizer, rich_device):
        out = denormalizer.denormalize(rich_device)
        assert out["url"] == "https://devices.example.org/dev-rich"

    def test_identifier_denormalization(self, denormalizer, rich_device):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert "DEV-12345" in out["identifier_values"]
        assert "SN-9876" in out["identifier_values"]
        sysvals = " ".join(out["identifier_systemCode"])
        assert "DEV-12345" in sysvals

    def test_biological_source_event_single_identifier(
        self, denormalizer, rich_device
    ):
        # FHIR R5 `Device.biologicalSourceEvent` is a SINGLE Identifier
        # (cardinality 0..1) — the extractor's _ensure_list adapts it
        # so the same projection logic works.
        out = denormalizer.denormalize(rich_device)["_search"]
        assert "BIO-EVT-1" in out["biologicalSourceEvent_values"]

    def test_code_targets_definition_concept(
        self, denormalizer, rich_device
    ):
        # `code` searches Device.definition.concept (CodeableReference's
        # CodeableConcept side).
        out = denormalizer.denormalize(rich_device)["_search"]
        assert "182722004" in out["code_codes"]

    def test_type_codeable_concept_array(self, denormalizer, rich_device):
        out = denormalizer.denormalize(rich_device)["_search"]
        # R5 cardinality is 0..* — both type entries must surface.
        assert "182722004" in out["type_codes"]
        assert "PUMP-IV" in out["type_codes"]

    def test_specification_nested_in_conforms_to(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert "ISO-13485" in out["specification_codes"]

    def test_device_name_unions_three_paths(
        self, denormalizer, rich_device
    ):
        # FHIR R5 `device-name` expression unions Device.name.value,
        # Device.type.coding.display, and Device.type.text.
        out = denormalizer.denormalize(rich_device)["_search"]
        names = out["deviceName"]
        assert "Acme Infusion Pump" in names  # name.value
        assert "Pump-3000-IV" in names  # name.value
        assert "Infusion pump" in names  # type.coding.display

    def test_device_name_lower_is_lowercased(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)["_search"]
        for n in out["deviceName_lower"]:
            assert n == n.lower()

    def test_string_scalars_have_lower_companions(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert out["manufacturer"] == "Acme Medical"
        assert out["manufacturer_lower"] == "acme medical"
        assert out["modelNumber"] == "Pump-3000"
        assert out["modelNumber_lower"] == "pump-3000"
        assert out["lotNumber"] == "LOT-42-A"
        assert out["lotNumber_lower"] == "lot-42-a"

    def test_serial_number_unions_scalar_and_identifiers(
        self, denormalizer, rich_device
    ):
        # FHIR R5 `serial-number` expression unions Device.serialNumber
        # and Device.identifier.where(type='SNO'). Our path resolver
        # doesn't filter by type, so the union surfaces all identifier
        # values; the top-level scalar is always present.
        out = denormalizer.denormalize(rich_device)["_search"]
        assert "SN-9876" in out["serialNumber"]

    def test_version_array_from_backbone_value(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert "v1.2.3" in out["version"]
        assert "OS-2024.08" in out["version"]

    def test_udi_carrier_extraction(self, denormalizer, rich_device):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert any(
            "01099876543210" in s for s in out["udiCarrier"]
        )
        assert "01099876543210" in out["udiDi"]

    def test_dates_preserved(self, denormalizer, rich_device):
        out = denormalizer.denormalize(rich_device)["_search"]
        # FHIR JSON ingestion stores ISO strings; the extractor passes
        # them through (production callers should pre-coerce to
        # `datetime` for BSON-typed range queries).
        assert "2028" in str(out["expirationDate"])
        assert "2023" in str(out["manufactureDate"])

    def test_definition_reference_extraction(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert out["definitionId"] == "dd-pump-3000"
        assert out["definitionType"] == "DeviceDefinition"

    def test_owner_reference_extraction(self, denormalizer, rich_device):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert out["ownerId"] == "hospital-1"
        assert out["ownerType"] == "Organization"

    def test_location_reference_extraction(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert out["locationId"] == "clinic-3"
        assert out["locationType"] == "Location"

    def test_parent_reference_extraction(self, denormalizer, rich_device):
        out = denormalizer.denormalize(rich_device)["_search"]
        assert out["parentId"] == "dev-parent"
        assert out["parentType"] == "Device"

    def test_minimal_device_sparse_output(
        self, denormalizer, minimal_device
    ):
        out = denormalizer.denormalize(minimal_device)
        # No _search projections should be created when the source is
        # bare. The compartment_membership rule still seeds the self
        # bucket because `include_self: true`.
        search = out.get("_search", {})
        for f in (
            "identifier_values",
            "manufacturer",
            "modelNumber",
            "serialNumber",
            "version",
        ):
            assert f not in search
        assert out["_compartments"]["Device"] == ["dev-min"]


# ===========================================================================
# 9) Resource purity
# ===========================================================================


class TestDeviceResourcePurity:
    """Denormalization MUST NOT mutate the original FHIR resource — only
    `_search` and `_compartments` may be added at the root."""

    def test_root_keys_are_resource_plus_buckets_only(
        self, denormalizer, rich_device
    ):
        original_keys = set(rich_device.keys())
        out = denormalizer.denormalize(rich_device)
        added = set(out.keys()) - original_keys
        assert added.issubset({"_search", "_compartments"}), (
            f"Denormalization added unexpected root keys: {added}"
        )

    def test_no_search_subfields_injected_at_root(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)
        # These projection-only fields must never escape `_search` to
        # the resource root. NOTE: `udiCarrier` is intentionally NOT
        # in this list because it's a real FHIR R5 field on the
        # Device resource (BackboneElement[] of UDI carriers); the
        # `_search.udiCarrier` projection happens to share the name
        # but lives one level deeper. The resource-purity guarantee
        # is that the root-level FHIR fields round-trip unchanged
        # (verified separately by `test_real_fhir_fields_preserved_unchanged`).
        forbidden = {
            "identifier_values",
            "identifier_systemCode",
            "biologicalSourceEvent_values",
            "biologicalSourceEvent_systemCode",
            "code_codes",
            "code_systemCode",
            "type_codes",
            "type_systemCode",
            "specification_codes",
            "specification_systemCode",
            "deviceName",
            "deviceName_lower",
            "manufacturer_lower",
            "modelNumber_lower",
            "lotNumber_lower",
            "serialNumber_lower",
            "udiCarrier_lower",
            "udiDi",
            "udiDi_lower",
            "definitionId",
            "definitionType",
            "ownerId",
            "ownerType",
            "parentId",
            "parentType",
            "locationId",
            "locationType",
        }
        for f in forbidden:
            assert f not in out, (
                f"Denormalization leaked '{f}' to the resource root"
            )

    def test_real_fhir_fields_preserved_unchanged(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)
        # All FHIR fields the resource came in with should round-trip
        # byte-for-byte — denormalization is additive.
        for field in (
            "status", "url", "manufacturer", "modelNumber",
            "lotNumber", "serialNumber", "identifier",
            "biologicalSourceEvent", "name", "type",
            "definition", "version", "udiCarrier", "conformsTo",
            "owner", "location", "parent",
        ):
            assert out[field] == rich_device[field], (
                f"FHIR field '{field}' was mutated by denormalization"
            )


# ===========================================================================
# 10) Self-compartment routing (Hybrid Approach 3 fast-path)
# ===========================================================================


class TestDeviceSelfCompartment:
    """Device opts into the precomputed fast-path for its own compartment
    so `Device/<id>/Device` collapses to a single indexed lookup."""

    def test_compartment_membership_populated(
        self, denormalizer, rich_device
    ):
        out = denormalizer.denormalize(rich_device)
        assert out["_compartments"]["Device"] == ["dev-rich"]

    def test_compartment_query_uses_fast_path(self, converter):
        q = converter.convert_with_compartment(
            "Device", "dev-7", "Device"
        )
        s = str(q)
        assert "_compartments.Device" in s
        assert "dev-7" in s
        # Hybrid fast-path: NO $or over linking parameters.
        assert "$or" not in s

    def test_compartment_query_is_single_field_equality(
        self, converter
    ):
        q = converter.convert_with_compartment(
            "Device", "dev-99", "Device"
        )
        assert q == {"_compartments.Device": "dev-99"}


# ===========================================================================
# 11) Cross-resource compartment routing
# ===========================================================================


class TestDeviceCrossResourceCompartment:
    """`Device/<id>/<resource>` queries for shipped configs route
    through the precomputed fast-path (Hybrid Approach 3 — Configuration
    1 pattern from the design doc) when the target resource opts in
    via `compartments.precomputed: [Device]`. Observation, Appointment,
    and Group all opt in; their `_compartments.Device` fields are
    populated at denormalization time from the linking parameters
    declared in `compartments/definitions/device.json`."""

    def test_observation_uses_precomputed_fast_path(self, converter):
        # device.json says Observation links via `subject, performer,
        # device`. Observation.yaml now precomputes `_compartments.Device`
        # from the union of those three (filtered to Device/*) so the
        # resolver collapses to a single indexed lookup.
        q = converter.convert_with_compartment(
            "Device", "dev-1", "Observation"
        )
        assert q == {"_compartments.Device": "dev-1"}

    def test_observation_compartment_does_not_use_dynamic_or(
        self, converter
    ):
        s = str(converter.convert_with_compartment(
            "Device", "abc-123", "Observation"
        ))
        # Fast-path means NO $or, NO dynamic linking-param lookups.
        assert "$or" not in s
        assert "subjectId" not in s
        assert "performerId" not in s
        assert "deviceId" not in s
        assert "abc-123" in s

    def test_appointment_uses_precomputed_fast_path(self, converter):
        # device.json says Appointment links via `actor`. Appointment.yaml
        # now precomputes `_compartments.Device` from
        # participant[*].actor filtered to Device/*.
        q = converter.convert_with_compartment(
            "Device", "dev-1", "Appointment"
        )
        assert q == {"_compartments.Device": "dev-1"}

    def test_appointment_compartment_does_not_use_dynamic_actor_ids(
        self, converter
    ):
        s = str(converter.convert_with_compartment(
            "Device", "dev-1", "Appointment"
        ))
        assert "$or" not in s
        assert "actorIds" not in s

    def test_group_uses_precomputed_fast_path(self, converter):
        # Group.yaml has precomputed Device since it shipped — assert
        # parity with Observation/Appointment.
        q = converter.convert_with_compartment(
            "Device", "dev-1", "Group"
        )
        assert q == {"_compartments.Device": "dev-1"}

    def test_unconfigured_resource_in_compartment_raises_or_returns_safely(
        self, converter
    ):
        # `DeviceUsage` is in device.json but we don't ship a config
        # for it; the converter should raise a known error class
        # rather than silently returning an empty query.
        with pytest.raises(Exception):
            converter.convert_with_compartment(
                "Device", "dev-1", "DeviceUsage"
            )


# ===========================================================================
# 12) MongoDB end-to-end
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
class TestDeviceMongoDB:
    """Seed denormalized Devices into a MongoDB collection, generate
    MQL via the converter, execute, assert the right docs come back."""

    @pytest.fixture(scope="class")
    def mongo_collection(self):
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017")
        db = client["fhir_search_to_mql_tests"]
        coll = db["devices_e2e"]
        coll.delete_many({})
        yield coll
        coll.delete_many({})
        client.close()

    @pytest.fixture(scope="class")
    def seeded(self, mongo_collection, denormalizer):
        # Date scalars seeded as Python `datetime` (not ISO strings) so
        # MongoDB BSON-typed range comparisons line up with the
        # converter's `datetime` query operands. Same convention used
        # in test_practitioner_comprehensive.py and
        # test_practitionerrole_comprehensive.py.
        devices: List[Dict[str, Any]] = [
            {
                "resourceType": "Device",
                "id": "dev-pump-A",
                "status": "active",
                "manufacturer": "Acme",
                "modelNumber": "Pump-3000",
                "lotNumber": "LOT-A1",
                "serialNumber": "SN-AAA",
                "manufactureDate": datetime(2023, 1, 15),
                "expirationDate": datetime(2028, 1, 15),
                "type": [
                    {
                        "coding": [
                            {"system": "http://snomed.info/sct", "code": "182722004"}
                        ]
                    }
                ],
                "identifier": [
                    {"system": "http://hospital.org/dev", "value": "DEV-AAA"}
                ],
                "owner": {"reference": "Organization/hospital-1"},
                "location": {"reference": "Location/clinic-3"},
            },
            {
                "resourceType": "Device",
                "id": "dev-pump-B",
                "status": "active",
                "manufacturer": "Globex",
                "modelNumber": "Pump-3000",
                "lotNumber": "LOT-B2",
                "serialNumber": "SN-BBB",
                "manufactureDate": datetime(2024, 6, 1),
                "expirationDate": datetime(2029, 6, 1),
                "type": [
                    {
                        "coding": [
                            {"system": "http://snomed.info/sct", "code": "182722004"}
                        ]
                    }
                ],
                "identifier": [
                    {"system": "http://hospital.org/dev", "value": "DEV-BBB"}
                ],
                "owner": {"reference": "Organization/hospital-2"},
                "location": {"reference": "Location/clinic-1"},
            },
            {
                "resourceType": "Device",
                "id": "dev-monitor-C",
                "status": "inactive",
                "manufacturer": "Acme",
                "modelNumber": "Monitor-9",
                "lotNumber": "LOT-C3",
                "serialNumber": "SN-CCC",
                "manufactureDate": datetime(2020, 3, 10),
                "expirationDate": datetime(2025, 3, 10),
                "type": [
                    {
                        "coding": [
                            {"system": "http://snomed.info/sct", "code": "27290002"}
                        ]
                    }
                ],
                "owner": {"reference": "Organization/hospital-1"},
                "parent": {"reference": "Device/dev-pump-A"},
            },
        ]
        denorm = [denormalizer.denormalize(d) for d in devices]
        mongo_collection.insert_many(denorm)
        return mongo_collection

    # --- single-param queries ---

    def test_query_status_active(self, seeded, converter):
        q = converter.convert("Device", "status=active")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A", "dev-pump-B"}

    def test_query_manufacturer(self, seeded, converter):
        q = converter.convert("Device", "manufacturer=acme")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A", "dev-monitor-C"}

    def test_query_manufacturer_exact(self, seeded, converter):
        q = converter.convert("Device", "manufacturer:exact=Globex")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-B"}

    def test_query_model(self, seeded, converter):
        q = converter.convert("Device", "model=pump-3000")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A", "dev-pump-B"}

    def test_query_serial_number(self, seeded, converter):
        q = converter.convert("Device", "serial-number=sn-bbb")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-B"}

    def test_query_type(self, seeded, converter):
        q = converter.convert("Device", "type=182722004")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A", "dev-pump-B"}

    def test_query_identifier(self, seeded, converter):
        q = converter.convert("Device", "identifier=DEV-AAA")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A"}

    def test_query_organization(self, seeded, converter):
        q = converter.convert("Device", "organization=hospital-1")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A", "dev-monitor-C"}

    def test_query_location(self, seeded, converter):
        q = converter.convert("Device", "location=clinic-3")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A"}

    def test_query_parent_finds_child(self, seeded, converter):
        # `parent=dev-pump-A` should find devices whose parent points
        # to dev-pump-A.
        q = converter.convert("Device", "parent=dev-pump-A")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-monitor-C"}

    # --- date range queries ---

    def test_query_expiration_date_ge(self, seeded, converter):
        q = converter.convert("Device", "expiration-date=ge2028-01-01")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A", "dev-pump-B"}

    def test_query_expiration_date_lt(self, seeded, converter):
        q = converter.convert("Device", "expiration-date=lt2026-01-01")
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-monitor-C"}

    def test_query_manufacture_date_range(self, seeded, converter):
        q = converter.convert(
            "Device",
            "manufacture-date=ge2023-01-01&manufacture-date=le2024-12-31",
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A", "dev-pump-B"}

    # --- compartment fast-path queries ---

    def test_compartment_self_returns_single_device(
        self, seeded, converter
    ):
        q = converter.convert_with_compartment(
            "Device", "dev-pump-A", "Device"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A"}

    def test_compartment_self_unknown_id_returns_empty(
        self, seeded, converter
    ):
        q = converter.convert_with_compartment(
            "Device", "dev-nonexistent", "Device"
        )
        assert list(seeded.find(q)) == []

    # --- combined queries ---

    def test_compartment_self_plus_status_filter(
        self, seeded, converter
    ):
        compartment_q = converter.convert_with_compartment(
            "Device", "dev-pump-A", "Device"
        )
        status_q = converter.convert("Device", "status=active")
        combined = {"$and": [compartment_q, status_q]}
        ids = {r["id"] for r in seeded.find(combined)}
        assert ids == {"dev-pump-A"}

    def test_organization_and_status_intersection(
        self, seeded, converter
    ):
        q = converter.convert(
            "Device", "organization=hospital-1&status=active"
        )
        ids = {r["id"] for r in seeded.find(q)}
        assert ids == {"dev-pump-A"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
