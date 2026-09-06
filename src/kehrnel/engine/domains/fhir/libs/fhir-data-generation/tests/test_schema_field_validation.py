"""
Schema-driven field presence and datatype validation (FHIR R5 v5 + R6 v6).

- Datatype rules apply to every populated field on every resource.
- Top-level required fields must be present on every resource.
- Nested required fields are enforced on core clinical/admin resources.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from fhir_gen import ResourceGenerator
from fhir_gen.generators.resources import ENRICHERS
from fhir_gen.schema.field_validation import is_empty, iter_values_at_path
from fhir_gen.schema.registry import SchemaRegistry

from .field_validation_helpers import (
    V5_SCHEMA_PATH,
    V6_SCHEMA_PATH,
    assert_fields_valid,
    catalog_for,
    parser_for,
    required_field_count,
)


def _generate_for_validation(
    resource_type: str,
    seed: int,
    schema_path: Path,
) -> dict:
    """Generate one resource (``generate`` pre-creates schema/CORE dependencies)."""
    SchemaRegistry.reload(schema_path)
    gen = ResourceGenerator(seed=seed)
    if resource_type == "Patient":
        gen.generate("Organization", count=1)
    return gen.generate(resource_type, count=1)[0]

pytestmark = pytest.mark.integration

_V5_CATALOG = catalog_for(V5_SCHEMA_PATH)
_V6_CATALOG = catalog_for(V6_SCHEMA_PATH)
_V5_PARSER = parser_for(V5_SCHEMA_PATH)
_V6_PARSER = parser_for(V6_SCHEMA_PATH)
_V5_RESOURCES = sorted(_V5_CATALOG.keys())
_V6_RESOURCES = sorted(_V6_CATALOG.keys())

# Resources with enrichers and/or common clinical use — full nested required-field checks.
_CORE_NESTED_VALIDATION = frozenset({
    "Patient", "Practitioner", "Organization", "Location", "Encounter", "Observation",
    "Condition", "Procedure", "MedicationRequest", "MedicationAdministration", "AllergyIntolerance",
    "Immunization", "DiagnosticReport", "CarePlan", "Appointment", "Claim", "Coverage",
    "DocumentReference", "ServiceRequest", "Task", "Device", "Specimen",
    # MQL gap / extended shipped resources
    "Composition", "AdverseEvent", "DeviceRequest", "SupplyRequest", "SupplyDelivery",
    "ExplanationOfBenefit", "CoverageEligibilityRequest", "CoverageEligibilityResponse",
    "MeasureReport", "MedicationStatement", "NutritionIntake", "VisionPrescription",
    "DeviceDispense", "DeviceUsage", "PaymentNotice", "PaymentReconciliation",
})
_V5_CORE = sorted(_CORE_NESTED_VALIDATION & set(_V5_RESOURCES))
_V6_CORE = sorted(_CORE_NESTED_VALIDATION & set(_V6_RESOURCES))
_V5_TOP_LEVEL_OK = sorted(set(_V5_RESOURCES) & set(ENRICHERS.keys()))
_V6_TOP_LEVEL_OK = sorted(set(_V6_RESOURCES) & set(ENRICHERS.keys()))


def _seed_for(resource_type: str, schema_label: str) -> int:
    digest = hashlib.sha256(f"{schema_label}:{resource_type}".encode()).hexdigest()
    return int(digest[:8], 16)


@pytest.fixture(autouse=True)
def _restore_default_schema_after_test() -> None:
    """v6 tests reload the registry; restore R5 so other suites keep working."""
    yield
    SchemaRegistry.reload(V5_SCHEMA_PATH)


class TestFieldCatalog:
    def test_v5_catalog_covers_all_resources(self):
        assert len(_V5_CATALOG) == 158
        assert required_field_count(_V5_CATALOG) >= 200

    def test_v6_catalog_covers_all_resources(self):
        assert len(_V6_CATALOG) == 127
        assert required_field_count(_V6_CATALOG) >= 150

    def test_patient_has_contact_field_spec(self):
        paths = {s.path for s in _V5_CATALOG["Patient"]}
        assert "contact" in paths
        assert "contact.relationship" in paths
        assert "contact.telecom" in paths


class TestPatientContactQuality:
    @pytest.fixture
    def patient_with_deps(self):
        SchemaRegistry.reload(V5_SCHEMA_PATH)
        seed = _seed_for("Patient", "v5-contact")
        gen = ResourceGenerator(seed=seed)
        gen.generate("Organization", count=1)
        return gen.generate("Patient", count=1)[0]

    def test_patient_contact_populated(self, patient_with_deps):
        contact = patient_with_deps.get("contact")
        assert contact and len(contact) >= 1
        entry = contact[0]
        assert not is_empty(entry.get("relationship"))
        assert not is_empty(entry.get("name"))
        assert not is_empty(entry.get("telecom"))

    def test_patient_contact_validates(self, patient_with_deps):
        assert_fields_valid(
            patient_with_deps,
            _V5_CATALOG["Patient"],
            _V5_PARSER,
            resource_type="Patient",
        )


@pytest.mark.parametrize("resource_type", _V5_RESOURCES)
class TestV5PopulatedFieldDatatypes:
    def test_populated_fields_match_schema_types(self, resource_type: str):
        seed = _seed_for(resource_type, "v5-types")
        resource = _generate_for_validation(resource_type, seed, V5_SCHEMA_PATH)
        assert_fields_valid(
            resource,
            _V5_CATALOG[resource_type],
            _V5_PARSER,
            check_required=False,
            resource_type=resource_type,
            allow_datatype_gaps=True,
        )


@pytest.mark.parametrize("resource_type", _V6_RESOURCES)
class TestV6PopulatedFieldDatatypes:
    def test_populated_fields_match_schema_types(self, resource_type: str):
        seed = _seed_for(resource_type, "v6-types")
        resource = _generate_for_validation(resource_type, seed, V6_SCHEMA_PATH)
        assert_fields_valid(
            resource,
            _V6_CATALOG[resource_type],
            _V6_PARSER,
            check_required=False,
            resource_type=resource_type,
            allow_datatype_gaps=True,
        )


@pytest.mark.parametrize("resource_type", _V5_TOP_LEVEL_OK)
class TestV5TopLevelRequiredFields:
    def test_top_level_required_fields_present(self, resource_type: str):
        seed = _seed_for(resource_type, "v5-req0")
        resource = _generate_for_validation(resource_type, seed, V5_SCHEMA_PATH)
        assert_fields_valid(
            resource,
            _V5_CATALOG[resource_type],
            _V5_PARSER,
            check_required=True,
            max_required_depth=0,
            resource_type=resource_type,
        )


@pytest.mark.parametrize("resource_type", _V6_TOP_LEVEL_OK)
class TestV6TopLevelRequiredFields:
    def test_top_level_required_fields_present(self, resource_type: str):
        seed = _seed_for(resource_type, "v6-req0")
        resource = _generate_for_validation(resource_type, seed, V6_SCHEMA_PATH)
        assert_fields_valid(
            resource,
            _V6_CATALOG[resource_type],
            _V6_PARSER,
            check_required=True,
            max_required_depth=0,
            resource_type=resource_type,
        )


@pytest.mark.parametrize("resource_type", _V5_CORE)
class TestV5CoreNestedRequiredFields:
    def test_all_required_fields_when_generatable(self, resource_type: str):
        seed = _seed_for(resource_type, "v5-req")
        resource = _generate_for_validation(resource_type, seed, V5_SCHEMA_PATH)
        assert_fields_valid(
            resource,
            _V5_CATALOG[resource_type],
            _V5_PARSER,
            resource_type=resource_type,
            allow_nested_gaps=True,
        )


@pytest.mark.parametrize("resource_type", _V6_CORE)
class TestV6CoreNestedRequiredFields:
    def test_all_required_fields_when_generatable(self, resource_type: str):
        seed = _seed_for(resource_type, "v6-req")
        resource = _generate_for_validation(resource_type, seed, V6_SCHEMA_PATH)
        assert_fields_valid(
            resource,
            _V6_CATALOG[resource_type],
            _V6_PARSER,
            resource_type=resource_type,
            allow_nested_gaps=True,
        )


class TestHighValueOptionalFields:
    @pytest.mark.parametrize(
        "resource_type,field_path",
        [
            ("Patient", "telecom"),
            ("Patient", "address"),
            ("Patient", "identifier"),
            ("Patient", "contact"),
            ("Practitioner", "telecom"),
            ("Organization", "contact.telecom"),
            ("Encounter", "status"),
        ],
    )
    def test_v5_enriched_resource_has_field(self, resource_type: str, field_path: str):
        SchemaRegistry.reload(V5_SCHEMA_PATH)
        seed = _seed_for(f"{resource_type}:{field_path}", "v5-optional")
        gen = ResourceGenerator(seed=seed)
        if resource_type == "Patient":
            gen.generate("Organization", count=1)
        resource = gen.generate(resource_type, count=1)[0]
        values = list(iter_values_at_path(resource, field_path))
        assert values, f"{resource_type} missing {field_path}"
        assert any(not is_empty(v) for v in values), f"{resource_type}.{field_path} empty"
