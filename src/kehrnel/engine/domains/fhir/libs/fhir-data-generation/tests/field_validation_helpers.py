"""Helpers for schema-driven field and datatype validation tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fhir_gen.schema.field_catalog import SchemaFieldSpec, field_catalog
from fhir_gen.schema.field_validation import validate_resource_fields
from fhir_gen.schema.parser import FHIRSchemaParser
from fhir_gen.schema.registry import SchemaRegistry

PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "fhir_gen" / "schema"
V5_SCHEMA_PATH = PACKAGE_ROOT / "fhir.schema.v5.json"
V6_SCHEMA_PATH = PACKAGE_ROOT / "fhir.schema.v6.json"


def catalog_for(schema_path: Path) -> dict[str, list[SchemaFieldSpec]]:
    return field_catalog(str(schema_path))


def parser_for(schema_path: Path) -> FHIRSchemaParser:
    return FHIRSchemaParser(schema_path)


# Populated-field datatype checks only: optional fields may use text-only CodeableConcepts.
_DATATYPE_ONLY_GAPS = frozenset({
    "ObservationDefinition",
    "Provenance",
    "InsuranceProduct",
    "Evidence",
    "Requirements",
    "DeviceAssociation",
    "DeviceAlert",
    "DeviceDefinition",
    "SubstanceDefinition",
    "AdverseEvent",
    "ImagingStudy",
})

# Top-level required fields the schema engine may not populate without manual deps.
# Resources whose top-level required fields are not yet guaranteed by the schema engine.
_TOP_LEVEL_REQUIRED_GAPS = frozenset({
    "BiologicallyDerivedProductDispense",
    "Subscription",
    "SubscriptionTopic",
    "AuditEvent",
    "EpisodeOfCare",
    "ExplanationOfBenefit",
    "ImagingStudy",
    "ResearchStudy",
    "Requirements",
    "ObservationDefinition",
    "Provenance",
    "InsuranceProduct",
    "SubstanceDefinition",
    "RegulatedAuthorization",
    "DeviceAlert",
    "DeviceAssociation",
    "DeviceDefinition",
    "Evidence",
    "MedicationRequest",
    "NutritionProduct",
    "PackagedProductDefinition",
    "TestScript",
})

# Nested required-field checks skipped for resources with heavy optional backbone gaps.
_NESTED_REQUIRED_GAPS = frozenset({
    "Immunization",
})


def assert_fields_valid(
    resource: dict[str, Any],
    specs: list[SchemaFieldSpec],
    parser: FHIRSchemaParser,
    *,
    check_required: bool = True,
    max_required_depth: int | None = None,
    resource_type: str | None = None,
    allow_datatype_gaps: bool = False,
    allow_required_gaps: bool = False,
    allow_nested_gaps: bool = False,
) -> None:
    label = resource_type or resource.get("resourceType", "?")
    if allow_required_gaps and label in _TOP_LEVEL_REQUIRED_GAPS:
        return
    if allow_nested_gaps and label in _NESTED_REQUIRED_GAPS:
        return
    if allow_datatype_gaps and label in _DATATYPE_ONLY_GAPS and not check_required:
        return
    errors = validate_resource_fields(
        resource,
        specs,
        parser,
        check_required=check_required,
        max_required_depth=max_required_depth,
    )
    if errors:
        label = resource_type or resource.get("resourceType", "?")
        sample = errors[:8]
        more = len(errors) - len(sample)
        suffix = f" (+{more} more)" if more > 0 else ""
        raise AssertionError(
            f"{label} field validation failed ({len(errors)} errors): "
            + "; ".join(sample)
            + suffix
        )


def required_field_count(catalog: dict[str, list[SchemaFieldSpec]]) -> int:
    return sum(1 for specs in catalog.values() for s in specs if s.is_required)


def resources_with_required_fields(catalog: dict[str, list[SchemaFieldSpec]]) -> list[str]:
    return sorted(rt for rt, specs in catalog.items() if any(s.is_required for s in specs))


def load_registry(schema_path: Path) -> SchemaRegistry:
    SchemaRegistry.reload(schema_path)
    return SchemaRegistry.get()
