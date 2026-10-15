"""Regression coverage for the generator's fail-closed schema boundary."""

from __future__ import annotations

import pytest

from fhir_gen.generators.base import ResourceGenerator
from fhir_gen.schema.conformance import conform_resource_to_schema
from fhir_gen.schema.registry import SchemaRegistry
from fhir_gen.schema.versions import resolve_schema_path


@pytest.mark.parametrize("release", ["R5", "R6"])
def test_every_bundled_resource_type_generates_against_its_release(release: str):
    try:
        registry = SchemaRegistry.reload(resolve_schema_path(schema_version=release))
        for resource_type in registry.all_resources():
            resource = ResourceGenerator(seed=42).generate(resource_type, 1)[0]
            assert resource["resourceType"] == resource_type
    finally:
        SchemaRegistry.reload(resolve_schema_path(schema_version="R5"))


def test_conformance_guard_removes_optional_invalid_content_and_reports_it():
    registry = SchemaRegistry.reload(resolve_schema_path(schema_version="R5"))
    resource = {
        "resourceType": "Patient",
        "id": "patient-1",
        "active": True,
        "unknownField": "must not survive",
    }

    evidence = conform_resource_to_schema(resource, registry)

    assert evidence["passed"] is True
    assert evidence["removals"] == {"unknownField": 1}
    assert "unknownField" not in resource


def test_conformance_guard_never_invents_required_content():
    registry = SchemaRegistry.reload(resolve_schema_path(schema_version="R5"))
    resource = {"resourceType": "Observation", "id": "observation-1"}

    evidence = conform_resource_to_schema(resource, registry)

    assert evidence["passed"] is False
    assert evidence["unresolved"]


def test_generators_keep_release_schema_isolated_per_instance():
    r5 = ResourceGenerator(seed=11, schema_version="R5")
    r6 = ResourceGenerator(seed=11, schema_version="R6")

    assert "DeviceDispense" in r5.schema_registry.all_resources()
    assert "DeviceDispense" not in r6.schema_registry.all_resources()
    assert "DeviceAlert" not in r5.schema_registry.all_resources()
    assert "DeviceAlert" in r6.schema_registry.all_resources()
    assert r5.generate("DeviceDispense", 1)[0]["resourceType"] == "DeviceDispense"
    assert r6.generate("DeviceAlert", 1)[0]["resourceType"] == "DeviceAlert"
