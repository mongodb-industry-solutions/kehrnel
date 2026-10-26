"""
Schema-driven reference integrity integration tests (FHIR R5 v5 + R6 v6 schemas).

Validates that generated resources only contain ``ResourceType/id`` references
that resolve against the session ReferenceStore, and that every schema-defined
Reference field path that is populated points at a registered resource.
"""

from __future__ import annotations

import hashlib

import pytest

from fhir_gen import ResourceGenerator
from fhir_gen.schema.reference_paths import collect_reference_paths
from fhir_gen.schema.registry import SchemaRegistry

from .reference_validation import (
    V5_SCHEMA_PATH,
    V6_SCHEMA_PATH,
    assert_all_references_resolve,
    assert_schema_paths_resolve,
    catalog_for,
    find_references,
    resources_with_reference_fields,
    total_reference_paths,
)

pytestmark = pytest.mark.integration

_V5_CATALOG = catalog_for(V5_SCHEMA_PATH)
_V6_CATALOG = catalog_for(V6_SCHEMA_PATH)
_V5_ALL_RESOURCES = sorted(_V5_CATALOG.keys())
_V6_ALL_RESOURCES = sorted(_V6_CATALOG.keys())
_V5_REF_RESOURCES = resources_with_reference_fields(_V5_CATALOG)
_V6_REF_RESOURCES = resources_with_reference_fields(_V6_CATALOG)


def _seed_for(resource_type: str, schema_label: str) -> int:
    digest = hashlib.sha256(f"{schema_label}:{resource_type}".encode()).hexdigest()
    return int(digest[:8], 16)


@pytest.fixture(scope="session", autouse=True)
def _restore_default_schema() -> None:
    """Return global registry to packaged R5 schema after this module."""
    yield
    SchemaRegistry.reload(V5_SCHEMA_PATH)


@pytest.fixture(scope="module")
def v5_catalog() -> dict:
    return catalog_for(V5_SCHEMA_PATH)


@pytest.fixture(scope="module")
def v6_catalog() -> dict:
    return catalog_for(V6_SCHEMA_PATH)


@pytest.fixture(scope="module")
def v5_resources_with_refs(v5_catalog) -> list[str]:
    return resources_with_reference_fields(v5_catalog)


@pytest.fixture(scope="module")
def v6_resources_with_refs(v6_catalog) -> list[str]:
    return resources_with_reference_fields(v6_catalog)


class TestSchemaReferenceCatalog:
    def test_v5_catalog_covers_resources_with_references(self, v5_catalog):
        assert len(v5_catalog) == 158
        with_refs = resources_with_reference_fields(v5_catalog)
        assert len(with_refs) >= 120
        assert total_reference_paths(v5_catalog) >= 800

    def test_v6_catalog_covers_resources_with_references(self, v6_catalog):
        assert len(v6_catalog) == 127
        with_refs = resources_with_reference_fields(v6_catalog)
        assert len(with_refs) >= 95
        assert total_reference_paths(v6_catalog) >= 650

    def test_nested_reference_paths_discovered_for_encounter(self, v5_catalog, v6_catalog):
        for catalog in (v5_catalog, v6_catalog):
            paths = {field.path for field in catalog["Encounter"]}
            assert "subject" in paths
            assert any("participant" in path and path.endswith("actor") for path in paths)

    def test_patient_managing_organization_in_catalog(self, v5_catalog, v6_catalog):
        for catalog in (v5_catalog, v6_catalog):
            paths = {field.path for field in catalog["Patient"]}
            assert "managingOrganization" in paths


class TestV5ReferenceIntegrity:
    @pytest.mark.parametrize("resource_type", _V5_ALL_RESOURCES)
    def test_generated_resource_all_references_resolve(self, resource_type: str, v5_catalog):
        seed = _seed_for(resource_type, "v5")
        SchemaRegistry.reload(V5_SCHEMA_PATH)
        gen = ResourceGenerator(seed=seed)
        generated = gen.generate(resource_type, count=1, schema_path=str(V5_SCHEMA_PATH))[0]

        assert generated["resourceType"] == resource_type
        for resource in gen.store.all_resources():
            assert_all_references_resolve(
                resource, gen.store, session_store=gen.store,
            )

        schema_paths = v5_catalog[resource_type]
        if schema_paths:
            assert_schema_paths_resolve(
                generated, gen.store, schema_paths, session_store=gen.store,
            )

    @pytest.mark.parametrize("resource_type", _V5_REF_RESOURCES)
    def test_each_schema_reference_field_path_when_present(self, resource_type: str, v5_catalog):
        """Every populated schema Reference path must resolve (field-level coverage)."""
        seed = _seed_for(resource_type, "v5-paths")
        SchemaRegistry.reload(V5_SCHEMA_PATH)
        gen = ResourceGenerator(seed=seed)
        resource = gen.generate(resource_type, count=1, schema_path=str(V5_SCHEMA_PATH))[0]
        assert_schema_paths_resolve(
            resource, gen.store, v5_catalog[resource_type], session_store=gen.store,
        )

    def test_v5_clinical_bundle_full_store_integrity(self, v5_catalog):
        SchemaRegistry.reload(V5_SCHEMA_PATH)
        gen = ResourceGenerator(seed=2025)
        types = [
            "Organization", "Patient", "Practitioner", "Location",
            "Encounter", "Condition", "Observation", "Medication",
            "MedicationRequest", "DiagnosticReport", "Claim", "Coverage",
        ]
        gen.generate_many(
            types,
            counts={
                "Organization": 3,
                "Patient": 8,
                "Practitioner": 3,
                "Location": 2,
                "Encounter": 10,
                "Condition": 12,
                "Observation": 20,
                "Medication": 5,
                "MedicationRequest": 8,
                "DiagnosticReport": 6,
                "Coverage": 4,
                "Claim": 4,
            },
        )
        for resource in gen.store.all_resources():
            assert_all_references_resolve(
                resource, gen.store, session_store=gen.store,
            )
            paths = v5_catalog.get(resource["resourceType"], [])
            if paths:
                assert_schema_paths_resolve(
                    resource, gen.store, paths, session_store=gen.store,
                )

    def test_v5_no_orphan_type_id_in_primary_resource(self, v5_catalog):
        SchemaRegistry.reload(V5_SCHEMA_PATH)
        gen = ResourceGenerator(seed=77)
        for resource_type in ("Patient", "MedicationRequest", "Claim", "CarePlan"):
            resource = gen.generate(resource_type, count=1, schema_path=str(V5_SCHEMA_PATH))[0]
            refs = find_references(resource)
            for ref in refs:
                assert gen.store.reference_is_valid(ref.reference), (
                    f"{resource_type}.{ref.path} -> {ref.reference}"
                )


class TestV6ReferenceIntegrity:
    @pytest.mark.parametrize("resource_type", _V6_ALL_RESOURCES)
    def test_generated_resource_all_references_resolve(self, resource_type: str, v6_catalog):
        seed = _seed_for(resource_type, "v6")
        SchemaRegistry.reload(V6_SCHEMA_PATH)
        gen = ResourceGenerator(seed=seed)
        generated = gen.generate(resource_type, count=1, schema_path=str(V6_SCHEMA_PATH))[0]

        assert generated["resourceType"] == resource_type
        for resource in gen.store.all_resources():
            assert_all_references_resolve(
                resource, gen.store, session_store=gen.store,
            )

        schema_paths = v6_catalog[resource_type]
        if schema_paths:
            assert_schema_paths_resolve(
                generated, gen.store, schema_paths, session_store=gen.store,
            )

    @pytest.mark.parametrize("resource_type", _V6_REF_RESOURCES)
    def test_each_schema_reference_field_path_when_present(self, resource_type: str, v6_catalog):
        seed = _seed_for(resource_type, "v6-paths")
        SchemaRegistry.reload(V6_SCHEMA_PATH)
        gen = ResourceGenerator(seed=seed)
        resource = gen.generate(resource_type, count=1, schema_path=str(V6_SCHEMA_PATH))[0]
        assert_schema_paths_resolve(
            resource, gen.store, v6_catalog[resource_type], session_store=gen.store,
        )

    def test_v6_clinical_bundle_full_store_integrity(self, v6_catalog):
        SchemaRegistry.reload(V6_SCHEMA_PATH)
        gen = ResourceGenerator(seed=2026)
        types = [
            "Organization", "Patient", "Practitioner", "Location",
            "Encounter", "Condition", "Observation", "MedicationRequest",
        ]
        gen.generate_many(
            types,
            counts={
                "Organization": 2,
                "Patient": 5,
                "Practitioner": 2,
                "Location": 2,
                "Encounter": 6,
                "Condition": 8,
                "Observation": 12,
                "MedicationRequest": 5,
            },
        )
        for resource in gen.store.all_resources():
            assert_all_references_resolve(
                resource, gen.store, session_store=gen.store,
            )
            paths = v6_catalog.get(resource["resourceType"], [])
            if paths:
                assert_schema_paths_resolve(
                    resource, gen.store, paths, session_store=gen.store,
                )

    def test_v5_and_v6_same_resource_reference_rules(self):
        """Patient references resolve under both schema versions when generated."""
        for schema_path, label in ((V5_SCHEMA_PATH, "v5"), (V6_SCHEMA_PATH, "v6")):
            SchemaRegistry.reload(schema_path)
            gen = ResourceGenerator(seed=42)
            gen.generate_many(
                ["Organization", "Patient"],
                counts={"Organization": 2, "Patient": 3},
            )
            org_ids = {e["id"] for e in gen.store._store.get("Organization", [])}
            for entry in gen.store._store.get("Patient", []):
                patient = entry["resource"]
                mo = patient.get("managingOrganization") or {}
                ref = mo.get("reference", "")
                if ref.startswith("Organization/"):
                    assert ref.split("/", 1)[1] in org_ids, f"{label} Patient org ref broken"


class TestReferencePathDiscovery:
    @pytest.mark.parametrize(
        ("schema_path", "resource_type", "expected_path"),
        [
            (V5_SCHEMA_PATH, "Encounter", "subject"),
            (V5_SCHEMA_PATH, "MedicationRequest", "subject"),
            (V6_SCHEMA_PATH, "Observation", "subject"),
            (V6_SCHEMA_PATH, "Claim", "provider"),
        ],
    )
    def test_known_paths_in_schema(self, schema_path, resource_type, expected_path):
        from fhir_gen.schema.parser import FHIRSchemaParser

        parser = FHIRSchemaParser(schema_path)
        paths = {field.path for field in collect_reference_paths(parser, resource_type)}
        assert expected_path in paths
