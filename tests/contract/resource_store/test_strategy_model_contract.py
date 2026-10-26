"""Static contract for the FHIR model rendered by Healthcare Data Lab."""

from __future__ import annotations

import json
from pathlib import Path

from kehrnel.engine.strategies.fhir.resource_store.scripts.serialization import (
    OPERATIONAL_FIELDS,
)
from kehrnel.engine.strategies.fhir.resource_store.scripts.strategy import _KNOWN_OPS


SPEC_DIR = (
    Path(__file__).resolve().parents[3]
    / "src"
    / "kehrnel"
    / "engine"
    / "strategies"
    / "fhir"
    / "resource_store"
    / "specification"
)


def _load(name: str) -> dict:
    return json.loads((SPEC_DIR / name).read_text(encoding="utf-8"))


def test_strategy_identity_is_resource_store():
    manifest = _load("manifest.json")

    assert manifest["id"] == "fhir.resource_store"
    assert manifest["name"] == "FHIR Resource Store"
    assert manifest["entrypoint"] == (
        "kehrnel.engine.strategies.fhir.resource_store.strategy:"
        "FHIRResourceStoreStrategy"
    )


def test_fhir_model_declares_only_views_it_can_populate():
    spec = _load("spec.json")
    assert [view["id"] for view in spec["visualization"]["canvas"]["views"]] == [
        "architecture",
        "collections",
        "transform",
    ]


def test_fhir_model_describes_the_polymorphic_collection_family():
    spec = _load("spec.json")
    source_type = spec["logicalModel"]["source"]["types"][0]
    template = spec["logicalModel"]["destinations"]["collectionTemplate"]

    assert source_type["kind"] == "polymorphic-document"
    assert source_type["discriminator"] == "resourceType"
    assert source_type["identity"] == ["resourceType", "id"]
    assert "choice[x]" in source_type["fields"]
    assert source_type["fields"]["reference"]["type"] == "Reference<TargetUnion>"
    assert template["title"] == "FHIR resource collection family"
    assert template["cardinality"].startswith("N = runtime.fhir_capabilities")

    destinations = spec["logicalModel"]["destinations"]["types"]
    stores = spec["storageModel"]["stores"]
    entities = spec["visualization"]["collectionModel"]["entities"]
    assert [item["kind"] for item in destinations] == ["collection-family"]
    assert [item["destinationType"] for item in stores] == ["collection-family"]
    assert [item["id"] for item in entities if item["id"].startswith("coll.resource")] == [
        "coll.resource_template"
    ]


def test_serialization_and_model_share_one_operational_field_contract():
    spec = _load("spec.json")
    documented = set(
        spec["physicalProfiles"]["operationalProjection"]["serializationDenylist"]
    )
    assert documented == set(OPERATIONAL_FIELDS)


def test_model_does_not_duplicate_runtime_resource_counts_or_search_indexes():
    spec = _load("spec.json")
    manifest = _load("manifest.json")
    serialized = json.dumps({"spec": spec, "manifest": manifest})

    assert "52" not in serialized
    assert manifest["ui"]["index_contract"]["configResolved"] is True
    assert spec["storageModel"]["indexContract"]["search"]["source"] == (
        "active fhir-mql resource configuration"
    )
    assert all(
        [index["name"] for index in store["indexes"]] == ["id_unique"]
        for store in spec["storageModel"]["stores"]
    )
    assert spec["meta"]["compat"]["optional"] == ["search.atlas_vector"]


def test_collection_model_uses_one_explained_resource_family_without_operational_job_collections():
    spec = _load("spec.json")
    entities = spec["visualization"]["collectionModel"]["entities"]
    family = next(item for item in entities if item["id"] == "coll.resource_template")

    assert family["kind"] == "collection-family"
    assert family["label"] == "FHIR resource collection family"
    assert "Reference<T>" in family["fields"]
    assert spec["visualization"]["collectionModel"]["examples"] == [
        "Patient",
        "Observation",
        "Encounter",
        "…",
    ]
    assert "polymorphic" not in family["badges"]
    assert not any(item["id"] == "coll.migration_control" for item in entities)
    assert not any(item["label"] in {"Patient", "Observation", "Encounter"} for item in entities)


def test_collection_model_is_a_strategy_invariant_not_activation_configuration():
    schema = _load("schema.json")
    defaults = _load("defaults.json")

    assert "collections" not in schema["properties"]
    assert "collections" not in schema["required"]
    assert "collections" not in defaults


def test_query_modes_follow_the_shared_strategy_shape():
    spec = _load("spec.json")
    modes = spec["queryModel"]["modes"]

    assert {mode["id"] for mode in modes} == {
        "resource_read",
        "type_search",
        "compartment_search",
        "compile_explain",
    }
    assert all(mode.get("uses") and mode.get("pattern") and mode.get("notes") for mode in modes)


def test_manifest_advertises_only_implemented_fhir_operations():
    manifest = _load("manifest.json")
    advertised = {operation["name"] for operation in manifest["ops"]}

    assert advertised == set(_KNOWN_OPS)
    assert all(operation["kind"] != "extension" for operation in manifest["ops"])


def test_manifest_contains_no_cross_domain_query_language_copy():
    manifest_text = json.dumps(_load("manifest.json")).lower()

    assert "aql" not in manifest_text
    assert "openehr" not in manifest_text
    assert "negotiate_fhir_search" not in manifest_text


def test_activation_sample_tracks_manifest_version():
    manifest = _load("manifest.json")
    activation = _load("activate_dev.json")
    spec = _load("spec.json")

    assert activation["version"] == manifest["version"]
    assert spec["meta"]["specVersion"] == manifest["spec"]["version"]


def test_every_fhir_configuration_field_has_user_guidance():
    schema = _load("schema.json")
    missing: list[str] = []

    def visit(node: object, path: str) -> None:
        if not isinstance(node, dict):
            return
        for name, child in (node.get("properties") or {}).items():
            child_path = f"{path}.{name}"
            if not child.get("description"):
                missing.append(child_path)
            visit(child, child_path)
        visit(node.get("items"), f"{path}[]")
        additional = node.get("additionalProperties")
        if isinstance(additional, dict):
            visit(additional, f"{path}.*")

    visit(schema, "config")
    assert missing == []


def test_ig_package_configuration_declares_guided_upload_and_profile_selection():
    schema = _load("schema.json")
    ig = schema["properties"]["implementation_guides"]["properties"]

    assert ig["packages"]["x-ui"]["control"] == "fhir-package-upload"
    assert ig["packages"]["x-ui"]["profileOptionsPath"] == (
        "implementation_guides.active_profiles"
    )
    assert ig["active_profiles"]["x-ui"]["control"] == "tag-list"
