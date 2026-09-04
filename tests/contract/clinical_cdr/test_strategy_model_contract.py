"""Static contract for the FHIR model rendered by Healthcare Data Lab."""

from __future__ import annotations

import json
from pathlib import Path

from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import (
    OPERATIONAL_FIELDS,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.strategy import _KNOWN_OPS


SPEC_DIR = (
    Path(__file__).resolve().parents[3]
    / "src"
    / "kehrnel"
    / "engine"
    / "strategies"
    / "fhir"
    / "clinical_cdr"
    / "specification"
)


def _load(name: str) -> dict:
    return json.loads((SPEC_DIR / name).read_text(encoding="utf-8"))


def test_fhir_model_uses_the_shared_rps_tab_contract():
    spec = _load("spec.json")
    assert [view["id"] for view in spec["visualization"]["canvas"]["views"]] == [
        "architecture",
        "collections",
        "dictionaries",
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
    assert template["title"] == "Collection<ResourceType>"


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


def test_collection_links_are_fhir_references_not_implicit_joins():
    spec = _load("spec.json")
    links = [
        link
        for entity in spec["visualization"]["collectionModel"]["entities"]
        for link in entity.get("links", [])
    ]
    assert links
    assert all(link["type"] == "reference" for link in links)
    assert all("reference" in link["on"] for link in links)


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

    assert activation["version"] == manifest["version"]
