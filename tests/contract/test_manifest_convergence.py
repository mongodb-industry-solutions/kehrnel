import json
from pathlib import Path

import jsonschema
from pydantic import ValidationError
import pytest

from kehrnel.contracts.manifest import (
    StrategyManifest as ContractManifest,
    StrategyManifestSource,
    read_strategy_manifest,
)
from kehrnel.engine.core.manifest import StrategyManifest as RuntimeManifest
from kehrnel.engine.core.manifest_digest import compute_manifest_digest
from kehrnel.engine.core.pack_loader import iter_strategy_manifest_paths, load_strategy_manifest
from kehrnel.strategy_sdk.capabilities import StrategyCapability
from kehrnel.strategy_sdk.manifest import StrategyManifest as SDKManifest


STRATEGY_ROOT = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "engine" / "strategies"
EXPECTED_IDS = {
    "cdisc.sdr",
    "contextobjects.rps",
    "fhir.resource_store",
    "genomics.variant_first",
    "openehr.rps_dual",
    "openehr.rps_dual_ibm",
    "snomedct.mongodb",
    "x12.co_single",
}


def test_runtime_sdk_and_contract_imports_are_the_same_class():
    assert RuntimeManifest is SDKManifest is ContractManifest


def test_every_discoverable_strategy_loads_through_one_compatibility_path():
    paths = list(iter_strategy_manifest_paths(STRATEGY_ROOT))
    loaded = [load_strategy_manifest(path) for path in paths]

    assert len(paths) == len(EXPECTED_IDS)
    assert {manifest.id for manifest in loaded} == EXPECTED_IDS
    for manifest in loaded:
        assert manifest.package_digest and manifest.package_digest.startswith("sha256:")
        assert manifest.contract_digest and manifest.contract_digest.startswith("sha256:")


def test_ibm_profile_declares_inherited_rps_dual_runtime_contracts():
    primary = load_strategy_manifest(
        STRATEGY_ROOT / "openehr" / "rps_dual" / "manifest.json"
    )
    ibm = load_strategy_manifest(
        STRATEGY_ROOT / "openehr" / "rps_dual_ibm" / "manifest.json"
    )
    primary_ops = {operation.name: operation for operation in primary.ops}
    ibm_ops = {operation.name: operation for operation in ibm.ops}

    assert ibm.adapters.model_dump() == primary.adapters.model_dump()
    assert ibm.default_config["database"] == primary.default_config["database"]
    assert "database" in ibm.config_schema["required"]
    assert {
        name: operation.kind for name, operation in ibm_ops.items()
    } == {
        name: operation.kind for name, operation in primary_ops.items()
    }

    for operation_name in {
        "ensure_atlas_search_index",
        "semantic_projection_preflight",
        "rebuild_slim_search_collection",
    }:
        assert ibm_ops[operation_name].model_dump() == primary_ops[
            operation_name
        ].model_dump()


def test_qualified_and_sdk_enum_capabilities_share_one_representation():
    manifest = ContractManifest(
        id="terminology.example",
        name="Terminology",
        version="1",
        domain="terminology",
        capabilities=[StrategyCapability.QUERY, "terminology.subsumes"],
    )
    assert manifest.capabilities == ["query", "terminology.subsumes"]
    assert manifest.capability_set == {"query", "terminology.subsumes"}


def test_legacy_compatibility_shape_round_trips_without_injected_empty_fields():
    path = STRATEGY_ROOT / "cdisc" / "sdr" / "manifest.json"
    source = json.loads(path.read_text(encoding="utf-8"))
    manifest = read_strategy_manifest(source)
    assert manifest.model_dump()["compatibility"] == source["compatibility"]
    assert dict(manifest.compatibility) == source["compatibility"]
    assert dict(manifest.compatibility.items()) == source["compatibility"]


def test_strict_source_contract_requires_version_marker_and_rejects_extensions():
    base = {
        "manifestContract": "strategy-manifest/v1",
        "id": "example.strict",
        "name": "Strict",
        "version": "1.0.0",
        "domain": "example",
        "entrypoint": "example.strict:Plugin",
        "ops": [],
    }
    StrategyManifestSource.model_validate(base)
    with pytest.raises(ValidationError):
        StrategyManifestSource.model_validate({**base, "unknownExtension": True})
    with pytest.raises(ValidationError):
        StrategyManifestSource.model_validate({**base, "packageDigest": "sha256:" + "0" * 64})


@pytest.mark.parametrize(
    "patch",
    [
        {"entrypoint": None},
        {"ops": None},
        {"ops": [{"name": "missing-input-schema"}]},
        {"capabilities": [1]},
        {"adapters": None},
        {"name": " Padded "},
        {"packageDigest": "sha256:" + "0" * 64},
    ],
)
def test_strict_source_runtime_and_json_schema_reject_the_same_invalid_shapes(patch):
    base = {
        "manifestContract": "strategy-manifest/v1",
        "id": "example.strict",
        "name": "Strict",
        "version": "1.0.0",
        "domain": "example",
        "entrypoint": "example.strict:Plugin",
        "ops": [],
    }
    candidate = {**base, **patch}
    with pytest.raises(ValidationError):
        StrategyManifestSource.model_validate(candidate)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(
            StrategyManifestSource.model_json_schema()
        ).validate(candidate)


def test_strict_source_requires_entrypoint_and_ops_in_runtime_and_schema():
    candidate = {
        "manifestContract": "strategy-manifest/v1",
        "id": "example.strict",
        "name": "Strict",
        "version": "1.0.0",
        "domain": "example",
    }
    with pytest.raises(ValidationError):
        StrategyManifestSource.model_validate(candidate)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(
            StrategyManifestSource.model_json_schema()
        ).validate(candidate)


def test_strict_source_serializes_back_to_its_own_public_schema():
    source = StrategyManifestSource.model_validate(
        {
            "manifestContract": "strategy-manifest/v1",
            "id": "example.strict",
            "name": "Strict",
            "version": "1.0.0",
            "domain": "example",
            "entrypoint": "example.strict:Plugin",
            "ops": [],
        }
    )
    payload = source.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert not {"pack_spec", "packageDigest", "contractDigest"}.intersection(payload)
    jsonschema.Draft202012Validator(
        StrategyManifestSource.model_json_schema(by_alias=True)
    ).validate(payload)


def test_legacy_activation_manifest_digest_is_unchanged_by_runtime_identity():
    fhir_path = STRATEGY_ROOT / "fhir" / "resource_store" / "specification" / "manifest.json"
    manifest = load_strategy_manifest(fhir_path)
    before = compute_manifest_digest(manifest)
    manifest.package_digest = "sha256:" + "1" * 64
    manifest.contract_digest = "sha256:" + "2" * 64
    assert before
    assert compute_manifest_digest(manifest) == before
