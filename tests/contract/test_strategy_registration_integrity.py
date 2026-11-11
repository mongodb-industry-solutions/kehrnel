import json
from pathlib import Path

import pytest

from kehrnel.engine.core.activation import EnvironmentActivation
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.strategy_identity import package_reference


def pack(root, version="1.0.0", payload="first"):
    root.mkdir(exist_ok=True)
    data = {"id": "synthetic.private", "name": "Synthetic", "version": version, "domain": "synthetic", "entrypoint": "never_import:Strategy"}
    (root / "manifest.json").write_text(json.dumps(data))
    (root / "code.py").write_text(payload)
    manifest = StrategyManifest(**data)
    manifest.pack_root = str(root.resolve())
    return manifest


def test_exact_repeat_preserves_registry_bytes_and_activation(tmp_path):
    manifest = pack(tmp_path / "pack")
    registry = FileActivationRegistry(tmp_path / "registry.json")
    registry.register_manifest(manifest)
    activation = registry.activate(EnvironmentActivation("runtime", "synthetic", manifest.id, manifest.version))
    before = registry.path.read_bytes()
    registry.register_manifest(manifest.model_copy())
    assert registry.path.read_bytes() == before
    assert registry.get_activation("runtime", "synthetic") is activation
    assert FileActivationRegistry(registry.path).manifest_identities == registry.manifest_identities


@pytest.mark.parametrize("change", ["version", "content", "different_root"])
def test_conflict_precedes_memory_or_persisted_mutation(tmp_path, change):
    manifest = pack(tmp_path / "pack")
    registry = FileActivationRegistry(tmp_path / "registry.json")
    registry.register_manifest(manifest)
    before = registry.path.read_bytes()
    if change == "version":
        candidate = pack(tmp_path / "candidate", "1.0.1")
    elif change == "different_root":
        candidate = pack(tmp_path / "candidate", payload="conflicting bytes")
    else:
        (tmp_path / "pack/code.py").write_text("changed after registration")
        candidate = manifest.model_copy()
    with pytest.raises(KehrnelError) as error:
        registry.register_manifest(candidate)
    assert error.value.status == 409
    assert registry.path.read_bytes() == before
    assert registry.get_manifest(manifest.id) is manifest
    assert registry.manifest_identities[manifest.id] == json.loads(before)["manifest_identities"][manifest.id]


def test_sdk_manifest_only_registration_remains_usable_but_not_http_admitted(tmp_path):
    registry = FileActivationRegistry(tmp_path / "registry.json")
    manifest = StrategyManifest(id="sdk", name="SDK", domain="synthetic", version="1")
    registry.register_manifest(manifest)
    registry.register_manifest(manifest.model_copy())
    assert registry.get_manifest("sdk") is manifest
    with pytest.raises(KehrnelError):
        package_reference(manifest)
    with pytest.raises(KehrnelError) as error:
        registry.register_manifest(manifest.model_copy(update={"version": "2"}))
    assert error.value.status == 409


def test_startup_batch_and_sidecar_conflicts_leave_registry_bytes_unchanged(tmp_path, monkeypatch):
    from kehrnel.api import app as app_module
    first = pack(tmp_path / "first")
    registry = FileActivationRegistry(tmp_path / "registry.json")
    registry.register_manifest(first)
    before = registry.path.read_bytes()
    candidate = pack(tmp_path / "candidate", "2")
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "false")
    monkeypatch.setattr(app_module, "_load_manifests", lambda: ([candidate], [], {}))
    with pytest.raises(KehrnelError):
        app_module.create_app(str(registry.path), str(tmp_path / "bundles"))
    assert registry.path.read_bytes() == before
    ref = package_reference(first)
    sidecar = tmp_path / "access.json"
    sidecar.write_text(json.dumps({"contract": "strategy-access/v1", "records": [{"ref": ref}, {"ref": ref, "owner": {"kind": "team", "id": "other"}}]}))
    sidecar_before = sidecar.read_bytes()
    monkeypatch.setenv("KEHRNEL_STRATEGY_ACCESS_FILE", str(sidecar))
    monkeypatch.setattr(app_module, "_load_manifests", lambda: ([first], [], {}))
    with pytest.raises(ValueError, match="Duplicate"):
        app_module.create_app(str(registry.path), str(tmp_path / "bundles"))
    assert registry.path.read_bytes() == before and sidecar.read_bytes() == sidecar_before


def test_manifest_version_must_match_package_bytes(tmp_path):
    manifest = pack(tmp_path / "pack")
    with pytest.raises(KehrnelError):
        FileActivationRegistry(tmp_path / "registry.json").register_manifest(manifest.model_copy(update={"version": "2"}))


def test_sdk_fractional_config_unicode_keys_roundtrip_and_conflict_detection(tmp_path):
    from kehrnel.engine.core.strategy_identity import registration_identity
    config = {"threshold": 0.5, "límit": 1.25, "nested": {"β": -0.25, "a": [0.125, {"é": 2.5}]}}
    schema = {"type": "object", "properties": {"límit": {"type": "number", "minimum": 0.125}}}
    manifest = StrategyManifest(id="sdk.fractional", name="SDK", domain="synthetic", version="1",
                                default_config=config, config_schema=schema)
    registry = FileActivationRegistry(tmp_path / "registry.json")
    registry.register_manifest(manifest)
    before = registry.path.read_bytes()
    reordered = manifest.model_copy(update={"default_config": {"nested": {"a": [0.125, {"é": 2.5}], "β": -0.25}, "límit": 1.25, "threshold": 0.5}})
    assert registration_identity(reordered) == registration_identity(manifest)
    registry.register_manifest(reordered)
    assert registry.path.read_bytes() == before
    reloaded = FileActivationRegistry(registry.path)
    saved = reloaded.get_manifest(manifest.id)
    assert saved.default_config == config and saved.config_schema == schema
    assert isinstance(saved.default_config["threshold"], float)
    reloaded.register_manifest(saved.model_copy())
    assert reloaded.path.read_bytes() == before
    changed = saved.model_copy(update={"default_config": {**config, "threshold": 0.75}})
    with pytest.raises(KehrnelError) as error:
        reloaded.register_manifest(changed)
    assert error.value.status == 409 and reloaded.path.read_bytes() == before
    with pytest.raises(KehrnelError):
        package_reference(saved)


def test_sdk_identity_does_not_relax_package_canonical_subset(tmp_path):
    from kehrnel.contracts.package_digest import canonical_json_bytes, compute_package_digest
    from kehrnel.engine.core.strategy_identity import registration_identity
    for value in ({"threshold": 0.5}, {"límit": 1}):
        with pytest.raises(TypeError):
            canonical_json_bytes(value)
    manifest = pack(tmp_path / "pack")
    digest = compute_package_digest(manifest.pack_root)
    assert registration_identity(manifest)["packageDigest"] == digest
    (Path(manifest.pack_root) / "code.py").write_text("changed bytes")
    assert compute_package_digest(manifest.pack_root) != digest


def test_symlinked_manifest_is_rejected_before_external_metadata_read(tmp_path, monkeypatch):
    from kehrnel.api import app as app_module
    from kehrnel.contracts.package_digest import PackageLayoutError
    from kehrnel.engine.core.pack_loader import load_strategy
    from kehrnel.engine.core.strategy_identity import registration_identity
    root = tmp_path / "pack"; root.mkdir()
    outside = tmp_path / "outside.json"
    data = {"id": "synthetic.link", "name": "Synthetic", "version": "1", "domain": "synthetic"}
    outside.write_text(json.dumps(data))
    (root / "manifest.json").symlink_to(outside)
    original_read = Path.read_text
    def guarded_read(path, *args, **kwargs):
        if path.resolve() == outside.resolve():
            raise AssertionError("External metadata must not be read")
        return original_read(path, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", guarded_read)
    with pytest.raises(KehrnelError):
        load_strategy(data["id"], root)
    manifest = StrategyManifest(**data); manifest.pack_root = str(root)
    with pytest.raises(PackageLayoutError):
        registration_identity(manifest)
    monkeypatch.setattr(app_module, "_strategy_paths", lambda: [root])
    manifests, diagnostics, _ = app_module._load_manifests()
    assert manifests == [] and diagnostics
    assert "External metadata" not in str(diagnostics)
