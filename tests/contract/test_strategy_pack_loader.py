from pathlib import Path
from tempfile import TemporaryDirectory

from kehrnel.core.pack_loader import load_strategy
from kehrnel.core.runtime import StrategyRuntime
from kehrnel.core.registry import FileActivationRegistry
from kehrnel.core.errors import KehrnelError
from kehrnel.core.pack_validator import StrategyPackValidator


def test_rps_dual_pack_loads_with_spec_and_defaults():
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    assert manifest.id == "openehr.rps_dual"
    # pack_spec is attached for strategy-pack/v1 packs
    assert getattr(manifest, "pack_spec", None)
    # defaults should be hydrated into the manifest payload
    assert manifest.default_config.get("collections", {}).get("compositions", {}).get("name") == "compositions_rps"


def test_pack_config_encoding_profile_validation():
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    with TemporaryDirectory() as tmpdir:
        rt = StrategyRuntime(FileActivationRegistry(Path(tmpdir) / "registry.json"))
        # valid config passes
        rt._validate_pack_config(manifest, manifest.default_config)
        bad_config = {
            "collections": {
                "compositions": {"name": "foo", "encodingProfile": "not_real"},
                "search": {"name": "bar", "encodingProfile": "profile.search_shortcuts"},
            }
        }
        try:
            rt._validate_pack_config(manifest, bad_config)
            assert False, "Expected KehrnelError for invalid encoding profile"
        except KehrnelError as exc:
            assert exc.code == "PACK_CONFIG_INVALID"


def test_pack_validator_rejects_invalid_spec():
    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        (base / "manifest.json").write_text(
            '{"id":"tmp.invalid","version":"0.0.1","domain":"openEHR","pack_format":"strategy-pack/v1","spec":{"path":"spec.json"},"entrypoint":"foo:bar","ops":[]}',
            encoding="utf-8",
        )
        # spec missing required meta/logical fields
        (base / "spec.json").write_text('{"meta": {"strategyId": "tmp.invalid"}}', encoding="utf-8")
        errors = StrategyPackValidator({"id": "tmp.invalid", "version": "0.0.1", "domain": "openEHR", "pack_format": "strategy-pack/v1", "spec": {"path": "spec.json"}, "entrypoint": "foo:bar", "ops": []}, base).validate()
        assert any("spec.json validation error" in e for e in errors)


def test_pack_validator_missing_bundle():
    with TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        (base / "manifest.json").write_text(
            '{"id":"tmp.invalid","version":"0.0.1","domain":"openEHR","pack_format":"strategy-pack/v1","spec":{"path":"spec.json"},"entrypoint":"foo:bar","ops":[]}',
            encoding="utf-8",
        )
        (base / "spec.json").write_text(
            '{"meta":{"strategyId":"tmp.invalid","specVersion":"1.0"},"logical":{"sourceTypes":[],"destinationTypes":[]},"encodingProfiles":[],"storage":{"stores":[]},"mapping":{"pipeline":[]},"bundles":{"dictionaries":"bundles/d.json"}}',
            encoding="utf-8",
        )
        errors = StrategyPackValidator({"id": "tmp.invalid", "version": "0.0.1", "domain": "openEHR", "pack_format": "strategy-pack/v1", "spec": {"path": "spec.json"}, "entrypoint": "foo:bar", "ops": []}, base).validate()
        assert any("bundle file missing" in e for e in errors)


def test_store_profiles_materialized():
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    with TemporaryDirectory() as tmpdir:
        rt = StrategyRuntime(FileActivationRegistry(Path(tmpdir) / "registry.json"))
        profiles = rt._build_store_profiles(manifest, manifest.default_config)
        assert profiles.get("store:compositions", {}).get("collection") == "compositions_rps"
        assert profiles.get("store:search", {}).get("encodingProfile") == "profile.search_shortcuts"
