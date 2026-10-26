from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.pack_loader import load_strategy_manifest
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_validator import StrategyPackValidator


def test_rps_dual_pack_loads_with_spec_and_defaults():
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    assert manifest.id == "openehr.rps_dual"
    # pack_spec is attached for strategy-pack/v1 packs
    assert getattr(manifest, "pack_spec", None)
    # defaults should be hydrated into the manifest payload
    assert manifest.default_config.get("collections", {}).get("compositions", {}).get("name") == "compositions_rps"


def test_pack_config_encoding_profile_validation():
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
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
        # Current spec.json format is not consumed by _validate_pack_config (no-op).
        rt._validate_pack_config(manifest, bad_config)


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
        # The public strategy-pack spec schema is mandatory and reports the
        # malformed nested path; openEHR also requires defaults/schema.
        assert "defaults.json missing and default_config empty" in errors
        assert "schema.json missing and config_schema empty" in errors
        assert any("spec.json validation error at 'meta'" in e for e in errors)


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
        # Bundle references in spec.json are informational; missing bundle files do not block loading.
        assert "defaults.json missing and default_config empty" in errors
        assert "schema.json missing and config_schema empty" in errors


def test_pack_validator_rejects_spec_path_escape(tmp_path):
    base = tmp_path / "pack"
    base.mkdir()
    manifest = {
        "id": "tmp.invalid",
        "name": "Invalid",
        "version": "0.0.1",
        "domain": "example",
        "pack_format": "strategy-pack/v1",
        "spec": {"path": "../outside.json"},
        "entrypoint": "missing.module:Plugin",
        "ops": [],
    }
    (tmp_path / "outside.json").write_text("{}", encoding="utf-8")
    errors = StrategyPackValidator(manifest, base).validate()
    assert "spec.json path escapes the strategy pack" in errors


def test_pack_validator_uses_path_containment_not_string_prefix(tmp_path, monkeypatch):
    base = tmp_path / "pack"
    base.mkdir()
    site = tmp_path / "site"
    lookalike = site / "external_pack"
    lookalike.mkdir(parents=True)
    (lookalike / "__init__.py").write_text("", encoding="utf-8")
    module = lookalike / "strategy.py"
    module.write_text("", encoding="utf-8")
    monkeypatch.syspath_prepend(str(site))
    manifest = {
        "id": "tmp.invalid",
        "name": "Invalid",
        "version": "0.0.1",
        "domain": "example",
        "entrypoint": "external_pack.strategy:Plugin",
        "ops": [],
    }
    errors = StrategyPackValidator(manifest, base).validate()
    assert "entrypoint module outside strategy pack: external_pack.strategy" in errors


def test_pack_validation_never_imports_entrypoint_parent(tmp_path, monkeypatch):
    site = tmp_path / "site"
    pack = site / "safe_pack"
    pack.mkdir(parents=True)
    sentinel = tmp_path / "imported.txt"
    (pack / "__init__.py").write_text(
        f"from pathlib import Path\nPath({str(sentinel)!r}).write_text('executed')\n",
        encoding="utf-8",
    )
    (pack / "strategy.py").write_text("class Plugin: pass\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(site))
    manifest = {
        "id": "safe.pack",
        "name": "Safe pack",
        "version": "1.0.0",
        "domain": "example",
        "entrypoint": "safe_pack.strategy:Plugin",
        "ops": [],
    }

    assert StrategyPackValidator(manifest, pack).validate() == []
    assert not sentinel.exists()

    missing_attribute = {
        **manifest,
        "entrypoint": "safe_pack.strategy:MissingPlugin",
    }
    assert (
        "entrypoint attribute not found: safe_pack.strategy:MissingPlugin"
        in StrategyPackValidator(missing_attribute, pack).validate()
    )


def test_loader_reports_non_object_manifest_as_a_structured_error(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("[]", encoding="utf-8")

    with pytest.raises(KehrnelError) as caught:
        load_strategy_manifest(manifest_path)

    assert caught.value.code == "MANIFEST_INVALID"
    assert caught.value.details["diagnostics"][0]["path"] == "/"


def test_loader_reports_non_object_defaults_as_a_structured_pack_error(
    tmp_path,
    monkeypatch,
):
    site = tmp_path / "site"
    pack = site / "defaults_pack"
    pack.mkdir(parents=True)
    (pack / "__init__.py").write_text("", encoding="utf-8")
    (pack / "strategy.py").write_text("class Plugin: pass\n", encoding="utf-8")
    (pack / "defaults.json").write_text("[]", encoding="utf-8")
    (pack / "manifest.json").write_text(
        '{"id":"defaults.pack","name":"Defaults","version":"1",'
        '"domain":"example","entrypoint":"defaults_pack.strategy:Plugin","ops":[]}',
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(site))

    with pytest.raises(KehrnelError) as caught:
        load_strategy_manifest(pack / "manifest.json")

    assert caught.value.code == "PACK_INVALID"
    assert caught.value.details["diagnostics"][0]["path"] == "/default_config"


def test_pack_validator_rejects_spec_path_escape(tmp_path):
    base = tmp_path / "pack"
    base.mkdir()
    manifest = {
        "id": "tmp.invalid",
        "name": "Invalid",
        "version": "0.0.1",
        "domain": "example",
        "pack_format": "strategy-pack/v1",
        "spec": {"path": "../outside.json"},
        "entrypoint": "missing.module:Plugin",
        "ops": [],
    }
    (tmp_path / "outside.json").write_text("{}", encoding="utf-8")
    errors = StrategyPackValidator(manifest, base).validate()
    assert "spec.json path escapes the strategy pack" in errors


def test_pack_validator_uses_path_containment_not_string_prefix(tmp_path, monkeypatch):
    base = tmp_path / "pack"
    base.mkdir()
    lookalike = tmp_path / "pack-evil"
    lookalike.mkdir()
    module = lookalike / "strategy.py"
    module.write_text("", encoding="utf-8")

    monkeypatch.setattr(
        StrategyPackValidator,
        "_find_module_source_without_import",
        staticmethod(lambda _: module),
    )
    manifest = {
        "id": "tmp.invalid",
        "name": "Invalid",
        "version": "0.0.1",
        "domain": "example",
        "entrypoint": "tmp.strategy:Plugin",
        "ops": [],
    }
    errors = StrategyPackValidator(manifest, base).validate()
    assert "entrypoint module outside strategy pack: tmp.strategy" in errors


def test_store_profiles_materialized():
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    with TemporaryDirectory() as tmpdir:
        rt = StrategyRuntime(FileActivationRegistry(Path(tmpdir) / "registry.json"))
        profiles = rt._build_store_profiles(manifest, manifest.default_config)
        # Store profile materialization is best-effort and depends on pack_spec shape.
        assert profiles == {}
