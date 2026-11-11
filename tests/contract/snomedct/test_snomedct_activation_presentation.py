"""The activation GUI exposes only qualified choices from the pack schema."""
from pathlib import Path
from kehrnel.engine.core.pack_loader import load_strategy


def test_hosted_activation_requires_tenant_workspace_and_does_not_seed_catalog():
    manifest = load_strategy("snomedct.mongodb", Path("src/kehrnel/engine/strategies/snomedct/mongodb"))
    schema = manifest.config_schema
    assert manifest.default_config["database"] == "snomed-ct-mongodb"
    assert schema["properties"]["database"]["default"] == "snomed-ct-mongodb"
    assert schema["properties"]["database"]["x-ui"]["useAsActivationDefault"] is False
    assert schema["x-ui"]["activationFields"] == ["database", "source.mode", "source.id"]
    assert schema["x-ui"]["activeFields"] == []
    assert manifest.default_config["release"]["id"] == "20260601"
    assert {field["path"] for field in schema["x-ui"]["readOnlyFields"]} == {"release.id", "release.label", "languages"}
    # Settings remain in the runtime contract and defaults; presentation does not
    # pretend to remove supported programmatic configuration.
    assert "collections" in schema["properties"]
    assert manifest.default_config["collections"]["sidecar_enabled"] is True

    assert manifest.default_config["source"]["mode"] == "hosted_reference"
    assert getattr(manifest.activation, "seed", None) is None
