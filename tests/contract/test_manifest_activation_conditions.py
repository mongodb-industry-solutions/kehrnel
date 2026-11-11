"""Manifest-borne activation conditions, attestation recording, and seed guardrails."""
import os
from pathlib import Path

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.strategy_sdk.plugin import StrategyBindings


PACK_DIR = Path("src/kehrnel/engine/strategies/snomedct/mongodb")
MONGODB_BINDINGS = {
    "db": {
        "provider": "mongodb",
        "uri": "mongodb://localhost:27017/tenant_core",
        "database": "tenant_core",
    }
}


def _snomed_manifest():
    return load_strategy("snomedct.mongodb", PACK_DIR)


def _snomed_config(manifest, database: str = "tenant_snomedct"):
    config = dict(manifest.default_config or {})
    config["database"] = database
    return config


def _runtime_with_snomed(tmp_path) -> StrategyRuntime:
    runtime = StrategyRuntime(FileActivationRegistry(tmp_path / "registry.json"))
    runtime.register_manifest(_snomed_manifest())
    return runtime


def test_snomed_pack_declares_activation_contract():
    manifest = _snomed_manifest()

    assert manifest.ui.activatable is True
    assert manifest.maturity == "preview"

    activation = manifest.activation
    assert activation is not None
    assert activation.requires_database is True
    assert activation.required_attestation_ids == ["snomed_license_attestation"]
    assert activation.readiness_op == "snomed_readiness"

    attestation = next(c for c in activation.conditions if c.id == "snomed_license_attestation")
    assert attestation.kind == "attestation"
    assert attestation.statement_version == 1
    assert attestation.statement
    assert "es" in attestation.statement_translations
    urls = {reference.url for reference in attestation.references}
    assert "https://www.snomed.org/get-snomed" in urls
    assert any("sanidad.gob.es" in url for url in urls)

    assert activation.seed is None



@pytest.mark.asyncio
async def test_activation_without_required_attestation_is_refused(tmp_path):
    runtime = _runtime_with_snomed(tmp_path)
    manifest = _snomed_manifest()

    with pytest.raises(KehrnelError) as exc:
        await runtime.activate(
            "env-1",
            manifest.id,
            manifest.version,
            _snomed_config(manifest),
            StrategyBindings(**MONGODB_BINDINGS),
            allow_plaintext_bindings=True,
        )

    assert exc.value.code == "SNOMED_LICENSE_ATTESTATION_REQUIRED"
    assert exc.value.status == 422
    assert exc.value.details["condition_id"] == "snomed_license_attestation"
    assert exc.value.details["statement_version"] == 1
    assert runtime.registry.get_activation("env-1", "snomedct") is None


@pytest.mark.asyncio
async def test_activation_records_accepted_attestation(tmp_path):
    runtime = _runtime_with_snomed(tmp_path)
    manifest = _snomed_manifest()

    activation = await runtime.activate(
        "env-1",
        manifest.id,
        manifest.version,
        _snomed_config(manifest),
        StrategyBindings(**MONGODB_BINDINGS),
        allow_plaintext_bindings=True,
        condition_acceptances=[
            {"id": "snomed_license_attestation", "accepted": True, "accepted_by": "qa@example.org"}
        ],
    )

    records = activation.condition_acceptances
    assert len(records) == 1
    assert records[0]["id"] == "snomed_license_attestation"
    assert records[0]["statement_version"] == 1
    assert records[0]["accepted_by"] == "qa@example.org"
    assert records[0]["accepted_at"]

    persisted = runtime.registry.get_activation("env-1", "snomedct")
    assert persisted.condition_acceptances == records


@pytest.mark.asyncio
async def test_unknown_condition_acceptance_is_rejected(tmp_path):
    runtime = _runtime_with_snomed(tmp_path)
    manifest = _snomed_manifest()

    with pytest.raises(KehrnelError) as exc:
        await runtime.activate(
            "env-1",
            manifest.id,
            manifest.version,
            _snomed_config(manifest),
            StrategyBindings(**MONGODB_BINDINGS),
            allow_plaintext_bindings=True,
            condition_acceptances=[{"id": "not_declared", "accepted": True}],
        )

    assert exc.value.code == "INVALID_CONDITION_ACCEPTANCE"


@pytest.mark.asyncio
async def test_prior_acceptance_stays_valid_for_same_statement_version(tmp_path):
    runtime = _runtime_with_snomed(tmp_path)
    manifest = _snomed_manifest()
    config = _snomed_config(manifest)

    await runtime.activate(
        "env-1",
        manifest.id,
        manifest.version,
        config,
        StrategyBindings(**MONGODB_BINDINGS),
        allow_plaintext_bindings=True,
        condition_acceptances=[{"id": "snomed_license_attestation", "accepted": True}],
    )

    # Re-activating without a new acceptance keeps the recorded one.
    activation = await runtime.activate(
        "env-1",
        manifest.id,
        manifest.version,
        config,
        StrategyBindings(**MONGODB_BINDINGS),
        allow_plaintext_bindings=True,
    )

    assert activation.already_active is True
    assert len(activation.condition_acceptances) == 1


@pytest.mark.asyncio
async def test_statement_version_bump_requires_fresh_acceptance(tmp_path):
    runtime = _runtime_with_snomed(tmp_path)
    manifest = _snomed_manifest()
    config = _snomed_config(manifest)

    await runtime.activate(
        "env-1",
        manifest.id,
        manifest.version,
        config,
        StrategyBindings(**MONGODB_BINDINGS),
        allow_plaintext_bindings=True,
        condition_acceptances=[{"id": "snomed_license_attestation", "accepted": True}],
    )

    bumped = manifest.model_copy(deep=True)
    bumped.activation.conditions[0].statement_version = 2
    bumped.activation.conditions[0].statement = "Updated statement."
    runtime.register_manifest(bumped)

    with pytest.raises(KehrnelError) as exc:
        await runtime.activate(
            "env-1",
            manifest.id,
            manifest.version,
            config,
            StrategyBindings(**MONGODB_BINDINGS),
            allow_plaintext_bindings=True,
        )

    assert exc.value.code == "SNOMED_LICENSE_ATTESTATION_REQUIRED"

    activation = await runtime.activate(
        "env-1",
        manifest.id,
        manifest.version,
        config,
        StrategyBindings(**MONGODB_BINDINGS),
        allow_plaintext_bindings=True,
        condition_acceptances=[{"id": "snomed_license_attestation", "accepted": True}],
    )
    assert activation.condition_acceptances[0]["statement_version"] == 2


@pytest.mark.asyncio
async def test_seed_without_recorded_attestation_is_refused(tmp_path):
    runtime = _runtime_with_snomed(tmp_path)
    manifest = _snomed_manifest()

    activation = await runtime.activate(
        "env-1",
        manifest.id,
        manifest.version,
        _snomed_config(manifest),
        StrategyBindings(**MONGODB_BINDINGS),
        allow_plaintext_bindings=True,
        condition_acceptances=[{"id": "snomed_license_attestation", "accepted": True}],
    )
    # Simulate a legacy activation whose attestation was never recorded.
    activation.condition_acceptances = []

    assert await runtime.provision_activation_seed("env-1", activation) is None
    with pytest.raises(KehrnelError) as exc:
        await runtime.provision_activation_seed("env-1", activation, force=True)
    assert exc.value.code == "SNOMED_DISTRIBUTION_DISABLED"


def test_seed_target_collection_respects_configured_names():
    manifest = _snomed_manifest()
    defaults = manifest.default_config

    assert (
        StrategyRuntime._seed_target_collection(manifest, defaults, "snomed_concepts")
        == "snomed_concepts"
    )

    overridden = dict(defaults)
    overridden["collections"] = dict(defaults["collections"])
    overridden["collections"]["concepts"] = "snomed-irbd"
    overridden["collections"]["terms"] = "snomed-term-search"

    assert (
        StrategyRuntime._seed_target_collection(manifest, overridden, "snomed_concepts")
        == "snomed-irbd"
    )
    assert (
        StrategyRuntime._seed_target_collection(manifest, overridden, "snomed_terms")
        == "snomed-term-search"
    )
    # Collections without a store mapping keep their declared name.
    assert (
        StrategyRuntime._seed_target_collection(manifest, overridden, "snomed_gold_notes")
        == "snomed_gold_notes"
    )


@pytest.mark.asyncio
async def test_seed_refuses_to_copy_a_collection_onto_itself():
    from types import SimpleNamespace

    db = SimpleNamespace(name="same_database")
    with pytest.raises(KehrnelError) as exc:
        await StrategyRuntime._copy_seed_collection(db, db, "snomed_concepts", "snomed_concepts")
    assert exc.value.code == "SEED_SOURCE_EQUALS_TARGET"


@pytest.mark.asyncio
async def test_legacy_manifest_seed_cannot_distribute_snomed(tmp_path):
    from types import SimpleNamespace
    runtime = _runtime_with_snomed(tmp_path)
    manifest = _snomed_manifest()
    # Even a previously registered manifest advertising a seed must be blocked.
    from kehrnel.contracts.manifest import ActivationSeed
    manifest.activation.seed = ActivationSeed(kind="collection_copy", source_database="terminology", collections=[{"name":"snomed-irbd", "from":"snomed-irbd"}], license_class="sample")
    runtime.registry.get_manifest = lambda strategy_id: manifest
    activation = SimpleNamespace(strategy_id=manifest.id)
    assert await runtime.provision_activation_seed("env-1", activation) is None
    with pytest.raises(KehrnelError) as exc:
        await runtime.provision_activation_seed("env-1", activation, force=True)
    assert exc.value.code == "SNOMED_DISTRIBUTION_DISABLED"
