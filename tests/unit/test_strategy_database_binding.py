import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.manifest import AdapterRequirements, StrategyManifest
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.strategy_sdk.plugin import StrategyBindings


def _mongo_manifest() -> StrategyManifest:
    return StrategyManifest(
        id="test.mongo",
        name="Mongo strategy",
        version="1.0.0",
        domain="test",
        adapters=AdapterRequirements(storage=["mongodb"]),
    )


def test_mongo_strategy_requires_explicit_activation_database():
    with pytest.raises(KehrnelError) as exc:
        StrategyRuntime._strategy_database(_mongo_manifest(), {})

    assert exc.value.code == "STRATEGY_DATABASE_REQUIRED"


@pytest.mark.asyncio
async def test_manifest_database_default_is_only_a_suggestion(tmp_path):
    manifest = _mongo_manifest().model_copy(
        update={"default_config": {"database": "suggested_default"}}
    )
    runtime = StrategyRuntime(FileActivationRegistry(tmp_path / "registry.json"))
    runtime.register_manifest(manifest)

    with pytest.raises(KehrnelError) as exc:
        await runtime.activate(
            "env-1",
            manifest.id,
            manifest.version,
            {},
            StrategyBindings(extras={"db": {"provider": "none"}}),
            allow_plaintext_bindings=True,
        )

    assert exc.value.code == "STRATEGY_DATABASE_REQUIRED"


def test_strategy_database_overrides_environment_binding_database():
    bindings = StrategyRuntime._bind_strategy_database(
        _mongo_manifest(),
        {"database": "tenant_fhir"},
        {
            "db": {
                "provider": "mongodb",
                "uri": "mongodb://localhost:27017/tenant_core",
                "database": "tenant_core",
            }
        },
    )

    assert bindings["db"]["database"] == "tenant_fhir"
    assert bindings["db"]["uri"].endswith("/tenant_core")


@pytest.mark.parametrize("database", ["", "bad.name", "bad/name", "x" * 64])
def test_strategy_database_rejects_invalid_names(database):
    with pytest.raises(KehrnelError):
        StrategyRuntime._strategy_database(_mongo_manifest(), {"database": database})
