from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.persistence.artifacts import FileSystemArtifactStore
from kehrnel.persistence.validation import CommandValidationEngine


def test_runtime_builds_filesystem_artifact_adapter(tmp_path):
    runtime = StrategyRuntime(FileActivationRegistry(tmp_path / "registry.json"))

    adapters = runtime._build_adapters(
        "env-artifacts",
        {"artifact": {"provider": "filesystem", "root": str(tmp_path / "objects")}},
    )

    assert isinstance(adapters["artifact_store"], FileSystemArtifactStore)
    assert adapters["artifact_store"].root == (tmp_path / "objects").resolve()


def test_runtime_builds_command_validation_adapter(tmp_path):
    runtime = StrategyRuntime(FileActivationRegistry(tmp_path / "registry.json"))

    adapters = runtime._build_adapters(
        "env-validation",
        {"validation": {"provider": "command", "argv": ["validator", "{input}"]}},
    )

    assert isinstance(adapters["validation_engine"], CommandValidationEngine)
