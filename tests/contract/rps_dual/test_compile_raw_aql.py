from pathlib import Path

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.data_target import resolved_data_target_digest
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.strategy_sdk import StrategyBindings


@pytest.mark.asyncio
async def test_compile_query_accepts_raw_aql_and_compiles_patient_pipeline(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = Path(__file__).resolve().parents[3] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    rt.register_manifest(manifest)

    env_id = "env"
    bindings = StrategyBindings(extras={"db": {"provider": "none"}})
    await rt.activate(env_id, manifest.id, manifest.version, manifest.default_config or {}, bindings, allow_plaintext_bindings=True)

    raw_aql = """
    SELECT c/uid/value AS uid
    FROM EHR e CONTAINS COMPOSITION c
    WHERE e/ehr_id/value = 'p1'
    """
    res = await rt.dispatch(env_id, "compile_query", {"domain": "openEHR", "aql": raw_aql, "debug": True})
    pipeline = (res or {}).get("plan", {}).get("pipeline", []) if isinstance(res, dict) else []

    assert pipeline
    assert "$match" in pipeline[0]


@pytest.mark.asyncio
async def test_compile_query_rejects_invalid_aql_before_execute(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = Path(__file__).resolve().parents[3] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    rt.register_manifest(manifest)

    env_id = "env"
    bindings = StrategyBindings(extras={"db": {"provider": "none"}})
    await rt.activate(env_id, manifest.id, manifest.version, manifest.default_config or {}, bindings, allow_plaintext_bindings=True)

    with pytest.raises(KehrnelError) as exc_info:
        await rt.dispatch(env_id, "compile_query", {"domain": "openEHR", "aql": "SELECT FROM"})

    assert exc_info.value.code == "INVALID_AQL"


@pytest.mark.asyncio
async def test_dispatch_rejects_a_stale_activation_precondition(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = (
        Path(__file__).resolve().parents[3]
        / "src"
        / "kehrnel"
        / "engine"
        / "strategies"
        / "openehr"
        / "rps_dual"
    )
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    rt.register_manifest(manifest)
    activation = await rt.activate(
        "env",
        manifest.id,
        manifest.version,
        manifest.default_config or {},
        StrategyBindings(extras={"db": {"provider": "none"}}),
        allow_plaintext_bindings=True,
    )

    with pytest.raises(KehrnelError) as exc_info:
        await rt.dispatch(
            "env",
            "compile_query",
            {
                "domain": "openEHR",
                "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c",
                "__activation_precondition": {
                    "activationId": "superseded-activation",
                    "strategyId": activation.strategy_id,
                    "configHash": activation.config_hash,
                    "manifestDigest": activation.manifest_digest,
                    "bindingsHash": activation.bindings_hash,
                },
            },
        )

    assert exc_info.value.code == "ACTIVATION_PRECONDITION_FAILED"
    assert exc_info.value.status == 409
    assert exc_info.value.details["mismatches"]["activationId"] == {
        "expected": "superseded-activation",
        "actual": activation.activation_id,
    }


@pytest.mark.asyncio
async def test_dispatch_rejects_a_changed_resolved_data_target(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = (
        Path(__file__).resolve().parents[3]
        / "src"
        / "kehrnel"
        / "engine"
        / "strategies"
        / "openehr"
        / "rps_dual"
    )
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    rt.register_manifest(manifest)
    binding = {
        "provider": "mongodb",
        "uri": "mongodb://user:secret@cluster-a.example:27017/source",
        "database": "ignored-by-activation",
    }
    activation = await rt.activate(
        "env",
        manifest.id,
        manifest.version,
        manifest.default_config or {},
        StrategyBindings(db=binding),
        allow_plaintext_bindings=True,
    )
    other_target = resolved_data_target_digest(
        {
            "db": {
                "provider": "mongodb",
                "uri": "mongodb://cluster-b.example:27017",
                "database": manifest.default_config["database"],
            }
        }
    )

    with pytest.raises(KehrnelError) as exc_info:
        await rt.dispatch(
            "env",
            "compile_query",
            {
                "domain": "openEHR",
                "aql": "SELECT c/uid/value AS uid FROM EHR e CONTAINS COMPOSITION c",
                "__activation_precondition": {
                    "activationId": activation.activation_id,
                    "strategyId": activation.strategy_id,
                    "configHash": activation.config_hash,
                    "manifestDigest": activation.manifest_digest,
                    "bindingsHash": activation.bindings_hash,
                    "dataTargetDigest": other_target,
                },
            },
        )

    assert exc_info.value.code == "DATA_TARGET_PRECONDITION_FAILED"
    assert exc_info.value.status == 409


def test_resolved_data_target_digest_excludes_credentials():
    first = resolved_data_target_digest(
        {
            "db": {
                "provider": "mongodb",
                "uri": "mongodb://alice:secret-a@cluster.example:27017/source",
                "database": "reviewed",
            }
        }
    )
    rotated = resolved_data_target_digest(
        {
            "db": {
                "provider": "mongodb",
                "uri": "mongodb://bob:secret-b@cluster.example:27017/other",
                "database": "reviewed",
            }
        }
    )
    moved = resolved_data_target_digest(
        {
            "db": {
                "provider": "mongodb",
                "uri": "mongodb://cluster-2.example:27017",
                "database": "reviewed",
            }
        }
    )

    assert first == rotated
    assert first != moved
    assert "secret" not in str(first)
