from pathlib import Path

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.engine.core.strategy_sdk import StrategyBindings


@pytest.mark.asyncio
async def test_compile_query_accepts_raw_aql_and_compiles_patient_pipeline(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
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
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    rt.register_manifest(manifest)

    env_id = "env"
    bindings = StrategyBindings(extras={"db": {"provider": "none"}})
    await rt.activate(env_id, manifest.id, manifest.version, manifest.default_config or {}, bindings, allow_plaintext_bindings=True)

    with pytest.raises(KehrnelError) as exc_info:
        await rt.dispatch(env_id, "compile_query", {"domain": "openEHR", "aql": "SELECT FROM"})

    assert exc_info.value.code == "INVALID_AQL"
