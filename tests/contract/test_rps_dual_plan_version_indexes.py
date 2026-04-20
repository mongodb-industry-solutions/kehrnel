from pathlib import Path

import pytest

from kehrnel.core.pack_loader import load_strategy
from kehrnel.core.registry import FileActivationRegistry
from kehrnel.core.runtime import StrategyRuntime
from kehrnel.strategy_sdk import StrategyBindings


@pytest.mark.asyncio
async def test_plan_includes_version_commit_indexes_and_search_sort_mapping(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = Path(__file__).resolve().parents[2] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
    manifest = load_strategy("openehr.rps_dual", pack_dir)
    rt.register_manifest(manifest)

    env_id = "env"
    bindings = StrategyBindings(extras={"db": {"provider": "none"}})
    await rt.activate(env_id, manifest.id, manifest.version, manifest.default_config or {}, bindings, allow_plaintext_bindings=True)

    plan = await rt.dispatch(env_id, "plan", {})
    artifacts = (plan or {}).get("artifacts", {})
    indexes = artifacts.get("indexes", [])
    search_indexes = artifacts.get("search_indexes", [])

    assert any(
        idx.get("collection") == "compositions_rps"
        and [field for field, _ in idx.get("keys", [])] == ["ehr_id", "tid", "time_c", "comp_id"]
        for idx in indexes
    )
    assert any(
        si.get("collection") == "compositions_search"
        and si.get("definition", {}).get("mappings", {}).get("fields", {}).get("sort_time", {}).get("type") == "date"
        for si in search_indexes
    )
