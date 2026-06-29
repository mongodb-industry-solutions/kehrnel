from copy import deepcopy
from pathlib import Path

import pytest

from kehrnel.engine.core.pack_loader import load_strategy
from kehrnel.engine.core.registry import FileActivationRegistry
from kehrnel.engine.core.runtime import StrategyRuntime
from kehrnel.strategy_sdk import StrategyBindings


@pytest.mark.asyncio
async def test_plan_includes_patient_path_index_and_search_sort_mapping(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = Path(__file__).resolve().parents[3] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual"
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
        and [field for field, _ in idx.get("keys", [])] == ["ehr_id", "cn.p", "time_c"]
        for idx in indexes
    )
    assert any(
        idx.get("collection") == "compositions_rps"
        and [field for field, _ in idx.get("keys", [])] == ["tid", "time_c"]
        for idx in indexes
    )
    assert any(
        idx.get("collection") == "compositions_rps"
        and [field for field, _ in idx.get("keys", [])] == ["tid", "time_c", "v"]
        for idx in indexes
    )
    assert any(
        idx.get("collection") == "compositions_rps"
        and [field for field, _ in idx.get("keys", [])] == ["tid", "cn.data.ani", "cn.p", "time_c"]
        for idx in indexes
    )
    assert not any(
        idx.get("collection") == "compositions_rps"
        and [field for field, _ in idx.get("keys", [])] == ["ehr_id", "v"]
        for idx in indexes
    )
    assert not any(
        idx.get("collection") == "compositions_rps"
        and [field for field, _ in idx.get("keys", [])] == ["ehr_id", "tid", "time_c", "comp_id"]
        for idx in indexes
    )
    assert any(
        si.get("collection") == "compositions_search"
        and si.get("definition", {}).get("mappings", {}).get("fields", {}).get("sort_time", {}).get("type") == "date"
        for si in search_indexes
    )


@pytest.mark.asyncio
async def test_plan_resolves_indexes_through_configured_ibm_field_names(tmp_path):
    rt = StrategyRuntime(FileActivationRegistry(tmp_path / "reg.json"))
    pack_dir = Path(__file__).resolve().parents[3] / "src" / "kehrnel" / "engine" / "strategies" / "openehr" / "rps_dual_ibm"
    manifest = load_strategy("openehr.rps_dual_ibm", pack_dir)
    rt.register_manifest(manifest)

    cfg = deepcopy(manifest.default_config or {})
    cfg["collections"]["compositions"]["name"] = "ibm-semiflattened-compositions"
    cfg["collections"]["search"]["name"] = "compositions_search_ibm"
    cfg["fields"]["document"].update(
        {
            "tid": "template",
            "v": "version",
            "time_committed": "creation_date",
            "sort_time": "creation_date",
        }
    )

    env_id = "env-ibm"
    bindings = StrategyBindings(extras={"db": {"provider": "none"}})
    await rt.activate(env_id, manifest.id, manifest.version, cfg, bindings, allow_plaintext_bindings=True)

    plan = await rt.dispatch(env_id, "plan", {})
    indexes = ((plan or {}).get("artifacts") or {}).get("indexes", [])

    assert any(
        idx.get("collection") == "ibm-semiflattened-compositions"
        and [field for field, _ in idx.get("keys", [])] == ["ehr_id", "cn.p", "creation_date"]
        for idx in indexes
    )
    assert any(
        idx.get("collection") == "ibm-semiflattened-compositions"
        and [field for field, _ in idx.get("keys", [])] == ["template", "creation_date"]
        for idx in indexes
    )
    assert any(
        idx.get("collection") == "ibm-semiflattened-compositions"
        and [field for field, _ in idx.get("keys", [])] == ["template", "creation_date", "version"]
        for idx in indexes
    )
    assert any(
        idx.get("collection") == "ibm-semiflattened-compositions"
        and [field for field, _ in idx.get("keys", [])] == ["template", "cn.data.ani", "cn.p", "creation_date"]
        for idx in indexes
    )
