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
    ui_index_contract = (manifest.model_dump().get("ui") or {}).get("index_contract") or {}
    spec_index_contract = ((manifest.pack_spec or {}).get("storageModel") or {}).get("indexContract") or {}

    assert ui_index_contract.get("requiredForOptimizedAql") == ["idx_patient_path", "idx_template_commit_version"]
    assert spec_index_contract.get("configResolved") is True

    assert any(
        idx.get("collection") == "compositions_rps"
        and [field for field, _ in idx.get("keys", [])] == ["ehr_id", "cn.p", "time_c"]
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

    patient_path = next(idx for idx in indexes if idx.get("id") == "idx_patient_path")
    template_commit_version = next(idx for idx in indexes if idx.get("id") == "idx_template_commit_version")
    assert patient_path.get("requirement") == "required"
    assert patient_path.get("logicalFields") == ["ehr_id", "cn.p", "time_c"]
    assert template_commit_version.get("requirement") == "required"
    assert template_commit_version.get("logicalFields") == ["tid", "time_c", "v"]
    assert not any(idx.get("id") == "idx_template_commit" for idx in indexes)

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
    search_index = next(si for si in search_indexes if si.get("collection") == "compositions_search")
    assert search_index.get("requirement") == "conditional"
    assert search_index.get("kind") == "atlas_search"
    assert "analytics_sidecar_filter" in search_index.get("workloads", [])


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
        and [field for field, _ in idx.get("keys", [])] == ["template", "creation_date", "version"]
        for idx in indexes
    )
    assert any(
        idx.get("collection") == "ibm-semiflattened-compositions"
        and [field for field, _ in idx.get("keys", [])] == ["template", "cn.data.ani", "cn.p", "creation_date"]
        for idx in indexes
    )
    template_commit_version = next(idx for idx in indexes if idx.get("id") == "idx_template_commit_version")
    assert template_commit_version.get("requirement") == "required"
    assert template_commit_version.get("logicalFields") == ["tid", "time_c", "v"]
    assert not any(idx.get("id") == "idx_template_commit" for idx in indexes)
