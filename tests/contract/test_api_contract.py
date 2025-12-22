import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app, _load_manifests
from kehrnel.core.runtime import StrategyRuntime
from strategy_sdk import StrategyBindings
from tests.helpers.fixture_storage import FixtureStorage


def _pick_rps_manifest(runtime: StrategyRuntime):
    for m in runtime.list_strategies():
        if "rps_dual" in m.id:
            return m
    return runtime.list_strategies()[0]


def _activate_with_fixture(runtime: StrategyRuntime, manifest, fixture_dir: Path, env_id: str = "env-contract"):
    cfg = manifest.default_config or {}
    bindings = StrategyBindings(extras={"db": {"provider": "none"}})

    async def _act():
        await runtime.activate(env_id, manifest.id, manifest.version, cfg, bindings, allow_plaintext_bindings=True)

    asyncio.get_event_loop().run_until_complete(_act())
    runtime._env_cache[env_id] = {
        "adapters": {"storage": FixtureStorage(fixture_dir)},
        "dict_cache": {},
    }
    return env_id


def test_strategies_endpoint_contract(tmp_path: Path):
    app = create_app(str(tmp_path / "reg.json"))
    client = TestClient(app)
    res = client.get("/v1/strategies")
    assert res.status_code == 200
    data = res.json()
    assert "strategies" in data
    assert any("ops" in s and "config_schema" in s for s in data.get("strategies", []))


def test_compile_query_contract(tmp_path: Path):
    app = create_app(str(tmp_path / "reg.json"))
    client = TestClient(app)
    runtime: StrategyRuntime = app.state.strategy_runtime
    manifest = _pick_rps_manifest(runtime)
    env_id = _activate_with_fixture(runtime, manifest, Path("tests/fixtures/rps_dual"))

    payload = {
        "protocol": "openehr",
            "query": {
                "scope": "patient",
                "predicates": [{"path": "ehr_id", "op": "eq", "value": "p1"}],
                "select": [{"path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string", "alias": "Centro", "agg": "first"}],
                "projection": None,
                "limit": None,
                "sort": None,
                "offset": None,
            },
        }
    res = client.post(f"/v1/environments/{env_id}/compile_query", json=payload, params={"debug": "true"})
    assert res.status_code == 200
    body = res.json()
    assert body.get("ok") is True
    result = body["result"]
    assert "engine" in result and "plan" in result
    plan = result["plan"]
    assert "pipeline" in plan and "explain" in plan
    explain = plan["explain"]
    assert "builder" in explain and "chosen" in explain["builder"]
    assert "dicts" in explain and "stage0" in explain


def test_query_and_ops_contract(tmp_path: Path):
    app = create_app(str(tmp_path / "reg.json"))
    client = TestClient(app)
    runtime: StrategyRuntime = app.state.strategy_runtime
    manifest = _pick_rps_manifest(runtime)
    env_id = _activate_with_fixture(runtime, manifest, Path("tests/fixtures/rps_dual"))

    # query should succeed with empty result shape
    q_payload = {
        "protocol": "openehr",
        "query": {
            "scope": "patient",
            "predicates": [],
            "select": [{"path": "ehr_id", "alias": "ehr_id"}],
            "projection": None,
            "limit": None,
            "sort": None,
            "offset": None,
        },
    }
    res_q = client.post(f"/v1/environments/{env_id}/query", json=q_payload)
    assert res_q.status_code == 200
    assert res_q.json().get("ok") is True
