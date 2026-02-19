from pathlib import Path

import pytest

from tests.helpers.fake_runtime import setup_test_app


@pytest.fixture
def test_env(tmp_path):
    fixture_dir = Path("tests/fixtures/rps_dual")
    app, client, runtime, manifest, env_id = setup_test_app(tmp_path / "reg.json", fixture_dir)
    return app, client, runtime, manifest, env_id


def test_strategies_endpoints(test_env):
    app, client, runtime, manifest, env_id = test_env
    res = client.get("/strategies")
    assert res.status_code == 200
    data = res.json()
    assert data["strategies"], "strategies should not be empty"
    entry = data["strategies"][0]
    assert {"id", "version", "entrypoint", "ops", "capabilities"} <= set(entry.keys())

    res_one = client.get(f"/strategies/{manifest.id}")
    assert res_one.status_code == 200
    one = res_one.json()
    assert one["id"] == manifest.id


def test_activate_endpoint(test_env, tmp_path):
    app, client, runtime, manifest, env_id = test_env
    env2 = "env-activate"
    payload = {
        "strategy_id": manifest.id,
        "version": manifest.version,
        "config": manifest.default_config,
        # Runtime requires bindings; in test/dev we allow plaintext bindings.
        "bindings": {"extras": {"db": {"provider": "none"}}},
        "allow_plaintext_bindings": True,
        "domain": getattr(manifest, "domain", "openEHR"),
    }
    res = client.post(f"/environments/{env2}/activate", json=payload)
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["activation"]["strategy_id"] == manifest.id


def test_compile_query_patient_and_cross(test_env):
    app, client, runtime, manifest, env_id = test_env
    patient_payload = {
        "domain": "openEHR",
        "query": {
            "scope": "patient",
            "predicates": [{"path": "ehr_id", "op": "eq", "value": "p1"}],
            "select": [{"path": "ehr_id", "alias": "ehr_id"}],
            "projection": None,
            "limit": None,
            "sort": None,
            "offset": None,
        },
    }
    res = client.post(f"/environments/{env_id}/compile_query", json=patient_payload, params={"debug": "true"})
    assert res.status_code == 200
    plan = res.json()["result"]["plan"]
    assert plan["pipeline"]
    assert "$match" in plan["pipeline"][0]
    explain = plan["explain"]
    # for non-openEHR strategies this may differ
    assert "builder" in explain

    cross_payload = {
        "domain": "openEHR",
        "query": {
            "scope": "cross_patient",
            "predicates": [{"path": "text", "op": "eq", "value": "hello"}],
            "select": [{"path": "ehr_id", "alias": "ehr_id"}],
            "projection": None,
            "limit": None,
            "sort": None,
            "offset": None,
        },
    }
    res2 = client.post(f"/environments/{env_id}/compile_query", json={"domain": "openEHR", **cross_payload}, params={"debug": "true"})
    assert res2.status_code == 200
    plan2 = res2.json()["result"]["plan"]
    stage0 = list(plan2["pipeline"][0].keys())[0]
    assert stage0 == "$search"
    explain2 = plan2["explain"]
    assert explain2["builder"]["chosen"] == "search_pipeline_builder"
    assert explain2["stage0"] == "$search"


def test_query_and_ops(test_env):
    app, client, runtime, manifest, env_id = test_env
    # ensure ops list present
    if not manifest.ops:
        pytest.skip("strategy has no ops defined")
    q_payload = {
        "domain": "openEHR",
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
    res_q = client.post(f"/environments/{env_id}/query", json=q_payload)
    assert res_q.status_code == 200
    assert res_q.json().get("ok") is True

    # valid op
    op_name = manifest.ops[0].name
    res_op = client.post(f"/environments/{env_id}/extensions/{manifest.id}/{op_name}", json={})
    assert res_op.status_code == 200
    # invalid op
    res_bad = client.post(f"/environments/{env_id}/extensions/{manifest.id}/does_not_exist", json={})
    assert res_bad.status_code >= 400
    body_bad = res_bad.json()
    assert "error" in body_bad and "code" in body_bad["error"]
