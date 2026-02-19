import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.engine.core.runtime import StrategyRuntime


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    return app, TestClient(app)


def test_activation_records_digest_and_version(client):
    app, cl = client
    res = cl.post("/environments/envI/activate", json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"})
    assert res.status_code == 200
    payload = res.json()
    # Keep top-level compatibility fields for HDL proxy/service parsing.
    assert payload.get("activation_id")
    assert payload.get("strategy_id") == "fhir.resource_first"
    assert payload.get("domain") == "fhir"
    assert payload.get("status") == "ok"
    act = cl.get("/environments/envI/activations").json()["activations"]
    any_act = list(act.values())[0]
    assert any_act["manifest_digest"]
    assert any_act["version"]


def test_manifest_mismatch_triggers_conflict(client, monkeypatch):
    app, cl = client
    cl.post("/environments/envM/activate", json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"})
    rt: StrategyRuntime = app.state.strategy_runtime
    manifest = rt.registry.get_manifest("fhir.resource_first")
    # simulate manifest version change
    manifest.version = "9.9.9"
    res = cl.post(
        "/environments/envM/compile_query",
        json={"domain": "fhir", "query": {"scope": "patient", "predicates": [], "select": [{"path": "id", "alias": "id"}]}},
        params={"debug": "true"},
    )
    assert res.status_code == 409
    body = res.json()
    assert body.get("error", {}).get("code") == "ACTIVATION_STRATEGY_MISMATCH"


def test_upgrade_endpoint_refreshes_digest(client):
    app, cl = client
    cl.post("/environments/envU/activate", json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"})
    res_before = cl.get("/environments/envU/activations").json()
    before_digest = list(res_before["activations"].values())[0]["manifest_digest"]
    # no manifest change but upgrade should produce new activation_id
    res_up = cl.post("/environments/envU/activations/fhir/upgrade")
    assert res_up.status_code == 200
    up = res_up.json()["activation"]
    assert up["activation_id"] != list(res_before["activations"].values())[0]["activation_id"]
    assert up["manifest_digest"] == before_digest


def test_upgrade_changes_digest_when_manifest_changes(client):
    app, cl = client
    cl.post("/environments/envUpgrade/activate", json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"})
    before = cl.get("/environments/envUpgrade/activations").json()
    before_act = list(before["activations"].values())[0]
    # mutate manifest so digest should change
    rt: StrategyRuntime = app.state.strategy_runtime
    manifest = rt.registry.get_manifest("fhir.resource_first")
    manifest.version = "9.9.9"

    res = cl.post("/environments/envUpgrade/activations/fhir/upgrade")
    assert res.status_code == 200
    new_act = res.json()["activation"]
    assert new_act["manifest_digest"] != before_act["manifest_digest"]


def test_rollback_restores_previous_digest_and_hash(client):
    app, cl = client
    cl.post("/environments/envRollback/activate", json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"})
    before = cl.get("/environments/envRollback/activations").json()
    before_act = list(before["activations"].values())[0]
    rt: StrategyRuntime = app.state.strategy_runtime
    manifest = rt.registry.get_manifest("fhir.resource_first")
    manifest.version = "2.0.0"
    res_up = cl.post("/environments/envRollback/activations/fhir/upgrade")
    assert res_up.status_code == 200
    after_upgrade = res_up.json()["activation"]
    assert after_upgrade["manifest_digest"] != before_act["manifest_digest"]

    res_rb = cl.post("/environments/envRollback/activations/fhir/rollback")
    assert res_rb.status_code == 200
    rolled = res_rb.json()["activation"]
    assert rolled["manifest_digest"] == before_act["manifest_digest"]
    assert rolled["config_hash"] == before_act["config_hash"]


def test_delete_activation_blocks_future_queries(client):
    app, cl = client
    cl.post("/environments/envDelete/activate", json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"})
    res_del = cl.delete("/environments/envDelete/activations/fhir")
    assert res_del.status_code == 200

    res_query = cl.post("/environments/envDelete/compile_query", json={"domain": "fhir", "query": {"scope": "patient", "predicates": [], "select": [{"path": "id", "alias": "id"}]}}, params={"debug": "true"})
    assert res_query.status_code == 404
    assert res_query.json().get("error", {}).get("code") == "ACTIVATION_NOT_FOUND"
