import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    return app, TestClient(app)


def _activate(client: TestClient, env: str, domain: str = "openEHR", force: bool = False, reason: str | None = None):
    body = {
        "strategy_id": "openehr.rps_dual",
        "version": "0.1.0",
        "config": {},
        "bindings": {},
        "allow_plaintext_bindings": True,
        "domain": domain,
        "force": force,
    }
    if reason:
        body["reason"] = reason
    return client.post(f"/v1/environments/{env}/activate", json=body)


def test_activate_domain_twice_without_force_returns_409(client):
    app, cl = client
    res1 = _activate(cl, "env-force")
    assert res1.status_code == 200
    res2 = _activate(cl, "env-force")
    assert res2.status_code == 200
    activation = res2.json().get("activation") or {}
    assert activation.get("already_active") is True


def test_activate_domain_twice_with_force_replaces_and_records_history(client):
    app, cl = client
    res1 = _activate(cl, "env-force-ok")
    assert res1.status_code == 200
    first_act = res1.json()["activation"]

    res2 = _activate(cl, "env-force-ok", force=True, reason="replace-test")
    assert res2.status_code == 200
    second_act = res2.json()["activation"]
    assert second_act["activation_id"] != first_act["activation_id"]
    assert second_act["replaced"] is True
    assert second_act["previous_activation_id"] == first_act["activation_id"]

    # history should have grown
    res_hist = cl.get("/v1/environments/env-force-ok/activations")
    assert res_hist.status_code == 200
    hist = res_hist.json()["history"]["openehr"]
    assert hist["count"] >= 1
    assert hist["recent"][-1]["reason"] == "replace-test"
