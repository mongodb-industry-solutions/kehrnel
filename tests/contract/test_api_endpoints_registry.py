from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_endpoints_and_activation_registry(tmp_path):
    client = TestClient(create_app(str(tmp_path / "reg.json")))
    # health
    res = client.get("/health")
    assert res.status_code == 200
    # endpoints
    res = client.get("/v1/endpoints")
    assert res.status_code == 200
    # strategy endpoints
    res = client.get("/v1/strategies/openehr.rps_single/endpoints")
    assert res.status_code == 200
    # activate via new endpoint
    res = client.post(
        "/v1/environments/env-api/activations",
        json={"strategyId": "openehr.rps_single", "domain": "openEHR", "config": {}, "bindings": {}, "allowPlaintextBindings": True},
    )
    assert res.status_code == 200
    activation = res.json().get("activation") or {}
    assert activation.get("activation_id")
