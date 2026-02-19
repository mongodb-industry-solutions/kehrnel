from fastapi.testclient import TestClient
import pytest

from kehrnel.api.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    return TestClient(app)


def test_invalid_config_missing_required(client):
    res = client.post(
        "/environments/env-config/activate",
        json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {"database": 123}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"},
    )
    assert res.status_code == 400
    body = res.json()
    assert body.get("error", {}).get("code") == "CONFIG_INVALID"
    assert "path" in body.get("error", {}).get("details", {})


def test_activation_returns_effective_config(client):
    res = client.post(
        "/environments/env-config/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    assert res.status_code == 200
    res_get = client.get("/environments/env-config")
    assert res_get.status_code == 200
    data = res_get.json()
    activations = data.get("activations", {})
    any_act = list(activations.values())[0]
    assert any_act["strategy_id"] == "openehr.rps_dual"
    assert "activation_id" in any_act
    assert "config" in any_act
