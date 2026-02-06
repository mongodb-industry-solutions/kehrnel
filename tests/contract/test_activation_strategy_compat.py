from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_activate_strategy_compat(tmp_path):
    client = TestClient(create_app(str(tmp_path / "reg.json")))
    res = client.put(
        "/v1/environments/strategy",
        json={"envId": "env-compat", "strategyId": "fhir.resource_first", "domain": "FHIR", "configOverrides": {}},
    )
    assert res.status_code == 200
    activation = res.json().get("activation") or {}
    assert activation.get("activation_id")
