from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_activation_idempotent_and_switchable(tmp_path):
    client = TestClient(create_app(str(tmp_path / "reg.json")))
    # first activation
    res1 = client.put(
        "/environments/strategy",
        json={"envId": "env-switch", "strategyId": "fhir.resource_first", "domain": "FHIR", "configOverrides": {}},
    )
    assert res1.status_code == 200
    act1 = res1.json().get("activation") or {}
    assert act1.get("already_active") is False
    # second activation same strategy/domain -> alreadyActive
    res2 = client.put(
        "/environments/strategy",
        json={"envId": "env-switch", "strategyId": "fhir.resource_first", "domain": "fhir", "configOverrides": {}},
    )
    assert res2.status_code == 200
    act2 = res2.json().get("activation") or {}
    assert act2.get("already_active") is True
    # switch to different strategy (openEHR) should replace
    res3 = client.put(
        "/environments/strategy",
        json={"envId": "env-switch", "strategyId": "openehr.rps_dual", "domain": "openEHR", "configOverrides": {}},
    )
    assert res3.status_code == 200
    act3 = res3.json().get("activation") or {}
    assert act3.get("replaced") is False  # different domain, separate activation
