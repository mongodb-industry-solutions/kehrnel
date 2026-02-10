from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_ops_endpoints(tmp_path):
    client = TestClient(create_app(str(tmp_path / "reg.json")))
    res = client.get("/v1/ops")
    assert res.status_code == 200
    ops = res.json().get("ops") or []
    assert any(op.get("name") for op in ops)
    # activate and run an op
    client.post(
        "/v1/environments/env-ops/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    res = client.post("/v1/ops", json={"environment": "env-ops", "domain": "openEHR", "op": "ensure_dictionaries", "payload": {}})
    assert res.status_code == 200
    assert res.json().get("ok") is True
