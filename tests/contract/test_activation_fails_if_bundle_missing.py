from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_activation_fails_when_bundle_missing(tmp_path):
    client = TestClient(create_app(str(tmp_path / "reg.json"), bundle_path=str(tmp_path / "bundles")))
    res = client.post(
        "/v1/environments/env-bundle/activate",
        json={
            "strategy_id": "openehr.rps_dual",
            "version": "0.1.0",
            "domain": "openEHR",
            "config": {"slim_search": {"enabled": True, "bundle_id": "missing.bundle.v1"}},
            "bindings": {},
            "allow_plaintext_bindings": True,
        },
    )
    assert res.status_code == 400 or res.status_code == 404
    err = res.json().get("error") or {}
    assert err.get("code") == "BUNDLE_NOT_FOUND"
