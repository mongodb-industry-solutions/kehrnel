import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_invalid_op_returns_error_schema(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    client = TestClient(app)
    res = client.post("/v1/environments/dev/extensions/openehr.rps_dual/does_not_exist", json={})
    assert res.status_code == 400 or res.status_code == 404
    body = res.json()
    assert "error" in body and "code" in body["error"] and "message" in body["error"]


def test_unknown_strategy_returns_not_found(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    client = TestClient(app)
    res = client.get("/v1/strategies/unknown")
    assert res.status_code == 404
    body = res.json()
    assert body.get("error", {}).get("code") == "NOT_FOUND"


def test_internal_error_returns_500(tmp_path, monkeypatch):
    app = create_app(str(tmp_path / "reg.json"))
    client = TestClient(app)
    rt = app.state.strategy_runtime
    # force a runtime dispatch to raise an unexpected RuntimeError
    async def boom(*args, **kwargs):
        raise RuntimeError("boom")
    monkeypatch.setattr(rt, "dispatch", boom)  # type: ignore
    res = client.post("/v1/environments/dev/query", json={"domain": "openEHR", "query": {"scope": "patient", "predicates": []}})
    assert res.status_code == 500
    body = res.json()
    assert body.get("error", {}).get("code") == "INTERNAL_ERROR"
