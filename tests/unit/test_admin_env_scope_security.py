"""Regression tests for tenant isolation on admin/runtime-config routes and rate-limit identity."""
import json

from fastapi.testclient import TestClient

from kehrnel.api.app import create_app

TENANT_KEY = "tenant-a-key"
GLOBAL_KEY = "platform-key"


def _scoped_auth(monkeypatch, rate_limit="1000"):
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "true")
    monkeypatch.setenv("KEHRNEL_API_KEYS", f"{TENANT_KEY},{GLOBAL_KEY}")
    monkeypatch.delenv("KEHRNEL_ADMIN_API_KEYS", raising=False)
    monkeypatch.setenv(
        "KEHRNEL_API_KEY_ENV_SCOPES",
        json.dumps({TENANT_KEY: ["tenant-a"], GLOBAL_KEY: "*"}),
    )
    monkeypatch.setenv("KEHRNEL_RATE_LIMIT", rate_limit)


class _RecordingRuntime:
    def __init__(self):
        self.calls = []

    async def dispatch(self, env_id, kind, payload):
        self.calls.append((env_id, kind, payload))
        return {"dispatched": env_id}


def _client_with_runtime(tmp_path):
    app = create_app(str(tmp_path / "registry.json"))
    client = TestClient(app)
    runtime = _RecordingRuntime()
    app.state.strategy_runtime = runtime
    return client, runtime


def test_post_ops_rejects_environment_outside_key_scope(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch)
    client, runtime = _client_with_runtime(tmp_path)

    res = client.post(
        "/v1/ops",
        headers={"X-API-Key": TENANT_KEY},
        json={"environment": "tenant-b", "domain": "openehr", "op": "ensure_dictionaries", "payload": {}},
    )

    assert res.status_code == 403
    assert res.json()["error"]["code"] == "FORBIDDEN"
    assert runtime.calls == []


def test_post_ops_allows_environment_inside_key_scope(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch)
    client, runtime = _client_with_runtime(tmp_path)

    res = client.post(
        "/v1/ops",
        headers={"X-API-Key": TENANT_KEY},
        json={"environment": "tenant-a", "domain": "openehr", "op": "ensure_dictionaries", "payload": {}},
    )

    assert res.status_code == 200
    assert [call[0] for call in runtime.calls] == ["tenant-a"]


def test_post_ops_env_id_is_normalized_before_scope_check(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch)
    client, runtime = _client_with_runtime(tmp_path)

    res = client.post(
        "/v1/ops",
        headers={"X-API-Key": TENANT_KEY},
        json={"env_id": " tenant-a ", "domain": "openehr", "op": "ensure_dictionaries"},
    )

    assert res.status_code == 200
    assert runtime.calls[0][0] == "tenant-a"


def test_rps_dual_config_requires_api_key(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch)
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    res = client.post("/api/strategies/openehr/rps_dual/config", json={})

    assert res.status_code == 401


def test_rps_dual_config_rejects_env_scoped_key(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch)
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    res = client.post(
        "/api/strategies/openehr/rps_dual/config",
        headers={"X-API-Key": TENANT_KEY},
        json={
            "collections": {
                "compositions": {"store_canonical": False, "name": "compositions"},
                "semiflattened_compositions": {"name": "hijacked"},
                "search": {"name": "hijacked_search", "enabled": True},
            }
        },
    )

    assert res.status_code == 403
    assert res.json()["detail"]["code"] == "FORBIDDEN"


def test_rps_dual_config_rejects_non_admin_key(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch)
    monkeypatch.setenv("KEHRNEL_ADMIN_API_KEYS", "some-other-admin-key")
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    res = client.post(
        "/api/strategies/openehr/rps_dual/config",
        headers={"X-API-Key": GLOBAL_KEY},
        json={},
    )

    assert res.status_code == 403


def test_rps_dual_config_admits_unscoped_admin_key_to_validation(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch)
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    # Authorized callers reach payload validation (422 for an empty body), not the auth guard.
    res = client.post(
        "/api/strategies/openehr/rps_dual/config",
        headers={"X-API-Key": GLOBAL_KEY},
        json={},
    )

    assert res.status_code == 422


def test_rate_limit_buckets_valid_keys_separately(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch, rate_limit="2")
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    for _ in range(2):
        assert client.get("/v1/ops", headers={"X-API-Key": TENANT_KEY}).status_code != 429
    assert client.get("/v1/ops", headers={"X-API-Key": TENANT_KEY}).status_code == 429
    # Same client IP, different valid key: its own bucket.
    assert client.get("/v1/ops", headers={"X-API-Key": GLOBAL_KEY}).status_code != 429


def test_rate_limit_throttles_invalid_keys_by_ip(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch, rate_limit="2")
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    statuses = [
        client.get("/v1/ops", headers={"X-API-Key": f"guess-{i}"}).status_code
        for i in range(3)
    ]

    assert statuses[:2] == [401, 401]
    assert statuses[2] == 429


def test_rps_dual_config_fails_closed_on_empty_scope_policy(tmp_path, monkeypatch):
    _scoped_auth(monkeypatch)
    monkeypatch.setenv("KEHRNEL_API_KEY_ENV_SCOPES", "{}")
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    res = client.post(
        "/api/strategies/openehr/rps_dual/config",
        headers={"X-API-Key": GLOBAL_KEY},
        json={},
    )

    assert res.status_code == 403


def test_rate_limit_eviction_never_drops_current_bucket():
    import asyncio
    from types import SimpleNamespace

    from kehrnel.api.app import RateLimitMiddleware

    async def call_next(request):
        return "ok"

    mw = RateLimitMiddleware(app=None, requests_per_minute=10, max_clients=2)
    mw._request_counts = {"key:a": [], "key:b": []}
    request = SimpleNamespace(
        url=SimpleNamespace(path="/v1/ops"),
        state=SimpleNamespace(rate_limit_key="key:c"),
        headers={},
        client=None,
    )

    assert asyncio.run(mw.dispatch(request, call_next)) == "ok"
    assert "key:c" in mw._request_counts
    assert len(mw._request_counts) == 2
