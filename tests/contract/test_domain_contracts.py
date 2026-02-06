import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    return TestClient(app)


def test_domain_routing_requires_domain_param(client):
    client.post(
        "/v1/environments/env-domains/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    res = client.post("/v1/environments/env-domains/compile_query", json={"query": {"scope": "patient"}})
    assert res.status_code == 400
    assert res.json().get("error", {}).get("code") == "DOMAIN_REQUIRED"


def test_activate_stores_domain_keyed_activation(client):
    res = client.post(
        "/v1/environments/envA/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    assert res.status_code == 200
    acts = client.get("/v1/environments/envA/activations").json()["activations"]
    assert "openehr" in acts
    act = acts["openehr"]
    for key in ("activation_id", "strategy_id", "strategy_version", "manifest_digest", "config"):
        assert key in act


def test_compile_query_domain_dispatch_stage0_strict(client):
    client.post(
        "/v1/environments/envB/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    payload = {
        "domain": "openEHR",
        "query": {
            "scope": "patient",
            "predicates": [{"path": "ehr_id", "op": "eq", "value": "p1"}],
            "select": [{"path": "ehr_id", "alias": "ehr_id"}],
        },
    }
    res = client.post("/v1/environments/envB/compile_query", json=payload, params={"debug": "true"})
    assert res.status_code == 200
    plan = res.json()["result"]["plan"]
    stage0 = list(plan["pipeline"][0].keys())[0]
    assert stage0 == "$match"
    assert plan["explain"]["domain"] == "openehr"


def test_endpoints_introspection_contains_expected_urls(client):
    client.post(
        "/v1/environments/envC/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    res = client.get("/v1/environments/envC/endpoints")
    assert res.status_code == 200
    payload = res.json()
    endpoints = payload["endpoints"]
    for key in ("compile_query", "query", "activations", "ops"):
        assert key in endpoints
        assert "envC" in endpoints[key]["url"]
    assert payload["domains"]
    assert payload["domains"][0]["domain"] == "openehr"
    assert "domain" in endpoints["compile_query"]["payload_example"]
    assert "domain" in endpoints["query"]["payload_example"]


def test_strategy_manifest_contract_has_domain_defaults_schema_ops(client):
    res = client.get("/v1/strategies")
    assert res.status_code == 200
    strategies = res.json().get("strategies", [])
    assert strategies
    for manifest in strategies:
        assert manifest.get("id")
        assert manifest.get("domain")
        assert manifest.get("version")
        assert isinstance(manifest.get("config_schema", {}), dict)
        assert isinstance(manifest.get("default_config", {}), dict)
        for op in manifest.get("ops") or []:
            assert op.get("name")
            assert op.get("kind")
            assert isinstance(op.get("input_schema", {}), dict)
