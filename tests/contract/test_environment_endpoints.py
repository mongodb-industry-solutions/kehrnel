import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    return TestClient(app)


def _activate(client, env_id: str, strategy_id: str = "openehr.rps_dual", domain: str = "openEHR"):
    res = client.post(
        f"/environments/{env_id}/activate",
        json={"strategy_id": strategy_id, "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": domain},
    )
    assert res.status_code == 200


def test_endpoints_introspection_is_canonical(client):
    _activate(client, "envEP")
    res = client.get("/environments/envEP/endpoints")
    assert res.status_code == 200
    body = res.json()
    assert body["domains"]
    domain_entry = body["domains"][0]
    assert domain_entry["domain"] == "openehr"
    endpoints = body["endpoints"]
    for key in ("compile_query", "query", "ops", "activations"):
        assert key in endpoints
        assert "url" in endpoints[key]
    assert "domain" in endpoints["compile_query"]["payload_example"]
    assert "domain" in endpoints["query"]["payload_example"]
    assert "domain" in endpoints["compile_query"]["required_params"]
    assert "domain" in endpoints["query"]["required_params"]
    assert "strategy_id" in endpoints["ops"]["required_params"]
    assert "op" in endpoints["ops"]["required_params"]
    assert endpoints["ops"]["url"].endswith("/ops")
    assert "/activations/" in endpoints["ops"]["url"]
    assert "ops_legacy_extension" in endpoints


def test_activations_endpoint_includes_history_summary(client):
    _activate(client, "envHist", domain="fhir", strategy_id="fhir.resource_first")
    # create a history entry via upgrade
    res_up = client.post("/environments/envHist/activations/fhir/upgrade")
    assert res_up.status_code == 200

    res = client.get("/environments/envHist/activations")
    assert res.status_code == 200
    body = res.json()
    history = body["history"]["fhir"]
    assert history["count"] >= 1
    assert history["recent"]
    last = history["recent"][-1]
    for key in ("activation_id", "strategy_id", "reason", "timestamp"):
        assert key in last
