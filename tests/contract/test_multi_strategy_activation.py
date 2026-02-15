import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


@pytest.fixture
def app_client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    client = TestClient(app)
    return app, client


def test_strategies_list_domains(app_client):
    app, client = app_client
    res = client.get("/v1/strategies")
    assert res.status_code == 200
    ids = {s["id"] for s in res.json().get("strategies", [])}
    assert "openehr.rps_dual" in ids
    assert "fhir.resource_first" in ids


def test_activation_is_env_scoped(app_client):
    app, client = app_client
    # activate openehr in envA
    res_a = client.post(
        "/v1/environments/envA/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {"extras": {"db": {"provider": "none"}}}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    assert res_a.status_code == 200
    # activate fhir in envB
    res_b = client.post(
        "/v1/environments/envB/activate",
        json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {}, "bindings": {"extras": {"db": {"provider": "none"}}}, "allow_plaintext_bindings": True, "domain": "fhir"},
    )
    assert res_b.status_code == 200

    # compile in envA uses openehr -> stage0 $match
    res_compile_a = client.post(
        "/v1/environments/envA/compile_query",
        json={"domain": "openEHR", "query": {"scope": "patient", "predicates": [], "select": [{"path": "ehr_id", "alias": "ehr_id"}]}},
        params={"debug": "true"},
    )
    assert res_compile_a.status_code == 200
    pipeline_a = res_compile_a.json()["result"]["plan"]["pipeline"]
    assert "$match" in pipeline_a[0]
    explain_a = res_compile_a.json()["result"]["plan"]["explain"]
    assert explain_a["strategy_id"] == "openehr.rps_dual"

    # compile in envB uses fhir -> dummy pipeline $match but explain shows fhir strategy
    res_compile_b = client.post(
        "/v1/environments/envB/compile_query",
        json={"domain": "fhir", "query": {"scope": "patient", "predicates": [], "select": [{"path": "id", "alias": "id"}]}},
        params={"debug": "true"},
    )
    assert res_compile_b.status_code == 200
    explain_b = res_compile_b.json()["result"]["plan"]["explain"]
    assert explain_b["strategy_id"] == "fhir.resource_first"


def test_activation_endpoint_returns_state(app_client):
    app, client = app_client
    client.post(
        "/v1/environments/envState/activate",
        json={
            "strategy_id": "openehr.rps_dual",
            "version": "0.1.0",
            "config": {},
            "bindings": {"extras": {"db": {"provider": "none"}}},
            "allow_plaintext_bindings": True,
            "domain": "openEHR",
        },
    )
    res = client.get("/v1/environments/envState")
    assert res.status_code == 200
    data = res.json()
    activations = data.get("activations", {})
    assert activations
    any_act = list(activations.values())[0]
    assert any_act["strategy_id"] == "openehr.rps_dual"
    assert "config" in any_act
