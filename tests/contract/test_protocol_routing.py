import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(str(tmp_path / "reg.json"))
    return TestClient(app)


def test_missing_domain_returns_400(client):
    # activate openEHR so env exists
    client.post(
        "/v1/environments/envX/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {"database": "env_x_openehr"}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    res = client.post("/v1/environments/envX/compile_query", json={"query": {"scope": "patient"}})
    assert res.status_code == 400
    assert res.json().get("error", {}).get("code") == "DOMAIN_REQUIRED"


def test_missing_activation_for_domain(client):
    # no activation for fhir in envX
    res = client.post(
        "/v1/environments/envX/compile_query",
        json={"domain": "fhir", "query": {"scope": "patient", "predicates": [], "select": []}},
    )
    assert res.status_code == 404
    assert res.json().get("error", {}).get("code") == "ACTIVATION_NOT_FOUND"


def test_multi_domain_routing(client):
    client.post(
        "/v1/environments/envY/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {"database": "env_y_openehr"}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    client.post(
        "/v1/environments/envY/activate",
        json={"strategy_id": "fhir.clinical_cdr", "version": "0.1.0", "config": {"database": "env_y_fhir"}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"},
    )
    res_act = client.get("/v1/environments/envY/activations")
    assert res_act.status_code == 200
    acts = res_act.json().get("activations", {})
    assert "openehr" in acts or "openehr" in "".join(acts.keys()).lower()
    res_o = client.post(
        "/v1/environments/envY/compile_query",
        json={"domain": "openEHR", "query": {"scope": "patient", "predicates": [], "select": [{"path": "ehr_id", "alias": "ehr_id"}]}},
        params={"debug": "true"},
    )
    assert res_o.status_code == 200
    explain_o = res_o.json()["result"]["plan"]["explain"]
    assert explain_o["strategy_id"] == "openehr.rps_dual"
    assert explain_o["domain"] == "openehr"
    res_f = client.post(
        "/v1/environments/envY/compile_query",
        json={
            "domain": "fhir",
            "query": {"resource_type": "Patient", "criteria": {"gender": "female"}, "explain_only": True},
        },
        params={"debug": "true"},
    )
    assert res_f.status_code == 200
    explain_f = res_f.json()["result"]["plan"]["explain"]
    assert explain_f["strategy_id"] == "fhir.clinical_cdr"
    assert explain_f["domain"] == "fhir"
