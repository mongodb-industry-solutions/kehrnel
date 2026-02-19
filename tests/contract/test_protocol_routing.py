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
        "/environments/envX/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    res = client.post("/environments/envX/compile_query", json={"query": {"scope": "patient"}})
    assert res.status_code == 400
    assert res.json().get("error", {}).get("code") == "DOMAIN_REQUIRED"


def test_missing_activation_for_domain(client):
    # no activation for fhir in envX
    res = client.post(
        "/environments/envX/compile_query",
        json={"domain": "fhir", "query": {"scope": "patient", "predicates": [], "select": []}},
    )
    assert res.status_code == 404
    assert res.json().get("error", {}).get("code") == "ACTIVATION_NOT_FOUND"


def test_multi_domain_routing(client):
    client.post(
        "/environments/envY/activate",
        json={"strategy_id": "openehr.rps_dual", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "openEHR"},
    )
    client.post(
        "/environments/envY/activate",
        json={"strategy_id": "fhir.resource_first", "version": "0.1.0", "config": {}, "bindings": {}, "allow_plaintext_bindings": True, "domain": "fhir"},
    )
    res_act = client.get("/environments/envY/activations")
    assert res_act.status_code == 200
    acts = res_act.json().get("activations", {})
    assert "openehr" in acts or "openehr" in "".join(acts.keys()).lower()
    res_o = client.post(
        "/environments/envY/compile_query",
        json={"domain": "openEHR", "query": {"scope": "patient", "predicates": [], "select": [{"path": "ehr_id", "alias": "ehr_id"}]}},
        params={"debug": "true"},
    )
    assert res_o.status_code == 200
    stage0 = list(res_o.json()["result"]["plan"]["pipeline"][0].keys())[0]
    assert stage0 == "$match"
    res_f = client.post(
        "/environments/envY/compile_query",
        json={"domain": "fhir", "query": {"scope": "patient", "predicates": [], "select": [{"path": "id", "alias": "id"}]}},
        params={"debug": "true"},
    )
    assert res_f.status_code == 200
    explain_f = res_f.json()["result"]["plan"]["explain"]
    assert explain_f["strategy_id"] == "fhir.resource_first"
    assert explain_f["domain"] == "fhir"
