from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_endpoints_and_activation_registry(tmp_path):
    client = TestClient(create_app(str(tmp_path / "reg.json")))
    # health
    res = client.get("/health")
    assert res.status_code == 200
    # endpoints
    res = client.get("/v1/endpoints")
    assert res.status_code == 200
    assert res.json()["endpoints"]["compile_architecture_resolution"] == (
        "/architecture-resolutions/compile"
    )
    assert res.json()["endpoints"]["mappings"] == "/mappings"
    assert res.json()["endpoints"]["outcome_catalog"] == "/outcome-catalog"
    assert "instantiate_reference_architecture" not in res.json()["endpoints"]
    assert res.json()["endpoints"]["canonicalize_solution_blueprint"] == (
        "/solution-blueprints/canonicalize"
    )
    assert res.json()["endpoints"]["simulate_mapping"] == (
        "/mappings/{mapping_id}/simulate"
    )
    # strategy endpoints
    res = client.get("/v1/strategies/openehr.rps_dual_ibm/endpoints")
    assert res.status_code == 200
    # activate via new endpoint
    res = client.post(
        "/v1/environments/env-api/activations",
        json={"strategyId": "openehr.rps_dual_ibm", "domain": "openEHR", "config": {"database": "kehrnel_contract_test"}, "bindings": {}, "allowPlaintextBindings": True},
    )
    assert res.status_code == 200
    activation = res.json().get("activation") or {}
    assert activation.get("activation_id")
