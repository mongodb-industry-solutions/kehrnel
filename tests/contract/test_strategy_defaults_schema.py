from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_strategies_include_defaults_and_schema():
    client = TestClient(create_app())
    res = client.get("/v1/strategies")
    assert res.status_code == 200
    strategies = res.json().get("strategies") or []
    rps = next((s for s in strategies if s.get("id") == "openehr.rps_dual"), None)
    assert rps, "openehr.rps_dual not found"
    assert rps.get("default_config"), "default_config should be present"
    schema = rps.get("config_schema") or {}
    assert schema.get("properties"), "config_schema.properties should not be empty"
    rps_ibm = next((s for s in strategies if s.get("id") == "openehr.rps_dual_ibm"), None)
    assert rps_ibm, "openehr.rps_dual_ibm not found"
    assert rps_ibm.get("default_config"), "default_config should be present for rps_dual_ibm"
    schema_ibm = rps_ibm.get("config_schema") or {}
    assert schema_ibm.get("properties"), "config_schema.properties should not be empty for rps_dual_ibm"
    rps_single = next((s for s in strategies if s.get("id") == "openehr.rps_single"), None)
    assert rps_single, "openehr.rps_single not found"
    assert rps_single.get("default_config"), "default_config should be present for rps_single"
    schema_single = rps_single.get("config_schema") or {}
    assert schema_single.get("properties"), "config_schema.properties should not be empty for rps_single"
