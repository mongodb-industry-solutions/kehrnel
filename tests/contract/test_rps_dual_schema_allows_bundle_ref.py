from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_rps_dual_schema_includes_bundle_ref():
    client = TestClient(create_app())
    res = client.get("/v1/strategies")
    assert res.status_code == 200
    strat = next((s for s in res.json().get("strategies") or [] if s.get("id") == "openehr.rps_dual"), None)
    assert strat, "openehr.rps_dual not found"
    schema = strat.get("config_schema") or {}
    props = schema.get("properties") or {}
    slim = props.get("slim_search") or {}
    slim_props = slim.get("properties") or {}
    assert "bundle_id" in slim_props, "slim_search.bundle_id missing from schema"
    defaults = strat.get("default_config") or {}
    assert defaults.get("slim_search", {}).get("bundle_id") == "openehr.analytics.example.v1"
