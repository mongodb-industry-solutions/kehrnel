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
    collections = props.get("collections") or {}
    collections_props = collections.get("properties") or {}
    search = collections_props.get("search") or {}
    search_props = search.get("properties") or {}
    atlas_index = search_props.get("atlasIndex") or {}
    atlas_index_props = atlas_index.get("properties") or {}
    assert "definition" in atlas_index_props, "collections.search.atlasIndex.definition missing from schema"
    defaults = strat.get("default_config") or {}
    assert isinstance(defaults.get("collections", {}).get("search", {}).get("atlasIndex", {}).get("definition"), str)
