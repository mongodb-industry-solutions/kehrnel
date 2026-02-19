import json
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def _sample_bundle():
    return {
        "bundle_id": "openehr.analytics.sample.v1",
        "domain": "openEHR",
        "kind": "slim_search_definition",
        "version": "1.0.0",
        "created_at": "2025-01-01T00:00:00Z",
        "created_by": "cli",
        "payload": {
            "templates": [
                {
                    "templateId": "Sample",
                    "analytics_fields": [{"name": "Standard Drinks", "path": "/content[openEHR-EHR-CLUSTER.admin_salut.v0]/value", "rmType": "DV_QUANTITY"}],
                    "rules": [
                        {"when": {"pathChain": ["openEHR-EHR-CLUSTER.admin_salut.v0"]}, "copy": ["data.value"]}
                    ]
                }
            ]
        }
    }


def test_bundles_crud(tmp_path):
    app = create_app(bundle_path=str(tmp_path / "bundles"))
    client = TestClient(app)
    bundle = _sample_bundle()
    # create
    res = client.post("/bundles", json=bundle)
    assert res.status_code == 200
    # list
    res = client.get("/bundles")
    assert res.status_code == 200
    bundles = res.json().get("bundles") or []
    assert any(b["bundle_id"] == bundle["bundle_id"] for b in bundles)
    # get
    res = client.get(f"/bundles/{bundle['bundle_id']}")
    assert res.status_code == 200
    got = res.json()["bundle"]
    assert got["bundle_id"] == bundle["bundle_id"]
    # delete
    res = client.delete(f"/bundles/{bundle['bundle_id']}")
    assert res.status_code == 200
    res = client.get(f"/bundles/{bundle['bundle_id']}")
    assert res.status_code == 404
