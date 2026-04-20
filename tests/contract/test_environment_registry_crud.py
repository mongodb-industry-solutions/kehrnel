from fastapi.testclient import TestClient

from kehrnel.api.app import create_app


def test_environment_registry_crud_and_activation_fallback(tmp_path):
    client = TestClient(create_app(str(tmp_path / "reg.json")))

    create_res = client.post(
        "/v1/environments",
        json={
            "env_id": "env-meta",
            "name": "Demo Environment",
            "description": "Portable runtime target",
            "metadata": {"owner": "cli"},
            "bindings_ref": "env:env-meta",
        },
    )
    assert create_res.status_code == 200
    assert create_res.json()["environment"]["name"] == "Demo Environment"

    list_res = client.get("/v1/environments")
    assert list_res.status_code == 200
    env_ids = {row["env_id"] for row in list_res.json()["environments"]}
    assert "env-meta" in env_ids

    show_res = client.get("/v1/environments/env-meta")
    assert show_res.status_code == 200
    assert show_res.json()["environment"]["bindings_ref"] == "env:env-meta"
    assert show_res.json()["activations"] == {}

    activate_res = client.post(
        "/v1/environments/env-meta/activate",
        json={
            "strategy_id": "fhir.resource_first",
            "version": "0.1.0",
            "config": {},
            "domain": "fhir",
            "bindings": {},
        },
    )
    assert activate_res.status_code == 200
    assert activate_res.json()["activation"]["bindings_ref"] == "env:env-meta"

    conflict_res = client.delete("/v1/environments/env-meta")
    assert conflict_res.status_code == 409

    delete_res = client.delete("/v1/environments/env-meta?force=true")
    assert delete_res.status_code == 200

    missing_res = client.get("/v1/environments/env-meta")
    assert missing_res.status_code == 404
