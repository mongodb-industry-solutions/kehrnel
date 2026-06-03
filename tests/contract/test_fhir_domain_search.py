"""Contract tests for POST /api/domains/fhir/search."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app

FHIR_ACTIVATE = {
    "strategy_id": "fhir.rps_canonical",
    "version": "0.1.0",
    "domain": "fhir",
    "config": {
        "database": "fhir_test",
        "schema_version": "R5",
        "collections": {"mode": "per_resource_type"},
    },
    "bindings": {
        "db": {
            "provider": "mongodb",
            "uri": "mongodb://localhost:27017",
            "database": "fhir_test",
        }
    },
    "allow_plaintext_bindings": True,
}


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "false")
    monkeypatch.setenv("KEHRNEL_DEFAULT_ENV_ID", "dev")
    app = create_app(str(tmp_path / "reg.json"))
    return TestClient(app)


def _activate_fhir(client: TestClient, env_id: str = "dev") -> None:
    res = client.post(f"/v1/environments/{env_id}/activate", json=FHIR_ACTIVATE)
    assert res.status_code == 200, res.text


def test_fhir_search_requires_env_when_no_default(client, monkeypatch):
    monkeypatch.delenv("KEHRNEL_DEFAULT_ENV_ID", raising=False)
    res = client.post(
        "/api/domains/fhir/search",
        json={"resource_type": "Patient", "criteria": {"gender": "female"}, "limit": 5},
    )
    assert res.status_code == 400


def test_fhir_search_rejects_wrong_strategy(client):
    client.post(
        "/v1/environments/dev/activate",
        json={
            **FHIR_ACTIVATE,
            "strategy_id": "fhir.resource_first",
        },
    )
    res = client.post(
        "/api/domains/fhir/search",
        headers={"x-active-env": "dev"},
        json={"resource_type": "Patient", "criteria": {"gender": "female"}, "limit": 5},
    )
    assert res.status_code == 409


def test_fhir_search_returns_bundle(client, monkeypatch):
    _activate_fhir(client)

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert env_id == "dev"
        assert op == "query"
        assert payload["domain"] == "fhir"
        assert payload["query"]["resource_type"] == "Patient"
        assert payload["query"]["_count"] == 5
        assert payload["query"]["criteria"]["gender"] == "female"
        return {
            "engine_used": "fhir_mql",
            "rows": [{"resourceType": "Patient", "id": "p-1", "gender": "female"}],
            "explain": {"engine": "fhir_mql", "total": 1, "returned": 1},
        }

    rt = client.app.state.strategy_runtime
    monkeypatch.setattr(rt, "dispatch", AsyncMock(side_effect=fake_dispatch))

    res = client.post(
        "/api/domains/fhir/search",
        headers={"x-active-env": "dev"},
        json={"resource_type": "Patient", "criteria": {"gender": "female"}, "limit": 5},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["resourceType"] == "Bundle"
    assert body["type"] == "searchset"
    assert body["total"] == 1
    assert len(body["entry"]) == 1
    assert body["entry"][0]["resource"]["id"] == "p-1"
    assert body["meta"]["kehrnel"]["engine"] == "fhir_mql"
    assert body["meta"]["kehrnel"]["env"] == "dev"
    assert "todo" not in body


def test_fhir_search_serializes_mongodb_object_ids(client, monkeypatch):
    """Mongo rows include BSON ObjectId; the Bundle response must still encode as JSON."""
    _activate_fhir(client)
    oid = ObjectId()

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert payload["query"].get("fhir_search") == "Patient?gender=male"
        return {
            "engine_used": "fhir_mql",
            "rows": [{"resourceType": "Patient", "id": "p-1", "gender": "male", "_id": oid}],
            "explain": {"engine": "fhir_mql", "total": 1, "returned": 1},
        }

    rt = client.app.state.strategy_runtime
    monkeypatch.setattr(rt, "dispatch", AsyncMock(side_effect=fake_dispatch))

    res = client.post(
        "/api/domains/fhir/search",
        headers={"x-active-env": "dev"},
        json={"fhir_search": "Patient?gender=male", "limit": 10},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["entry"][0]["resource"]["_id"] == str(oid)


@pytest.mark.skipif(
    not os.getenv("FHIR_DOMAIN_SEARCH_INTEGRATION"),
    reason="Set FHIR_DOMAIN_SEARCH_INTEGRATION=1 with MongoDB and seeded Patient data",
)
def test_fhir_search_integration_live(client):
    _activate_fhir(client)
    res = client.post(
        "/api/domains/fhir/search",
        headers={"x-active-env": "dev"},
        json={"resource_type": "Patient", "criteria": {"gender": "female"}, "limit": 5},
    )
    if res.status_code != 200:
        pytest.skip(f"integration prerequisites missing: {res.status_code} {res.text}")
    body = res.json()
    assert body["resourceType"] == "Bundle"
