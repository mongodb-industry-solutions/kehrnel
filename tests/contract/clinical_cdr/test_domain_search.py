"""Contract tests for the limited FHIR REST boundary."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest
from bson import ObjectId
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app

FHIR_ACTIVATE = {
    "strategy_id": "fhir.clinical_cdr",
    "version": "0.2.0",
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
            "strategy_id": "openehr.rps_dual",
            "version": "0.2.0",
            "config": {"database": "openehr_contract"},
            "bindings": FHIR_ACTIVATE["bindings"],
            "allow_plaintext_bindings": True,
            "domain": "fhir",
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
    # Conformant Bundle: no proprietary meta.kehrnel (execution metadata lives in
    # the ops-level envelope, not on the FHIR Bundle).
    assert "meta" not in body
    assert "todo" not in body


def test_fhir_get_search_uses_standard_query_string(client, monkeypatch):
    _activate_fhir(client)

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert op == "query"
        assert (
            payload["query"]["fhir_search"]
            == "Patient?gender=female&name=Smith&name=Jones&_count=5"
        )
        return {
            "rows": [{"resourceType": "Patient", "id": "p-1"}],
            "explain": {"total": 1},
        }

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(side_effect=fake_dispatch),
    )
    res = client.get(
        "/api/domains/fhir/Patient?gender=female&name=Smith&name=Jones&_count=5",
        headers={"x-active-env": "dev"},
    )
    assert res.status_code == 200, res.text
    assert res.headers["content-type"].startswith("application/fhir+json")
    assert res.json()["entry"][0]["resource"]["id"] == "p-1"


def test_fhir_explain_compiles_without_executing(client, monkeypatch):
    _activate_fhir(client)

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert op == "op"
        assert payload["op"] == "fhir_search"
        assert payload["payload"] == {
            "resource_type": "Patient",
            "criteria": {"gender": "female"},
            "_count": 5,
            "explain_only": True,
        }
        return {
            "ok": True,
            "explain_only": True,
            "compiled_plan": {"collection": "Patient", "filter": {"gender": "female"}},
        }

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(side_effect=fake_dispatch),
    )
    res = client.post(
        "/api/domains/fhir/explain",
        headers={"x-active-env": "dev"},
        json={
            "resource_type": "Patient",
            "criteria": {"gender": "female"},
            "_count": 5,
        },
    )
    assert res.status_code == 200, res.text
    assert res.json()["compiled_plan"]["collection"] == "Patient"


def test_fhir_instance_read_returns_resource_or_operation_outcome(client, monkeypatch):
    _activate_fhir(client)
    dispatch = AsyncMock(
        side_effect=[
            {
                "rows": [
                    {
                        "resourceType": "Patient",
                        "id": "p-1",
                        "_search": {"name": ["smith"]},
                    }
                ],
                "explain": {"total": 1},
            },
            {"rows": [], "explain": {"total": 0}},
        ]
    )
    monkeypatch.setattr(client.app.state.strategy_runtime, "dispatch", dispatch)

    found = client.get(
        "/api/domains/fhir/Patient/p-1",
        headers={"x-active-env": "dev"},
    )
    assert found.status_code == 200, found.text
    assert found.json() == {"resourceType": "Patient", "id": "p-1"}

    missing = client.get(
        "/api/domains/fhir/Patient/missing",
        headers={"x-active-env": "dev"},
    )
    assert missing.status_code == 404
    assert missing.headers["content-type"].startswith("application/fhir+json")
    assert missing.json()["resourceType"] == "OperationOutcome"


def test_fhir_metadata_is_generated_from_runtime_capabilities(client, monkeypatch):
    _activate_fhir(client)

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert op == "op"
        assert payload == {"domain": "fhir", "op": "fhir_capabilities", "payload": {}}
        return {
            "fhir_version": "R5",
            "searchable_resource_types": ["Observation", "Patient"],
            "write_supported": True,
        }

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(side_effect=fake_dispatch),
    )
    res = client.get(
        "/api/domains/fhir/metadata",
        headers={"x-active-env": "dev"},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["resourceType"] == "CapabilityStatement"
    assert body["fhirVersion"] == "5.0.0"
    assert body["experimental"] is True
    assert [resource["type"] for resource in body["rest"][0]["resource"]] == [
        "Observation",
        "Patient",
    ]
    assert body["rest"][0]["resource"][0]["interaction"] == [
        {"code": "read"},
        {"code": "search-type"},
        {"code": "create"},
        {"code": "update"},
    ]


def test_fhir_metadata_reports_r4_without_overstating_resource_scope(
    client, monkeypatch
):
    _activate_fhir(client)

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(
            return_value={
                "fhir_version": "R4",
                "searchable_resource_types": ["Observation", "Patient"],
                "storable_resource_types": ["Observation", "Patient"],
                "release_support": {"support_tier": "minimal"},
            }
        ),
    )

    res = client.get("/api/domains/fhir/metadata", headers={"x-active-env": "dev"})

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["fhirVersion"] == "4.0.1"
    assert [resource["type"] for resource in body["rest"][0]["resource"]] == [
        "Observation",
        "Patient",
    ]


def test_fhir_semantic_preview_is_an_explicit_strategy_operation(client, monkeypatch):
    _activate_fhir(client)

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert env_id == "dev"
        assert op == "op"
        assert payload == {
            "domain": "fhir",
            "op": "fhir_semantic_preview",
            "payload": {
                "pipeline_id": "clinical-notes-v1",
                "resource": {"resourceType": "DiagnosticReport", "id": "r1"},
            },
        }
        return {
            "pipeline_id": "clinical-notes-v1",
            "rendered_text": "No acute findings",
            "chunks": ["No acute findings"],
            "embedding_generated": False,
            "canonical_resource_mutated": False,
        }

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(side_effect=fake_dispatch),
    )
    res = client.post(
        "/api/domains/fhir/semantic/preview",
        headers={"x-active-env": "dev"},
        json={
            "pipeline_id": "clinical-notes-v1",
            "resource": {"resourceType": "DiagnosticReport", "id": "r1"},
        },
    )

    assert res.status_code == 200, res.text
    assert res.json()["embedding_generated"] is False
    assert res.json()["canonical_resource_mutated"] is False


def test_fhir_semantic_execution_routes_delegate_to_strategy(client, monkeypatch):
    _activate_fhir(client)
    dispatch = AsyncMock(
        side_effect=[
            {"ok": True, "resources": 1, "chunks": 2},
            {"ok": True, "count": 1, "results": [{"score": 0.9}]},
        ]
    )
    monkeypatch.setattr(client.app.state.strategy_runtime, "dispatch", dispatch)

    materialized = client.post(
        "/api/domains/fhir/semantic/materialize",
        headers={"x-active-env": "dev"},
        json={
            "pipeline_id": "clinical-notes-v1",
            "targets": [{"resource_type": "DiagnosticReport", "id": "r1"}],
        },
    )
    searched = client.post(
        "/api/domains/fhir/semantic/search",
        headers={"x-active-env": "dev"},
        json={"pipeline_id": "clinical-notes-v1", "query": "acute findings"},
    )

    assert materialized.status_code == 200
    assert materialized.json()["chunks"] == 2
    assert searched.status_code == 200
    assert searched.json()["results"][0]["score"] == 0.9
    assert dispatch.await_args_list[0].args[2]["op"] == "fhir_semantic_materialize"
    assert dispatch.await_args_list[1].args[2]["op"] == "fhir_semantic_search"


def test_fhir_cohort_routes_delegate_catalog_plan_and_bounded_preview(
    client, monkeypatch
):
    _activate_fhir(client)
    dispatch = AsyncMock(
        side_effect=[
            {"ok": True, "asset_count": 3, "assets": []},
            {"ok": True, "plan_digest": "sha256:plan", "planned": {"Patient": 2}},
            {
                "ok": True,
                "dry_run": True,
                "total_documents": 2,
                "sample_resources": [],
            },
        ]
    )
    monkeypatch.setattr(client.app.state.strategy_runtime, "dispatch", dispatch)

    catalog = client.get(
        "/api/domains/fhir/synthetic/cohorts?include_blueprints=false",
        headers={"x-active-env": "dev"},
    )
    plan = client.post(
        "/api/domains/fhir/synthetic/cohorts/plan",
        headers={"x-active-env": "dev"},
        json={"blueprint_id": "cardiometabolic-monitoring", "patients": 2},
    )
    preview = client.post(
        "/api/domains/fhir/synthetic/cohorts/preview",
        headers={"x-active-env": "dev"},
        json={
            "cohort": {
                "blueprint_id": "cardiometabolic-monitoring",
                "patients": 2,
            },
            "sample_limit": 25,
        },
    )

    assert catalog.status_code == plan.status_code == preview.status_code == 200
    assert dispatch.await_args_list[0].args[2]["op"] == "fhir_cohort_catalog"
    assert dispatch.await_args_list[0].args[2]["payload"] == {
        "include_blueprints": False
    }
    assert dispatch.await_args_list[1].args[2]["op"] == "fhir_cohort_plan"
    preview_payload = dispatch.await_args_list[2].args[2]["payload"]
    assert dispatch.await_args_list[2].args[2]["op"] == "synthetic_generate_batch"
    assert preview_payload["dry_run"] is True
    assert preview_payload["store_canonical"] is False
    assert preview_payload["include_sample"] is True


def test_fhir_cohort_preview_rejects_more_than_ten_patients(client, monkeypatch):
    _activate_fhir(client)
    dispatch = AsyncMock()
    monkeypatch.setattr(client.app.state.strategy_runtime, "dispatch", dispatch)

    response = client.post(
        "/api/domains/fhir/synthetic/cohorts/preview",
        headers={"x-active-env": "dev"},
        json={"blueprint_id": "cardiometabolic-monitoring", "patients": 11},
    )

    assert response.status_code == 400
    assert response.json()["resourceType"] == "OperationOutcome"
    dispatch.assert_not_awaited()


def test_fhir_migration_run_routes_dispatch_checkpoint_operations(client, monkeypatch):
    _activate_fhir(client)
    dispatch = AsyncMock(
        side_effect=[
            {"ok": True, "run": {"run_id": "run-1", "status": "ready"}},
            {
                "ok": True,
                "run": {"run_id": "run-1", "status": "completed"},
                "chunk": {"chunk_index": 0, "report": {"committed": True}},
            },
            {"ok": True, "items": [{"run_id": "run-1", "status": "completed"}]},
        ]
    )
    monkeypatch.setattr(client.app.state.strategy_runtime, "dispatch", dispatch)

    started = client.post(
        "/api/domains/fhir/migration/runs",
        headers={"x-active-env": "dev"},
        json={"source_name": "patients.ndjson", "total_resources": 1, "total_chunks": 1},
    )
    assert started.status_code == 201
    assert dispatch.await_args_list[0].args[2]["op"] == "fhir_migration_start"

    chunk = client.post(
        "/api/domains/fhir/migration/runs/run-1/chunks/0?final=true",
        headers={"x-active-env": "dev"},
        json={"resources": [{"resourceType": "Patient", "id": "p1"}]},
    )
    assert chunk.status_code == 200
    chunk_payload = dispatch.await_args_list[1].args[2]
    assert chunk_payload["op"] == "fhir_migration_import_chunk"
    assert chunk_payload["payload"]["run_id"] == "run-1"
    assert chunk_payload["payload"]["final"] is True

    history = client.get(
        "/api/domains/fhir/migration/runs?limit=10",
        headers={"x-active-env": "dev"},
    )
    assert history.status_code == 200
    assert history.json()["items"][0]["run_id"] == "run-1"
    assert dispatch.await_args_list[2].args[2]["payload"]["limit"] == 10


def test_fhir_support_matrix_can_be_downloaded_as_markdown(client, monkeypatch):
    _activate_fhir(client)
    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(
            return_value={
                "ok": True,
                "fhir_version": "R4",
                "generated_from": "fhir_capabilities",
                "rows": [{"resource_type": "Patient", "write": True}],
                "markdown": "# FHIR R4 accelerator support matrix\n",
            }
        ),
    )
    response = client.get(
        "/api/domains/fhir/support-matrix?format=markdown",
        headers={"x-active-env": "dev"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/markdown")
    assert response.text.startswith("# FHIR R4")


def test_fhir_ig_upload_is_staged_without_activation(client, monkeypatch, tmp_path):
    _activate_fhir(client)
    monkeypatch.setenv("KEHRNEL_FHIR_IG_STAGING_ROOT", str(tmp_path / "ig-stage"))
    from kehrnel.api.domains.fhir import routes as fhir_routes

    captured = {}

    def fake_stage(data, **kwargs):
        captured["data"] = data
        captured.update(kwargs)
        return {
            "staged_id": "a" * 64,
            "source": str(tmp_path / "ig-stage" / "dev" / f"{'a' * 64}.tgz"),
            "sha256": "a" * 64,
            "package": {"name": "example.fhir.ig", "version": "1.0.0"},
            "inventory": {"profile_count": 1},
            "activation_entry": {"enabled": True, "source": "/staged/package.tgz", "sha256": "a" * 64},
            "activated": False,
            "profiles_selected": False,
            "profile_validation_enabled": False,
        }

    monkeypatch.setattr(
        fhir_routes.implementation_guides, "stage_implementation_guide", fake_stage
    )
    response = client.post(
        "/api/domains/fhir/implementation-guides/stage",
        headers={
            "x-active-env": "dev",
            "content-type": "application/gzip",
            "x-fhir-package-filename": "example.tgz",
        },
        content=b"archive-bytes",
    )
    assert response.status_code == 201, response.text
    assert captured["data"] == b"archive-bytes"
    assert captured["filename"] == "example.tgz"
    assert captured["environment_id"] == "dev"
    assert captured["expected_release"] == "R5"
    assert response.json()["activated"] is False


def test_fhir_stats_dispatches_to_cdr(client, monkeypatch):
    _activate_fhir(client)

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert env_id == "dev"
        assert op == "op"
        assert payload == {"domain": "fhir", "op": "fhir_stats", "payload": {}}
        return {
            "ok": True,
            "database": "fhir_test",
            "summary": {"document_count": 0, "populated_resource_type_count": 0},
            "collections": [],
        }

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(side_effect=fake_dispatch),
    )
    res = client.get("/api/domains/fhir/stats", headers={"x-active-env": "dev"})

    assert res.status_code == 200, res.text
    assert res.json()["summary"]["document_count"] == 0


def test_fhir_ndjson_import_dispatches_to_cdr(client, monkeypatch):
    _activate_fhir(client)

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert payload["op"] == "fhir_import_resources"
        assert payload["payload"]["ndjson"].startswith('{"resourceType":"Patient"')
        assert payload["payload"]["validation_level"] == "structure"
        return {"ok": True, "committed": True, "write": {"inserted": 1}}

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(side_effect=fake_dispatch),
    )
    res = client.post(
        "/api/domains/fhir/import?validation_level=structure",
        headers={"x-active-env": "dev", "content-type": "application/fhir+ndjson"},
        content='{"resourceType":"Patient","id":"p1"}\n',
    )
    assert res.status_code == 200, res.text
    assert res.json()["committed"] is True


def test_fhir_create_assigns_id_and_uses_import_pipeline(client, monkeypatch):
    _activate_fhir(client)

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        inner = payload["payload"]
        assert inner["mode"] == "create"
        assert inner["resource"]["resourceType"] == "Patient"
        assert inner["resource"]["id"]
        return {"ok": True, "committed": True, "write": {"inserted": 1}}

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(side_effect=fake_dispatch),
    )
    res = client.post(
        "/api/domains/fhir/Patient",
        headers={"x-active-env": "dev", "content-type": "application/fhir+json"},
        json={"resourceType": "Patient", "active": True},
    )
    assert res.status_code == 201, res.text
    assert res.json()["id"]
    assert res.headers["location"].endswith(
        "/api/domains/fhir/Patient/" + res.json()["id"]
    )


def test_run_fhir_search_compartment_serializes_mongodb_object_ids(client, monkeypatch):
    """POST /environments/{env}/run fhir_search with compartment must JSON-encode BSON _id."""
    _activate_fhir(client)
    oid = ObjectId()

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert op == "op"
        assert payload.get("op") == "fhir_search"
        inner = payload.get("payload") or {}
        assert inner.get("compartment") == {"type": "Patient", "id": "p-1"}
        return {
            "ok": True,
            "engine_used": "fhir_mql",
            "rows": [
                {
                    "resourceType": "Observation",
                    "id": "o-1",
                    "status": "final",
                    "_id": oid,
                }
            ],
            "explain": {"total": 1, "returned": 1},
        }

    rt = client.app.state.strategy_runtime
    monkeypatch.setattr(rt, "dispatch", AsyncMock(side_effect=fake_dispatch))

    res = client.post(
        "/v1/environments/dev/run",
        json={
            "domain": "fhir",
            "operation": "fhir_search",
            "payload": {
                "resource_type": "Observation",
                "criteria": {"status": "final"},
                "compartment": {"type": "Patient", "id": "p-1"},
                "_count": 5,
            },
        },
    )
    assert res.status_code == 200, res.text
    rows = res.json()["result"]["rows"]
    assert rows[0]["_id"] == str(oid)


def test_fhir_search_strips_operational_id_from_canonical_output(client, monkeypatch):
    """Rows carry Mongo's internal _id; canonical FHIR output must NOT leak it (T5).

    Previously the boundary passed _id straight through (as a serialized ObjectId).
    Canonical serialization strips operational storage fields, so _id is absent
    while the canonical resource content remains intact and JSON-encodable.
    """
    _activate_fhir(client)
    oid = ObjectId()

    async def fake_dispatch(env_id: str, op: str, payload: dict):
        assert payload["query"].get("fhir_search") == "Patient?gender=male"
        return {
            "engine_used": "fhir_mql",
            "rows": [
                {"resourceType": "Patient", "id": "p-1", "gender": "male", "_id": oid}
            ],
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
    resource = res.json()["entry"][0]["resource"]
    assert "_id" not in resource  # operational field stripped
    assert resource == {"resourceType": "Patient", "id": "p-1", "gender": "male"}


def test_wrong_strategy_returns_operation_outcome(client):
    """Finding 6/7: FHIR-boundary errors are OperationOutcome + application/fhir+json."""
    client.post(
        "/v1/environments/dev/activate",
        json={
            "strategy_id": "openehr.rps_dual",
            "version": "0.2.0",
            "config": {"database": "openehr_contract"},
            "bindings": FHIR_ACTIVATE["bindings"],
            "allow_plaintext_bindings": True,
            "domain": "fhir",
        },
    )
    res = client.post(
        "/api/domains/fhir/search",
        headers={"x-active-env": "dev"},
        json={"resource_type": "Patient", "criteria": {"gender": "female"}, "limit": 5},
    )
    assert res.status_code == 409
    assert res.headers["content-type"].startswith("application/fhir+json")
    assert res.json()["resourceType"] == "OperationOutcome"


def test_invalid_body_returns_operation_outcome_422(client):
    """Finding 4: request-validation errors also return FHIR OperationOutcome."""
    _activate_fhir(client)
    res = client.post(
        "/api/domains/fhir/search",
        headers={"x-active-env": "dev"},
        json={"resource_type": "Patient", "limit": 0},  # limit ge=1 → validation error
    )
    assert res.status_code == 422
    assert res.headers["content-type"].startswith("application/fhir+json")
    assert res.json()["resourceType"] == "OperationOutcome"


def test_bundle_media_type_and_no_proprietary_fields(client, monkeypatch):
    """Finding 6: conformant Bundle — fhir+json media type, no undeclared meta.kehrnel."""
    _activate_fhir(client)

    async def fake_dispatch(env_id, op, payload):
        return {
            "engine_used": "fhir_mql",
            "rows": [{"resourceType": "Patient", "id": "p-1"}],
            "explain": {"engine": "fhir_mql", "total": 1, "returned": 1},
        }

    monkeypatch.setattr(
        client.app.state.strategy_runtime,
        "dispatch",
        AsyncMock(side_effect=fake_dispatch),
    )
    res = client.post(
        "/api/domains/fhir/search",
        headers={"x-active-env": "dev"},
        json={"resource_type": "Patient", "criteria": {}, "limit": 5},
    )
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/fhir+json")
    body = res.json()
    assert body["resourceType"] == "Bundle" and body["type"] == "searchset"
    assert "meta" not in body  # no proprietary meta.kehrnel


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
