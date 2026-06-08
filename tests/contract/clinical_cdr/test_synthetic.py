"""Contract tests: fhir.clinical_cdr synthetic batch + synthetic job (prompt 12)."""

from __future__ import annotations

import asyncio

import pytest

from kehrnel.engine.strategies.fhir.clinical_cdr.generation import synthetic_generate_batch
from tests.contract.clinical_cdr.contract_helpers import (
    activate_runtime,
    activation_payload,
    drop_database,
    make_test_client,
    requires_mongo,
    strategy_context,
    unique_db_name,
)

pytestmark = requires_mongo


@pytest.fixture
def contract_db():
    db_name = unique_db_name("fhir_syn")
    yield db_name
    drop_database(db_name)


@pytest.mark.asyncio
async def test_synthetic_generate_batch_patient_observation_counts(contract_db):
    ctx = strategy_context(database=contract_db)
    result = await synthetic_generate_batch(
        ctx,
        {
            "resources": {"Patient": 5, "Observation": 10},
            "seed": 42,
            "store_canonical": True,
            "denormalize_after": True,
        },
    )
    assert result["ok"] is True
    assert result["generated"]["Patient"] >= 5
    assert result["generated"]["Observation"] >= 10
    assert result["inserted"].get("Patient", 0) >= 5
    assert result["inserted"].get("Observation", 0) >= 10
    assert result.get("denormalized")


@pytest.fixture
def api_client(tmp_path, monkeypatch):
    return make_test_client(tmp_path, monkeypatch)


@pytest.mark.asyncio
async def test_synthetic_job_completes_via_manager(api_client, contract_db):
    app = api_client.app
    runtime = app.state.strategy_runtime
    manager = app.state.synthetic_job_manager
    env_id = "fhir-syn-contract"

    await activate_runtime(runtime, env_id=env_id, database=contract_db)

    job = await manager.create_job(
        env_id=env_id,
        domain="fhir",
        op="synthetic_generate_batch",
        payload={
            "resources": {"Patient": 5, "Observation": 10},
            "seed": 99,
            "store_canonical": True,
            "denormalize_after": True,
        },
    )
    job_id = job["job_id"]
    task = manager._tasks.get(job_id)
    assert task is not None
    await asyncio.wait_for(task, timeout=120)

    final = await manager.get_job(job_id)
    assert final is not None
    assert final["status"] == "completed", final
    assert final["progress"] == 100

    result = final.get("result") or {}
    assert result.get("ok") is True
    inserted = result.get("inserted") or {}
    assert inserted.get("Patient", 0) >= 5
    assert inserted.get("Observation", 0) >= 10


@pytest.mark.asyncio
async def test_synthetic_job_http_endpoint(api_client, contract_db, monkeypatch):
    from httpx import ASGITransport, AsyncClient

    monkeypatch.setenv("KEHRNEL_DEFAULT_ENV_ID", "fhir-syn-http")
    env_id = "fhir-syn-http"
    app = api_client.app

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        res = await client.post(
            f"/v1/environments/{env_id}/activate",
            json=activation_payload(database=contract_db),
        )
        assert res.status_code == 200, res.text

        create = await client.post(
            f"/v1/environments/{env_id}/synthetic/jobs",
            json={
                "domain": "fhir",
                "op": "synthetic_generate_batch",
                "payload": {
                    "resources": {"Patient": 5, "Observation": 10},
                    "seed": 7,
                    "store_canonical": True,
                    "denormalize_after": True,
                },
            },
        )
        assert create.status_code == 202, create.text
        created = create.json()["job"]
        job_id = created["job_id"]
        assert created.get("status") in ("queued", "running")

        final = None
        for _ in range(400):
            poll = await client.get(f"/v1/environments/{env_id}/synthetic/jobs/{job_id}")
            assert poll.status_code == 200, poll.text
            job = poll.json()["job"]
            if job.get("status") in ("completed", "failed", "canceled"):
                final = job
                break
            await asyncio.sleep(0.1)

    assert final is not None, "synthetic job did not finish in time"
    assert final["status"] == "completed", final
    inserted = (final.get("result") or {}).get("inserted") or {}
    assert inserted.get("Patient", 0) >= 5
    assert inserted.get("Observation", 0) >= 10
