import asyncio

import pytest

from kehrnel.engine.core.synthetic_jobs import SyntheticJobManager


class MemoryJobStore:
    def __init__(self, jobs):
        self.jobs = {job["job_id"]: dict(job) for job in jobs}

    def list(self, *, env_id=None, domain=None):
        return list(self.jobs.values())

    def upsert(self, record):
        self.jobs[record["job_id"]] = dict(record)

    def patch(self, job_id, record):
        self.jobs[job_id] = dict(record)


class Runtime:
    def __init__(self):
        self.calls = []

    async def dispatch(self, env_id, operation, payload):
        self.calls.append((env_id, operation, payload))
        return {"ok": True, "env": env_id, "op": payload["op"]}


def interrupted_job(job_id="job-1"):
    return {
        "job_id": job_id, "status": "running", "phase": "generating", "progress": 40,
        "env_id": "env", "domain": "cdisc", "op": "cdisc_generate_synthetic_study",
        "payload": {"recipe": {"studyId": "S"}}, "created_at": "2026-01-01T00:00:00Z",
    }


@pytest.mark.asyncio
async def test_job_recovery_marks_interrupted_work_failed_by_default():
    store = MemoryJobStore([interrupted_job()])
    manager = SyntheticJobManager(Runtime(), store=store)

    counts = await manager.recover_incomplete_jobs()

    assert counts == {"found": 1, "retried": 0, "failed": 1}
    assert store.jobs["job-1"]["error"]["code"] == "JOB_INTERRUPTED"


@pytest.mark.asyncio
async def test_job_recovery_can_retry_idempotent_strategy_operations():
    store = MemoryJobStore([interrupted_job()])
    manager = SyntheticJobManager(Runtime(), store=store)

    counts = await manager.recover_incomplete_jobs(retry=True)
    for _ in range(50):
        if store.jobs["job-1"].get("status") == "completed":
            break
        await asyncio.sleep(0.01)

    assert counts == {"found": 1, "retried": 1, "failed": 0}
    assert store.jobs["job-1"]["status"] == "completed"


@pytest.mark.asyncio
async def test_job_dispatch_carries_internal_job_id_for_resource_provenance():
    runtime = Runtime()
    manager = SyntheticJobManager(runtime)

    created = await manager.create_job(
        env_id="env",
        domain="fhir",
        op="synthetic_generate_batch",
        payload={"resources": {"Patient": 1}},
    )
    job_id = created["job_id"]
    await manager._tasks[job_id]

    assert runtime.calls
    _, operation, dispatch_payload = runtime.calls[0]
    assert operation == "op"
    assert dispatch_payload["__job_id"] == job_id
