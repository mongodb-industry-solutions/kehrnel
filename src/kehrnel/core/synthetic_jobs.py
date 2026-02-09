"""Core synthetic dataset job orchestration."""
from __future__ import annotations

import asyncio
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict

from kehrnel.core.errors import KehrnelError
from kehrnel.core.runtime import StrategyRuntime


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SyntheticJobManager:
    """Async in-process job manager.

    The manager is generic and delegates generation logic to strategy ops.
    """

    def __init__(self, runtime: StrategyRuntime):
        self.runtime = runtime
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._cancel_events: Dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()

    async def create_job(
        self,
        *,
        env_id: str,
        domain: str,
        op: str,
        payload: Dict[str, Any],
        requested_by: str | None = None,
    ) -> Dict[str, Any]:
        job_id = str(uuid.uuid4())
        now = _now_iso()
        rec = {
            "job_id": job_id,
            "type": "synthetic",
            "env_id": env_id,
            "domain": domain,
            "op": op,
            "status": "queued",
            "phase": "queued",
            "progress": 0,
            "requested_by": self._redact_requester(requested_by),
            "payload": payload,
            "result": None,
            "error": None,
            "stats": {},
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "completed_at": None,
        }
        async with self._lock:
            self._jobs[job_id] = rec
            cancel_event = asyncio.Event()
            self._cancel_events[job_id] = cancel_event
            self._tasks[job_id] = asyncio.create_task(
                self._run_job(job_id=job_id, env_id=env_id, domain=domain, op=op, payload=payload)
            )
        return self._public(rec)

    async def _run_job(self, *, job_id: str, env_id: str, domain: str, op: str, payload: Dict[str, Any]) -> None:
        await self._patch(job_id, status="running", phase="starting", progress=1, started_at=_now_iso())
        cancel_event = self._cancel_events.get(job_id) or asyncio.Event()

        async def progress_cb(*, progress: int | None = None, phase: str | None = None, stats: Dict[str, Any] | None = None):
            patch: Dict[str, Any] = {"updated_at": _now_iso()}
            if progress is not None:
                patch["progress"] = max(0, min(100, int(progress)))
            if phase:
                patch["phase"] = str(phase)
            if stats:
                current = (self._jobs.get(job_id) or {}).get("stats") or {}
                patch["stats"] = {**current, **stats}
            await self._patch(job_id, **patch)

        def should_cancel() -> bool:
            return cancel_event.is_set()

        try:
            result = await self.runtime.dispatch(
                env_id,
                "op",
                {
                    "domain": domain,
                    "op": op,
                    "payload": payload,
                    "__progress_cb": progress_cb,
                    "__should_cancel": should_cancel,
                },
            )
            if cancel_event.is_set():
                await self._patch(
                    job_id,
                    status="canceled",
                    phase="canceled",
                    progress=100,
                    completed_at=_now_iso(),
                    updated_at=_now_iso(),
                    error="Canceled",
                )
                return
            await self._patch(
                job_id,
                status="completed",
                phase="completed",
                progress=100,
                completed_at=_now_iso(),
                updated_at=_now_iso(),
                result=result,
            )
        except KehrnelError as exc:
            status = "canceled" if exc.code in ("JOB_CANCELED", "CANCELED") or cancel_event.is_set() else "failed"
            phase = "canceled" if status == "canceled" else "failed"
            await self._patch(
                job_id,
                status=status,
                phase=phase,
                completed_at=_now_iso(),
                updated_at=_now_iso(),
                error={"code": exc.code, "message": str(exc), "details": exc.details},
            )
        except Exception as exc:
            await self._patch(
                job_id,
                status="failed",
                phase="failed",
                completed_at=_now_iso(),
                updated_at=_now_iso(),
                error={"code": "INTERNAL_ERROR", "message": str(exc)},
            )

    async def _patch(self, job_id: str, **patch: Any) -> None:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if not rec:
                return
            rec.update(patch)
            rec["updated_at"] = patch.get("updated_at") or _now_iso()
            self._jobs[job_id] = rec

    async def get_job(self, job_id: str) -> Dict[str, Any] | None:
        async with self._lock:
            rec = self._jobs.get(job_id)
            return self._public(rec) if rec else None

    async def list_jobs(self, env_id: str | None = None, domain: str | None = None) -> list[Dict[str, Any]]:
        async with self._lock:
            items = list(self._jobs.values())
        if env_id:
            items = [j for j in items if j.get("env_id") == env_id]
        if domain:
            items = [j for j in items if str(j.get("domain") or "").lower() == str(domain).lower()]
        items.sort(key=lambda j: j.get("created_at") or "", reverse=True)
        return [self._public(j) for j in items]

    async def cancel_job(self, job_id: str) -> Dict[str, Any]:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if not rec:
                raise KehrnelError(code="JOB_NOT_FOUND", status=404, message=f"Job {job_id} not found")
            status = rec.get("status")
            if status in ("completed", "failed", "canceled"):
                return self._public(rec)
            ev = self._cancel_events.get(job_id)
            if ev:
                ev.set()
            rec["status"] = "canceling"
            rec["phase"] = "canceling"
            rec["updated_at"] = _now_iso()
            self._jobs[job_id] = rec
            return self._public(rec)

    def _public(self, rec: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if rec is None:
            return None
        data = dict(rec)
        # Backward/consumer compatibility: expose both keys.
        data["id"] = data.get("job_id")
        # Do not expose requester metadata to API consumers.
        data.pop("requested_by", None)
        # Do not echo full payload back for large jobs unless needed by caller.
        data["payload"] = {"keys": sorted(list((rec.get("payload") or {}).keys()))}
        return data

    def _redact_requester(self, requester: str | None) -> str | None:
        if not requester:
            return None
        digest = hashlib.sha256(requester.encode("utf-8")).hexdigest()[:12]
        return f"key:{digest}"
