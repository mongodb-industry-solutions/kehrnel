"""Core synthetic dataset job orchestration."""
from __future__ import annotations

import asyncio
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.runtime import StrategyRuntime


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SyntheticJobManager:
    """Async in-process job manager.

    The manager is generic and delegates generation logic to strategy ops.
    """

    def __init__(self, runtime: StrategyRuntime, store: Any | None = None):
        self.runtime = runtime
        self.store = store
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
        metadata: Dict[str, Any] | None = None,
        requested_by: str | None = None,
        requester_id: str | None = None,
        team_id: str | None = None,
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
            "requester_id": (requester_id or "").strip() or None,
            "team_id": (team_id or "").strip() or None,
            "payload": payload,
            "result": None,
            "error": None,
            "stats": {},
            "source_database": (
                payload.get("source_database")
                or payload.get("source_db")
                or payload.get("sourceDatabase")
            ),
            "source_collection": payload.get("source_collection") or payload.get("sourceCollection"),
            "target_database": (metadata or {}).get("target_database"),
            "target_collections": (metadata or {}).get("target_collections"),
            "model_source": (metadata or {}).get("model_source"),
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
        await self._persist_upsert(rec)
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
                source_database=(result or {}).get("source_database") if isinstance(result, dict) else None,
                source_collection=(result or {}).get("source_collection") if isinstance(result, dict) else None,
                target_database=(result or {}).get("target_database") if isinstance(result, dict) else None,
                target_collections=(
                    ((result or {}).get("target_collections") or (result or {}).get("target"))
                    if isinstance(result, dict)
                    else None
                ),
                model_source=(result or {}).get("model_source") if isinstance(result, dict) else None,
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
        await self._persist_patch(job_id, rec)

    async def get_job(self, job_id: str) -> Dict[str, Any] | None:
        async with self._lock:
            rec = self._jobs.get(job_id)
        if rec:
            return self._public(rec)
        if self.store:
            try:
                persisted = await asyncio.to_thread(self.store.get, job_id)
                if persisted:
                    async with self._lock:
                        self._jobs[job_id] = persisted
                    return self._public(persisted)
            except Exception:
                return None
        return None

    async def list_jobs(self, env_id: str | None = None, domain: str | None = None) -> list[Dict[str, Any]]:
        items: list[Dict[str, Any]]
        if self.store:
            try:
                items = await asyncio.to_thread(self.store.list, env_id=env_id, domain=domain)
            except Exception:
                async with self._lock:
                    items = list(self._jobs.values())
        else:
            async with self._lock:
                items = list(self._jobs.values())
        # Overlay in-memory records (active updates can be fresher than persisted state).
        async with self._lock:
            for jid, mem in self._jobs.items():
                items = [j for j in items if j.get("job_id") != jid]
                items.append(mem)
        if env_id:
            items = [j for j in items if j.get("env_id") == env_id]
        if domain:
            items = [j for j in items if str(j.get("domain") or "").lower() == str(domain).lower()]
        items.sort(key=lambda j: j.get("created_at") or "", reverse=True)
        return [self._public(j) for j in items]

    async def cancel_job(self, job_id: str) -> Dict[str, Any]:
        # NOTE: Cancellation is currently in-process (asyncio task + cancel Event).
        # If the API process restarts, jobs loaded from the persistent store will not
        # have an in-memory task/event. In that case, we must not leave jobs stuck in
        # "canceling" forever; instead, finalize immediately as "canceled".
        async with self._lock:
            rec = self._jobs.get(job_id)
            if not rec:
                raise KehrnelError(code="JOB_NOT_FOUND", status=404, message=f"Job {job_id} not found")

            status = rec.get("status")
            if status in ("completed", "failed", "canceled"):
                return self._public(rec)

            task = self._tasks.get(job_id)
            ev = self._cancel_events.get(job_id)
            if ev is None:
                ev = asyncio.Event()
                self._cancel_events[job_id] = ev
            ev.set()

            # If there is no in-memory task (or it has already finished), treat the job as
            # effectively canceled. This prevents a persisted job from remaining "canceling"
            # across restarts.
            if task is None or task.done():
                now = _now_iso()
                rec["status"] = "canceled"
                rec["phase"] = "canceled"
                rec["progress"] = 100
                rec["completed_at"] = rec.get("completed_at") or now
                rec["updated_at"] = now
                rec["error"] = rec.get("error") or "Canceled"
                self._jobs[job_id] = rec
                self._tasks.pop(job_id, None)
            else:
                rec["status"] = "canceling"
                rec["phase"] = "canceling"
                rec["updated_at"] = _now_iso()
                self._jobs[job_id] = rec

        await self._persist_upsert(rec)
        return self._public(rec)

    async def _persist_upsert(self, rec: Dict[str, Any]) -> None:
        if not self.store:
            return
        try:
            await asyncio.to_thread(self.store.upsert, rec)
        except Exception:
            return

    async def _persist_patch(self, job_id: str, rec: Dict[str, Any]) -> None:
        if not self.store:
            return
        try:
            await asyncio.to_thread(self.store.patch, job_id, rec)
        except Exception:
            return

    def _public(self, rec: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if rec is None:
            return None
        data = dict(rec)
        # Backward/consumer compatibility: expose both keys.
        data["id"] = data.get("job_id")
        # Do not expose requester metadata to API consumers.
        data.pop("requested_by", None)
        data["requester_id"] = rec.get("requester_id")
        data["team_id"] = rec.get("team_id")
        payload = (rec.get("payload") or {}) if isinstance(rec.get("payload"), dict) else {}
        # Do not echo full payload back for large jobs unless needed by caller.
        data["payload"] = {"keys": sorted(list(payload.keys()))}
        # Expose minimal request summary for UI traceability.
        data["request"] = {
            "patient_count": payload.get("patient_count"),
            "model_count": len(payload.get("models") or []) if isinstance(payload.get("models"), list) else None,
            "plan_only": payload.get("plan_only"),
            "dry_run": payload.get("dry_run"),
            "source_collection": payload.get("source_collection"),
            "model_source": payload.get("model_source") if isinstance(payload.get("model_source"), dict) else None,
        }
        has_models = isinstance(payload.get("models"), list) and len(payload.get("models")) > 0
        has_templates = isinstance(payload.get("templates"), list) and len(payload.get("templates")) > 0
        has_source = bool(payload.get("source_collection"))
        if has_models:
            data["request"]["input_mode"] = "model_catalog"
        elif has_templates:
            data["request"]["input_mode"] = "template_list"
        elif has_source:
            data["request"]["input_mode"] = "source_collection"
        else:
            data["request"]["input_mode"] = "auto"
        if data["request"].get("patient_count") is not None and data.get("patient_count") is None:
            data["patient_count"] = data["request"]["patient_count"]
        result = rec.get("result") if isinstance(rec.get("result"), dict) else {}
        target = result.get("target") if isinstance(result.get("target"), dict) else {}
        source = result.get("source") if isinstance(result.get("source"), dict) else {}
        data["target_database"] = data.get("target_database") or result.get("target_database")
        data["target_collections"] = (
            data.get("target_collections")
            if isinstance(data.get("target_collections"), dict)
            else (
                result.get("target_collections")
                if isinstance(result.get("target_collections"), dict)
                else target
            )
        )
        data["source_database"] = (
            result.get("source_database")
            or source.get("database")
            or data.get("source_database")
        )
        data["source_collection"] = (
            result.get("source_collection")
            or source.get("collection")
            or data.get("source_collection")
        )
        # Backward compatibility for HDL/proxy field names.
        data["targetDatabase"] = data.get("target_database")
        data["targetCollections"] = data.get("target_collections")
        data["sourceDatabase"] = data.get("source_database")
        data["sourceCollection"] = data.get("source_collection")
        return data

    def _redact_requester(self, requester: str | None) -> str | None:
        if not requester:
            return None
        digest = hashlib.sha256(requester.encode("utf-8")).hexdigest()[:12]
        return f"key:{digest}"
