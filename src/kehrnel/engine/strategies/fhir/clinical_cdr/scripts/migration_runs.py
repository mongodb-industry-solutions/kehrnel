"""Tenant-scoped, resumable FHIR migration runs.

The migration coordinator deliberately stores only run metadata, checkpoints,
digests, and bounded reports. Clinical source payloads remain with the caller and
are sent in explicit chunks, avoiding copies in Kehrnel's transversal job store.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.import_resources import (
    fhir_import_resources,
    parse_import_payload,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import (
    canonical_resource,
)

RUNS_COLLECTION = "_kehrnel_fhir_migration_runs"
CHUNKS_COLLECTION = "_kehrnel_fhir_migration_chunks"
RUN_CONTRACT_VERSION = "1.0"
MAX_HISTORY_LIMIT = 100
MAX_STORED_FINDINGS = 250
_REFERENCE_TYPE = re.compile(r"^[A-Z][A-Za-z0-9]{0,63}$")
_TERMINAL_STATES = frozenset({"completed", "completed_with_errors", "failed", "canceled"})


def _now() -> datetime:
    return datetime.now(UTC)


def _database(ctx: StrategyContext) -> tuple[Any, str, str]:
    uri, database, prefix = bridge.resolve_mongo(ctx)
    return bridge.get_mongo_client(uri)[database], database, prefix


def _ensure_indexes(db: Any) -> None:
    db[RUNS_COLLECTION].create_index("run_id", unique=True, name="run_id_unique")
    db[RUNS_COLLECTION].create_index(
        [("environment_id", 1), ("created_at", -1)],
        name="environment_created",
    )
    db[CHUNKS_COLLECTION].create_index(
        [("run_id", 1), ("chunk_index", 1)],
        unique=True,
        name="run_chunk_unique",
    )


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _clean(item) for key, item in value.items() if key != "_id"}
    if isinstance(value, list):
        return [_clean(item) for item in value]
    return value


def _public_run(run: dict[str, Any], *, chunks: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    result = _clean(dict(run))
    if chunks is not None:
        result["chunks"] = [_clean(item) for item in chunks]
    return result


def _bounded_report(report: dict[str, Any]) -> dict[str, Any]:
    bounded = _clean(dict(report))
    validation = bounded.get("validation")
    if isinstance(validation, dict):
        findings = validation.get("findings")
        if isinstance(findings, list) and len(findings) > MAX_STORED_FINDINGS:
            validation["findings"] = findings[:MAX_STORED_FINDINGS]
            validation["findings_truncated"] = True
            validation["finding_count"] = len(findings)
    return bounded


def _payload_digest(payload: dict[str, Any]) -> str:
    wrapper_keys = {"bundle", "resources", "resource", "ndjson"}
    material = {key: payload[key] for key in wrapper_keys if key in payload}
    if not material and payload.get("resourceType"):
        material = {
            key: value
            for key, value in payload.items()
            if key not in {"run_id", "chunk_index", "final"}
        }
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_run(db: Any, ctx: StrategyContext, run_id: str) -> dict[str, Any]:
    run = db[RUNS_COLLECTION].find_one(
        {"run_id": run_id, "environment_id": ctx.environment_id}
    )
    if not run:
        raise KehrnelError(
            code="FHIR_MIGRATION_RUN_NOT_FOUND",
            status=404,
            message=f"FHIR migration run {run_id!r} was not found",
        )
    return run


def _validate_chunk_size(ctx: StrategyContext, value: Any) -> int:
    cfg = bridge.resolve_strategy_config(ctx)
    import_cfg = cfg.get("import") if isinstance(cfg.get("import"), dict) else {}
    request_max = int(import_cfg.get("max_resources_per_request") or 10_000)
    try:
        chunk_size = int(value or min(500, request_max))
    except (TypeError, ValueError) as exc:
        raise KehrnelError(
            code="INVALID_INPUT", status=400, message="chunk_size must be an integer"
        ) from exc
    if chunk_size < 1 or chunk_size > request_max:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message=f"chunk_size must be between 1 and {request_max}",
        )
    return chunk_size


async def fhir_migration_start(
    ctx: StrategyContext, payload: dict[str, Any]
) -> dict[str, Any]:
    """Create an auditable run without retaining its clinical source payload."""
    db, database, _prefix = _database(ctx)
    cfg = bridge.resolve_strategy_config(ctx)
    chunk_size = _validate_chunk_size(ctx, payload.get("chunk_size"))
    source_format = str(payload.get("source_format") or "ndjson").strip().lower()
    if source_format not in {"ndjson", "bundle", "resources", "resource"}:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="source_format must be ndjson, bundle, resources, or resource",
        )
    mode = str(payload.get("mode") or "upsert").strip().lower()
    if mode not in {"upsert", "create"}:
        raise KehrnelError(
            code="INVALID_INPUT", status=400, message="mode must be upsert or create"
        )
    total_resources = payload.get("total_resources")
    total_chunks = payload.get("total_chunks")
    for name, value in (("total_resources", total_resources), ("total_chunks", total_chunks)):
        if value is not None and (not isinstance(value, int) or value < 0):
            raise KehrnelError(
                code="INVALID_INPUT", status=400, message=f"{name} must be a non-negative integer"
            )

    retry_of = str(payload.get("retry_of") or "").strip() or None
    if retry_of:
        _require_run(db, ctx, retry_of)

    await asyncio.to_thread(_ensure_indexes, db)
    now = _now()
    run = {
        "contract_version": RUN_CONTRACT_VERSION,
        "run_id": str(uuid.uuid4()),
        "environment_id": ctx.environment_id,
        "activation_id": (ctx.meta or {}).get("activation_id"),
        "database": database,
        "fhir_release": str(cfg.get("schema_version") or "R5").upper(),
        "status": "ready",
        "phase": "awaiting_chunk",
        "progress": 0,
        "cancel_requested": False,
        "source": {
            "name": str(payload.get("source_name") or "FHIR migration")[:240],
            "format": source_format,
            "sha256": str(payload.get("source_sha256") or "").strip() or None,
            "retained": False,
        },
        "options": {
            "chunk_size": chunk_size,
            "validation_level": payload.get("validation_level"),
            "mode": mode,
            "dry_run": bool(payload.get("dry_run", False)),
            "fail_on_error": bool(payload.get("fail_on_error", True)),
        },
        "totals": {
            "resources": total_resources,
            "chunks": total_chunks,
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "written": 0,
            "resource_counts": {},
        },
        "checkpoint": {"next_chunk": 0, "last_completed_chunk": None},
        "retry_of": retry_of,
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
    }
    await asyncio.to_thread(db[RUNS_COLLECTION].insert_one, run)
    return {"ok": True, "run": _public_run(run)}


async def fhir_migration_list(
    ctx: StrategyContext, payload: dict[str, Any]
) -> dict[str, Any]:
    db, _database_name, _prefix = _database(ctx)
    try:
        limit = max(1, min(MAX_HISTORY_LIMIT, int(payload.get("limit") or 25)))
    except (TypeError, ValueError) as exc:
        raise KehrnelError(
            code="INVALID_INPUT", status=400, message="limit must be an integer"
        ) from exc
    query: dict[str, Any] = {"environment_id": ctx.environment_id}
    status = str(payload.get("status") or "").strip().lower()
    if status:
        query["status"] = status
    items = await asyncio.to_thread(
        lambda: list(db[RUNS_COLLECTION].find(query).sort("created_at", -1).limit(limit))
    )
    return {"ok": True, "items": [_public_run(item) for item in items], "limit": limit}


async def fhir_migration_get(
    ctx: StrategyContext, payload: dict[str, Any]
) -> dict[str, Any]:
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="run_id is required")
    db, _database_name, _prefix = _database(ctx)
    run = await asyncio.to_thread(_require_run, db, ctx, run_id)
    include_chunks = bool(payload.get("include_chunks", True))
    chunks: list[dict[str, Any]] | None = None
    if include_chunks:
        chunks = await asyncio.to_thread(
            lambda: list(db[CHUNKS_COLLECTION].find({"run_id": run_id}).sort("chunk_index", 1))
        )
    return {"ok": True, "run": _public_run(run, chunks=chunks)}


async def fhir_migration_cancel(
    ctx: StrategyContext, payload: dict[str, Any]
) -> dict[str, Any]:
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="run_id is required")
    db, _database_name, _prefix = _database(ctx)
    run = await asyncio.to_thread(_require_run, db, ctx, run_id)
    if run.get("status") in _TERMINAL_STATES:
        return {"ok": True, "run": _public_run(run)}
    next_status = "canceling" if run.get("inflight_chunk") is not None else "canceled"
    now = _now()
    await asyncio.to_thread(
        db[RUNS_COLLECTION].update_one,
        {"run_id": run_id, "environment_id": ctx.environment_id},
        {
            "$set": {
                "cancel_requested": True,
                "status": next_status,
                "phase": next_status,
                "updated_at": now,
                **({"completed_at": now} if next_status == "canceled" else {}),
            }
        },
    )
    return await fhir_migration_get(ctx, {"run_id": run_id, "include_chunks": False})


def _add_counts(current: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    validation = report.get("validation") if isinstance(report.get("validation"), dict) else {}
    write = report.get("write") if isinstance(report.get("write"), dict) else {}
    counts = Counter(current.get("resource_counts") or {})
    counts.update(report.get("resource_counts") or {})
    return {
        **current,
        "processed": int(current.get("processed") or 0) + int(validation.get("received") or 0),
        "valid": int(current.get("valid") or 0) + int(validation.get("valid") or 0),
        "invalid": int(current.get("invalid") or 0) + int(validation.get("invalid") or 0),
        "written": int(current.get("written") or 0) + int(write.get("processed") or 0),
        "resource_counts": dict(sorted(counts.items())),
    }


async def fhir_migration_import_chunk(
    ctx: StrategyContext,
    payload: dict[str, Any],
    *,
    should_cancel: Any = None,
) -> dict[str, Any]:
    """Import one ordered, idempotently replayable chunk and advance a checkpoint."""
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="run_id is required")
    try:
        chunk_index = int(payload.get("chunk_index"))
    except (TypeError, ValueError) as exc:
        raise KehrnelError(
            code="INVALID_INPUT", status=400, message="chunk_index must be an integer"
        ) from exc
    if chunk_index < 0:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="chunk_index cannot be negative")

    db, _database_name, _prefix = _database(ctx)
    run = await asyncio.to_thread(_require_run, db, ctx, run_id)
    digest = _payload_digest(payload)
    existing = await asyncio.to_thread(
        db[CHUNKS_COLLECTION].find_one,
        {"run_id": run_id, "chunk_index": chunk_index},
    )
    if existing and existing.get("status") == "completed":
        if existing.get("sha256") != digest:
            raise KehrnelError(
                code="FHIR_MIGRATION_CHUNK_CONFLICT",
                status=409,
                message="This chunk index was already completed with different content",
            )
        return {
            "ok": True,
            "replayed": True,
            "chunk": _clean(existing),
            "run": _public_run(run),
        }
    if run.get("cancel_requested") or run.get("status") == "canceled":
        raise KehrnelError(
            code="JOB_CANCELED", status=409, message="FHIR migration run is canceled"
        )
    expected = int((run.get("checkpoint") or {}).get("next_chunk") or 0)
    if chunk_index != expected:
        raise KehrnelError(
            code="FHIR_MIGRATION_CHECKPOINT_MISMATCH",
            status=409,
            message=f"Expected chunk {expected}, received {chunk_index}",
            details={"expected_chunk": expected, "received_chunk": chunk_index},
        )
    if run.get("inflight_chunk") is not None:
        raise KehrnelError(
            code="FHIR_MIGRATION_CHUNK_IN_PROGRESS",
            status=409,
            message=f"Chunk {run.get('inflight_chunk')} is already running",
        )

    chunk_resources, _source_format, _parse_findings = parse_import_payload(payload)
    configured_chunk_size = int((run.get("options") or {}).get("chunk_size") or 1)
    if len(chunk_resources) > configured_chunk_size:
        raise KehrnelError(
            code="FHIR_MIGRATION_CHUNK_TOO_LARGE",
            status=413,
            message=(
                f"Chunk contains {len(chunk_resources)} resources; this run is configured "
                f"for at most {configured_chunk_size}"
            ),
        )
    declared_chunks = (run.get("totals") or {}).get("chunks")
    final = bool(payload.get("final", False))
    if isinstance(declared_chunks, int) and declared_chunks > 0:
        expected_final = chunk_index == declared_chunks - 1
        if final != expected_final:
            raise KehrnelError(
                code="FHIR_MIGRATION_FINAL_CHUNK_MISMATCH",
                status=409,
                message=(
                    f"Run declares {declared_chunks} chunks; chunk {chunk_index} "
                    f"must set final={str(expected_final).lower()}"
                ),
            )

    now = _now()
    lease = str(uuid.uuid4())
    claim = await asyncio.to_thread(
        db[RUNS_COLLECTION].update_one,
        {
            "run_id": run_id,
            "environment_id": ctx.environment_id,
            "checkpoint.next_chunk": chunk_index,
            "inflight_chunk": {"$exists": False},
            "cancel_requested": False,
        },
        {
            "$set": {
                "status": "running",
                "phase": "importing",
                "inflight_chunk": chunk_index,
                "inflight_lease": lease,
                "updated_at": now,
            }
        },
    )
    if int(getattr(claim, "modified_count", 0) or 0) != 1:
        raise KehrnelError(
            code="FHIR_MIGRATION_CHUNK_IN_PROGRESS",
            status=409,
            message="The migration checkpoint changed while this chunk was being claimed",
        )

    attempts = int((existing or {}).get("attempt") or 0) + 1
    attempt_history = list((existing or {}).get("attempt_history") or [])
    if existing:
        attempt_history.append(
            {
                "attempt": existing.get("attempt"),
                "sha256": existing.get("sha256"),
                "status": existing.get("status"),
                "error": existing.get("error"),
                "completed_at": existing.get("completed_at"),
            }
        )
        attempt_history = attempt_history[-10:]
    chunk_record = {
        "contract_version": RUN_CONTRACT_VERSION,
        "run_id": run_id,
        "environment_id": ctx.environment_id,
        "chunk_index": chunk_index,
        "sha256": digest,
        "status": "running",
        "attempt": attempts,
        "attempt_history": attempt_history,
        "created_at": (existing or {}).get("created_at") or now,
        "updated_at": now,
        "completed_at": None,
        "report": None,
    }
    await asyncio.to_thread(
        db[CHUNKS_COLLECTION].replace_one,
        {"run_id": run_id, "chunk_index": chunk_index},
        chunk_record,
        upsert=True,
    )

    last_cancel_check = 0.0
    canceled = False

    def cancel_requested() -> bool:
        nonlocal last_cancel_check, canceled
        if canceled or (should_cancel and should_cancel()):
            canceled = True
            return True
        current = time.monotonic()
        if current - last_cancel_check < 0.25:
            return False
        last_cancel_check = current
        current_run = db[RUNS_COLLECTION].find_one(
            {"run_id": run_id}, {"cancel_requested": 1}
        )
        canceled = bool((current_run or {}).get("cancel_requested"))
        return canceled

    options = run.get("options") or {}
    import_payload = {
        key: value
        for key, value in payload.items()
        if key in {"bundle", "resources", "resource", "ndjson", "resourceType", "id"}
    }
    import_payload.update(
        {
            "validation_level": options.get("validation_level"),
            "mode": options.get("mode") or "upsert",
            "dry_run": bool(options.get("dry_run")),
            "fail_on_error": bool(options.get("fail_on_error", True)),
        }
    )
    try:
        report = await fhir_import_resources(
            ctx,
            import_payload,
            should_cancel=cancel_requested,
            provenance={"migration_run_id": run_id, "migration_chunk_index": chunk_index},
        )
        successful = bool(report.get("committed") or report.get("dry_run")) and bool(
            report.get("ok") or not options.get("fail_on_error", True)
        )
        if not successful:
            raise KehrnelError(
                code="FHIR_MIGRATION_CHUNK_REJECTED",
                status=422,
                message=report.get("message") or "FHIR migration chunk was rejected",
                details={"report": _bounded_report(report)},
            )
    except Exception as exc:
        is_canceled = isinstance(exc, KehrnelError) and exc.code == "JOB_CANCELED"
        failed_at = _now()
        failure = {
            "code": getattr(exc, "code", "FHIR_MIGRATION_CHUNK_FAILED"),
            "message": str(exc),
            "details": _clean(getattr(exc, "details", None)),
        }
        await asyncio.to_thread(
            db[CHUNKS_COLLECTION].update_one,
            {"run_id": run_id, "chunk_index": chunk_index},
            {
                "$set": {
                    "status": "canceled" if is_canceled else "failed",
                    "error": failure,
                    "updated_at": failed_at,
                    "completed_at": failed_at,
                }
            },
        )
        await asyncio.to_thread(
            db[RUNS_COLLECTION].update_one,
            {"run_id": run_id, "inflight_lease": lease},
            {
                "$set": {
                    "status": "canceled" if is_canceled else "failed",
                    "phase": "canceled" if is_canceled else "chunk_failed",
                    "last_error": failure,
                    "updated_at": failed_at,
                    **({"completed_at": failed_at} if is_canceled else {}),
                },
                "$unset": {"inflight_chunk": "", "inflight_lease": ""},
            },
        )
        raise

    completed_at = _now()
    bounded = _bounded_report(report)
    await asyncio.to_thread(
        db[CHUNKS_COLLECTION].update_one,
        {"run_id": run_id, "chunk_index": chunk_index},
        {
            "$set": {
                "status": "completed",
                "report": bounded,
                "updated_at": completed_at,
                "completed_at": completed_at,
            }
        },
    )
    current_totals = run.get("totals") if isinstance(run.get("totals"), dict) else {}
    totals = _add_counts(current_totals, report)
    total_chunks = totals.get("chunks")
    next_chunk = chunk_index + 1
    progress = (
        min(100, int(next_chunk * 100 / total_chunks))
        if isinstance(total_chunks, int) and total_chunks > 0
        else min(99, next_chunk)
    )
    final_status = "completed_with_errors" if totals.get("invalid") else "completed"
    declared_resources = totals.get("resources")
    declared_resources_match = (
        declared_resources is None
        or int(declared_resources) == int(totals.get("processed") or 0)
    )
    if final and not declared_resources_match:
        final_status = "completed_with_errors"
    run_update = {
        "status": final_status if final else "ready",
        "phase": "completed" if final else "awaiting_chunk",
        "progress": 100 if final else progress,
        "totals": totals,
        "checkpoint": {
            "next_chunk": next_chunk,
            "last_completed_chunk": chunk_index,
            "last_chunk_sha256": digest,
        },
        "updated_at": completed_at,
        "last_error": None,
        "completion_warning": (
            None
            if declared_resources_match
            else (
                f"Run declared {declared_resources} resources but processed "
                f"{totals.get('processed') or 0}"
            )
        ),
        **({"completed_at": completed_at} if final else {}),
    }
    await asyncio.to_thread(
        db[RUNS_COLLECTION].update_one,
        {"run_id": run_id, "inflight_lease": lease},
        {"$set": run_update, "$unset": {"inflight_chunk": "", "inflight_lease": ""}},
    )
    refreshed = await asyncio.to_thread(_require_run, db, ctx, run_id)
    return {
        "ok": True,
        "replayed": False,
        "chunk": _clean({**chunk_record, "status": "completed", "report": report}),
        "run": _public_run(refreshed),
    }


def _walk_references(value: Any, path: str = "") -> Iterable[tuple[str, str]]:
    if isinstance(value, dict):
        for key, item in value.items():
            child = f"{path}.{key}" if path else key
            if key == "reference" and isinstance(item, str):
                yield child, item
            else:
                yield from _walk_references(item, child)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_references(item, f"{path}[{index}]")


def _relative_reference(value: str) -> tuple[str, str] | None:
    reference = value.strip()
    if not reference or reference.startswith(("#", "urn:")) or "://" in reference:
        return None
    parts = reference.split("/")
    if len(parts) >= 2 and _REFERENCE_TYPE.fullmatch(parts[0]) and parts[1]:
        return parts[0], parts[1]
    return None


async def fhir_reference_integrity(
    ctx: StrategyContext, payload: dict[str, Any]
) -> dict[str, Any]:
    """Report unresolved relative references without changing canonical resources."""
    db, database, prefix = _database(ctx)
    run_id = str(payload.get("run_id") or "").strip() or None
    try:
        max_resources = max(1, min(100_000, int(payload.get("max_resources") or 10_000)))
        max_findings = max(1, min(10_000, int(payload.get("max_findings") or 1_000)))
    except (TypeError, ValueError) as exc:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="max_resources and max_findings must be integers",
        ) from exc

    if run_id:
        run = await asyncio.to_thread(_require_run, db, ctx, run_id)
        resource_types = sorted((run.get("totals") or {}).get("resource_counts") or {})
        source_query = {"_kehrnel.provenance.migration_run_id": run_id}
    else:
        requested = payload.get("resource_types") or []
        if not isinstance(requested, list) or not requested:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Provide run_id or a non-empty resource_types array",
            )
        resource_types = sorted({str(item) for item in requested})
        source_query = {}

    sources: list[tuple[str, dict[str, Any]]] = []
    remaining = max_resources
    for resource_type in resource_types:
        if remaining <= 0 or not _REFERENCE_TYPE.fullmatch(resource_type):
            break
        collection = db[bridge.collection_name(prefix, resource_type)]
        rows = await asyncio.to_thread(
            lambda c=collection, q=source_query, n=remaining: list(c.find(q).limit(n))
        )
        sources.extend((resource_type, row) for row in rows)
        remaining -= len(rows)

    references: list[dict[str, str]] = []
    ignored = Counter()
    targets: dict[str, set[str]] = defaultdict(set)
    for source_type, resource in sources:
        source_id = str(resource.get("id") or "")
        for path, raw in _walk_references(canonical_resource(resource)):
            parsed = _relative_reference(raw)
            if parsed is None:
                ignored["contained" if raw.startswith("#") else "external_or_nonrelative"] += 1
                continue
            target_type, target_id = parsed
            targets[target_type].add(target_id)
            references.append(
                {
                    "source": f"{source_type}/{source_id}",
                    "path": path,
                    "reference": raw,
                    "target_type": target_type,
                    "target_id": target_id,
                }
            )

    existing: dict[str, set[str]] = {}
    for target_type, ids in targets.items():
        if not _REFERENCE_TYPE.fullmatch(target_type):
            existing[target_type] = set()
            continue
        collection = db[bridge.collection_name(prefix, target_type)]
        rows = await asyncio.to_thread(
            lambda c=collection, values=list(ids): list(
                c.find({"id": {"$in": values}}, {"id": 1, "_id": 0})
            )
        )
        existing[target_type] = {str(row.get("id")) for row in rows if row.get("id")}

    missing = [
        {key: item[key] for key in ("source", "path", "reference")}
        for item in references
        if item["target_id"] not in existing.get(item["target_type"], set())
    ]
    report = {
        "ok": not missing,
        "contract_version": RUN_CONTRACT_VERSION,
        "mode": "informational",
        "database": database,
        "run_id": run_id,
        "checked_resources": len(sources),
        "resource_limit_reached": remaining == 0,
        "references": {
            "found": len(references),
            "resolved": len(references) - len(missing),
            "missing": len(missing),
            "ignored": dict(sorted(ignored.items())),
        },
        "findings": missing[:max_findings],
        "findings_truncated": len(missing) > max_findings,
        "checked_at": _now(),
    }
    if run_id:
        await asyncio.to_thread(
            db[RUNS_COLLECTION].update_one,
            {"run_id": run_id, "environment_id": ctx.environment_id},
            {"$set": {"reference_integrity": report, "updated_at": _now()}},
        )
    return report
