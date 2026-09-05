"""Snapshot lifecycle and traceability operations."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext

from ..common import collections, config, replace_documents, storage_adapter


def _identity(value: Dict[str, Any]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(raw).hexdigest()


class LineageService:
    async def record(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        operation = str(payload.get("operation") or "").strip()
        inputs = [str(value) for value in payload.get("inputRefs") or [] if str(value).strip()]
        outputs = [str(value) for value in payload.get("outputRefs") or [] if str(value).strip()]
        if not operation or not inputs or not outputs:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="operation, inputRefs, and outputRefs are required",
            )
        cfg, storage = config(ctx), storage_adapter(ctx)
        identity = _identity(
            {
                "tenantId": cfg["tenant_id"],
                "operation": operation,
                "inputRefs": inputs,
                "outputRefs": outputs,
                "mappingId": payload.get("mappingId"),
                "mappingVersion": payload.get("mappingVersion"),
            }
        )
        now = datetime.now(timezone.utc).isoformat()
        document = {
            "_id": f"{cfg['tenant_id']}:transformation:{identity}",
            "executionId": identity,
            "tenantId": str(cfg["tenant_id"]),
            "operation": operation,
            "status": "succeeded",
            "inputRefs": inputs,
            "outputRefs": outputs,
            "mappingId": payload.get("mappingId"),
            "mappingVersion": payload.get("mappingVersion"),
            "codeVersion": payload.get("codeVersion"),
            "parameters": payload.get("parameters") or {},
            "startedAt": payload.get("startedAt") or now,
            "completedAt": payload.get("completedAt") or now,
        }
        collection = collections(cfg)["transformations"]
        existing = await storage.find_one(collection, {"_id": document["_id"]})
        if existing:
            stable_fields = (
                "tenantId", "operation", "inputRefs", "outputRefs", "mappingId",
                "mappingVersion", "codeVersion", "parameters",
            )
            if any(existing.get(key) != document.get(key) for key in stable_fields):
                raise KehrnelError(code="CDISC_LINEAGE_CONFLICT", status=409, message="Lineage identity conflict.")
        if not existing:
            await replace_documents(storage, collection, [document])
        return {"ok": True, "transformation": existing or document, "created": not bool(existing)}

    async def inspect(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if not study_id or not snapshot_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="studyId and snapshotId are required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        coll = collections(cfg)
        snapshot_ref = f"{cfg['tenant_id']}:{study_id}:{snapshot_id}"
        snapshot = await storage.find_one(
            coll["snapshots"], {"_id": snapshot_ref, "tenantId": str(cfg["tenant_id"])}
        )
        if not snapshot:
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Snapshot was not found.")
        datasets = []
        for dataset_id in snapshot.get("datasetIds") or []:
            dataset = await storage.find_one(
                coll["datasets"], {"_id": dataset_id, "tenantId": str(cfg["tenant_id"])}
            )
            if dataset:
                datasets.append(dataset)
        artifacts = []
        for artifact_id in snapshot.get("artifactIds") or []:
            artifact = await storage.find_one(
                coll["artifacts"], {"_id": artifact_id, "tenantId": str(cfg["tenant_id"])}
            )
            if artifact:
                artifacts.append(artifact)
        runs = await storage.aggregate(
            coll["validation_runs"],
            [{"$match": {"tenantId": str(cfg["tenant_id"]), "snapshotRef": snapshot_ref}}],
        )
        transformations = await storage.aggregate(
            coll["transformations"], [{"$match": {"tenantId": str(cfg["tenant_id"])}}]
        )
        related = set(snapshot.get("datasetIds") or []) | set(snapshot.get("artifactIds") or []) | {snapshot_ref}
        transformations = [
            item
            for item in transformations
            if related.intersection(item.get("inputRefs") or [])
            or related.intersection(item.get("outputRefs") or [])
        ]
        return {
            "ok": True,
            "snapshot": snapshot,
            "datasets": datasets,
            "artifacts": artifacts,
            "validationRuns": runs,
            "transformations": transformations,
        }

    async def supersede(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        replacement_id = str(payload.get("replacementSnapshotId") or "").strip()
        reason = str(payload.get("reason") or "").strip()
        if not study_id or not snapshot_id or not replacement_id or not reason:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="studyId, snapshotId, replacementSnapshotId, and reason are required",
            )
        if snapshot_id == replacement_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="A snapshot cannot supersede itself.")
        cfg, storage = config(ctx), storage_adapter(ctx)
        collection = collections(cfg)["snapshots"]
        old_ref = f"{cfg['tenant_id']}:{study_id}:{snapshot_id}"
        new_ref = f"{cfg['tenant_id']}:{study_id}:{replacement_id}"
        old = await storage.find_one(collection, {"_id": old_ref, "tenantId": str(cfg["tenant_id"])})
        new = await storage.find_one(collection, {"_id": new_ref, "tenantId": str(cfg["tenant_id"])})
        if not old or not new:
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Both snapshots must exist.")
        if old.get("state") == "superseded" and old.get("supersededBy") == new_ref:
            return {"ok": True, "snapshotRef": old_ref, "supersededBy": new_ref, "alreadySuperseded": True}
        if old.get("state") != "published" or new.get("state") != "published":
            raise KehrnelError(
                code="CDISC_SNAPSHOT_NOT_SUPERSEDEABLE",
                status=409,
                message="Both snapshots must be published before supersession.",
            )
        timestamp = datetime.now(timezone.utc).isoformat()
        await replace_documents(
            storage,
            collection,
            [
                {**old, "state": "superseded", "supersededBy": new_ref, "supersededAt": timestamp, "supersessionReason": reason},
                {**new, "supersedes": sorted(set(new.get("supersedes") or []) | {old_ref})},
            ],
        )
        return {"ok": True, "snapshotRef": old_ref, "supersededBy": new_ref, "alreadySuperseded": False}
