"""Read-only, tenant-scoped repository discovery for the CDISC Workbench."""

from __future__ import annotations

import base64
import json
from typing import Any, Dict, List

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext

from ..common import collections, config, storage_adapter


DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def _page_size(payload: Dict[str, Any]) -> int:
    return min(max(int(payload.get("pageSize") or DEFAULT_PAGE_SIZE), 1), MAX_PAGE_SIZE)


def _decode_cursor(cursor: Any, resource: str) -> int:
    if cursor in (None, ""):
        return 0
    try:
        value = str(cursor)
        padding = "=" * (-len(value) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(value + padding).decode("utf-8"))
        if (
            decoded.get("version") != 1
            or decoded.get("resource") != resource
            or not isinstance(decoded.get("offset"), int)
            or decoded["offset"] < 0
        ):
            raise ValueError
        return decoded["offset"]
    except Exception as exc:
        raise KehrnelError(
            code="INVALID_CURSOR",
            status=400,
            message=f"cursor is not a valid {resource} cursor",
        ) from exc


def _encode_cursor(resource: str, offset: int) -> str:
    raw = json.dumps(
        {"version": 1, "resource": resource, "offset": offset},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


class RepositoryService:
    async def _page(
        self,
        ctx: StrategyContext,
        payload: Dict[str, Any],
        *,
        resource: str,
        collection: str,
        match: Dict[str, Any],
        sort: Dict[str, int],
    ) -> Dict[str, Any]:
        storage = storage_adapter(ctx)
        size = _page_size(payload)
        offset = _decode_cursor(payload.get("cursor"), resource)
        pipeline: List[Dict[str, Any]] = [{"$match": match}, {"$sort": sort}]
        if offset:
            pipeline.append({"$skip": offset})
        pipeline.append({"$limit": size + 1})
        rows = await storage.aggregate(collection, pipeline)
        has_more = len(rows) > size
        items = rows[:size]
        return {
            "ok": True,
            "items": items,
            "page": {
                "size": len(items),
                "hasMore": has_more,
                "nextCursor": _encode_cursor(resource, offset + size) if has_more else None,
            },
        }

    @staticmethod
    def _tenant(ctx: StrategyContext) -> tuple[Dict[str, Any], str]:
        cfg = config(ctx)
        return cfg, str(cfg["tenant_id"])

    @staticmethod
    def _snapshot_ref(tenant_id: str, payload: Dict[str, Any]) -> str:
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if not study_id or not snapshot_id:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="studyId and snapshotId are required",
            )
        return f"{tenant_id}:{study_id}:{snapshot_id}"

    async def list_studies(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg, tenant_id = self._tenant(ctx)
        coll = collections(cfg)
        page = await self._page(
            ctx,
            payload,
            resource="studies",
            collection=coll["studies"],
            match={"tenantId": tenant_id},
            sort={"updatedAt": -1, "_id": 1},
        )
        study_ids = [str(item.get("studyId")) for item in page["items"] if item.get("studyId")]
        latest_by_study: Dict[str, Dict[str, Any]] = {}
        if study_ids:
            snapshots = await storage_adapter(ctx).aggregate(
                coll["snapshots"],
                [
                    {"$match": {"tenantId": tenant_id, "studyId": {"$in": study_ids}}},
                    {"$sort": {"createdAt": -1, "_id": 1}},
                ],
            )
            for snapshot in snapshots:
                latest_by_study.setdefault(str(snapshot.get("studyId")), snapshot)
        page["items"] = [
            {**study, "latestSnapshot": latest_by_study.get(str(study.get("studyId")))}
            for study in page["items"]
        ]
        return page

    async def list_snapshots(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg, tenant_id = self._tenant(ctx)
        study_id = str(payload.get("studyId") or "").strip()
        if not study_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="studyId is required")
        match: Dict[str, Any] = {"tenantId": tenant_id, "studyId": study_id}
        if payload.get("state"):
            match["state"] = str(payload["state"])
        return await self._page(
            ctx,
            payload,
            resource="snapshots",
            collection=collections(cfg)["snapshots"],
            match=match,
            sort={"createdAt": -1, "_id": 1},
        )

    async def snapshot_summary(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg, tenant_id = self._tenant(ctx)
        coll = collections(cfg)
        snapshot_ref = self._snapshot_ref(tenant_id, payload)
        snapshot = await storage_adapter(ctx).find_one(
            coll["snapshots"], {"_id": snapshot_ref, "tenantId": tenant_id}
        )
        if not snapshot:
            raise KehrnelError(
                code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Snapshot was not found."
            )
        storage = storage_adapter(ctx)
        datasets = await storage.aggregate(
            coll["datasets"],
            [{"$match": {"tenantId": tenant_id, "snapshotRef": snapshot_ref}}, {"$sort": {"domain": 1, "_id": 1}}],
        )
        runs = await storage.aggregate(
            coll["validation_runs"],
            [{"$match": {"tenantId": tenant_id, "snapshotRef": snapshot_ref}}, {"$sort": {"completedAt": -1, "_id": 1}}],
        )
        artifacts = await storage.aggregate(
            coll["artifacts"],
            [{"$match": {"tenantId": tenant_id, "_id": {"$in": list(snapshot.get("artifactIds") or [])}}}],
        ) if snapshot.get("artifactIds") else []
        return {
            "ok": True,
            "snapshot": snapshot,
            "summary": {
                "datasetCount": len(datasets),
                "recordCount": sum(int(item.get("recordCount") or 0) for item in datasets),
                "artifactCount": len(artifacts),
                "validationRunCount": len(runs),
                "latestValidationRun": runs[0] if runs else None,
                "domains": sorted({str(item.get("domain")) for item in datasets if item.get("domain")}),
            },
            "datasets": datasets,
            "artifacts": artifacts,
        }

    async def list_datasets(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg, tenant_id = self._tenant(ctx)
        match: Dict[str, Any] = {"tenantId": tenant_id}
        if payload.get("studyId"):
            match["studyId"] = str(payload["studyId"])
        if payload.get("snapshotId"):
            match["snapshotId"] = str(payload["snapshotId"])
        if payload.get("profile"):
            match["profile"] = str(payload["profile"]).lower()
        if payload.get("domain"):
            match["domain"] = str(payload["domain"]).upper()
        return await self._page(
            ctx,
            payload,
            resource="datasets",
            collection=collections(cfg)["datasets"],
            match=match,
            sort={"studyId": 1, "snapshotId": 1, "domain": 1, "_id": 1},
        )

    async def list_validation_runs(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg, tenant_id = self._tenant(ctx)
        match: Dict[str, Any] = {"tenantId": tenant_id}
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if study_id or snapshot_id:
            match["snapshotRef"] = self._snapshot_ref(tenant_id, payload)
        if payload.get("status"):
            match["status"] = str(payload["status"])
        return await self._page(
            ctx,
            payload,
            resource="validation-runs",
            collection=collections(cfg)["validation_runs"],
            match=match,
            sort={"completedAt": -1, "_id": 1},
        )

    async def list_standards(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg, tenant_id = self._tenant(ctx)
        match: Dict[str, Any] = {"tenantId": tenant_id}
        if payload.get("profile"):
            match["profile"] = str(payload["profile"]).lower()
        return await self._page(
            ctx,
            payload,
            resource="standards",
            collection=collections(cfg)["standards"],
            match=match,
            sort={"createdAt": -1, "_id": 1},
        )

    async def list_artifacts(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg, tenant_id = self._tenant(ctx)
        coll = collections(cfg)
        match: Dict[str, Any] = {"tenantId": tenant_id}
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if study_id or snapshot_id:
            snapshot_ref = self._snapshot_ref(tenant_id, payload)
            snapshot = await storage_adapter(ctx).find_one(
                coll["snapshots"], {"_id": snapshot_ref, "tenantId": tenant_id}
            )
            if not snapshot:
                raise KehrnelError(
                    code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Snapshot was not found."
                )
            match["_id"] = {"$in": list(snapshot.get("artifactIds") or [])}
        if payload.get("kind"):
            match["metadata.kind"] = str(payload["kind"])
        return await self._page(
            ctx,
            payload,
            resource="artifacts",
            collection=coll["artifacts"],
            match=match,
            sort={"acquiredAt": -1, "_id": 1},
        )
