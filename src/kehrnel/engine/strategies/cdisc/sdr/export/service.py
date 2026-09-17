"""Dataset export, semantic equivalence, and execution evidence."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext, TransformResult
from kehrnel.engine.domains.cdisc.exchange import compare_export_to_canonical, encode_dataset_json, export_dataset_json
from kehrnel.engine.domains.cdisc.xpt import dataset_json_to_xpt, xpt_to_dataset_json

from ..artifacts import ArtifactService
from ..common import collections, config, model_doc, replace_documents, storage_adapter


class ExportService:
    def __init__(self, artifacts: ArtifactService):
        self.artifacts = artifacts

    async def _link_artifact_to_dataset_snapshot(
        self, ctx: StrategyContext, dataset_id: str, artifact_id: str | None
    ) -> None:
        if not artifact_id:
            return
        cfg, storage = config(ctx), storage_adapter(ctx)
        coll = collections(cfg)
        dataset = await storage.find_one(
            coll["datasets"], {"_id": dataset_id, "tenantId": str(cfg["tenant_id"])}
        )
        snapshot_ref = (dataset or {}).get("snapshotRef")
        if not snapshot_ref:
            return
        snapshot = await storage.find_one(
            coll["snapshots"], {"_id": snapshot_ref, "tenantId": str(cfg["tenant_id"])}
        )
        if snapshot:
            await replace_documents(storage, coll["snapshots"], [{
                **snapshot,
                "artifactIds": sorted(set(snapshot.get("artifactIds") or []) | {artifact_id}),
            }])

    async def load_dataset(self, ctx: StrategyContext, dataset_id: str) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
        cfg, storage = config(ctx), storage_adapter(ctx)
        coll = collections(cfg)
        dataset = await storage.find_one(coll["datasets"], {"_id": dataset_id, "tenantId": str(cfg["tenant_id"])})
        if not dataset or dataset.get("tenantId") != str(cfg["tenant_id"]):
            raise KehrnelError(code="CDISC_DATASET_NOT_FOUND", status=404, message=f"Dataset {dataset_id} was not found.")
        records = await storage.aggregate(coll["records"], [
            {"$match": {"tenantId": str(cfg["tenant_id"]), "datasetId": dataset_id}},
            {"$sort": {"rowOrdinal": 1}},
        ])
        if len(records) != int(dataset.get("recordCount", -1)):
            raise KehrnelError(code="CDISC_DATASET_INCOMPLETE", status=409, message="Stored record count does not match dataset metadata.")
        return dataset, records

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        dataset_id = str(payload.get("datasetId") or payload.get("dataset_id") or "").strip()
        if not dataset_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="datasetId is required")
        dataset, records = await self.load_dataset(ctx, dataset_id)
        try:
            document = export_dataset_json(dataset, records)
            report = compare_export_to_canonical(dataset, document)
        except Exception as exc:
            raise KehrnelError(code="CDISC_EXPORT_FAILED", status=422, message=str(exc)) from exc
        if not report.equivalent:
            raise KehrnelError(code="CDISC_EXPORT_NOT_EQUIVALENT", status=500, message="Generated export is not semantically equivalent.", details=model_doc(report))
        return TransformResult(base=document, meta={"format": "Dataset-JSON", "version": document["datasetJSONVersion"], "equivalence": model_doc(report)})

    async def export(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        transformed = await self.transform(ctx, payload)
        dataset_id = str(payload.get("datasetId") or "").strip()
        artifact_result = None
        if bool(payload.get("persistArtifact", True)):
            artifact_result = await self.artifacts.store(
                ctx, content=encode_dataset_json(transformed.base),
                media_type="application/vnd.cdisc.dataset-json+json",
                source_name=f"{transformed.base.get('name', 'dataset')}.dataset.json",
                kind="generated-dataset-json",
                metadata={"datasetId": dataset_id, "equivalenceGuarantee": "semantic"},
                enforce_inline_limit=False,
            )
        execution_id = str(uuid4())
        now = datetime.now(timezone.utc).isoformat()
        cfg, storage = config(ctx), storage_adapter(ctx)
        artifact_id = (artifact_result or {}).get("artifact", {}).get("artifactId")
        await self._link_artifact_to_dataset_snapshot(ctx, dataset_id, artifact_id)
        await replace_documents(storage, collections(cfg)["transformations"], [{
            "_id": execution_id, "executionId": execution_id, "tenantId": str(cfg["tenant_id"]),
            "operation": "cdisc_export_dataset_json", "status": "succeeded", "inputRefs": [dataset_id],
            "outputRefs": [artifact_id] if artifact_id else [], "equivalence": transformed.meta["equivalence"],
            "startedAt": now, "completedAt": now,
        }])
        return {
            "ok": True, "datasetJSON": transformed.base, "equivalence": transformed.meta["equivalence"],
            "artifact": (artifact_result or {}).get("artifact"), "artifactCreated": (artifact_result or {}).get("created", False),
            "executionId": execution_id,
        }

    async def export_xpt(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        dataset_id = str(payload.get("datasetId") or "").strip()
        version = int(payload.get("version") or 5)
        if not dataset_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="datasetId is required")
        transformed = await self.transform(ctx, {"datasetId": dataset_id})
        try:
            content = dataset_json_to_xpt(transformed.base, version=version)
            round_trip = xpt_to_dataset_json(content, study_oid=transformed.base.get("studyOID"))
        except Exception as exc:
            raise KehrnelError(code="CDISC_XPT_EXPORT_FAILED", status=422, message=str(exc)) from exc
        expected_names = [column["name"] for column in transformed.base["columns"]]
        actual_names = [column["name"] for column in round_trip["columns"]]
        equivalent = (
            expected_names == actual_names
            and len(transformed.base["rows"]) == len(round_trip["rows"])
            and all(
                all(left == right or (left is None and right == "") for left, right in zip(expected, actual))
                for expected, actual in zip(transformed.base["rows"], round_trip["rows"])
            )
        )
        if not equivalent:
            raise KehrnelError(
                code="CDISC_EXPORT_NOT_EQUIVALENT",
                status=500,
                message="Generated XPT did not preserve columns and values on verification readback.",
            )
        stored = await self.artifacts.store(
            ctx,
            content=content,
            media_type="application/x-sas-xport",
            source_name=f"{str(transformed.base.get('name') or 'dataset').lower()}.xpt",
            kind="generated-xpt",
            metadata={"datasetId": dataset_id, "xptVersion": version, "equivalenceGuarantee": "verified-readback"},
            enforce_inline_limit=False,
        )
        await self._link_artifact_to_dataset_snapshot(
            ctx, dataset_id, stored["artifact"]["artifactId"]
        )
        execution_id = str(uuid4())
        now = datetime.now(timezone.utc).isoformat()
        cfg, storage = config(ctx), storage_adapter(ctx)
        await replace_documents(storage, collections(cfg)["transformations"], [{
            "_id": execution_id, "executionId": execution_id, "tenantId": str(cfg["tenant_id"]),
            "operation": "cdisc_export_xpt", "status": "succeeded", "inputRefs": [dataset_id],
            "outputRefs": [stored["artifact"]["artifactId"]],
            "equivalence": {"equivalent": True, "method": "XPT readback columns-and-values"},
            "startedAt": now, "completedAt": now,
        }])
        return {
            "ok": True,
            "datasetId": dataset_id,
            "xptVersion": version,
            "recordCount": len(round_trip["rows"]),
            "equivalence": {"equivalent": True, "method": "XPT readback columns-and-values"},
            "artifact": stored["artifact"],
            "artifactCreated": stored["created"],
            "executionId": execution_id,
        }
