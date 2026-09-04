"""Multi-dataset import and deterministic portable package export."""

from __future__ import annotations

import io
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext

from ..artifacts import ArtifactService
from ..common import collections, config, ensure_not_cancelled, replace_documents, report_progress, storage_adapter
from ..export import ExportService
from ..ingest import IngestionService
from ..projections import ProjectionService
from ..validation import ValidationService


def _zip_entry(name: str, content: bytes) -> tuple[zipfile.ZipInfo, bytes]:
    info = zipfile.ZipInfo(name, date_time=(2000, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    return info, content


class PackageService:
    def __init__(
        self,
        artifacts: ArtifactService,
        ingestion: IngestionService,
        exports: ExportService,
        validation: ValidationService,
        projections: ProjectionService,
    ):
        self.artifacts = artifacts
        self.ingestion = ingestion
        self.exports = exports
        self.validation = validation
        self.projections = projections

    async def ingest_package(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        documents = payload.get("datasets")
        if not isinstance(documents, list) or not documents:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="datasets must be a non-empty array")
        common = {
            "packageId": payload.get("packageId"),
            "snapshotId": payload.get("snapshotId"),
            "standardsPackageId": payload.get("standardsPackageId"),
            "profile": payload.get("profile"),
            "standard": payload.get("standard"),
            "publicationState": "staged",
        }
        prepared: List[Dict[str, Any]] = []
        studies = set()
        domains = set()
        await report_progress(ctx, progress=2, phase="preflight")
        for item in documents:
            ensure_not_cancelled(ctx)
            if not isinstance(item, dict):
                raise KehrnelError(code="INVALID_INPUT", status=400, message="Each datasets item must be an object")
            request = {
                **common,
                "datasetJSON": item.get("datasetJSON") if "datasetJSON" in item else item,
                "sourceArtifactId": item.get("sourceArtifactId"),
                "dryRun": True,
            }
            _, _, dataset, _ = self.ingestion.canonicalize(ctx, request)
            if dataset.domain in domains:
                raise KehrnelError(code="CDISC_DUPLICATE_DOMAIN", status=409, message=f"Package contains duplicate domain {dataset.domain}.")
            studies.add(dataset.study_id)
            domains.add(dataset.domain)
            prepared.append(request)
        if len(studies) != 1:
            raise KehrnelError(code="CDISC_PACKAGE_STUDY_MISMATCH", status=409, message="All package datasets must belong to one study.")
        ingested = []
        for index, request in enumerate(prepared, start=1):
            ensure_not_cancelled(ctx)
            request.pop("dryRun", None)
            ingested.append(await self.ingestion.ingest(ctx, request))
            await report_progress(
                ctx,
                progress=10 + round(60 * index / len(prepared)),
                phase="ingesting",
                stats={"datasetsCompleted": index, "datasetsTotal": len(prepared)},
            )
        study_id = next(iter(studies))
        snapshot_id = str(payload.get("snapshotId"))
        validation = None
        if bool(payload.get("validate", True)):
            validation = await self.validation.validate_snapshot(
                ctx, {"studyId": study_id, "snapshotId": snapshot_id, "options": payload.get("validationOptions") or {}}
            )
            await report_progress(ctx, progress=85, phase="validated", stats={"validationPassed": validation["ok"]})
        publication = None
        if bool(payload.get("publish", False)):
            if validation is not None and not validation["ok"]:
                publication = {"state": "blocked", "reason": "validation_failed"}
            else:
                publication = await self.ingestion.publish(ctx, {"studyId": study_id, "snapshotId": snapshot_id})
                publication["projections"] = await self.projections.rebuild(
                    ctx, {"studyId": study_id, "snapshotId": snapshot_id}
                )
        await report_progress(ctx, progress=100, phase="completed")
        return {
            "ok": validation is None or validation["ok"],
            "studyId": study_id,
            "snapshotId": snapshot_id,
            "domains": sorted(domains),
            "ingested": ingested,
            "validation": validation,
            "publication": publication,
        }

    async def export_package(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
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
        if snapshot.get("state") != "published" and not bool(payload.get("allowUnpublished", False)):
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_PUBLISHED", status=409, message="Only published snapshots can be packaged.")
        exports = []
        manifest_datasets = []
        for dataset_id in sorted(snapshot.get("datasetIds") or []):
            transformed = await self.exports.transform(ctx, {"datasetId": dataset_id})
            document = transformed.base
            encoded = json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
            filename = f"datasets/{str(document.get('name') or 'dataset').lower()}.dataset.json"
            exports.append(_zip_entry(filename, encoded))
            manifest_datasets.append(
                {
                    "datasetId": dataset_id,
                    "domain": document.get("name"),
                    "file": filename,
                    "records": document.get("records"),
                    "equivalence": transformed.meta.get("equivalence"),
                }
            )
        manifest = {
            "format": "kehrnel-cdisc-package/v1",
            "studyId": study_id,
            "snapshotId": snapshot_id,
            "snapshotRef": snapshot_ref,
            "profile": snapshot.get("profile"),
            "standardsPackageId": snapshot.get("standardsPackageId"),
            "sourceArtifactIds": sorted(snapshot.get("artifactIds") or []),
            "datasets": manifest_datasets,
        }
        exports.append(
            _zip_entry(
                "manifest.json",
                json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True).encode() + b"\n",
            )
        )
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            for info, content in sorted(exports, key=lambda item: item[0].filename):
                archive.writestr(info, content)
        stored = await self.artifacts.store(
            ctx,
            content=buffer.getvalue(),
            media_type="application/zip",
            source_name=f"{study_id}-{snapshot_id}-dataset-json.zip",
            kind="generated-cdisc-package",
            metadata={"snapshotRef": snapshot_ref, "format": manifest["format"]},
        )
        await replace_documents(storage, coll["snapshots"], [{
            **snapshot,
            "artifactIds": sorted(
                set(snapshot.get("artifactIds") or []) | {stored["artifact"]["artifactId"]}
            ),
        }])
        now = datetime.now(timezone.utc).isoformat()
        execution_id = hashlib.sha256(
            f"cdisc_export_package:{snapshot_ref}:{stored['artifact']['artifactId']}".encode()
        ).hexdigest()
        await replace_documents(storage, coll["transformations"], [{
            "_id": f"{cfg['tenant_id']}:package-export:{execution_id}",
            "executionId": execution_id, "tenantId": str(cfg["tenant_id"]),
            "operation": "cdisc_export_package", "status": "succeeded",
            "inputRefs": list(snapshot.get("datasetIds") or []),
            "outputRefs": [stored["artifact"]["artifactId"]],
            "startedAt": now, "completedAt": now,
        }])
        return {
            "ok": True,
            "snapshotRef": snapshot_ref,
            "datasetCount": len(manifest_datasets),
            "manifest": manifest,
            "artifact": stored["artifact"],
            "artifactCreated": stored["created"],
            "executionId": execution_id,
        }
