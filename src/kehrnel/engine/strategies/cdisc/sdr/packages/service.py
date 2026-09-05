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
from ..common import (
    MODEL_SCHEMA_VERSION,
    collections,
    config,
    ensure_not_cancelled,
    replace_documents,
    report_progress,
    storage_adapter,
)
from ..export import ExportService
from ..ingest import IngestionService
from ..projections import ProjectionService
from ..validation import ValidationService


def _zip_entry(name: str, content: bytes) -> tuple[zipfile.ZipInfo, bytes]:
    info = zipfile.ZipInfo(name, date_time=(2000, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    return info, content


SOLUTION_EVIDENCE_API_VERSION = "kehrnel.dev/cdisc-solution-evidence/v1"


def _portable(document: Dict[str, Any]) -> Dict[str, Any]:
    """Remove deployment scope while retaining source identities for lineage."""

    result = dict(document)
    result.pop("tenantId", None)
    if result.get("_id") is not None:
        result["sourceId"] = result.pop("_id")
    if result.get("snapshotRef") is not None:
        result["sourceSnapshotRef"] = result.pop("snapshotRef")
    if result.get("datasetId") is not None:
        result["sourceDatasetId"] = result.pop("datasetId")
    if result.get("datasetIds") is not None:
        result["sourceDatasetIds"] = result.pop("datasetIds")
    if result.get("artifactIds") is not None:
        result["sourceArtifactIds"] = result.pop("artifactIds")
    return result


def _canonical_json_value(value: Any) -> Any:
    """Normalize JSON numbers so Python and JavaScript hash the same payload."""

    if isinstance(value, dict):
        return {key: _canonical_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical_json_value(item) for item in value]
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


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
            enforce_inline_limit=False,
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

    async def export_solution_evidence(
        self, ctx: StrategyContext, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Export a deployment-neutral evidence handoff for a business solution."""

        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if not study_id or not snapshot_id:
            raise KehrnelError(
                code="INVALID_INPUT", status=400, message="studyId and snapshotId are required"
            )
        cfg, storage = config(ctx), storage_adapter(ctx)
        coll = collections(cfg)
        tenant_id = str(cfg["tenant_id"])
        snapshot_ref = f"{tenant_id}:{study_id}:{snapshot_id}"
        snapshot = await storage.find_one(
            coll["snapshots"], {"_id": snapshot_ref, "tenantId": tenant_id}
        )
        if not snapshot:
            raise KehrnelError(
                code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Snapshot was not found."
            )
        if snapshot.get("state") != "published" and not bool(
            payload.get("allowUnpublished", False)
        ):
            raise KehrnelError(
                code="CDISC_SNAPSHOT_NOT_PUBLISHED",
                status=409,
                message="Only published snapshots can be exported to a solution.",
            )

        scope = {"tenantId": tenant_id, "snapshotRef": snapshot_ref}
        datasets = await storage.aggregate(
            coll["datasets"], [{"$match": scope}, {"$sort": {"domain": 1, "_id": 1}}]
        )
        records = await storage.aggregate(
            coll["records"],
            [{"$match": scope}, {"$sort": {"domain": 1, "rowOrdinal": 1, "_id": 1}}],
        )
        entities = await storage.aggregate(
            coll["entities"],
            [{"$match": scope}, {"$sort": {"entityType": 1, "entityId": 1, "_id": 1}}],
        )
        materializations = await storage.aggregate(
            coll["materializations"],
            [{"$match": scope}, {"$sort": {"_id": 1}}],
        )
        validation_runs = await storage.aggregate(
            coll["validation_runs"],
            [{"$match": scope}, {"$sort": {"completedAt": -1, "_id": 1}}],
        )
        run_ids = [item.get("runId") for item in validation_runs if item.get("runId")]
        validation_findings = (
            await storage.aggregate(
                coll["validation_findings"],
                [
                    {"$match": {"tenantId": tenant_id, "runId": {"$in": run_ids}}},
                    {"$sort": {"runId": 1, "ruleId": 1, "_id": 1}},
                ],
            )
            if run_ids
            else []
        )
        artifact_ids = list(snapshot.get("artifactIds") or [])
        artifacts = (
            await storage.aggregate(
                coll["artifacts"],
                [
                    {"$match": {"tenantId": tenant_id, "_id": {"$in": artifact_ids}}},
                    {"$sort": {"sourceName": 1, "_id": 1}},
                ],
            )
            if artifact_ids
            else []
        )
        all_transformations = await storage.aggregate(
            coll["transformations"], [{"$match": {"tenantId": tenant_id}}]
        )
        related = set(snapshot.get("datasetIds") or []) | set(artifact_ids) | {snapshot_ref}
        transformations = [
            item
            for item in all_transformations
            if item.get("operation") != "cdisc_export_solution_evidence"
            and (
                related.intersection(item.get("inputRefs") or [])
                or related.intersection(item.get("outputRefs") or [])
            )
        ]

        evidence = _canonical_json_value({
            "snapshot": _portable(snapshot),
            "datasets": [_portable(item) for item in datasets],
            "records": [_portable(item) for item in records],
            "entities": [_portable(item) for item in entities],
            "materializations": [_portable(item) for item in materializations],
            "sourceArtifacts": [_portable(item) for item in artifacts],
            "validationRuns": [_portable(item) for item in validation_runs],
            "validationFindings": [_portable(item) for item in validation_findings],
            "transformations": [_portable(item) for item in transformations],
        })
        canonical = json.dumps(
            evidence,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        ).encode()
        digest = hashlib.sha256(canonical).hexdigest()
        counts = {
            "datasets": len(datasets),
            "records": len(records),
            "entities": len(entities),
            "materializations": len(materializations),
            "sourceArtifacts": len(artifacts),
            "validationRuns": len(validation_runs),
            "validationFindings": len(validation_findings),
            "transformations": len(transformations),
        }
        package = {
            "apiVersion": SOLUTION_EVIDENCE_API_VERSION,
            "kind": "CDISCSolutionEvidencePackage",
            "modelSchemaVersion": MODEL_SCHEMA_VERSION,
            "manifest": {
                "packageId": f"sha256:{digest}",
                "strategyId": "cdisc.sdr",
                "studyId": study_id,
                "snapshotId": snapshot_id,
                "profile": snapshot.get("profile"),
                "publicationState": snapshot.get("state"),
                "standardsPackageId": snapshot.get("standardsPackageId"),
                "contentDigest": {"algorithm": "sha256", "value": digest},
                "counts": counts,
            },
            "evidence": evidence,
        }
        encoded = json.dumps(
            package, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str
        ).encode()
        stored = await self.artifacts.store(
            ctx,
            content=encoded,
            media_type="application/vnd.kehrnel.cdisc-solution-evidence+json",
            source_name=f"{study_id}-{snapshot_id}-solution-evidence.json",
            kind="generated-cdisc-solution-evidence",
            metadata={
                "snapshotRef": snapshot_ref,
                "apiVersion": SOLUTION_EVIDENCE_API_VERSION,
                "contentDigest": digest,
            },
            enforce_inline_limit=False,
        )
        execution_id = hashlib.sha256(
            f"cdisc_export_solution_evidence:{snapshot_ref}:{digest}".encode()
        ).hexdigest()
        now = datetime.now(timezone.utc).isoformat()
        await replace_documents(
            storage,
            coll["transformations"],
            [
                {
                    "_id": f"{tenant_id}:solution-export:{execution_id}",
                    "executionId": execution_id,
                    "tenantId": tenant_id,
                    "operation": "cdisc_export_solution_evidence",
                    "status": "succeeded",
                    "inputRefs": list(snapshot.get("datasetIds") or []),
                    "outputRefs": [stored["artifact"]["artifactId"]],
                    "contentDigest": digest,
                    "startedAt": now,
                    "completedAt": now,
                }
            ],
        )
        return {
            "ok": True,
            "apiVersion": SOLUTION_EVIDENCE_API_VERSION,
            "packageId": package["manifest"]["packageId"],
            "snapshotRef": snapshot_ref,
            "counts": counts,
            "artifact": stored["artifact"],
            "artifactCreated": stored["created"],
            "executionId": execution_id,
        }
