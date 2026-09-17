"""Dataset-JSON/XPT ingestion and atomic snapshot publication."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext, TransformResult
from kehrnel.engine.domains.cdisc.dataset_json import canonicalize_dataset_json, parse_dataset_json
from kehrnel.engine.domains.cdisc.define_xml import parse_define_xml
from kehrnel.engine.domains.cdisc.models import CanonicalSnapshot, StandardsPackage
from kehrnel.engine.domains.cdisc.xpt import xpt_to_dataset_json

from ..artifacts import ArtifactService
from ..common import collections, config, model_doc, replace_documents, stamp_model_schema, storage_adapter


class IngestionService:
    def __init__(self, artifacts: ArtifactService):
        self.artifacts = artifacts

    def canonicalize(self, ctx: StrategyContext, payload: Dict[str, Any]):
        cfg = config(ctx)
        dataset_json = payload.get("datasetJSON") or payload.get("dataset_json")
        package_id = str(payload.get("packageId") or payload.get("package_id") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or payload.get("snapshot_id") or "").strip()
        profile = str(payload.get("profile") or "").strip().lower()
        standard = payload.get("standard")
        if not isinstance(dataset_json, dict):
            raise KehrnelError(code="INVALID_DATASET_JSON", status=400, message="payload.datasetJSON must be an object")
        rows = dataset_json.get("rows")
        maximum_records = int((cfg.get("ingest") or {}).get("max_records_per_dataset", 2_000_000))
        if isinstance(rows, list) and len(rows) > maximum_records:
            raise KehrnelError(
                code="CDISC_DATASET_TOO_LARGE",
                status=413,
                message="Dataset exceeds the configured synchronous record limit.",
                details={"records": len(rows), "maximum": maximum_records},
            )
        if not package_id or not snapshot_id or not profile or not isinstance(standard, dict):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="packageId, snapshotId, profile, and standard are required")
        state = str(payload.get("publicationState") or (cfg.get("ingest") or {}).get("default_publication_state") or "staged").lower()
        if state not in {"staged", "published"}:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="publicationState must be staged or published")
        if state == "published" and bool((cfg.get("validation") or {}).get("require_before_publish", False)):
            raise KehrnelError(
                code="CDISC_VALIDATION_REQUIRED",
                status=409,
                message="This activation requires staged ingestion followed by validation and publication.",
            )
        contract = {
            "package_id": package_id,
            "snapshot_id": snapshot_id,
            "profile": profile,
            "standard": standard,
            "source_artifact_id": payload.get("sourceArtifactId"),
            "related_artifact_ids": [
                str(value) for value in payload.get("relatedArtifactIds") or [] if str(value).strip()
            ],
            "publication_state": state,
        }
        try:
            dataset, records = canonicalize_dataset_json(
                dataset_json,
                tenant_id=str(cfg["tenant_id"]),
                package_id=package_id,
                snapshot_id=snapshot_id,
                profile=profile,
                standard=standard,
                source_artifact_id=contract["source_artifact_id"],
                publication_state=state,
            )
        except Exception as exc:
            raise KehrnelError(code="INVALID_DATASET_JSON", status=400, message=str(exc)) from exc
        return cfg, contract, dataset, records

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        _, _, dataset, records = self.canonicalize(ctx, payload)
        return TransformResult(base=model_doc(dataset), meta={"records": [model_doc(item) for item in records], "recordCount": len(records)})

    async def ingest(
        self,
        ctx: StrategyContext,
        payload: Dict[str, Any],
        *,
        lineage_operation: str = "cdisc_ingest_dataset_json",
    ) -> Dict[str, Any]:
        cfg, contract, dataset, records = self.canonicalize(ctx, payload)
        coll = collections(cfg)
        dataset_doc = model_doc(dataset)
        record_docs = [model_doc(record) for record in records]
        snapshot_ref = dataset.snapshot_ref
        snapshot = CanonicalSnapshot(
            snapshotId=contract["snapshot_id"], tenantId=str(cfg["tenant_id"]), studyId=dataset.study_id,
            packageId=contract["package_id"], profile=dataset.profile,
            standardsPackageId=str(payload.get("standardsPackageId") or contract["package_id"]),
            state="published" if contract["publication_state"] == "published" else "canonicalized",
            datasetIds=[dataset.id], artifactIds=sorted(set(
                ([contract["source_artifact_id"]] if contract["source_artifact_id"] else [])
                + contract["related_artifact_ids"]
            )),
            publishedAt=datetime.now(timezone.utc) if contract["publication_state"] == "published" else None,
        )
        snapshot_doc = model_doc(snapshot)
        snapshot_doc["_id"] = snapshot_ref
        if payload.get("dryRun") or payload.get("dry_run"):
            return {"ok": True, "dryRun": True, "dataset": dataset_doc, "recordCount": len(records), "snapshot": snapshot_doc}
        storage = storage_adapter(ctx)
        existing_dataset = await storage.find_one(coll["datasets"], {"_id": dataset.id})
        if existing_dataset and existing_dataset.get("contentHash") != dataset.content_hash:
            raise KehrnelError(code="CDISC_SNAPSHOT_IMMUTABLE", status=409, message="Dataset identity already has different content.")
        existing_snapshot = await storage.find_one(coll["snapshots"], {"_id": snapshot_ref})
        if existing_snapshot:
            expected = {
                "tenantId": str(cfg["tenant_id"]), "studyId": dataset.study_id,
                "packageId": contract["package_id"], "profile": dataset.profile.value,
                "standardsPackageId": str(payload.get("standardsPackageId") or contract["package_id"]),
            }
            if any(existing_snapshot.get(key) != value for key, value in expected.items()):
                raise KehrnelError(code="CDISC_SNAPSHOT_IDENTITY_CONFLICT", status=409, message="Snapshot metadata conflicts with its existing identity.")
            if existing_snapshot.get("state") == "published" and dataset.id not in existing_snapshot.get("datasetIds", []):
                raise KehrnelError(code="CDISC_SNAPSHOT_IMMUTABLE", status=409, message="Published snapshots cannot accept datasets.")
            snapshot_doc["datasetIds"] = sorted(set(existing_snapshot.get("datasetIds", [])) | {dataset.id})
            snapshot_doc["artifactIds"] = sorted(set(existing_snapshot.get("artifactIds", [])) | set(snapshot_doc.get("artifactIds", [])))
            snapshot_doc["createdAt"] = existing_snapshot.get("createdAt", snapshot_doc["createdAt"])
            if existing_snapshot.get("state") == "published":
                snapshot_doc.update(state="published", publishedAt=existing_snapshot.get("publishedAt"))
        study_id = f"{cfg['tenant_id']}:{dataset.study_id}"
        existing_study = await storage.find_one(coll["studies"], {"_id": study_id})
        study_doc = {
            "_id": study_id, "tenantId": cfg["tenant_id"],
            "studyId": dataset.study_id,
            "profiles": sorted(set((existing_study or {}).get("profiles") or []) | {dataset.profile.value}),
            "createdAt": (existing_study or {}).get("createdAt") or datetime.now(timezone.utc).isoformat(),
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }
        await replace_documents(
            storage,
            coll["records"],
            record_docs,
            batch_size=int((cfg.get("ingest") or {}).get("batch_size", 1000)),
        )
        await replace_documents(storage, coll["datasets"], [dataset_doc])
        await replace_documents(storage, coll["studies"], [study_doc])
        now = datetime.now(timezone.utc).isoformat()
        audit_id = hashlib.sha256(f"{dataset.id}:{dataset.content_hash}".encode()).hexdigest()
        await replace_documents(storage, coll["transformations"], [{
            "_id": f"{cfg['tenant_id']}:ingest:{audit_id}", "executionId": audit_id,
            "tenantId": str(cfg["tenant_id"]), "operation": lineage_operation,
            "status": "succeeded", "inputRefs": snapshot_doc.get("artifactIds") or [],
            "outputRefs": [dataset.id], "contentHash": dataset.content_hash,
            "startedAt": now, "completedAt": now,
        }])
        # Visibility is committed last so a partial write remains staged and invisible.
        await replace_documents(storage, coll["snapshots"], [snapshot_doc])
        return {
            "ok": True, "tenantId": cfg["tenant_id"], "studyId": dataset.study_id, "datasetId": dataset.id,
            "snapshotId": contract["snapshot_id"], "snapshotRef": snapshot_ref,
            "publicationState": contract["publication_state"], "recordCount": len(records),
            "contentHash": dataset.content_hash, "idempotent": True,
        }

    async def publish(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if not study_id or not snapshot_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="studyId and snapshotId are required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        ref = f"{cfg['tenant_id']}:{study_id}:{snapshot_id}"
        snapshot = await storage.find_one(collections(cfg)["snapshots"], {"_id": ref})
        if not snapshot:
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Snapshot was not found.")
        if snapshot.get("state") == "published":
            return {"ok": True, "snapshotRef": ref, "state": "published", "alreadyPublished": True}
        require_validation = bool((cfg.get("validation") or {}).get("require_before_publish", False))
        allowed = {"validated"} if require_validation else {"canonicalized", "validated"}
        if snapshot.get("state") not in allowed or not snapshot.get("datasetIds"):
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_PUBLISHABLE", status=409, message="Snapshot has not passed its publication gate.")
        published = {**snapshot, "state": "published", "publishedAt": datetime.now(timezone.utc).isoformat()}
        audit_id = hashlib.sha256(f"publish:{ref}:{snapshot.get('latestValidationRunId')}".encode()).hexdigest()
        await replace_documents(storage, collections(cfg)["transformations"], [{
            "_id": f"{cfg['tenant_id']}:publish:{audit_id}", "executionId": audit_id,
            "tenantId": str(cfg["tenant_id"]), "operation": "cdisc_publish_snapshot",
            "status": "succeeded", "inputRefs": list(snapshot.get("datasetIds") or []),
            "outputRefs": [ref], "validationRunId": snapshot.get("latestValidationRunId"),
            "startedAt": published["publishedAt"], "completedAt": published["publishedAt"],
        }])
        await replace_documents(storage, collections(cfg)["snapshots"], [published])
        return {"ok": True, "snapshotRef": ref, "state": "published", "alreadyPublished": False, "datasetCount": len(published["datasetIds"])}

    async def inspect_dataset_json(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            document = parse_dataset_json(payload.get("datasetJSON"))
        except Exception as exc:
            raise KehrnelError(code="INVALID_DATASET_JSON", status=400, message=str(exc)) from exc
        return {"ok": True, "format": "Dataset-JSON", "version": document.dataset_json_version, "studyOID": document.study_oid, "name": document.name, "columns": len(document.columns), "records": len(document.rows)}

    async def inspect_define(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        artifact_id = str(payload.get("artifactId") or "").strip()
        if not artifact_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="artifactId is required")
        _, content = await self.artifacts.replay(ctx, artifact_id)
        try:
            parsed = parse_define_xml(content)
        except Exception as exc:
            raise KehrnelError(code="INVALID_DEFINE_XML", status=400, message=str(exc)) from exc
        return {"ok": True, "define": parsed.model_dump(by_alias=True, mode="json", exclude_none=True), "datasetCount": len(parsed.datasets)}

    async def ingest_xpt(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        xpt_id = str(payload.get("xptArtifactId") or "").strip()
        if not xpt_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="xptArtifactId is required")
        _, xpt = await self.artifacts.replay(ctx, xpt_id)
        define_id = str(payload.get("defineArtifactId") or "").strip()
        try:
            define = parse_define_xml((await self.artifacts.replay(ctx, define_id))[1]) if define_id else None
        except KehrnelError:
            raise
        except Exception as exc:
            raise KehrnelError(code="INVALID_DEFINE_XML", status=400, message=str(exc)) from exc
        try:
            document = xpt_to_dataset_json(xpt, define=define, study_oid=payload.get("studyOID"))
        except Exception as exc:
            raise KehrnelError(code="INVALID_XPT", status=400, message=str(exc)) from exc
        result = await self.ingest(
            ctx,
            {
                "datasetJSON": document,
                "packageId": payload.get("packageId"),
                "snapshotId": payload.get("snapshotId"),
                "standardsPackageId": payload.get("standardsPackageId"),
                "profile": payload.get("profile"),
                "standard": payload.get("standard"),
                "publicationState": payload.get("publicationState", "staged"),
                "sourceArtifactId": xpt_id,
                "relatedArtifactIds": [define_id] if define_id else [],
            },
            lineage_operation="cdisc_ingest_xpt",
        )
        return {**result, "sourceFormat": "XPT", "defineArtifactId": define_id or None}

    async def register_standards(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            package = StandardsPackage.model_validate(payload.get("package"))
        except Exception as exc:
            raise KehrnelError(code="INVALID_STANDARDS_PACKAGE", status=400, message=str(exc)) from exc
        cfg, storage = config(ctx), storage_adapter(ctx)
        for asset in package.assets:
            if not asset.artifact_id:
                continue
            artifact = await storage.find_one(
                collections(cfg)["artifacts"],
                {"_id": asset.artifact_id, "tenantId": str(cfg["tenant_id"])},
            )
            if not artifact:
                raise KehrnelError(
                    code="STANDARDS_ASSET_NOT_FOUND",
                    status=404,
                    message=f"Standards asset artifact {asset.artifact_id} was not found.",
                )
            expected = asset.digest.value if asset.digest else None
            if expected and artifact.get("digest", {}).get("value") != expected:
                raise KehrnelError(
                    code="STANDARDS_ASSET_CHECKSUM_MISMATCH",
                    status=422,
                    message=f"Standards asset {asset.asset_id} has the wrong checksum.",
                )
        doc = model_doc(package)
        doc["_id"] = f"{cfg['tenant_id']}:{package.package_id}"
        doc["tenantId"] = str(cfg["tenant_id"])
        stamp_model_schema(doc)
        existing = await storage.find_one(collections(cfg)["standards"], {"_id": doc["_id"]})
        if existing:
            comparable_existing = {key: value for key, value in existing.items() if key != "createdAt"}
            comparable_incoming = {key: value for key, value in doc.items() if key != "createdAt"}
            if comparable_existing != comparable_incoming:
                raise KehrnelError(code="STANDARDS_PACKAGE_IMMUTABLE", status=409, message="Standards package already exists with different content.")
        if not existing:
            await replace_documents(storage, collections(cfg)["standards"], [doc])
        return {"ok": True, "packageId": package.package_id, "created": not bool(existing), "package": existing or doc}

    async def get_standards(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        package_id = str(payload.get("packageId") or "").strip()
        if not package_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="packageId is required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        document = await storage.find_one(
            collections(cfg)["standards"],
            {"_id": f"{cfg['tenant_id']}:{package_id}", "tenantId": str(cfg["tenant_id"])},
        )
        if not document:
            raise KehrnelError(code="STANDARDS_PACKAGE_NOT_FOUND", status=404, message="Standards package was not found.")
        return {"ok": True, "package": document}
