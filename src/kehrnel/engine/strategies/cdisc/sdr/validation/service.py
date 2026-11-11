"""Reproducible validation runs and normalized findings."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import uuid4

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.cdisc.models import ValidationFinding, ValidationRun

from ..common import collections, config, model_doc, replace_documents, storage_adapter


def _finding_id(run_id: str, finding: Dict[str, Any]) -> str:
    payload = json.dumps(finding, sort_keys=True, separators=(",", ":"), default=str).encode()
    return f"{run_id}:{hashlib.sha256(payload).hexdigest()}"


class ValidationService:
    async def get_run(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        run_id = str(payload.get("runId") or "").strip()
        if not run_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="runId is required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        coll = collections(cfg)
        run = await storage.find_one(
            coll["validation_runs"], {"_id": run_id, "tenantId": str(cfg["tenant_id"])}
        )
        if not run:
            raise KehrnelError(code="CDISC_VALIDATION_RUN_NOT_FOUND", status=404, message="Validation run was not found.")
        match: Dict[str, Any] = {"tenantId": str(cfg["tenant_id"]), "runId": run_id}
        if payload.get("severity"):
            match["severity"] = str(payload["severity"]).lower()
        findings = await storage.aggregate(
            coll["validation_findings"],
            [{"$match": match}, {"$limit": min(max(int(payload.get("limit") or 1000), 1), 10000)}],
        )
        return {"ok": True, "run": run, "findings": findings}

    async def register_waiver(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        rule_id = str(payload.get("ruleId") or "").strip()
        justification = str(payload.get("justification") or "").strip()
        approved_by = str(payload.get("approvedBy") or "").strip()
        if not rule_id or not justification or not approved_by:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="ruleId, justification, and approvedBy are required",
            )
        expires_at = payload.get("expiresAt")
        if expires_at:
            try:
                parsed_expiry = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
                if parsed_expiry.tzinfo is None:
                    raise ValueError("timezone required")
                expires_at = parsed_expiry.astimezone(timezone.utc).isoformat()
            except ValueError as exc:
                raise KehrnelError(
                    code="INVALID_INPUT", status=400, message="expiresAt must be an ISO-8601 timestamp with timezone"
                ) from exc
        cfg, storage = config(ctx), storage_adapter(ctx)
        waiver_id = str(payload.get("waiverId") or uuid4())
        document = {
            "_id": f"{cfg['tenant_id']}:waiver:{waiver_id}",
            "waiverId": waiver_id,
            "tenantId": str(cfg["tenant_id"]),
            "ruleId": rule_id,
            "justification": justification,
            "approvedBy": approved_by,
            "studyId": payload.get("studyId"),
            "snapshotId": payload.get("snapshotId"),
            "datasetId": payload.get("datasetId"),
            "recordId": payload.get("recordId"),
            "variable": payload.get("variable"),
            "expiresAt": expires_at,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        collection = collections(cfg)["validation_waivers"]
        existing = await storage.find_one(collection, {"_id": document["_id"]})
        if existing:
            stable_fields = (
                "tenantId", "ruleId", "justification", "approvedBy", "studyId",
                "snapshotId", "datasetId", "recordId", "variable", "expiresAt",
            )
            if any(existing.get(key) != document.get(key) for key in stable_fields):
                raise KehrnelError(code="CDISC_WAIVER_IMMUTABLE", status=409, message="Waiver already exists.")
        if not existing:
            await replace_documents(storage, collection, [document])
        return {"ok": True, "waiver": existing or document, "created": not bool(existing)}

    async def validate_snapshot(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        study_id = str(payload.get("studyId") or "").strip()
        snapshot_id = str(payload.get("snapshotId") or "").strip()
        if not study_id or not snapshot_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="studyId and snapshotId are required")
        cfg, storage = config(ctx), storage_adapter(ctx)
        coll = collections(cfg)
        snapshot_ref = f"{cfg['tenant_id']}:{study_id}:{snapshot_id}"
        snapshot = await storage.find_one(coll["snapshots"], {"_id": snapshot_ref})
        if not snapshot:
            raise KehrnelError(code="CDISC_SNAPSHOT_NOT_FOUND", status=404, message="Snapshot was not found.")
        run_id = str(uuid4())
        started = datetime.now(timezone.utc).isoformat()
        datasets = []
        findings: List[Dict[str, Any]] = []
        standards = await storage.find_one(
            coll["standards"], {"_id": f"{cfg['tenant_id']}:{snapshot.get('standardsPackageId')}"}
        )
        if bool((cfg.get("validation") or {}).get("require_registered_standards")) and not standards:
            findings.append({
                "severity": "error",
                "ruleId": "SDR.STANDARDS.UNREGISTERED",
                "message": "Snapshot references an unregistered standards package.",
                "details": {"standardsPackageId": snapshot.get("standardsPackageId")},
            })
        for dataset_id in snapshot.get("datasetIds", []):
            dataset = await storage.find_one(coll["datasets"], {"_id": dataset_id})
            records = await storage.aggregate(coll["records"], [
                {"$match": {"tenantId": str(cfg["tenant_id"]), "datasetId": dataset_id}},
                {"$sort": {"rowOrdinal": 1}},
            ])
            if not dataset:
                findings.append({"severity": "error", "ruleId": "SDR.DATASET.MISSING", "message": "Snapshot references a missing dataset.", "datasetId": dataset_id})
                continue
            datasets.append({"dataset": dataset, "records": records})
            if len(records) != dataset.get("recordCount"):
                findings.append({"severity": "error", "ruleId": "SDR.RECORD_COUNT", "message": "Stored record count differs from metadata.", "datasetId": dataset_id})
            identity_metadata = (dataset.get("sourceMetadata") or {}).get("recordIdentity") or {}
            if identity_metadata.get("duplicateDeclaredKeys"):
                findings.append({
                    "severity": "warning",
                    "ruleId": "SDR.KEY.DUPLICATE_SOURCE",
                    "message": "Source rows contain duplicate declared keys; canonical identities were disambiguated by source row ordinal.",
                    "datasetId": dataset_id,
                    "details": {
                        "duplicateDeclaredKeys": identity_metadata["duplicateDeclaredKeys"],
                        "affectedRecords": identity_metadata.get("affectedRecords", 0),
                        "identityStrategy": identity_metadata.get("strategy"),
                    },
                })
            variables_by_name = {
                str(variable.get("name")): variable
                for variable in dataset.get("variables") or []
                if variable.get("name")
            }
            keys = [
                key for key in dataset.get("keyVariables", [])
                if not (variables_by_name.get(key, {}).get("origin") or {})
                or str((variables_by_name.get(key, {}).get("origin") or {}).get("mandatory") or "").lower() == "yes"
            ]
            for record in records:
                record_study = record.get("data", {}).get("STUDYID")
                if record_study not in (None, "") and str(record_study) != study_id:
                    findings.append({
                        "severity": "error", "ruleId": "SDR.STUDYID.CONSISTENT",
                        "message": "Record STUDYID differs from the snapshot study.",
                        "datasetId": dataset_id, "recordId": record.get("_id"), "variable": "STUDYID",
                        "details": {"expected": study_id, "actual": record_study},
                    })
                record_domain = record.get("data", {}).get("DOMAIN")
                if record_domain not in (None, "") and str(record_domain).upper() != str(dataset.get("domain") or "").upper():
                    findings.append({
                        "severity": "error", "ruleId": "SDR.DOMAIN.CONSISTENT",
                        "message": "Record DOMAIN differs from the dataset domain.",
                        "datasetId": dataset_id, "recordId": record.get("_id"), "variable": "DOMAIN",
                        "details": {"expected": dataset.get("domain"), "actual": record_domain},
                    })
                for variable in keys:
                    if record.get("data", {}).get(variable) in (None, ""):
                        findings.append({
                            "severity": "error", "ruleId": "SDR.KEY.REQUIRED", "message": "Declared key value is missing.",
                            "datasetId": dataset_id, "recordId": record.get("_id"), "variable": variable,
                        })
                required_by_domain = {
                    "DM": ("USUBJID", "SEX"),
                    "AE": ("USUBJID", "AESEQ", "AETERM", "AEDECOD"),
                    "LB": ("USUBJID", "LBSEQ", "LBTESTCD"),
                    "VS": ("USUBJID", "VSSEQ", "VSTESTCD"),
                }
                profile_required = {
                    "send": {
                        "DM": ("USUBJID", "SEX"),
                        "MI": ("USUBJID", "MISEQ", "MITESTCD", "MISPEC"),
                    },
                    "adam": {
                        "ADSL": ("USUBJID",),
                        "ADAE": ("USUBJID", "ASEQ", "PARAMCD"),
                        "ADLB": ("USUBJID", "ASEQ", "PARAMCD"),
                    },
                    "tig": {
                        "PROD": ("PRODUCTID",),
                        "BATCH": ("BATCHID", "PRODUCTID"),
                        "EVID": ("EVIDID", "PRODUCTID", "EVIDTYPE"),
                    },
                }
                required = profile_required.get(str(snapshot.get("profile") or ""), {}).get(
                    dataset.get("domain"), required_by_domain.get(dataset.get("domain"), ())
                )
                for variable in required:
                    if record.get("data", {}).get(variable) in (None, ""):
                        findings.append({
                            "severity": "error", "ruleId": f"SDR.{dataset.get('domain')}.{variable}.REQUIRED",
                            "message": "Required profile value is missing.", "datasetId": dataset_id,
                            "recordId": record.get("_id"), "variable": variable,
                        })

        anchor_domain = "ADSL" if str(snapshot.get("profile") or "") == "adam" else "DM"
        dm_subjects = {
            str(record.get("data", {}).get("USUBJID"))
            for item in datasets if item["dataset"].get("domain") == anchor_domain
            for record in item["records"] if record.get("data", {}).get("USUBJID") not in (None, "")
        }
        profile = str(snapshot.get("profile") or "")
        if dm_subjects and profile in {"sdtm", "send", "adam"}:
            for item in datasets:
                if item["dataset"].get("domain") == anchor_domain:
                    continue
                for record in item["records"]:
                    subject = record.get("data", {}).get("USUBJID")
                    if subject not in (None, "") and str(subject) not in dm_subjects:
                        findings.append({
                            "severity": "error",
                            "ruleId": "SDR.SUBJECT.UNKNOWN",
                            "message": "Record references a subject not present in DM.",
                            "datasetId": item["dataset"].get("_id"),
                            "recordId": record.get("_id"),
                            "variable": "USUBJID",
                            "details": {"subjectId": subject},
                        })
        if profile == "tig":
            product_ids = {
                str(record.get("data", {}).get("PRODUCTID"))
                for item in datasets if item["dataset"].get("domain") == "PROD"
                for record in item["records"] if record.get("data", {}).get("PRODUCTID") not in (None, "")
            }
            if product_ids:
                for item in datasets:
                    if item["dataset"].get("domain") == "PROD":
                        continue
                    for record in item["records"]:
                        product_id = record.get("data", {}).get("PRODUCTID")
                        if product_id not in (None, "") and str(product_id) not in product_ids:
                            findings.append({
                                "severity": "error", "ruleId": "SDR.PRODUCT.UNKNOWN",
                                "message": "Evidence references a product not present in PROD.",
                                "datasetId": item["dataset"].get("_id"), "recordId": record.get("_id"),
                                "variable": "PRODUCTID", "details": {"productId": product_id},
                            })

        engine_name = "kehrnel.structural"
        engine_version = "1"
        external = (ctx.adapters or {}).get("validation_engine")
        external_coverage: Dict[str, Any] = {}
        if external is not None:
            try:
                result = await external.validate(snapshot=snapshot, datasets=datasets, options=payload.get("options") or {})
            except Exception as exc:
                if (cfg.get("validation") or {}).get("external_failure_policy", "finding") == "raise":
                    raise KehrnelError(code="CDISC_VALIDATION_FAILED", status=502, message=str(exc)) from exc
                result = {
                    "engine": type(external).__name__,
                    "version": "unavailable",
                    "coverage": {"completed": False},
                    "findings": [{
                        "severity": "error",
                        "ruleId": "SDR.VALIDATION_ENGINE.FAILURE",
                        "message": "The configured external validation engine failed.",
                        "details": {"error": str(exc)},
                    }],
                }
            engine_name = str(result.get("engine") or type(external).__name__)
            engine_version = str(result.get("version") or "unspecified")
            external_coverage = result.get("coverage") if isinstance(result.get("coverage"), dict) else {}
            for finding in result.get("findings", []):
                severity = str(finding.get("severity") or "error").lower()
                if severity not in {"error", "warning", "info"}:
                    severity = "error"
                findings.append({
                    "severity": severity,
                    "ruleId": str(finding.get("ruleId") or finding.get("rule_id") or "EXTERNAL"),
                    "message": str(finding.get("message") or "Validation finding"),
                    "datasetId": finding.get("datasetId"), "recordId": finding.get("recordId"),
                    "variable": finding.get("variable"), "details": finding.get("details") or {},
                })
        waivers = await storage.aggregate(
            coll["validation_waivers"], [{"$match": {"tenantId": str(cfg["tenant_id"])}}]
        )
        now = datetime.now(timezone.utc).isoformat()

        def matching_waiver(finding: Dict[str, Any]) -> Dict[str, Any] | None:
            for waiver in waivers:
                if waiver.get("ruleId") != finding.get("ruleId"):
                    continue
                if waiver.get("expiresAt") and str(waiver["expiresAt"]) < now:
                    continue
                scopes = {
                    "studyId": study_id,
                    "snapshotId": snapshot_id,
                    "datasetId": finding.get("datasetId"),
                    "recordId": finding.get("recordId"),
                    "variable": finding.get("variable"),
                }
                if all(not waiver.get(key) or waiver.get(key) == value for key, value in scopes.items()):
                    return waiver
            return None

        normalized = []
        for finding in findings:
            waiver = matching_waiver(finding)
            if waiver:
                original = finding["severity"]
                finding = {
                    **finding,
                    "severity": "info",
                    "waived": True,
                    "waiverId": waiver.get("waiverId"),
                    "details": {**(finding.get("details") or {}), "originalSeverity": original},
                }
            doc = {**finding, "runId": run_id, "tenantId": str(cfg["tenant_id"]), "snapshotRef": snapshot_ref}
            doc["_id"] = _finding_id(run_id, doc)
            normalized.append(model_doc(ValidationFinding.model_validate(doc)))
        errors = sum(item["severity"] == "error" for item in normalized)
        warnings = sum(item["severity"] == "warning" for item in normalized)
        waived = sum(bool(item.get("waived")) for item in normalized)
        blocking = set((cfg.get("validation") or {}).get("blocking_severities") or ["error"])
        status = "failed" if any(item["severity"] in blocking and not item.get("waived") for item in normalized) else "passed"
        completed = datetime.now(timezone.utc).isoformat()
        input_digest = "sha256:" + hashlib.sha256(json.dumps(
            {
                "snapshot": snapshot_ref,
                "datasets": [item["dataset"].get("contentHash") for item in datasets],
                "options": payload.get("options") or {},
                "standardsPackageId": snapshot.get("standardsPackageId"),
            }, sort_keys=True, separators=(",", ":"), default=str
        ).encode()).hexdigest()
        run = model_doc(ValidationRun.model_validate({
            "_id": run_id, "runId": run_id, "tenantId": str(cfg["tenant_id"]), "snapshotRef": snapshot_ref,
            "engine": engine_name, "engineVersion": engine_version, "status": status,
            "summary": {"errors": errors, "warnings": warnings, "waived": waived, "findings": len(normalized)},
            "inputDigest": input_digest,
            "standardsPackageId": snapshot.get("standardsPackageId"),
            "rulePackages": (standards or {}).get("rulePackages") or [],
            "coverage": {
                "builtIn": ["record-count", "identifier-consistency", "key-presence", "required-values", "subject-and-product-references"],
                "external": external_coverage,
            },
            "startedAt": started, "completedAt": completed,
        }))
        await replace_documents(storage, coll["validation_runs"], [run])
        await replace_documents(storage, coll["validation_findings"], normalized)
        snapshot_state = snapshot.get("state") if snapshot.get("state") == "published" else ("validated" if status == "passed" else "quarantined")
        updated = {**snapshot, "state": snapshot_state, "latestValidationRunId": run_id}
        await replace_documents(storage, coll["snapshots"], [updated])
        return {"ok": status == "passed", "run": run, "findings": normalized, "snapshotState": updated["state"]}
