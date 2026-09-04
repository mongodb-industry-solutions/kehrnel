"""FHIR Bundle/NDJSON migration import operation."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections import Counter
from typing import Any, Callable

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge, indexes
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.document_contract import (
    STORED_DOCUMENT_SCHEMA_VERSION,
    build_projection_versions,
    stamp_projection_metadata,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.storage_adapter import MongoFHIRStorageAdapter
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.validation import validate_level, validate_resource

ProgressCallback = Callable[..., Any]
CancelCallback = Callable[[], bool]


async def _emit(progress_cb: ProgressCallback | None, progress: int, phase: str, stats: dict[str, Any]) -> None:
    if not progress_cb:
        return
    result = progress_cb(progress=progress, phase=phase, stats=stats)
    if inspect.isawaitable(result):
        await result


def _check_canceled(should_cancel: CancelCallback | None) -> None:
    if should_cancel and should_cancel():
        raise KehrnelError(code="JOB_CANCELED", status=499, message="FHIR import canceled by user")


def parse_import_payload(payload: dict[str, Any]) -> tuple[list[Any], str, list[dict[str, Any]]]:
    """Normalize Bundle, resources, single-resource, or NDJSON input."""
    sources = sum(
        1
        for present in (
            payload.get("bundle") is not None,
            payload.get("resources") is not None,
            payload.get("resource") is not None,
            payload.get("ndjson") is not None,
        )
        if present
    )
    # A direct Bundle/resource body is accepted by the HTTP route and strategy op.
    direct = payload.get("resourceType")
    if direct:
        sources += 1
    if sources != 1:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Provide exactly one of bundle, resources, resource, ndjson, or a direct FHIR resource body",
        )

    parse_findings: list[dict[str, Any]] = []
    if payload.get("ndjson") is not None:
        text = payload.get("ndjson")
        if not isinstance(text, str):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="ndjson must be a string")
        resources: list[Any] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                resources.append(json.loads(line))
            except json.JSONDecodeError as exc:
                parse_findings.append({
                    "index": len(resources),
                    "line": line_number,
                    "severity": "error",
                    "code": "FHIR_NDJSON_INVALID",
                    "message": str(exc),
                })
        return resources, "ndjson", parse_findings

    source = payload.get("bundle")
    if source is None and direct == "Bundle":
        source = payload
    if source is not None:
        if not isinstance(source, dict) or source.get("resourceType") != "Bundle":
            raise KehrnelError(code="INVALID_INPUT", status=400, message="bundle must be a FHIR Bundle resource")
        entries = source.get("entry") or []
        if not isinstance(entries, list):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="Bundle.entry must be an array")
        resources = []
        for index, entry in enumerate(entries):
            if isinstance(entry, dict) and isinstance(entry.get("resource"), dict):
                resources.append(entry["resource"])
            else:
                parse_findings.append({
                    "index": index,
                    "severity": "error",
                    "code": "FHIR_BUNDLE_ENTRY_INVALID",
                    "message": "Bundle entry must contain a resource object",
                })
        return resources, f"bundle:{source.get('type') or 'unknown'}", parse_findings

    if payload.get("resources") is not None:
        resources = payload.get("resources")
        if not isinstance(resources, list):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="resources must be an array")
        return resources, "resources", parse_findings

    if payload.get("resource") is not None:
        return [payload.get("resource")], "resource", parse_findings
    return [payload], "resource", parse_findings


def _build_denormalizer(cfg: dict[str, Any]):
    try:
        from fhir_search_to_mql import ResourceDenormalizer
    except ImportError as exc:
        raise KehrnelError(
            code="FHIR_LIBS_NOT_INSTALLED",
            status=500,
            message="fhir-search-to-mql is required to import searchable FHIR resources",
            details={"import_error": str(exc)},
        ) from exc
    search = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    config_dir = search.get("config_dir")
    return ResourceDenormalizer(config_dir=config_dir) if config_dir else ResourceDenormalizer()


async def fhir_import_resources(
    ctx: StrategyContext,
    payload: dict[str, Any],
    *,
    progress_cb: ProgressCallback | None = None,
    should_cancel: CancelCallback | None = None,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate, project, and idempotently persist a bounded migration batch."""
    effective_provenance = {
        "source": "import",
        "operation": "fhir_import_resources",
        "activation_id": (ctx.meta or {}).get("activation_id"),
        **(provenance or {}),
    }
    if payload.get("ensure_indexes") is False:
        raise KehrnelError(
            code="FHIR_PERSISTENCE_INVARIANT_REQUIRED",
            status=400,
            message="FHIR search indexes are mandatory and cannot be disabled",
        )
    cfg = bridge.resolve_strategy_config(ctx)
    release = str(cfg.get("schema_version") or "R5").strip().upper()
    requested_level = payload.get("validation_level") or "base"
    try:
        level = validate_level(str(requested_level), release)
    except ValueError as exc:
        raise KehrnelError(code="FHIR_VALIDATION_LEVEL_UNAVAILABLE", status=400, message=str(exc)) from exc

    # The original CDR package is the resource-support boundary. Import is
    # intentionally limited to resource types that have bundled fhir-mql
    # configuration; a syntactically plausible resourceType must not silently
    # create a new, unsupported collection.
    denormalizer = _build_denormalizer(cfg)
    searchable_resource_types = set(
        bridge.supported_search_resource_types(denormalizer.config_loader)
    )
    supported_resource_types = (
        bridge.configured_cdr_resource_types(cfg) & searchable_resource_types
    )

    resources, source_format, findings = parse_import_payload(payload)
    import_cfg = cfg.get("import") if isinstance(cfg.get("import"), dict) else {}
    max_resources = int(import_cfg.get("max_resources_per_request") or 10_000)
    if len(resources) > max_resources:
        raise KehrnelError(
            code="FHIR_IMPORT_TOO_LARGE",
            status=413,
            message=f"Import contains {len(resources)} resources; request limit is {max_resources}. Send NDJSON in bounded chunks.",
            details={"resource_count": len(resources), "max_resources_per_request": max_resources},
        )

    fail_on_error = bool(payload.get("fail_on_error", True))
    dry_run = bool(payload.get("dry_run", False))
    mode = str(payload.get("mode") or "upsert").lower()
    if mode not in {"upsert", "create"}:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="mode must be 'upsert' or 'create'")

    await _emit(progress_cb, 5, "validating", {"received": len(resources), "source_format": source_format})
    valid: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for index, raw in enumerate(resources):
        _check_canceled(should_cancel)
        resource, item_findings = validate_resource(
            raw,
            index=index,
            level=level,
            release=release,
            supported_resource_types=supported_resource_types,
        )
        findings.extend(item_findings)
        if resource is None or any(item["severity"] == "error" for item in item_findings):
            continue
        key = (str(resource["resourceType"]), str(resource["id"]))
        if key in seen:
            findings.append({
                "index": index,
                "severity": "error",
                "code": "FHIR_DUPLICATE_IN_BATCH",
                "message": f"Duplicate {key[0]}/{key[1]} in the same import batch",
                "resource_type": key[0],
                "resource_id": key[1],
            })
            continue
        seen.add(key)
        valid.append(resource)

    errors = [item for item in findings if item.get("severity") == "error"]
    report: dict[str, Any] = {
        "ok": not errors,
        "contract_version": "1.0",
        "committed": False,
        "dry_run": dry_run,
        "source_format": source_format,
        "mode": mode,
        "fhir_release": release,
        "resource_support_source": "clinical-cdr-recipes-and-fhir-mql-config",
        "validation": {
            "level": level,
            "profile_conformance": False,
            "received": len(resources),
            "valid": len(valid),
            "invalid": len(resources) - len(valid) + len([f for f in findings if f.get("code", "").startswith("FHIR_NDJSON") or f.get("code") == "FHIR_BUNDLE_ENTRY_INVALID"]),
            "findings": findings,
        },
        "resource_counts": dict(sorted(Counter(str(resource["resourceType"]) for resource in valid).items())),
    }
    if errors and fail_on_error:
        report["message"] = "Validation failed; no resources were written"
        return report

    await _emit(progress_cb, 25, "projecting", {"valid": len(valid)})
    projected: list[dict[str, Any]] = []
    projected_counts: Counter[str] = Counter()
    projection_warnings: list[dict[str, Any]] = []
    resource_types = sorted({str(resource["resourceType"]) for resource in valid})
    search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    compartment_dir = (
        search_cfg.get("compartment_definitions_dir")
        or bridge._bundled_compartment_definitions_dir()
    )
    versions = build_projection_versions(
        denormalizer.config_loader,
        fhir_release=release,
        compartment_definitions_dir=compartment_dir,
        resource_types=resource_types,
    )
    for index, resource in enumerate(valid):
        _check_canceled(should_cancel)
        warning_text: list[str] = []
        projected_resource = denormalizer.denormalize(resource, warnings=warning_text)
        projected_resource = stamp_projection_metadata(
            projected_resource,
            versions,
            provenance=effective_provenance,
        )
        projected.append(projected_resource)
        projected_counts[str(resource["resourceType"])] += 1
        for warning in warning_text:
            projection_warnings.append({
                "index": index,
                "severity": "warning",
                "code": "FHIR_SEARCH_PROJECTION_WARNING",
                "message": warning,
                "resource_type": resource["resourceType"],
                "resource_id": resource["id"],
            })
    if projection_warnings:
        report["validation"]["findings"].extend(projection_warnings)
        raise KehrnelError(
            code="FHIR_PROJECTION_FAILED",
            status=500,
            message="FHIR search projection produced field-level failures; no resources were written",
            details={"findings": projection_warnings},
        )
    report["search_projection"] = {
        "projected": sum(projected_counts.values()),
        "by_resource_type": dict(sorted(projected_counts.items())),
        "unprojected": len(projected) - sum(projected_counts.values()),
    }
    report["document_contract"] = {
        "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
        "projection_contract_version": versions.projection_contract_version,
        "resource_projection_versions": dict(sorted(versions.resource_projection_versions.items())),
    }

    if dry_run:
        report["ok"] = not errors
        report["message"] = "Dry run completed; no resources were written"
        await _emit(progress_cb, 100, "completed", {"dry_run": True, "valid": len(valid)})
        return report

    if not projected:
        report["ok"] = not errors
        report["message"] = "No valid resources to write"
        return report

    searchable_types = sorted(projected_counts)
    await _emit(progress_cb, 50, "indexing", {"resource_types": searchable_types})
    index_result = await indexes.fhir_ensure_indexes(ctx, {"resource_types": searchable_types})
    if index_result.get("skipped"):
        raise KehrnelError(
            code="FHIR_INDEX_CONFIGURATION_MISSING",
            status=500,
            message="Mandatory FHIR indexes could not be ensured",
            details={"skipped": index_result["skipped"], "warnings": index_result.get("warnings") or []},
        )
    report["indexes"] = {
        "ensured": len(index_result.get("indexes") or []),
        "skipped": [],
        "warnings": index_result.get("warnings") or [],
    }

    uri, database, prefix = bridge.resolve_mongo(ctx)
    mql_ctx = bridge.build_mql_context(
        uri,
        database,
        prefix,
        (cfg.get("search") or {}).get("config_dir"),
        (cfg.get("search") or {}).get("compartment_definitions_dir"),
    )
    try:
        adapter = MongoFHIRStorageAdapter(
            mql_ctx.db,
            collection_prefix=prefix,
            projection_versions=versions,
        )
        await _emit(progress_cb, 70, "persisting", {"valid": len(projected), "database": database})
        write_result = await asyncio.to_thread(adapter.persist_many, projected, mode=mode)
    except Exception as exc:
        if exc.__class__.__name__ in {"BulkWriteError", "DuplicateKeyError"}:
            raise KehrnelError(code="FHIR_WRITE_CONFLICT", status=409, message="A FHIR logical id already exists", details={"mode": mode}) from exc
        raise
    finally:
        bridge.close_mql_context(mql_ctx)

    report["write"] = write_result
    report["database"] = database
    report["collections"] = [bridge.collection_name(prefix, resource_type) for resource_type in sorted(report["resource_counts"])]
    report["committed"] = True
    report["ok"] = not errors

    await _emit(progress_cb, 100, "completed", {"written": write_result.get("processed", 0)})
    return report
