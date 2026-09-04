"""FHIR search denormalization via fhir-mql ResourceDenormalizer."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from typing import Any, Callable

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import indexes as indexes_module
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.document_contract import (
    STORED_DOCUMENT_SCHEMA_VERSION,
    build_projection_versions,
    metadata_set_fields,
    normalize_projection_buckets,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import canonical_resource

ProgressCallback = Callable[..., Any]
CancelCallback = Callable[[], bool]


def _require_fhir_mql_denorm():
    try:
        from fhir_search_to_mql import ResourceDenormalizer
        from fhir_search_to_mql.denormalizer.mongodb_handler import MongoDBHandler
    except ImportError as exc:
        raise KehrnelError(
            code="FHIR_LIBS_NOT_INSTALLED",
            status=500,
            message="fhir-search-to-mql is not installed. Install kehrnel with the [fhir] extra.",
            details={"import_error": str(exc)},
        ) from exc
    return ResourceDenormalizer, MongoDBHandler


async def _emit_progress(
    progress_cb: ProgressCallback | None,
    *,
    progress: int | None = None,
    phase: str | None = None,
    stats: dict[str, Any] | None = None,
) -> None:
    if not progress_cb:
        return
    result = progress_cb(progress=progress, phase=phase, stats=stats)
    if inspect.isawaitable(result):
        await result


def _is_canceled(should_cancel: CancelCallback | None) -> bool:
    if not should_cancel:
        return False
    try:
        return bool(should_cancel())
    except Exception:
        return False


def _check_canceled(should_cancel: CancelCallback | None) -> None:
    if _is_canceled(should_cancel):
        raise KehrnelError(code="JOB_CANCELED", status=499, message="Denormalize canceled by user")


def _parse_resource_types(payload: dict[str, Any]) -> list[str]:
    raw = payload.get("resource_types")
    if raw is None:
        raw = payload.get("denormalize_resource_types")
    if not isinstance(raw, list) or not raw:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="resource_types is required (non-empty list of FHIR resource type names)",
        )
    types = [str(rt).strip() for rt in raw if str(rt).strip()]
    if not types:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="resource_types must contain at least one resource type name",
        )
    return types


def _build_query(collection: Any, limit: int | None) -> dict[str, Any]:
    if not limit or limit <= 0:
        return {}
    ids = [doc["_id"] for doc in collection.find({}, {"_id": 1}).limit(int(limit))]
    if not ids:
        return {"_id": {"$in": []}}
    return {"_id": {"$in": ids}}


def _denormalizer_for_type(
    denormalizer: Any,
    resource_type: str,
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Return a processor bound to a single resource type config."""

    def processor(resource: dict[str, Any], warnings: list[str] | None = None) -> dict[str, Any]:
        resource = canonical_resource(resource)
        if resource.get("resourceType") != resource_type:
            resource = {**resource, "resourceType": resource_type}
        if warnings is not None:
            projected = denormalizer.denormalize(resource, warnings=warnings)
        else:
            projected = denormalizer.denormalize(resource)
        return normalize_projection_buckets(projected)

    return processor


async def fhir_denormalize(
    ctx: StrategyContext,
    payload: dict[str, Any],
    *,
    progress_cb: ProgressCallback | None = None,
    should_cancel: CancelCallback | None = None,
) -> dict[str, Any]:
    """
    Rebuild mandatory ``_search`` and ``_compartments`` projections on stored resources.

    Uses fhir-mql ``MongoDBHandler.update_search_fields`` (same path as the CLI
    ``fhir-mql denormalize`` command).
    """
    cfg = bridge.resolve_strategy_config(ctx)
    resource_types = _parse_resource_types(payload)
    batch_size = max(1, int(payload.get("batch_size", 500) or 500))
    limit = payload.get("limit")
    limit_int = int(limit) if limit is not None and int(limit) > 0 else None
    dry_run = bool(payload.get("dry_run", False))
    rebuild = True
    if payload.get("skip_auto_index") is True:
        raise KehrnelError(
            code="FHIR_PERSISTENCE_INVARIANT_REQUIRED",
            status=400,
            message="FHIR search indexes are mandatory and cannot be skipped",
        )

    uri, database, prefix = bridge.resolve_mongo(ctx)
    search_cfg = cfg.get("search") or {}
    config_dir = search_cfg.get("config_dir")
    compartment_dir = search_cfg.get("compartment_definitions_dir")

    ResourceDenormalizer, MongoDBHandler = _require_fhir_mql_denorm()
    mql_ctx = bridge.build_mql_context(uri, database, prefix, config_dir, compartment_dir)
    denormalizer = (
        ResourceDenormalizer(config_dir=config_dir) if config_dir else ResourceDenormalizer()
    )

    supported = set(bridge.supported_search_resource_types(mql_ctx.config_loader))
    versions = build_projection_versions(
        mql_ctx.config_loader,
        fhir_release=str(cfg.get("schema_version") or "R5"),
        compartment_definitions_dir=mql_ctx.compartment_definitions_dir,
        resource_types=[resource_type for resource_type in resource_types if resource_type in supported],
    )
    denormalized: dict[str, dict[str, int]] = {}
    skipped: list[str] = []
    warnings: list[str] = []

    await _emit_progress(
        progress_cb,
        progress=0,
        phase="denormalizing",
        stats={"resource_types": resource_types},
    )

    try:
        for index, resource_type in enumerate(resource_types):
            _check_canceled(should_cancel)

            if resource_type not in supported:
                skipped.append(resource_type)
                warnings.append(f"No fhir-mql search config for {resource_type}; skipped")
                continue

            collection_name = bridge.collection_name(prefix, resource_type)
            collection = mql_ctx.collection(resource_type)
            query = _build_query(collection, limit_int)

            if dry_run:
                count = collection.count_documents(query)
                denormalized[resource_type] = {
                    "processed": count,
                    "updated": 0,
                    "failed": 0,
                    "dry_run": True,
                    "collection": collection_name,
                }
                await _emit_progress(
                    progress_cb,
                    phase="denormalizing",
                    stats={resource_type: denormalized[resource_type], "resource_type": resource_type},
                )
                continue

            cleared = MongoDBHandler.remove_search_fields(collection, query)
            denormalized.setdefault(resource_type, {})
            denormalized[resource_type]["cleared"] = int(cleared)

            processor = _denormalizer_for_type(denormalizer, resource_type)
            stats = MongoDBHandler.update_search_fields(
                collection=collection,
                query=query,
                processor=processor,
                batch_size=batch_size,
            )
            if int(stats.get("failed", 0)):
                raise KehrnelError(
                    code="FHIR_PROJECTION_FAILED",
                    status=500,
                    message=f"Mandatory FHIR projection failed for {resource_type}",
                    details={"resource_type": resource_type, "stats": stats},
                )

            projected_at = datetime.now(UTC)
            required_query: dict[str, Any] = {
                "$and": [
                    query,
                    {"_search": {"$exists": True, "$type": "object"}},
                    {"_compartments": {"$exists": True, "$type": "object"}},
                ]
            }
            collection.update_many(
                required_query,
                {
                    "$set": metadata_set_fields(
                        versions,
                        resource_type,
                        projected_at=projected_at,
                    ),
                    "$unset": {"_stored_at": "", "_fhir_resource_type": ""},
                },
            )
            collection.update_many(
                {"$and": [required_query, {"_kehrnel.stored_at": {"$exists": False}}]},
                {"$set": {"_kehrnel.stored_at": projected_at}},
            )
            expected = collection.count_documents(query)
            compliant = collection.count_documents(
                {
                    "$and": [
                        query,
                        {"_search": {"$exists": True, "$type": "object"}},
                        {"_compartments": {"$exists": True, "$type": "object"}},
                        {"_kehrnel.storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION},
                        {
                            "_kehrnel.resource_projection_version": versions.for_resource(
                                resource_type
                            )
                        },
                    ]
                }
            )
            if compliant != expected:
                raise KehrnelError(
                    code="FHIR_PROJECTION_INVARIANT_FAILED",
                    status=500,
                    message=f"Not every {resource_type} document satisfies the FHIR storage contract",
                    details={"resource_type": resource_type, "expected": expected, "compliant": compliant},
                )
            denormalized[resource_type] = {
                "processed": int(stats.get("processed", 0)),
                "updated": int(stats.get("updated", 0)),
                "failed": int(stats.get("failed", 0)),
                "field_failures": int(stats.get("field_failures", 0)),
                "documents_with_field_failures": int(
                    stats.get("documents_with_field_failures", 0)
                ),
                "collection": collection_name,
            }
            denormalized[resource_type]["rebuild"] = True
            denormalized[resource_type]["resource_projection_version"] = (
                versions.for_resource(resource_type)
            )

            progress = int((index + 1) / max(1, len(resource_types)) * 100)
            await _emit_progress(
                progress_cb,
                progress=min(99, progress),
                phase="denormalizing",
                stats={resource_type: denormalized[resource_type], "resource_type": resource_type},
            )
    finally:
        bridge.close_mql_context(mql_ctx)

    index_entries: list[dict[str, Any]] = []
    if not dry_run and denormalized:
        denorm_types = [rt for rt in denormalized if rt not in skipped]
        if denorm_types:
            index_result = await indexes_module.fhir_ensure_indexes(
                ctx,
                {"resource_types": denorm_types},
                progress_cb=progress_cb,
                should_cancel=should_cancel,
            )
            index_entries = list(index_result.get("indexes") or [])
            if index_result.get("warnings"):
                warnings.extend(index_result["warnings"])

    result: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "rebuild": rebuild,
        "database": database,
        "denormalized": denormalized,
        "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
        "projection_contract_version": versions.projection_contract_version,
    }
    if index_entries:
        result["indexes"] = index_entries
    if skipped:
        result["skipped"] = skipped
    if warnings:
        result["warnings"] = warnings

    await _emit_progress(
        progress_cb,
        progress=100,
        phase="completed",
        stats={"denormalized": denormalized, "indexes": len(index_entries)},
    )
    return result


def resolve_denormalize_resource_types(
    generated: dict[str, int],
    payload: dict[str, Any],
    supported: set[str],
) -> list[str]:
    """Pick resource types for inline denormalize after synthetic generation."""
    explicit = payload.get("denormalize_resource_types")
    if isinstance(explicit, list) and explicit:
        return [str(rt) for rt in explicit]
    return [rt for rt in generated if rt in supported]
