"""FHIR search denormalization via fhir-mql ResourceDenormalizer."""

from __future__ import annotations

import inspect
from typing import Any, Callable

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.rps_canonical.scripts import bridge
from kehrnel.engine.strategies.fhir.rps_canonical.scripts import indexes as indexes_module

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
        if resource.get("resourceType") != resource_type:
            resource = {**resource, "resourceType": resource_type}
        if warnings is not None:
            return denormalizer.denormalize(resource, warnings=warnings)
        return denormalizer.denormalize(resource)

    return processor


async def fhir_denormalize(
    ctx: StrategyContext,
    payload: dict[str, Any],
    *,
    progress_cb: ProgressCallback | None = None,
    should_cancel: CancelCallback | None = None,
) -> dict[str, Any]:
    """
    Build ``_search`` (and ``_compartments`` when configured) on stored FHIR resources.

    Uses fhir-mql ``MongoDBHandler.update_search_fields`` (same path as the CLI
    ``fhir-mql denormalize`` command).
    """
    cfg = bridge.resolve_strategy_config(ctx)
    resource_types = _parse_resource_types(payload)
    batch_size = max(1, int(payload.get("batch_size", 500) or 500))
    limit = payload.get("limit")
    limit_int = int(limit) if limit is not None and int(limit) > 0 else None
    dry_run = bool(payload.get("dry_run", False))
    rebuild = bool(payload.get("rebuild", False))

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

            if rebuild:
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
            if rebuild:
                denormalized[resource_type]["rebuild"] = True

            progress = int((index + 1) / max(1, len(resource_types)) * 100)
            await _emit_progress(
                progress_cb,
                progress=min(99, progress),
                phase="denormalizing",
                stats={resource_type: denormalized[resource_type], "resource_type": resource_type},
            )
    finally:
        if getattr(mql_ctx, "client", None) is not None:
            mql_ctx.client.close()

    index_entries: list[dict[str, Any]] = []
    if (
        not dry_run
        and denormalized
        and indexes_module.search_auto_index_enabled(cfg)
        and not bool(payload.get("skip_auto_index", False))
    ):
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
