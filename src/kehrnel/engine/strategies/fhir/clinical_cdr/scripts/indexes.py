"""FHIR search index management via fhir-mql YAML index specs."""

from __future__ import annotations

import inspect
from typing import Any, Callable

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.capabilities import (
    resolve_resource_capabilities,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.index_manifest import (
    DEFAULT_MANAGED_INDEX_BUDGET,
    build_index_manifest,
    expected_fingerprints,
)

ProgressCallback = Callable[..., Any]
CancelCallback = Callable[[], bool]


def _require_fhir_mql_indexes():
    try:
        from fhir_search_to_mql.denormalizer.mongodb_handler import MongoDBHandler
    except ImportError as exc:
        raise KehrnelError(
            code="FHIR_LIBS_NOT_INSTALLED",
            status=500,
            message="fhir-search-to-mql is not installed. Install kehrnel with the [fhir] extra.",
            details={"import_error": str(exc)},
        ) from exc
    return MongoDBHandler


def search_auto_index_enabled(strategy_config: dict[str, Any] | None) -> bool:
    """Indexes are a non-configurable FHIR persistence invariant."""
    return True


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
        raise KehrnelError(
            code="JOB_CANCELED", status=499, message="Index ensure canceled by user"
        )


def _resolve_resource_types(
    payload: dict[str, Any], loader: Any, *, allowed: set[str] | None = None
) -> list[str]:
    raw = payload.get("resource_types")
    if isinstance(raw, list) and raw:
        types = [str(rt).strip() for rt in raw if str(rt).strip()]
        if types:
            return [value for value in types if allowed is None or value in allowed]
    resolved = list(loader.list_resources())
    return [value for value in resolved if allowed is None or value in allowed]


def _existing_index_names(collection: Any) -> set[str]:
    return {
        str(info.get("name")) for info in collection.list_indexes() if info.get("name")
    }


def _index_key_fingerprint(key_spec: Any) -> tuple | None:
    """Normalize pymongo index key for comparison."""
    if isinstance(key_spec, str):
        return ((key_spec, 1),)
    if isinstance(key_spec, list):
        return tuple(
            tuple(item) if isinstance(item, (list, tuple)) else item
            for item in key_spec
        )
    if isinstance(key_spec, dict):
        return tuple(sorted(key_spec.items()))
    return None


def _existing_index_by_keys(collection: Any) -> dict[tuple, str]:
    """Map normalized key fingerprint -> existing index name."""
    mapping: dict[tuple, str] = {}
    for info in collection.list_indexes():
        name = info.get("name")
        key = info.get("key")
        if not name or key is None:
            continue
        fp = _index_key_fingerprint(dict(key))
        if fp is not None:
            mapping[fp] = str(name)
    return mapping


def _unmanaged_or_stale_indexes(
    collection: Any, expected: set[tuple]
) -> list[dict[str, Any]]:
    """Report unexpected indexes without deleting possibly customer-owned indexes."""
    found: list[dict[str, Any]] = []
    for info in collection.list_indexes():
        name = str(info.get("name") or "")
        if name == "_id_":
            continue
        fingerprint = _index_key_fingerprint(dict(info.get("key") or {}))
        if fingerprint is not None and fingerprint not in expected:
            found.append({"name": name, "fields": [list(item) for item in fingerprint]})
    return found


def _normalize_index_fields(index_spec: dict[str, Any]) -> Any:
    """Match fhir-mql MongoDBHandler.ensure_indexes field normalization."""
    fields = index_spec.get("fields", {})
    if isinstance(fields, str):
        return fields
    if isinstance(fields, dict):
        return [(k, v) for k, v in fields.items()]
    if isinstance(fields, list):
        normalized: list[Any] = []
        for entry in fields:
            if isinstance(entry, dict):
                normalized.extend((k, v) for k, v in entry.items())
            elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                normalized.append(tuple(entry))
            elif isinstance(entry, str):
                normalized.append((entry, 1))
        return normalized
    return fields


def _create_index_idempotent(
    collection: Any, index_spec: dict[str, Any]
) -> tuple[str, str]:
    """
    Create one index; skip if the same keys already exist (any name).

    Returns (index_name, status) where status is created | exists | skipped.
    """
    from pymongo.errors import OperationFailure

    options = dict(index_spec.get("options") or {})
    index_fields = _normalize_index_fields(index_spec)
    fp = _index_key_fingerprint(index_fields)
    existing_by_keys = _existing_index_by_keys(collection)

    if fp is not None and fp in existing_by_keys:
        return existing_by_keys[fp], "exists"

    try:
        name = collection.create_index(index_fields, **options)
        return str(name), "created"
    except OperationFailure as exc:
        if getattr(exc, "code", None) == 85 or "IndexOptionsConflict" in str(exc):
            if fp is not None and fp in _existing_index_by_keys(collection):
                return _existing_index_by_keys(collection)[fp], "exists"
        raise


def _ensure_indexes_idempotent(
    collection: Any,
    index_specs: list[dict[str, Any]],
) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    for spec in index_specs:
        name, status = _create_index_idempotent(collection, spec)
        results.append((name, status))
    return results


def _ensure_collection_indexes(
    collection: Any,
    collection_name: str,
    resource_type: str,
    index_specs: list[dict[str, Any]],
    *,
    dry_run: bool,
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if dry_run:
        for spec in index_specs:
            options = dict(spec.get("options") or {})
            entries.append(
                {
                    "collection": collection_name,
                    "resource_type": resource_type,
                    "name": options.get("name") or _planned_index_name(spec),
                    "status": "planned",
                    "dry_run": True,
                }
            )
        return entries

    before = _existing_index_names(collection)
    try:
        MongoDBHandler = _require_fhir_mql_indexes()
        created_names = MongoDBHandler.ensure_indexes(collection, index_specs)
        for name in created_names:
            entries.append(
                {
                    "collection": collection_name,
                    "resource_type": resource_type,
                    "name": name,
                    "status": "exists" if name in before else "created",
                }
            )
    except Exception as exc:
        if "IndexOptionsConflict" not in str(exc) and getattr(exc, "code", None) != 85:
            raise
        for name, status in _ensure_indexes_idempotent(collection, index_specs):
            entries.append(
                {
                    "collection": collection_name,
                    "resource_type": resource_type,
                    "name": name,
                    "status": status,
                }
            )
    return entries


def _planned_index_name(index_spec: dict[str, Any]) -> str:
    options = index_spec.get("options") or {}
    if isinstance(options, dict) and options.get("name"):
        return str(options["name"])
    fields = index_spec.get("fields")
    if isinstance(fields, str):
        return f"{fields}_1"
    if isinstance(fields, dict) and fields:
        first = next(iter(fields.keys()))
        return f"{first}_1"
    if isinstance(fields, list) and fields:
        first = fields[0]
        if isinstance(first, dict) and first:
            return f"{next(iter(first.keys()))}_1"
    return "index"


async def fhir_ensure_indexes(
    ctx: StrategyContext,
    payload: dict[str, Any],
    *,
    progress_cb: ProgressCallback | None = None,
    should_cancel: CancelCallback | None = None,
) -> dict[str, Any]:
    """
    Create indexes declared in fhir-mql resource YAML configs on ``_search.*`` fields.

    Mirrors the ``fhir-mql indexes`` CLI subcommand.
    """
    cfg = bridge.resolve_strategy_config(ctx)
    dry_run = bool(payload.get("dry_run", False))

    uri, database, prefix = bridge.resolve_mongo(ctx)
    search_cfg = cfg.get("search") or {}
    config_dir = search_cfg.get("config_dir")
    compartment_dir = search_cfg.get("compartment_definitions_dir")

    mql_ctx = bridge.build_mql_context(
        uri, database, prefix, config_dir, compartment_dir
    )
    loader = mql_ctx.config_loader
    allowed = set(resolve_resource_capabilities(cfg, loader).storable)
    resource_types = _resolve_resource_types(payload, loader, allowed=allowed)
    supported = set(bridge.supported_search_resource_types(loader))
    index_policy = cfg.get("index_policy") or {}
    budget = int(
        index_policy.get(
            "max_managed_indexes_per_collection", DEFAULT_MANAGED_INDEX_BUDGET
        )
    )
    manifest = build_index_manifest(
        loader,
        prefix,
        resource_types=resource_types,
        max_managed_indexes_per_collection=budget,
    )
    if not manifest["within_budget"]:
        bridge.close_mql_context(mql_ctx)
        raise KehrnelError(
            code="FHIR_INDEX_BUDGET_EXCEEDED",
            status=400,
            message="FHIR managed index plan exceeds the configured per-collection budget",
            details={
                "violations": manifest["violations"],
                "manifest_digest": manifest["digest"],
            },
        )

    index_entries: list[dict[str, Any]] = []
    skipped: list[str] = []
    warnings: list[str] = []
    stale_candidates: list[dict[str, Any]] = []

    await _emit_progress(
        progress_cb,
        progress=0,
        phase="indexing",
        stats={"resource_types": resource_types},
    )

    try:
        for index, resource_type in enumerate(resource_types):
            _check_canceled(should_cancel)

            if resource_type not in supported:
                skipped.append(resource_type)
                warnings.append(
                    f"No fhir-mql search config for {resource_type}; skipped"
                )
                continue

            try:
                resource_cfg = loader.get_config(resource_type)
            except Exception as exc:
                skipped.append(resource_type)
                warnings.append(f"Could not load config for {resource_type}: {exc}")
                continue

            index_specs = list(resource_cfg.get("indexes") or [])
            collection_name = bridge.collection_name(prefix, resource_type)
            collection = mql_ctx.collection(resource_type)

            entries = _ensure_collection_indexes(
                collection,
                collection_name,
                resource_type,
                index_specs,
                dry_run=dry_run,
            )
            index_entries.extend(entries)
            if not dry_run:
                unexpected = _unmanaged_or_stale_indexes(
                    collection,
                    expected_fingerprints(manifest, resource_type),
                )
                if unexpected:
                    stale_candidates.append(
                        {
                            "resource_type": resource_type,
                            "collection": collection_name,
                            "indexes": unexpected,
                        }
                    )

            progress = int((index + 1) / max(1, len(resource_types)) * 100)
            await _emit_progress(
                progress_cb,
                progress=min(99, progress),
                phase="indexing",
                stats={
                    "resource_type": resource_type,
                    "indexes": len(entries),
                    "collection": collection_name,
                },
            )
    finally:
        bridge.close_mql_context(mql_ctx)

    result: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "database": database,
        "indexes": index_entries,
        "index_manifest": {
            "manifest_version": manifest["manifest_version"],
            "digest": manifest["digest"],
            "resource_count": manifest["resource_count"],
            "managed_index_count": manifest["managed_index_count"],
            "within_budget": manifest["within_budget"],
        },
    }
    if skipped:
        result["skipped"] = skipped
    if warnings:
        result["warnings"] = warnings
    if stale_candidates:
        result["unmanaged_or_stale_indexes"] = stale_candidates

    await _emit_progress(
        progress_cb,
        progress=100,
        phase="completed",
        stats={"indexes": len(index_entries)},
    )
    return result


def fhir_index_manifest(
    ctx: StrategyContext, payload: dict[str, Any]
) -> dict[str, Any]:
    """Return the generated managed-index contract without mutating MongoDB."""
    cfg = bridge.resolve_strategy_config(ctx)
    uri, database, prefix = bridge.resolve_mongo(ctx)
    search_cfg = cfg.get("search") or {}
    mql_ctx = bridge.build_mql_context(
        uri,
        database,
        prefix,
        search_cfg.get("config_dir"),
        search_cfg.get("compartment_definitions_dir"),
    )
    try:
        allowed = set(
            resolve_resource_capabilities(cfg, mql_ctx.config_loader).storable
        )
        resource_types = _resolve_resource_types(
            payload, mql_ctx.config_loader, allowed=allowed
        )
        budget = int(
            (cfg.get("index_policy") or {}).get(
                "max_managed_indexes_per_collection", DEFAULT_MANAGED_INDEX_BUDGET
            )
        )
        return {
            "ok": True,
            "database": database,
            **build_index_manifest(
                mql_ctx.config_loader,
                prefix,
                resource_types=resource_types,
                max_managed_indexes_per_collection=budget,
            ),
        }
    finally:
        bridge.close_mql_context(mql_ctx)
