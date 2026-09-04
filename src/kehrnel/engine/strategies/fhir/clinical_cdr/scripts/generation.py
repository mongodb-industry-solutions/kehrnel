"""FHIR synthetic batch generation via fhir-gen."""

from __future__ import annotations

import inspect
import os
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr._paths import FHIR_GEN_ROOT
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import import_resources
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import watermark

ProgressCallback = Callable[..., Any]
CancelCallback = Callable[[], bool]


@contextmanager
def _fhir_gen_cwd_guard():
    """Use fhir-gen package root so its ``.env`` loads, not kehrnel repo root."""
    prev = os.getcwd()
    if FHIR_GEN_ROOT.is_dir():
        os.chdir(FHIR_GEN_ROOT)
    try:
        yield
    finally:
        os.chdir(prev)


def _import_fhir_gen():
    with _fhir_gen_cwd_guard():
        from fhir_gen.generators.base import ResourceGenerator
        from fhir_gen.resolvers.dependency import resolve_order
        from fhir_gen.schema.registry import SchemaRegistry
        from fhir_gen.schema.versions import resolve_schema_path

    return ResourceGenerator, resolve_order, SchemaRegistry, resolve_schema_path


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
        raise KehrnelError(code="JOB_CANCELED", status=499, message="Synthetic batch canceled by user")


def _parse_scenario_specs(scenarios: list[Any] | None) -> list[tuple[str, str]]:
    if not scenarios:
        return []
    parsed: list[tuple[str, str]] = []
    for entry in scenarios:
        if not isinstance(entry, str) or ":" not in entry:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Each scenario must be 'ResourceType:scenario_id'",
                details={"entry": entry},
            )
        resource_type, scenario_id = entry.split(":", 1)
        resource_type = resource_type.strip()
        scenario_id = scenario_id.strip()
        if not resource_type or not scenario_id:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Each scenario must be 'ResourceType:scenario_id'",
                details={"entry": entry},
            )
        parsed.append((resource_type, scenario_id))
    return parsed


def _count_resources_by_type(docs: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for doc in docs:
        rtype = doc.get("resourceType")
        if rtype:
            counts[str(rtype)] += 1
    return dict(counts)


def _configure_schema(schema_version: str | None, schema_path: str | None) -> None:
    if not schema_version and not schema_path:
        return
    _, _, SchemaRegistry, resolve_schema_path = _import_fhir_gen()
    path = resolve_schema_path(
        schema_version=schema_version,
        schema_path=Path(schema_path) if schema_path else None,
    )
    SchemaRegistry.reload(path)


async def synthetic_generate_batch(
    ctx: StrategyContext,
    payload: dict[str, Any],
    *,
    progress_cb: ProgressCallback | None = None,
    should_cancel: CancelCallback | None = None,
) -> dict[str, Any]:
    """
    Generate FHIR resources in batch and optionally persist via FHIRMongoStore.

    See ``README.md`` in this strategy pack for the payload contract.
    """
    cfg = bridge.resolve_strategy_config(ctx)
    effective_payload = bridge.resolve_generation_payload(cfg, payload)
    requested = bridge.parse_resources_payload(effective_payload)

    generation_cfg = cfg.get("generation") or {}
    seed = effective_payload.get("seed", generation_cfg.get("seed"))
    schema_version = str(effective_payload.get("schema_version") or cfg.get("schema_version") or "R5")
    schema_path = effective_payload.get("schema_path")
    dry_run = bool(effective_payload.get("dry_run", False))
    plan_only = bool(effective_payload.get("plan_only", False))
    store_canonical = bool(effective_payload.get("store_canonical", True))
    variants = bool(effective_payload.get("variants", False))
    variant_resources = effective_payload.get("variant_resources")
    scenarios = _parse_scenario_specs(effective_payload.get("scenarios"))

    if effective_payload.get("denormalize_after") is False:
        raise KehrnelError(
            code="FHIR_PERSISTENCE_INVARIANT_REQUIRED",
            status=400,
            message="Stored synthetic FHIR resources must always be projected and indexed",
        )

    if schema_version.upper() not in {"R5", "R6"}:
        raise KehrnelError(
            code="FHIR_GENERATION_VERSION_UNSUPPORTED",
            status=400,
            message=(
                f"Synthetic generation is not available for {schema_version}; "
                "use an R5 or R6 activation."
            ),
            details={"schema_version": schema_version, "supported": ["R5", "R6"]},
        )

    database = str(cfg["database"])
    prefix = str(cfg.get("collection_prefix") or "")
    generation_order = _import_fhir_gen()[1](list(requested.keys()))
    collection_names = [
        bridge.collection_name(prefix, rt) for rt in sorted(set(requested) | set(generation_order))
    ]

    if plan_only:
        return {
            "ok": True,
            "plan_only": True,
            "dry_run": dry_run,
            "recipe": effective_payload.get("recipe") or effective_payload.get("generation_recipe"),
            "planned": requested,
            "generation_order": generation_order,
            "database": database,
            "collections": collection_names,
        }

    watermark_enabled = watermark.watermark_enabled(cfg)
    await _emit_progress(
        progress_cb,
        progress=0,
        phase="queued",
        stats={"planned": requested, "resource_types": list(requested.keys())},
    )
    await _emit_progress(
        progress_cb,
        progress=1,
        phase="generating",
        stats={"planned": requested, "resource_types": list(requested.keys())},
    )
    _check_canceled(should_cancel)

    _configure_schema(schema_version, schema_path)
    ResourceGenerator, _, _, _ = _import_fhir_gen()
    generator = ResourceGenerator(seed=seed)

    generator.generate_many(list(requested.keys()), counts=requested)
    _check_canceled(should_cancel)

    for resource_type, scenario_id in scenarios:
        generator.generate_scenario(resource_type, scenario_id, register=True)
        _check_canceled(should_cancel)

    if variants:
        target_types = (
            [str(rt) for rt in variant_resources]
            if isinstance(variant_resources, list) and variant_resources
            else list(requested.keys())
        )
        for resource_type in target_types:
            generator.generate_variants(resource_type)
            _check_canceled(should_cancel)

    all_docs = generator.store.all_resources()
    all_counts = _count_resources_by_type(all_docs)
    generated = {rt: all_counts.get(rt, 0) for rt in requested}
    dependencies_auto_generated = {
        rt: count for rt, count in all_counts.items() if rt not in requested and count > 0
    }

    if watermark_enabled:
        all_docs = watermark.apply_watermark_many(all_docs, enabled=True)

    inserted: dict[str, int] = {}
    updated: dict[str, int] = {}
    persistence_report: dict[str, Any] | None = None
    if store_canonical and not dry_run and all_docs:
        await _emit_progress(
            progress_cb,
            progress=50,
            phase="saving",
            stats={"generated": generated, "total_documents": len(all_docs)},
        )
        persistence_report = await import_resources.fhir_import_resources(
            ctx,
            {
                "resources": all_docs,
                "validation_level": "base",
                "mode": "upsert",
                "fail_on_error": True,
            },
            progress_cb=progress_cb,
            should_cancel=should_cancel,
            provenance={
                "source": "synthetic",
                "operation": "synthetic_generate_batch",
                "job_id": (ctx.meta or {}).get("job_id"),
                "recipe": effective_payload.get("recipe") or effective_payload.get("generation_recipe"),
            },
        )
        if not persistence_report.get("committed"):
            raise KehrnelError(
                code="FHIR_GENERATED_RESOURCE_INVALID",
                status=500,
                message="Generated resources failed the mandatory FHIR persistence contract",
                details={"validation": persistence_report.get("validation") or {}},
            )
        by_type = ((persistence_report.get("write") or {}).get("by_resource_type") or {})
        inserted = {
            resource_type: int(values.get("inserted", 0))
            for resource_type, values in by_type.items()
        }
        updated = {
            resource_type: int(values.get("updated", 0))
            for resource_type, values in by_type.items()
        }

    result: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "plan_only": False,
        "recipe": effective_payload.get("recipe") or effective_payload.get("generation_recipe"),
        "generated": generated,
        "inserted": inserted,
        "updated": updated,
        "dependencies_auto_generated": dependencies_auto_generated,
        "database": database,
        "collections": sorted({bridge.collection_name(prefix, rt) for rt in all_counts}),
        "total_documents": len(all_docs),
        "schema_version": schema_version,
        "watermark_applied": watermark_enabled,
        "persistence_contract": "mandatory-search-compartments-indexes",
    }

    if persistence_report:
        result["search_projection"] = persistence_report.get("search_projection") or {}
        projected_by_type = (result["search_projection"].get("by_resource_type") or {})
        result["denormalized"] = {
            resource_type: {"processed": int(count), "failed": 0}
            for resource_type, count in projected_by_type.items()
        }
        result["indexes"] = persistence_report.get("indexes") or {}
        result["document_contract"] = persistence_report.get("document_contract") or {}

    await _emit_progress(
        progress_cb,
        progress=100,
        phase="completed",
        stats={"generated": generated, "inserted": inserted},
    )
    return result
