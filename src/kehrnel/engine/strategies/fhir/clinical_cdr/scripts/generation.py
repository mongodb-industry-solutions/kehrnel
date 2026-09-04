"""FHIR synthetic batch generation via fhir-gen."""

from __future__ import annotations

import copy
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
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import cohort_blueprints
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
        from fhir_gen.resolvers.reference import ReferenceStore
        from fhir_gen.schema.registry import SchemaRegistry
        from fhir_gen.schema.versions import resolve_schema_path

    return (
        ResourceGenerator,
        resolve_order,
        ReferenceStore,
        SchemaRegistry,
        resolve_schema_path,
    )


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
            code="JOB_CANCELED", status=499, message="Synthetic batch canceled by user"
        )


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


def _generation_schema_registry(
    schema_version: str | None, schema_path: str | None
):
    """Build a request-local registry so concurrent tenant releases cannot race."""
    _, _, _, SchemaRegistry, resolve_schema_path = _import_fhir_gen()
    path = resolve_schema_path(
        schema_version=schema_version,
        schema_path=Path(schema_path) if schema_path else None,
    )
    return SchemaRegistry(path), path


def _is_cohort_request(payload: dict[str, Any]) -> bool:
    cohort = payload.get("cohort")
    return (
        isinstance(cohort, dict)
        or "blueprint_id" in payload
        or "blueprint" in payload
    )


def _resource_key(resource: dict[str, Any]) -> tuple[str, str]:
    return str(resource.get("resourceType") or ""), str(resource.get("id") or "")


def _merge_conformance_reports(
    reports: list[dict[str, Any]],
) -> dict[str, Any]:
    removals: Counter[str] = Counter()
    resources_checked = 0
    for report in reports:
        resources_checked += int(report.get("resources_checked") or 0)
        removals.update(report.get("removals_by_path") or {})
    return {
        "passed": all(bool(report.get("passed")) for report in reports),
        "resources_checked": resources_checked,
        "optional_values_removed": sum(removals.values()),
        "removals_by_path": dict(removals.most_common(50)),
        "policy": "remove-invalid-optional-content;never-invent-required-content",
    }


def _filter_named_recipe_for_active_schema(
    cfg: dict[str, Any],
    original_payload: dict[str, Any],
    effective_payload: dict[str, Any],
    supported_resource_types: set[str],
) -> list[str]:
    """Omit release-incompatible recipe entries, never explicit user entries."""
    recipe_name = effective_payload.get("recipe") or effective_payload.get(
        "generation_recipe"
    )
    if not recipe_name:
        return []
    recipes = (cfg.get("generation") or {}).get("recipes") or {}
    recipe = recipes.get(str(recipe_name)) if isinstance(recipes, dict) else None
    recipe_resources = (recipe or {}).get("resources") or {}
    explicit_resources = original_payload.get("resources") or original_payload.get(
        "resource_counts"
    )
    explicit_types = (
        {str(resource_type) for resource_type in explicit_resources}
        if isinstance(explicit_resources, dict)
        else set()
    )
    omitted = sorted(
        str(resource_type)
        for resource_type in recipe_resources
        if str(resource_type) not in supported_resource_types
        and str(resource_type) not in explicit_types
    )
    resources = effective_payload.get("resources")
    if isinstance(resources, dict):
        effective_payload["resources"] = {
            str(resource_type): count
            for resource_type, count in resources.items()
            if str(resource_type) not in omitted
        }
    return omitted


async def _generate_cohort_documents(
    plan: dict[str, Any],
    blueprint: dict[str, Any],
    *,
    schema_path: Path,
    progress_cb: ProgressCallback | None,
    should_cancel: CancelCallback | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Generate isolated patient graphs while reusing shared directory assets."""
    ResourceGenerator, resolve_order, ReferenceStore, _, _ = _import_fhir_gen()
    seed = int(plan["cohort"]["seed"])
    shared_counts = plan["shared_resources"]
    shared_generator = ResourceGenerator(seed=seed, schema_path=schema_path)
    if shared_counts:
        shared_generator.generate_many(
            list(shared_counts), counts=shared_counts
        )
    shared_docs = shared_generator.store.all_resources()
    conformance_reports = [shared_generator.conformance_report()]
    shared_keys = {_resource_key(resource) for resource in shared_docs}

    all_docs: list[dict[str, Any]] = list(shared_docs)
    seen = set(shared_keys)
    patient_evidence: list[dict[str, Any]] = []
    patient_count = int(plan["cohort"]["patients"])
    count_iterator = cohort_blueprints.iter_patient_resource_counts(
        blueprint, patient_count, seed
    )
    progress_interval = max(1, patient_count // 20)

    for patient_index, requested in enumerate(count_iterator):
        _check_canceled(should_cancel)
        patient_store = ReferenceStore()
        for resource in shared_docs:
            patient_store.register(resource)
        generator = ResourceGenerator(
            seed=seed + (patient_index + 1) * 104_729,
            store=patient_store,
            schema_path=schema_path,
        )
        generator.generate("Patient", 1)
        for resource_type in resolve_order(
            [key for key, count in requested.items() if count > 0],
            generator.schema_registry,
        ):
            count = int(requested.get(resource_type, 0))
            if count:
                generator.generate(resource_type, count)
        conformance_reports.append(generator.conformance_report())

        patient_docs = [
            resource
            for resource in patient_store.all_resources()
            if _resource_key(resource) not in shared_keys
        ]
        evidence = cohort_blueprints.apply_patient_rules(
            patient_docs,
            blueprint,
            patient_index=patient_index,
            seed=seed,
            history_years=int(plan["cohort"]["history_years"]),
            reference_date=str(plan["cohort"]["reference_date"]),
        )
        patient_evidence.append(evidence)
        for resource in patient_docs:
            key = _resource_key(resource)
            if key not in seen:
                all_docs.append(resource)
                seen.add(key)

        if (
            patient_index == patient_count - 1
            or (patient_index + 1) % progress_interval == 0
        ):
            await _emit_progress(
                progress_cb,
                progress=min(45, 1 + round(44 * (patient_index + 1) / patient_count)),
                phase="generating-cohort",
                stats={
                    "blueprint_id": blueprint["id"],
                    "patients_generated": patient_index + 1,
                    "patients_planned": patient_count,
                    "documents_generated": len(all_docs),
                },
            )
    return (
        all_docs,
        patient_evidence,
        _merge_conformance_reports(conformance_reports),
    )


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
    cohort_plan: dict[str, Any] | None = None
    cohort_blueprint: dict[str, Any] | None = None
    if _is_cohort_request(effective_payload):
        cohort_plan = cohort_blueprints.compile_cohort_plan(ctx, effective_payload)
        cohort_blueprint, _ = cohort_blueprints.resolve_cohort_blueprint(
            effective_payload
        )
    schema_version = str(
        (cohort_plan or {}).get("release")
        or effective_payload.get("schema_version")
        or cfg.get("schema_version")
        or "R5"
    )
    schema_path = effective_payload.get("schema_path")

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
    schema_registry, resolved_schema_path = _generation_schema_registry(
        schema_version, schema_path
    )
    supported_generation_types = set(schema_registry.all_resources())
    omitted_recipe_resource_types: list[str] = []
    if cohort_plan:
        requested = bridge.parse_resources_payload(
            {"resources": cohort_plan["planned"]},
            known_resource_types=supported_generation_types,
        )
    else:
        omitted_recipe_resource_types = _filter_named_recipe_for_active_schema(
            cfg, payload, effective_payload, supported_generation_types
        )
        requested = bridge.parse_resources_payload(
            effective_payload,
            known_resource_types=supported_generation_types,
        )

    generation_cfg = cfg.get("generation") or {}
    seed = (
        cohort_plan["cohort"]["seed"]
        if cohort_plan
        else effective_payload.get("seed", generation_cfg.get("seed"))
    )
    dry_run = bool(effective_payload.get("dry_run", False))
    plan_only = bool(effective_payload.get("plan_only", False))
    store_canonical = bool(effective_payload.get("store_canonical", True))
    variants = bool(effective_payload.get("variants", False))
    variant_resources = effective_payload.get("variant_resources")
    scenarios = _parse_scenario_specs(effective_payload.get("scenarios"))

    if (
        cohort_plan
        and store_canonical
        and not dry_run
        and not cohort_plan["execution"]["persistable"]
    ):
        raise KehrnelError(
            code="FHIR_COHORT_NOT_PERSISTABLE",
            status=400,
            message=(
                "The cohort contains resource types without the mandatory active "
                "search/projection contract"
            ),
            details={
                "resource_types": cohort_plan["execution"][
                    "preview_only_resource_types"
                ]
            },
        )

    if effective_payload.get("denormalize_after") is False:
        raise KehrnelError(
            code="FHIR_PERSISTENCE_INVARIANT_REQUIRED",
            status=400,
            message="Stored synthetic FHIR resources must always be projected and indexed",
        )

    database = str(cfg["database"])
    prefix = str(cfg.get("collection_prefix") or "")
    generation_order = _import_fhir_gen()[1](
        list(requested.keys()), schema_registry
    )
    collection_names = [
        bridge.collection_name(prefix, rt)
        for rt in sorted(set(requested) | set(generation_order))
    ]

    if plan_only:
        result = {
            "ok": True,
            "plan_only": True,
            "dry_run": dry_run,
            "recipe": effective_payload.get("recipe")
            or effective_payload.get("generation_recipe"),
            "planned": requested,
            "generation_order": generation_order,
            "database": database,
            "collections": collection_names,
            "omitted_recipe_resource_types": omitted_recipe_resource_types,
        }
        if cohort_plan:
            result["cohort_plan"] = cohort_plan
        return result

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

    patient_evidence: list[dict[str, Any]] = []
    generation_conformance: dict[str, Any]
    if cohort_plan and cohort_blueprint:
        if scenarios or variants:
            raise KehrnelError(
                code="FHIR_COHORT_BLUEPRINT_INVALID",
                status=400,
                message="Cohort generation cannot be combined with variants or explicit scenarios",
            )
        all_docs, patient_evidence, generation_conformance = await _generate_cohort_documents(
            cohort_plan,
            cohort_blueprint,
            schema_path=resolved_schema_path,
            progress_cb=progress_cb,
            should_cancel=should_cancel,
        )
    else:
        ResourceGenerator, _, _, _, _ = _import_fhir_gen()
        generator = ResourceGenerator(seed=seed, schema_path=resolved_schema_path)

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
        generation_conformance = generator.conformance_report()
    all_counts = _count_resources_by_type(all_docs)
    generated = {rt: all_counts.get(rt, 0) for rt in requested}
    dependencies_auto_generated = {
        rt: count
        for rt, count in all_counts.items()
        if rt not in requested and count > 0
    }

    quality_report: dict[str, Any] | None = None
    if cohort_plan:
        all_docs, schema_evidence = (
            cohort_blueprints.conform_resources_to_base_schema(
                all_docs, schema_version.upper()
            )
        )
        all_counts = _count_resources_by_type(all_docs)
        generated = {rt: all_counts.get(rt, 0) for rt in requested}
        dependencies_auto_generated = {
            rt: count
            for rt, count in all_counts.items()
            if rt not in requested and count > 0
        }
        quality_report = cohort_blueprints.build_quality_report(
            all_docs,
            cohort_plan,
            patient_evidence,
            schema_evidence=schema_evidence,
        )

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
                "recipe": effective_payload.get("recipe")
                or effective_payload.get("generation_recipe"),
                "cohort_blueprint_id": (
                    cohort_plan["blueprint"]["id"] if cohort_plan else None
                ),
                "cohort_plan_digest": (
                    cohort_plan["plan_digest"] if cohort_plan else None
                ),
            },
        )
        if not persistence_report.get("committed"):
            raise KehrnelError(
                code="FHIR_GENERATED_RESOURCE_INVALID",
                status=500,
                message="Generated resources failed the mandatory FHIR persistence contract",
                details={"validation": persistence_report.get("validation") or {}},
            )
        by_type = (persistence_report.get("write") or {}).get("by_resource_type") or {}
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
        "recipe": effective_payload.get("recipe")
        or effective_payload.get("generation_recipe"),
        "generated": generated,
        "inserted": inserted,
        "updated": updated,
        "dependencies_auto_generated": dependencies_auto_generated,
        "database": database,
        "collections": sorted(
            {bridge.collection_name(prefix, rt) for rt in all_counts}
        ),
        "total_documents": len(all_docs),
        "schema_version": schema_version,
        "watermark_applied": watermark_enabled,
        "persistence_contract": "mandatory-search-compartments-indexes",
        "generation_conformance": generation_conformance,
        "omitted_recipe_resource_types": omitted_recipe_resource_types,
    }

    if cohort_plan:
        result["cohort_plan"] = cohort_plan
        result["quality_report"] = quality_report
        result["generation_level"] = cohort_plan["blueprint"]["maturity"]

    if bool(effective_payload.get("include_sample", False)):
        sample_limit = effective_payload.get("sample_limit", 25)
        if (
            not isinstance(sample_limit, int)
            or isinstance(sample_limit, bool)
            or not 1 <= sample_limit <= 100
        ):
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="sample_limit must be between 1 and 100",
            )
        result["sample_resources"] = copy.deepcopy(all_docs[:sample_limit])

    if persistence_report:
        result["search_projection"] = persistence_report.get("search_projection") or {}
        projected_by_type = result["search_projection"].get("by_resource_type") or {}
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
