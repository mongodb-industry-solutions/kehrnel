"""
FHIR Clinical CDR strategy pack.

Integrates fhir-gen (synthetic generation) and fhir-mql (search → MQL). Ops are
implemented incrementally; the bridge module resolves config and Mongo bindings.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.plugin import StrategyPlugin
from kehrnel.engine.core.types import (
    ApplyPlan,
    ApplyResult,
    QueryPlan,
    QueryResult,
    StrategyContext,
    TransformResult,
)
from kehrnel.engine.domains.fhir import implementation_guides
from kehrnel.engine.strategies.fhir.clinical_cdr._paths import SPEC_DIR
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import (
    bridge,
    cohort_blueprints,
    denormalize,
    generation,
    import_resources,
    indexes,
    migration_runs,
    profile_validation,
    resource_catalog,
    semantic,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import query as fhir_query
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import stats as fhir_stats_mod
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.capabilities import (
    resolve_resource_capabilities,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.index_manifest import (
    DEFAULT_MANAGED_INDEX_BUDGET,
    build_index_manifest,
)


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST_PATH = SPEC_DIR / "manifest.json"
SCHEMA_PATH = SPEC_DIR / "schema.json"
DEFAULTS_PATH = SPEC_DIR / "defaults.json"

MANIFEST = StrategyManifest(**_load_json(MANIFEST_PATH))

_KNOWN_OPS = frozenset(
    {
        "synthetic_generate_batch",
        "fhir_cohort_catalog",
        "fhir_cohort_plan",
        "fhir_denormalize",
        "fhir_ensure_indexes",
        "fhir_index_manifest",
        "fhir_search",
        "fhir_list_search_params",
        "fhir_resource_catalog",
        "fhir_capabilities",
        "fhir_support_matrix",
        "fhir_stats",
        "fhir_import_resources",
        "fhir_migration_start",
        "fhir_migration_import_chunk",
        "fhir_migration_list",
        "fhir_migration_get",
        "fhir_migration_cancel",
        "fhir_reference_integrity",
        "fhir_compile_implementation_guides",
        "fhir_semantic_preview",
        "fhir_semantic_materialize",
        "fhir_semantic_search",
    }
)


class FHIRClinicalCDRStrategy(StrategyPlugin):
    """Canonical per-resource-type FHIR storage with fhir-gen / fhir-mql integration."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        self.manifest = manifest
        if SCHEMA_PATH.exists():
            self.manifest.config_schema = _load_json(SCHEMA_PATH)
        if DEFAULTS_PATH.exists():
            self.manifest.default_config = bridge.load_pack_defaults()

    async def validate_config(self, ctx: StrategyContext | Dict[str, Any]) -> bool:
        raw_config = ctx.config if isinstance(ctx, StrategyContext) else ctx
        cfg = bridge.resolve_strategy_config(
            StrategyContext(
                environment_id="",
                config=raw_config or {},
                manifest=self.manifest,
            )
        )
        inspected = implementation_guides.inspect_configured_implementation_guides(cfg)
        implementation_guides.resolve_active_profiles(cfg, inspected)
        profile_validation.validate_profile_config(
            cfg, ctx.adapters if isinstance(ctx, StrategyContext) else None
        )
        semantic.validate_semantic_config(cfg)
        return True

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        """Discover which resource types need indexes ensured."""
        try:
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
                resource_types = sorted(
                    resolve_resource_capabilities(cfg, mql_ctx.config_loader).storable
                )
                index_budget = int(
                    (cfg.get("index_policy") or {}).get(
                        "max_managed_indexes_per_collection",
                        DEFAULT_MANAGED_INDEX_BUDGET,
                    )
                )
                index_plan = build_index_manifest(
                    mql_ctx.config_loader,
                    prefix,
                    resource_types=resource_types,
                    max_managed_indexes_per_collection=index_budget,
                )
            finally:
                bridge.close_mql_context(mql_ctx)
            ig_plan = implementation_guides.inspect_configured_implementation_guides(
                cfg
            )
            active_profiles = implementation_guides.resolve_active_profiles(
                cfg, ig_plan
            )
            semantic_plan = semantic.describe_semantic_config(cfg, ctx.adapters)
            materialize_indexes = bool(
                (cfg.get("index_policy") or {}).get("materialize_on_activation", False)
            )
            return ApplyPlan(
                artifacts={
                    "resource_types": resource_types if materialize_indexes else [],
                    "available_index_resource_types": resource_types,
                    "database": database,
                    "collection_prefix": prefix or "",
                    "index_mode": "activation"
                    if materialize_indexes
                    else "before-write",
                    "index_manifest": {
                        "manifest_version": index_plan["manifest_version"],
                        "digest": index_plan["digest"],
                        "resource_count": index_plan["resource_count"],
                        "managed_index_count": index_plan["managed_index_count"],
                        "within_budget": index_plan["within_budget"],
                        "violations": index_plan["violations"],
                    },
                    "conformance_mode": "implementation-guide-overlay"
                    if ig_plan
                    else "fhir-core",
                    "implementation_guides": [
                        {
                            "compiled_id": item["compiled_id"],
                            "package": item["package"],
                            "evidence": item["evidence"],
                        }
                        for item in ig_plan
                    ],
                    "active_profiles": active_profiles,
                    "semantic": semantic_plan,
                }
            )
        except Exception as exc:
            # Return a minimal plan so apply() can still attempt index creation
            return ApplyPlan(
                artifacts={
                    "resource_types": [],
                    "action": "ensure_indexes",
                    "plan_error": str(exc),
                }
            )

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        """Ensure MongoDB indexes for all FHIR resource types discovered in plan()."""
        # plan arrives as a plain dict when called via runtime dispatch (_to_dict conversion)
        if isinstance(plan, dict):
            artifacts = plan.get("artifacts") or {}
        else:
            artifacts = (plan.artifacts if plan else {}) or {}

        resource_types = artifacts.get("resource_types") or []
        index_plan = artifacts.get("index_manifest") or {}
        if index_plan and not index_plan.get("within_budget", False):
            raise ValueError(
                "FHIR managed index plan exceeds its configured per-collection budget"
            )
        cfg = bridge.resolve_strategy_config(ctx)
        compiled_igs = implementation_guides.compile_configured_implementation_guides(
            cfg
        )
        if resource_types:
            result = await indexes.fhir_ensure_indexes(
                ctx, {"resource_types": resource_types}
            )
        else:
            result = {"indexes": [], "skipped": [], "warnings": []}

        index_entries = result.get("indexes", [])
        return ApplyResult(
            created=[
                e["collection"] for e in index_entries if e.get("status") == "created"
            ]
            + [item["output"] for item in compiled_igs],
            updated=[
                e["collection"] for e in index_entries if e.get("status") == "exists"
            ],
            skipped=result.get("skipped", []),
            warnings=result.get("warnings", []),
        )

    async def transform(
        self, ctx: StrategyContext, payload: Dict[str, Any]
    ) -> TransformResult:
        raise NotImplementedError("fhir.clinical_cdr transform not implemented")

    async def reverse_transform(
        self, ctx: StrategyContext, payload: Dict[str, Any]
    ) -> TransformResult:
        raise NotImplementedError("fhir.clinical_cdr reverse_transform not implemented")

    async def ingest(
        self, ctx: StrategyContext, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Route the universal Kehrnel ingest surface through the FHIR import contract.

        The FHIR-specific op accepts an envelope, while the generic SDK/CLI ingest
        command commonly supplies a raw resource or Bundle.  Normalizing both here
        keeps the manifest's ``ingest`` capability truthful without creating a
        second persistence path.
        """
        normalized = dict(payload or {})
        if normalized.get("resourceType") == "Bundle":
            normalized = {"bundle": normalized}
        elif normalized.get("resourceType"):
            normalized = {"resource": normalized}
        elif isinstance(normalized.get("documents"), list):
            # ``kehrnel run ingest --set file_path=...`` expands JSON/NDJSON
            # files to the SDK-neutral ``documents`` key.
            normalized["resources"] = normalized.pop("documents")
        return await import_resources.fhir_import_resources(ctx, normalized)

    async def compile_query(
        self, ctx: StrategyContext, domain: str, query: Dict[str, Any]
    ) -> QueryPlan:
        return await fhir_query.compile_fhir_query(ctx, domain or "fhir", query)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        return await fhir_query.execute_fhir_query(ctx, plan)

    async def run_op(
        self, ctx: StrategyContext, op: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        if op not in _KNOWN_OPS:
            raise ValueError(f"Strategy op '{op}' not supported")

        meta = ctx.meta or {}
        progress_cb = meta.get("progress_cb")
        should_cancel = meta.get("should_cancel")

        if op == "synthetic_generate_batch":
            return await generation.synthetic_generate_batch(
                ctx,
                payload,
                progress_cb=progress_cb,
                should_cancel=should_cancel,
            )

        if op == "fhir_cohort_catalog":
            return cohort_blueprints.fhir_cohort_catalog(ctx, payload)

        if op == "fhir_cohort_plan":
            return cohort_blueprints.fhir_cohort_plan(ctx, payload)

        if op == "fhir_denormalize":
            return await denormalize.fhir_denormalize(
                ctx,
                payload,
                progress_cb=progress_cb,
                should_cancel=should_cancel,
            )

        if op == "fhir_ensure_indexes":
            return await indexes.fhir_ensure_indexes(
                ctx,
                payload,
                progress_cb=progress_cb,
                should_cancel=should_cancel,
            )

        if op == "fhir_index_manifest":
            return indexes.fhir_index_manifest(ctx, payload)

        if op == "fhir_search":
            return await fhir_query.fhir_search(ctx, payload)

        if op == "fhir_list_search_params":
            return fhir_query.fhir_list_search_params(ctx, payload)

        if op == "fhir_resource_catalog":
            return resource_catalog.fhir_resource_catalog(ctx, payload)

        if op == "fhir_capabilities":
            return fhir_query.fhir_capabilities(ctx, payload)

        if op == "fhir_support_matrix":
            return fhir_query.fhir_support_matrix(ctx, payload)

        if op == "fhir_stats":
            return await fhir_stats_mod.fhir_stats(ctx, payload)

        if op == "fhir_import_resources":
            return await import_resources.fhir_import_resources(
                ctx,
                payload,
                progress_cb=progress_cb,
                should_cancel=should_cancel,
            )

        if op == "fhir_migration_start":
            return await migration_runs.fhir_migration_start(ctx, payload)

        if op == "fhir_migration_import_chunk":
            return await migration_runs.fhir_migration_import_chunk(
                ctx,
                payload,
                should_cancel=should_cancel,
            )

        if op == "fhir_migration_list":
            return await migration_runs.fhir_migration_list(ctx, payload)

        if op == "fhir_migration_get":
            return await migration_runs.fhir_migration_get(ctx, payload)

        if op == "fhir_migration_cancel":
            return await migration_runs.fhir_migration_cancel(ctx, payload)

        if op == "fhir_reference_integrity":
            return await migration_runs.fhir_reference_integrity(ctx, payload)

        if op == "fhir_compile_implementation_guides":
            cfg = bridge.resolve_strategy_config(ctx)
            packages = implementation_guides.compile_configured_implementation_guides(
                cfg
            )
            return {
                "ok": True,
                "compiler_version": implementation_guides.COMPILER_VERSION,
                "conformance_mode": "implementation-guide-overlay"
                if packages
                else "fhir-core",
                "packages": packages,
            }

        if op == "fhir_semantic_preview":
            return semantic.fhir_semantic_preview(ctx, payload)

        if op == "fhir_semantic_materialize":
            return await semantic.fhir_semantic_materialize(ctx, payload)

        if op == "fhir_semantic_search":
            return await semantic.fhir_semantic_search(ctx, payload)

        raise NotImplementedError(f"fhir.clinical_cdr op '{op}' not implemented")
