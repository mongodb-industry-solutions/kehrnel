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
from kehrnel.engine.strategies.fhir.clinical_cdr._paths import SPEC_DIR
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import denormalize
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import generation
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import indexes
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import query as fhir_query
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import stats as fhir_stats_mod


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST_PATH = SPEC_DIR / "manifest.json"
SCHEMA_PATH = SPEC_DIR / "schema.json"
DEFAULTS_PATH = SPEC_DIR / "defaults.json"

MANIFEST = StrategyManifest(**_load_json(MANIFEST_PATH))

_KNOWN_OPS = frozenset(
    {
        "synthetic_generate_batch",
        "fhir_denormalize",
        "fhir_ensure_indexes",
        "fhir_search",
        "fhir_list_search_params",
        "fhir_stats",
        "negotiate_fhir_search",
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
        bridge.resolve_strategy_config(
            StrategyContext(
                environment_id="",
                config=raw_config or {},
                manifest=self.manifest,
            )
        )
        return True

    async def plan(self, ctx: StrategyContext) -> ApplyPlan:
        raise NotImplementedError("fhir.clinical_cdr plan not implemented")

    async def apply(self, ctx: StrategyContext, plan: ApplyPlan) -> ApplyResult:
        raise NotImplementedError("fhir.clinical_cdr apply not implemented")

    async def transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        raise NotImplementedError("fhir.clinical_cdr transform not implemented")

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> TransformResult:
        raise NotImplementedError("fhir.clinical_cdr reverse_transform not implemented")

    async def ingest(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("fhir.clinical_cdr ingest not implemented")

    async def compile_query(self, ctx: StrategyContext, domain: str, query: Dict[str, Any]) -> QueryPlan:
        return await fhir_query.compile_fhir_query(ctx, domain or "fhir", query)

    async def execute_query(self, ctx: StrategyContext, plan: QueryPlan) -> QueryResult:
        return await fhir_query.execute_fhir_query(ctx, plan)

    async def run_op(self, ctx: StrategyContext, op: str, payload: Dict[str, Any]) -> Dict[str, Any]:
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

        if op == "fhir_search":
            return await fhir_query.fhir_search(ctx, payload)

        if op == "fhir_list_search_params":
            return fhir_query.fhir_list_search_params(ctx, payload)

        if op == "fhir_stats":
            return await fhir_stats_mod.fhir_stats(ctx, payload)

        raise NotImplementedError(f"fhir.clinical_cdr op '{op}' not implemented")
