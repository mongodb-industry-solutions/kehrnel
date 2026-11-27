from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_coding_opts,
    build_flattener_config,
    normalize_bulk_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    BULK_DEFAULTS_PATH as BASE_BULK_DEFAULTS_PATH,
    BULK_SCHEMA_PATH as BASE_BULK_SCHEMA_PATH,
    RPSDualStrategy,
    load_json,
)
from kehrnel.engine.strategies.openehr.rps_dual_ibm.ingest.flattener import IBMCompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual_ibm.ingest.unflattener import IBMCompositionUnflattener


MANIFEST_PATH = Path(__file__).parent / "manifest.json"
SCHEMA_PATH = Path(__file__).parent / "schema.json"
DEFAULTS_PATH = Path(__file__).parent / "defaults.json"

MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))


class RPSDualIBMStrategy(RPSDualStrategy):
    """IBM-exact variant of the openEHR RPS dual strategy."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        super().__init__(manifest)
        self.strategy_base_dir = Path(__file__).parent
        self.schema = load_json(SCHEMA_PATH)
        self.defaults = load_json(DEFAULTS_PATH)
        self.bulk_schema = load_json(BASE_BULK_SCHEMA_PATH) if BASE_BULK_SCHEMA_PATH.exists() else {}
        self.bulk_defaults = load_json(BASE_BULK_DEFAULTS_PATH) if BASE_BULK_DEFAULTS_PATH.exists() else {}
        self.manifest.config_schema = self.schema
        self.manifest.default_config = self.defaults
        self.normalized_config = None
        self.normalized_bulk_config = None

    async def _build_flattener_for_context(self, ctx: StrategyContext) -> IBMCompositionFlattener:
        cfg = ctx.config or {}
        storage = (ctx.adapters or {}).get("storage")
        strategy_cfg = normalize_config(cfg)
        bulk_cfg = normalize_bulk_config((ctx.meta or {}).get("bulk_config", {}))
        flattener_config = build_flattener_config(strategy_cfg, bulk_cfg)
        coding_cfg = build_coding_opts(strategy_cfg)
        mappings_content = await self._resolve_mappings_content(ctx, strategy_cfg)

        mappings_path = str(
            Path(__file__).parents[1] / "rps_dual" / "ingest" / "config" / "flattener_mappings_f.jsonc"
        )

        return await IBMCompositionFlattener.create(
            db=getattr(storage, "db", None),
            config=flattener_config,
            mappings_path=mappings_path,
            mappings_content=mappings_content,
            field_map=None,
            coding_opts=coding_cfg,
        )

    async def reverse_transform(self, ctx: StrategyContext, payload: Dict[str, Any]) -> Dict[str, Any]:
        strategy_cfg = normalize_config(ctx.config or {})
        bulk_cfg = normalize_bulk_config({"role": "secondary", **(ctx.meta or {}).get("bulk_config", {})})

        flattener_config = build_flattener_config(strategy_cfg, bulk_cfg)
        coding_cfg = build_coding_opts(strategy_cfg)
        mappings_path = str(
            Path(__file__).parents[1] / "rps_dual" / "ingest" / "config" / "flattener_mappings_f.jsonc"
        )

        unflattener = await IBMCompositionUnflattener.create(
            db=getattr((ctx.adapters or {}).get("storage"), "db", None),
            config=flattener_config,
            mappings_path=mappings_path,
            mappings_content=(ctx.meta or {}).get("mappings") if ctx and ctx.meta else None,
            coding_opts=coding_cfg,
        )
        base_doc = payload.get("base") if isinstance(payload, dict) else payload
        if not isinstance(base_doc, dict):
            raise ValueError("Payload must include flattened base document under 'base'")
        return {"composition": unflattener.unflatten(base_doc)}


    def _coerce_ibm_config(self, config: dict[str, Any] | None) -> dict[str, Any]:
        coerced = deepcopy(config or self.defaults)
        fields = coerced.setdefault("fields", {})
        document_fields = fields.setdefault("document", {})
        version_field = document_fields.get("v")
        comp_id_field = document_fields.get("comp_id")
        if comp_id_field in (None, "", "comp_id", "version", version_field):
            document_fields["comp_id"] = "_id"
        node_fields = fields.setdefault("node", {})
        if node_fields.get("pi") in (None, "", "pi"):
            node_fields["pi"] = "li"
        return coerced



    def _coerce_ibm_context(self, ctx: StrategyContext) -> StrategyContext:
        return StrategyContext(
            environment_id=ctx.environment_id,
            config=self._coerce_ibm_config(ctx.config),
            bindings=ctx.bindings,
            adapters=ctx.adapters,
            manifest=ctx.manifest,
            trace_id=ctx.trace_id,
            logger=ctx.logger,
            meta=ctx.meta,
        )



    async def validate_config(self, ctx: StrategyContext | dict[str, Any]) -> None:
        if isinstance(ctx, StrategyContext):
            return await super().validate_config(self._coerce_ibm_context(ctx))
        return await super().validate_config(self._coerce_ibm_config(ctx))



    async def plan(self, ctx: StrategyContext):
        return await super().plan(self._coerce_ibm_context(ctx))



    async def apply(self, ctx: StrategyContext, plan):
        return await super().apply(self._coerce_ibm_context(ctx), plan)



    async def compile_query(self, ctx: StrategyContext, domain: str, query: dict[str, Any]):
        return await super().compile_query(self._coerce_ibm_context(ctx), domain, query)



    async def run_op(self, ctx: StrategyContext, op: str, payload: dict[str, Any]):
        return await super().run_op(self._coerce_ibm_context(ctx), op, payload)
