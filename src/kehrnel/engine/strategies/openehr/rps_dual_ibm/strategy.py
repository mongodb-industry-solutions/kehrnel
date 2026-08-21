from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from kehrnel.engine.core.manifest import StrategyManifest
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy


MANIFEST_PATH = Path(__file__).parent / "manifest.json"
SCHEMA_PATH = Path(__file__).parent / "schema.json"
DEFAULTS_PATH = Path(__file__).parent / "defaults.json"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST = StrategyManifest(**load_json(MANIFEST_PATH))
MANIFEST.config_schema = load_json(SCHEMA_PATH)
MANIFEST.default_config = load_json(DEFAULTS_PATH)


class RPSDualIBMStrategy(RPSDualStrategy):
    """Compatibility wrapper for environments activated as openehr.rps_dual_ibm."""

    def __init__(self, manifest: StrategyManifest = MANIFEST):
        super().__init__(manifest)
        self.schema = load_json(SCHEMA_PATH)
        self.defaults = load_json(DEFAULTS_PATH)
        self.manifest.config_schema = self.schema
        self.manifest.default_config = self.defaults

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

    async def transform(self, ctx: StrategyContext, payload: dict[str, Any]):
        return await super().transform(self._coerce_ibm_context(ctx), payload)

    async def ingest(self, ctx: StrategyContext, payload: dict[str, Any]):
        return await super().ingest(self._coerce_ibm_context(ctx), payload)

    async def reverse_transform(self, ctx: StrategyContext, payload: dict[str, Any]):
        return await super().reverse_transform(self._coerce_ibm_context(ctx), payload)

    async def compile_query(self, ctx: StrategyContext, domain: str, query: dict[str, Any]):
        return await super().compile_query(self._coerce_ibm_context(ctx), domain, query)

    async def run_op(self, ctx: StrategyContext, op: str, payload: dict[str, Any]):
        return await super().run_op(self._coerce_ibm_context(ctx), op, payload)
