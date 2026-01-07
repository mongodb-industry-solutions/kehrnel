"""Patient compiler wrapper using the vendored transformer stack."""
from __future__ import annotations

from typing import Any, Dict

from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.strategies.openehr.rps_dual.config import normalize_config
from kehrnel.strategies.openehr.rps_dual.query.compiler import build_query_pipeline


async def compile_patient(ir: AqlQueryIR, cfg: Dict[str, Any], dicts: Dict[str, Any]) -> Dict[str, Any]:
    cfg_model = normalize_config(cfg)
    engine, pipeline, stage0, schema_cfgs, ast_doc, builder_info = await build_query_pipeline(ir, cfg_model)
    return {
        "engine": engine,
        "pipeline": pipeline,
        "dicts": dicts,
        "warnings": [],
        "explain": {"stage0": stage0, "schema": schema_cfgs, "ast": ast_doc, "builder": builder_info},
    }
