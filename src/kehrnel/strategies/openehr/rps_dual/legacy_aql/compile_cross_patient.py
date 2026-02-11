"""Legacy cross-patient compiler wrapper."""
from __future__ import annotations

from typing import Any, Dict

from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.strategies.openehr.rps_dual.query.compiler_atlas_search import compile_atlas_search
from kehrnel.strategies.openehr.rps_dual.query.projection_compiler import compile_projection


def compile_cross_patient(ir: AqlQueryIR, cfg: Dict[str, Any], dicts: Dict[str, Any]) -> Dict[str, Any]:
    shortcuts = dicts.get("shortcuts", {}).get("items")
    plan = compile_atlas_search(ir, cfg, shortcuts=shortcuts)
    projection = compile_projection(ir, cfg, scope="cross_patient", after_lookup=plan.get("lookup_full_composition"), shortcuts=shortcuts)
    pipeline = plan.get("pipeline", [])[:]
    if projection:
        pipeline.append(projection)
    plan["engine"] = "atlas_search_dual"
    plan["pipeline"] = pipeline
    return plan
