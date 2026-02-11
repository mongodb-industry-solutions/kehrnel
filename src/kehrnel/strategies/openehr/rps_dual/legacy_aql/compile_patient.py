"""Legacy patient compiler wrapper.

For now this delegates to the current patient compiler to preserve behavior while
we stage the full legacy migration. The interface matches the intended legacy
contract so we can swap in the migrated modules with minimal churn.
"""
from __future__ import annotations

from typing import Any, Dict

from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.strategies.openehr.rps_dual.query.compiler_match import compile_match
from kehrnel.strategies.openehr.rps_dual.query.projection_compiler import compile_projection


def compile_patient(ir: AqlQueryIR, cfg: Dict[str, Any], dicts: Dict[str, Any]) -> Dict[str, Any]:
    shortcuts = dicts.get("shortcuts", {}).get("items")
    plan = compile_match(ir, cfg, shortcuts=shortcuts)
    projection = compile_projection(ir, cfg, scope="patient", after_lookup=False, shortcuts=shortcuts)
    pipeline = plan.get("pipeline", [])[:]
    if projection:
        pipeline.append(projection)
    plan["engine"] = "mongo_pipeline"
    plan["pipeline"] = pipeline
    return plan
