import pytest

from kehrnel.core.types import StrategyContext
from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.strategies.openehr.rps_dual.query.projection_compiler import compile_projection


@pytest.mark.asyncio
async def test_projection_uses_lookup_root():
    cfg = {
        "fields": {"composition": {"nodes": "cn", "path": "p"}, "search": {"nodes": "sn", "path": "p"}},
        "node_representation": {"path": {"token_joiner": "."}},
    }
    ir = AqlQueryIR(scope="cross_patient", predicates=[], select=[{"path": "medication/time/value", "alias": "t"}])
    proj = compile_projection(ir, cfg, scope="cross_patient", after_lookup=True)
    assert "$project" in proj
    tproj = proj["$project"]["t"]["$first"]["$map"]["input"]["$filter"]["input"]
    # ensure it references comp.cn path when lookup is present
    assert isinstance(tproj, str) and tproj.startswith("$comp.cn")
