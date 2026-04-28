from kehrnel.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.engine.strategies.openehr.rps_dual.query.projection_compiler import compile_projection


def test_projection_compiles_basic_fields():
    cfg = {
        "fields": {"composition": {"nodes": "cn", "path": "p"}, "search": {"nodes": "sn", "path": "p"}},
        "node_representation": {"path": {"token_joiner": "."}},
    }
    ir = AqlQueryIR(scope="patient", predicates=[], select=[{"path": "medication/time/value", "alias": "t"}])
    proj = compile_projection(ir, cfg, scope="patient")
    assert "$project" in proj
    assert "t" in proj["$project"]
    assert "$regexMatch" in proj["$project"]["t"]["$first"]["$map"]["input"]["$filter"]["cond"]
