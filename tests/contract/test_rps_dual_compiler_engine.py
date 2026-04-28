from pathlib import Path

import pytest
from kehrnel.core.types import StrategyContext
from kehrnel.domains.openehr.aql.parse import parse_aql
from kehrnel.engine.strategies.openehr.rps_dual.strategy import DEFAULTS_PATH, MANIFEST, RPSDualStrategy, load_json
from tests.helpers.fixture_storage import FixtureStorage


@pytest.mark.asyncio
async def test_patient_query_uses_current_patient_pipeline():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    ir = parse_aql("select Centro as Centro from compositions")
    ir.scope = "patient"
    ir.select = [
        {
            "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
            "alias": "Centro",
            "agg": "first",
        }
    ]
    ir.predicates = [
        {"path": "ehr_id", "op": "eq", "value": "p1"},
        {"path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string", "op": "eq", "value": "VAL1"},
        {"path": "med_ac/time/value", "op": "gt", "value": "2020-01-01T00:00:00"},
    ]
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert plan.engine == "mongo_pipeline"
    assert plan.explain["engine"] == "query_engine"
    assert pipeline and "$match" in pipeline[0]
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project is not None
    centro_proj = project["$project"]["Centro"]
    assert centro_proj in {"$data.v.df.cs", "$data.df.cs", "$data.value.defining_code.code_string"}
    assert plan.explain["builder"]["chosen"] == "pipeline_builder"


@pytest.mark.asyncio
async def test_cross_patient_query_uses_current_cross_patient_search_pipeline():
    cfg = load_json(DEFAULTS_PATH)
    storage = FixtureStorage(Path("tests/fixtures/rps_dual"))
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage})
    ir = parse_aql("select Centro as Centro from compositions where text = 'hello'")
    ir.scope = "cross_patient"
    ir.select = [
        {
            "path": "admin_salut/items[at0007]/items[at0014]/value/defining_code/code_string",
            "alias": "Centro",
            "agg": "first",
        }
    ]
    ir.predicates = [
        {"path": "text", "op": "eq", "value": "hello"},
        {"path": "med_ac/time/value", "op": "gt", "value": "2020-01-01T00:00:00"},
    ]
    plan = await strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert plan.engine == "text_search_dual"
    assert plan.explain["engine"] == "query_engine"
    assert pipeline and "$search" in pipeline[0]
    search_stage = pipeline[0]["$search"]
    assert "compound" in search_stage or "text" in search_stage
    project = next((stage for stage in pipeline if "$project" in stage), None)
    assert project is not None
    centro_proj = project["$project"]["Centro"]
    if isinstance(centro_proj, dict):
        preferred = centro_proj["$ifNull"][0] if "$ifNull" in centro_proj else centro_proj
        assert preferred["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["input"] == "$$node.p"
    else:
        assert isinstance(centro_proj, str)
    assert plan.explain["builder"]["chosen"] == "search_pipeline_builder"


def test_no_new_compiler_imports_in_rps_dual_stack():
    roots = [
        Path("src/kehrnel/engine/strategies/openehr/rps_dual/strategy.py"),
    ]
    roots.extend(Path("src/kehrnel/engine/strategies/openehr/rps_dual/query").rglob("*.py"))
    offenders = []
    for path in roots:
        text = path.read_text(encoding="utf-8")
        if "compiler_match" in text or "compiler_atlas_search" in text:
            offenders.append(path)
    assert not offenders, f"Forbidden compiler imports detected: {offenders}"
