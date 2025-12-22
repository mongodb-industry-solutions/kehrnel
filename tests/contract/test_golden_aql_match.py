from kehrnel.protocols.openehr.aql.ir import AqlQueryIR
from kehrnel.protocols.openehr.aql.parse import parse_aql
from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.core.types import StrategyContext


def test_patient_aql_compiles_to_match_first():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg)
    ir = parse_aql("select x as x from compositions where ehr_id = 'p1'")
    plan = strat.compile_query(ctx, "openehr", ir.to_dict())
    pipeline = plan.plan.get("pipeline", [])
    assert pipeline, "pipeline expected"
    assert "$match" in pipeline[0], "patient should start with $match"
    # ensure projection is appended when select present
    assert any("$project" in stage for stage in pipeline), "projection expected for select"
