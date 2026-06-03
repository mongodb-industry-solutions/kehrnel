"""Contract tests for fhir.rps_canonical compile_query / execute_query."""

from __future__ import annotations

import os

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import QueryPlan, StrategyContext
from kehrnel.engine.strategies.fhir.rps_canonical import query as fhir_query
from kehrnel.engine.strategies.fhir.rps_canonical.denormalize import fhir_denormalize
from kehrnel.engine.strategies.fhir.rps_canonical.generation import synthetic_generate_batch
from kehrnel.engine.strategies.fhir.rps_canonical.strategy import FHIRRPSCanonicalStrategy, MANIFEST


def _ctx(*, config: dict | None = None) -> StrategyContext:
    return StrategyContext(
        environment_id="test-env",
        config=config or {"database": "fhir_test"},
        bindings={
            "db": {
                "provider": "mongodb",
                "uri": "mongodb://localhost:27017",
                "database": "fhir_test",
            }
        },
        manifest=MANIFEST,
    )


fhir_mql = pytest.importorskip("fhir_search_to_mql")


def test_criteria_dict_to_query_string():
    assert fhir_query.criteria_dict_to_query_string({"gender": "female"}) == "gender=female"
    assert "family=Smith" in fhir_query.criteria_dict_to_query_string(
        {"family": "Smith", "birthdate": "gt1990-01-01"},
    )


def test_parse_fhir_search_resource_question_form():
    rt, qs = fhir_query.parse_fhir_search("Patient?gender=female&active=true")
    assert rt == "Patient"
    assert "gender=female" in qs


@pytest.mark.asyncio
async def test_compile_fhir_query_patient_gender_female():
    plan = await fhir_query.compile_fhir_query(
        _ctx(),
        "fhir",
        {"resource_type": "Patient", "criteria": {"gender": "female"}},
    )
    assert plan.engine == "fhir_mql"
    assert plan.plan["collection"] == "Patient"
    assert plan.plan["resource_type"] == "Patient"
    filt = plan.plan["filter"]
    assert isinstance(filt, dict)
    assert filt  # non-empty MQL for gender=female
    assert plan.explain
    assert plan.explain.get("engine") == "fhir_mql"
    assert plan.explain.get("domain") == "fhir"


@pytest.mark.asyncio
async def test_compile_fhir_query_via_fhir_search_string():
    plan = await fhir_query.compile_fhir_query(
        _ctx(),
        "fhir",
        {"fhir_search": "Patient?gender=female"},
    )
    assert plan.plan["resource_type"] == "Patient"
    assert plan.plan["filter"]


@pytest.mark.asyncio
async def test_compile_fhir_query_unconfigured_resource():
    with pytest.raises(KehrnelError) as exc:
        await fhir_query.compile_fhir_query(
            _ctx(),
            "fhir",
            {"resource_type": "NotAConfiguredResourceType", "criteria": {"id": "x"}},
        )
    assert exc.value.code == "FHIR_SEARCH_NOT_CONFIGURED"


@pytest.mark.asyncio
async def test_strategy_compile_query_patient():
    strat = FHIRRPSCanonicalStrategy(MANIFEST)
    plan = await strat.compile_query(
        _ctx(),
        "fhir",
        {"resource_type": "Patient", "criteria": {"gender": "female"}, "_count": 20},
    )
    assert plan.engine == "fhir_mql"
    assert plan.plan["query_input"]["_count"] == 20


def test_pagination_from_plan_count_and_offset():
    limit, skip = fhir_query.pagination_from_plan(
        {"query_input": {"_count": 20, "_offset": 5}},
    )
    assert limit == 20
    assert skip == 5


def test_coerce_query_plan_runtime_shape():
    qp = fhir_query._coerce_query_plan(
        {
            "engine": "fhir_mql",
            "plan": {
                "filter": {"_search.gender": "female"},
                "collection": "Patient",
            },
        },
    )
    assert qp.engine == "fhir_mql"
    assert qp.plan["collection"] == "Patient"


@pytest.mark.asyncio
async def test_execute_fhir_query_requires_plan():
    with pytest.raises(KehrnelError) as exc:
        await fhir_query.execute_fhir_query(_ctx(), None)
    assert exc.value.code == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_compile_and_execute_strategy_wiring():
    strat = FHIRRPSCanonicalStrategy(MANIFEST)
    plan = await strat.compile_query(
        _ctx(),
        "fhir",
        {"resource_type": "Patient", "criteria": {"gender": "female"}},
    )
    assert isinstance(plan, QueryPlan)
    # execute without Mongo would fail bindings — only assert plan shape here
    assert plan.plan["filter"]


@pytest.mark.skipif(
    not os.getenv("FHIR_QUERY_INTEGRATION"),
    reason="Set FHIR_QUERY_INTEGRATION=1 after local MongoDB is available",
)
@pytest.mark.asyncio
async def test_execute_query_patient_gender_female_returns_rows():
    db_name = os.getenv("FHIR_TEST_DB", "fhir_kehrnel_query_test")
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    ctx = _ctx(
        config={"database": db_name},
        bindings={
            "db": {
                "provider": "mongodb",
                "uri": uri,
                "database": db_name,
            }
        },
    )

    gen = await synthetic_generate_batch(
        ctx,
        {
            "resources": {"Patient": 8},
            "seed": 42,
            "store_canonical": True,
            "denormalize_after": True,
        },
    )
    assert gen["ok"] is True

    denorm = await fhir_denormalize(ctx, {"resource_types": ["Patient"]})
    assert denorm["ok"] is True

    strat = FHIRRPSCanonicalStrategy(MANIFEST)
    plan = await strat.compile_query(
        ctx,
        "fhir",
        {"resource_type": "Patient", "criteria": {"gender": "female"}, "_count": 50},
    )
    result = await strat.execute_query(ctx, plan)
    assert result.engine_used == "fhir_mql"
    assert len(result.rows) > 0
    assert result.explain
    assert result.explain.get("total", 0) >= len(result.rows)
    assert all(row.get("resourceType") == "Patient" for row in result.rows)
