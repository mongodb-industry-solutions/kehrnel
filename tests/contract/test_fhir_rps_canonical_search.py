"""Contract tests: golden FHIR search compile + execute (prompt 12)."""

from __future__ import annotations

import pytest

from kehrnel.engine.strategies.fhir.rps_canonical import query as fhir_query
from kehrnel.engine.strategies.fhir.rps_canonical.denormalize import fhir_denormalize
from kehrnel.engine.strategies.fhir.rps_canonical.generation import synthetic_generate_batch
from tests.contract.fhir_rps_contract_helpers import (
    drop_database,
    load_golden_cases,
    mql_contains_keys,
    requires_mongo,
    strategy_context,
    unique_db_name,
)

@pytest.mark.parametrize("case", load_golden_cases(), ids=lambda c: c["id"])
@pytest.mark.asyncio
async def test_golden_compile_mql_keys(case):
    """Compile-only: no Mongo required."""
    ctx = strategy_context(database="fhir_compile_only")
    plan = await fhir_query.compile_fhir_query(
        ctx,
        "fhir",
        {
            "resource_type": case["resource_type"],
            "criteria": case["criteria"],
        },
    )
    assert plan.engine == "fhir_mql"
    assert mql_contains_keys(plan.plan.get("filter"), case.get("expected_mql_keys") or [])


@requires_mongo
@pytest.mark.parametrize("case", load_golden_cases(), ids=lambda c: f"{c['id']}_execute")
@pytest.mark.asyncio
async def test_golden_search_execute(case):
    """Seed MongoDB, denormalize, run fhir_search, assert MQL keys and min row count."""
    db_name = unique_db_name("fhir_golden")
    try:
        ctx = strategy_context(database=db_name)
        seed_resources = case.get("seed_resources") or {"Patient": 10}
        seed = case.get("seed", 42)

        gen = await synthetic_generate_batch(
            ctx,
            {
                "resources": seed_resources,
                "seed": seed,
                "store_canonical": True,
                "denormalize_after": False,
            },
        )
        assert gen["ok"] is True

        denorm_types = case.get("denormalize_resource_types") or [case["resource_type"]]
        denorm = await fhir_denormalize(ctx, {"resource_types": denorm_types})
        assert denorm["ok"] is True

        search = await fhir_query.fhir_search(
            ctx,
            {
                "resource_type": case["resource_type"],
                "criteria": case["criteria"],
                "_count": 50,
            },
        )
        assert search["ok"] is True
        assert mql_contains_keys(search["plan"].get("filter"), case.get("expected_mql_keys") or [])

        min_rows = int(case.get("min_row_count", 0))
        if min_rows > 0:
            assert len(search.get("rows") or []) >= min_rows, (
                f"{case['id']}: expected >={min_rows} rows, got {len(search.get('rows') or [])}"
            )
    finally:
        drop_database(db_name)


@requires_mongo
@pytest.mark.asyncio
async def test_golden_scheduling_resources_compile_and_search():
    """At least one scheduling resource type is searchable end-to-end."""
    scheduling = [c for c in load_golden_cases() if c["resource_type"] in ("Schedule", "Slot", "Appointment")]
    assert len(scheduling) >= 3

    db_name = unique_db_name("fhir_sched")
    try:
        ctx = strategy_context(database=db_name)
        await synthetic_generate_batch(
            ctx,
            {
                "resources": {
                    "Patient": 4,
                    "Practitioner": 2,
                    "Schedule": 3,
                    "Slot": 8,
                    "Appointment": 5,
                },
                "seed": 42,
                "store_canonical": True,
            },
        )
        await fhir_denormalize(
            ctx,
            {"resource_types": ["Schedule", "Slot", "Appointment"]},
        )

        for case in scheduling:
            result = await fhir_query.fhir_search(
                ctx,
                {
                    "resource_type": case["resource_type"],
                    "criteria": case["criteria"],
                    "_count": 20,
                },
            )
            assert result["ok"] is True
            assert mql_contains_keys(result["plan"].get("filter"), case["expected_mql_keys"])
    finally:
        drop_database(db_name)
