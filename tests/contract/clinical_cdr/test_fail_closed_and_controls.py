"""Contract tests for T1 (fail-closed) + T2 (result-control correctness).

Governing rule: an unsupported search must fail at COMPILE (before any Mongo
call), and result controls (_count/_offset/_sort) must never reach the fhir-mql
converter.
"""

from __future__ import annotations

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext

from kehrnel.engine.strategies.fhir.clinical_cdr import query as fhir_query
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST

pytest.importorskip("fhir_search_to_mql")


def _ctx(*, config: dict | None = None) -> StrategyContext:
    return StrategyContext(
        environment_id="test-env",
        config=config or {"database": "fhir_test"},
        bindings={"db": {"provider": "mongodb", "uri": "mongodb://localhost:27017", "database": "fhir_test"}},
        manifest=MANIFEST,
    )


# ── T1: fail closed at compile, before Mongo ──────────────────────────────────

@pytest.mark.asyncio
async def test_unsupported_filter_param_fails_at_compile():
    with pytest.raises(KehrnelError) as exc:
        await fhir_query.compile_fhir_query(
            _ctx(), "fhir",
            {"resource_type": "Patient", "criteria": {"totally_unsupported": "x"}},
        )
    assert exc.value.code == "FHIR_SEARCH_UNSUPPORTED_PARAM"
    assert exc.value.status == 400


@pytest.mark.asyncio
async def test_lenient_records_ignored_and_does_not_raise():
    plan = await fhir_query.compile_fhir_query(
        _ctx(), "fhir",
        {"resource_type": "Patient", "criteria": {"gender": "male", "totally_unsupported": "x"},
         "handling": "lenient"},
    )
    assert plan.plan["handling"] == "lenient"
    ignored = plan.plan["ignored_parameters"]
    assert any(i["name"] == "totally_unsupported" for i in ignored)
    assert plan.plan["filter"]  # supported gender=male still applied


@pytest.mark.asyncio
async def test_lenient_all_dropped_still_fails_closed():
    with pytest.raises(KehrnelError):
        await fhir_query.compile_fhir_query(
            _ctx(), "fhir",
            {"resource_type": "Patient", "criteria": {"totally_unsupported": "x"},
             "handling": "lenient"},
        )


# ── T2: result controls extracted, never sent to converter ────────────────────

@pytest.mark.asyncio
async def test_url_form_count_equivalent_to_top_level():
    p_url = await fhir_query.compile_fhir_query(
        _ctx(), "fhir", {"fhir_search": "Patient?gender=female&_count=5&_offset=10"},
    )
    p_top = await fhir_query.compile_fhir_query(
        _ctx(), "fhir",
        {"resource_type": "Patient", "criteria": {"gender": "female"}, "_count": 5, "_offset": 10},
    )
    assert fhir_query.pagination_from_plan(p_url.plan) == (5, 10)
    assert fhir_query.pagination_from_plan(p_url.plan) == fhir_query.pagination_from_plan(p_top.plan)
    # controls must not have leaked into the filter
    assert p_url.plan["filter"] == p_top.plan["filter"]


@pytest.mark.asyncio
async def test_unsupported_result_control_rejected():
    with pytest.raises(KehrnelError) as exc:
        await fhir_query.compile_fhir_query(
            _ctx(), "fhir", {"fhir_search": "Patient?_summary=count"},
        )
    assert exc.value.code == "FHIR_SEARCH_UNSUPPORTED_PARAM"


def test_split_result_controls_strips_controls():
    filt, controls = fhir_query._split_result_controls(
        "gender=male&_count=5&_sort=birthdate&_offset=10"
    )
    assert filt == "gender=male"
    assert controls == {"_count": "5", "_sort": "birthdate", "_offset": "10"}
