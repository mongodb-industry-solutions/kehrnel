"""Contract tests for fhir.rps_canonical search ops (prompt 11)."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import QueryPlan, StrategyContext
from kehrnel.engine.strategies.fhir.rps_canonical import query as fhir_query
from kehrnel.engine.strategies.fhir.rps_canonical import stats as fhir_stats_mod
from kehrnel.engine.strategies.fhir.rps_canonical.strategy import FHIRRPSCanonicalStrategy, MANIFEST

pytest.importorskip("fhir_search_to_mql")


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


@pytest.mark.asyncio
async def test_fhir_list_search_params_patient():
    result = fhir_query.fhir_list_search_params(_ctx(), {"resource_type": "Patient"})
    assert result["ok"] is True
    assert result["resource_type"] == "Patient"
    assert result["parameter_count"] > 0
    names = {p["name"] for p in result["parameters"]}
    assert "gender" in names
    gender = next(p for p in result["parameters"] if p["name"] == "gender")
    assert gender["type"] == "token"


@pytest.mark.asyncio
async def test_fhir_list_search_params_requires_resource_type():
    with pytest.raises(KehrnelError) as exc:
        fhir_query.fhir_list_search_params(_ctx(), {})
    assert exc.value.code == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_fhir_search_explain_only():
    result = await fhir_query.fhir_search(
        _ctx(),
        {
            "resource_type": "Patient",
            "criteria": {"gender": "female"},
            "explain_only": True,
        },
    )
    assert result["ok"] is True
    assert result["explain_only"] is True
    assert result["engine"] == "fhir_mql"
    assert result["plan"]["filter"]
    assert "rows" not in result


@pytest.mark.asyncio
async def test_fhir_search_compile_and_execute():
    plan = QueryPlan(
        engine="fhir_mql",
        plan={"filter": {"_search.gender": "female"}, "collection": "Patient"},
        explain={"engine": "fhir_mql"},
    )
    with patch(
        "kehrnel.engine.strategies.fhir.rps_canonical.query.compile_fhir_query",
        new_callable=AsyncMock,
        return_value=plan,
    ) as compile_mock:
        with patch(
            "kehrnel.engine.strategies.fhir.rps_canonical.query.execute_fhir_query",
            new_callable=AsyncMock,
            return_value=type(
                "R",
                (),
                {
                    "engine_used": "fhir_mql",
                    "rows": [{"resourceType": "Patient", "id": "p1"}],
                    "explain": {"total": 1, "returned": 1},
                },
            )(),
        ) as execute_mock:
            result = await fhir_query.fhir_search(
                _ctx(),
                {"resource_type": "Patient", "criteria": {"gender": "female"}, "limit": 10},
            )

    compile_mock.assert_awaited_once()
    execute_mock.assert_awaited_once()
    assert result["ok"] is True
    assert len(result["rows"]) == 1
    assert result["total"] == 1
    compile_args = compile_mock.await_args.args[2]
    assert compile_args["_count"] == 10


@pytest.mark.asyncio
async def test_fhir_stats_dry_structure():
    class FakeCollection:
        def count_documents(self, filt):
            if filt == {}:
                return 5
            if filt == {"_search": {"$exists": True}}:
                return 3
            return 0

        def list_indexes(self):
            return [{"name": "_id_", "key": {"_id": 1}}, {"name": "idx_search", "key": {"_search.gender": 1}}]

    class FakeDb:
        def __getitem__(self, name):
            return FakeCollection()

    class FakeLoader:
        def list_resources(self):
            return ["Patient", "Observation"]

    class FakeClient:
        def close(self):
            return None

    class FakeMqlCtx:
        def __init__(self):
            self.config_loader = FakeLoader()
            self.db = FakeDb()
            self.client = FakeClient()

    with patch(
        "kehrnel.engine.strategies.fhir.rps_canonical.stats.bridge.build_mql_context",
        return_value=FakeMqlCtx(),
    ):
        with patch(
            "kehrnel.engine.strategies.fhir.rps_canonical.stats.bridge.known_generation_resource_types",
            return_value={"Patient", "Device"},
        ):
            result = await fhir_stats_mod.fhir_stats(_ctx(), {"resource_types": ["Patient"]})

    assert result["ok"] is True
    assert result["collections"][0]["resource_type"] == "Patient"
    assert result["collections"][0]["document_count"] == 5
    assert result["collections"][0]["denormalized_percent"] == 60.0
    assert result["collections"][0]["search_index_present"] is True
    assert "Patient" in result["indexed_resource_types"]


@pytest.mark.asyncio
async def test_strategy_run_op_fhir_search_explain_only():
    strat = FHIRRPSCanonicalStrategy(MANIFEST)
    result = await strat.run_op(
        _ctx(),
        "fhir_search",
        {"resource_type": "Patient", "criteria": {"gender": "female"}, "explain_only": True},
    )
    assert result["explain_only"] is True


@pytest.mark.asyncio
async def test_strategy_run_op_fhir_list_search_params():
    strat = FHIRRPSCanonicalStrategy(MANIFEST)
    result = await strat.run_op(_ctx(), "fhir_list_search_params", {"resource_type": "Patient"})
    assert result["parameter_count"] > 0


@pytest.mark.asyncio
async def test_strategy_run_op_negotiate_not_implemented():
    strat = FHIRRPSCanonicalStrategy(MANIFEST)
    with pytest.raises(NotImplementedError, match="negotiate_fhir_search"):
        await strat.run_op(_ctx(), "negotiate_fhir_search", {"text": "female patients"})


@pytest.mark.skipif(
    not os.getenv("FHIR_STATS_INTEGRATION"),
    reason="Set FHIR_STATS_INTEGRATION=1 for live MongoDB stats",
)
@pytest.mark.asyncio
async def test_fhir_stats_live_mongo():
    result = await fhir_stats_mod.fhir_stats(_ctx(), {"resource_types": ["Patient"]})
    assert result["ok"] is True
    assert result["collections"]
