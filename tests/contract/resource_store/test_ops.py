"""Contract tests for fhir.resource_store search ops (prompt 11)."""

from __future__ import annotations

import os
from unittest.mock import ANY, AsyncMock, patch

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import QueryPlan, StrategyContext
from kehrnel.engine.strategies.fhir.resource_store import query as fhir_query
from kehrnel.engine.strategies.fhir.resource_store import stats as fhir_stats_mod
from kehrnel.engine.strategies.fhir.resource_store.strategy import FHIRResourceStoreStrategy, MANIFEST

pytest.importorskip("fhir_search_to_mql")

DATA_TARGET_DIGEST = "sha256:" + "d" * 64


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
        meta={"data_target_digest": DATA_TARGET_DIGEST},
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
        "kehrnel.engine.strategies.fhir.resource_store.query.compile_fhir_query",
        new_callable=AsyncMock,
        return_value=plan,
    ) as compile_mock:
        with patch(
            "kehrnel.engine.strategies.fhir.resource_store.query.execute_fhir_query",
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
        def list_collection_names(self):
            return ["Patient"]

        def __getitem__(self, name):
            return FakeCollection()

    class FakeLoader:
        def list_resources(self):
            return ["Patient", "Observation"]

        def get_config(self, resource_type):
            return {"resourceType": resource_type, "indexes": []}

    class FakeClient:
        def close(self):
            return None

    class FakeMqlCtx:
        def __init__(self):
            self.config_loader = FakeLoader()
            self.db = FakeDb()
            self.client = FakeClient()
            self.compartment_definitions_dir = None

    with patch(
        "kehrnel.engine.strategies.fhir.resource_store.stats.bridge.build_mql_context",
        return_value=FakeMqlCtx(),
    ):
        with patch(
            "kehrnel.engine.strategies.fhir.resource_store.stats.bridge.known_generation_resource_types",
            return_value={"Patient", "Device"},
        ):
            result = await fhir_stats_mod.fhir_stats(_ctx(), {"resource_types": ["Patient"]})

    assert result["ok"] is True
    assert result["data_target_digest"] == DATA_TARGET_DIGEST
    assert result["collections"][0]["resource_type"] == "Patient"
    assert result["collections"][0]["collection_exists"] is True
    assert result["collections"][0]["document_count"] == 5
    assert result["collections"][0]["denormalized_percent"] == 60.0
    assert result["collections"][0]["search_index_present"] is True
    assert result["summary"]["document_count"] == 5
    assert result["summary"]["populated_resource_type_count"] == 1
    assert result["summary"]["unclassified_count"] == 5
    assert "Patient" in result["indexed_resource_types"]


@pytest.mark.asyncio
async def test_fhir_stats_emits_governed_condition_projection_receipt():
    observed_filters = []

    class FakeCollection:
        def count_documents(self, filt):
            observed_filters.append(filt)
            if filt == {}:
                return 1
            if "$expr" in filt:
                return 0
            if "_kehrnel.resource_projection_version" in filt:
                return 1
            if filt == {"_search": {"$exists": True}}:
                return 1
            if filt == {"_compartments": {"$exists": True}}:
                return 1
            return 0

        def list_indexes(self):
            return [
                {"name": "_id_", "key": {"_id": 1}},
                {"name": "condition_code", "key": {"_search.code_systemCode": 1}},
            ]

        def find_one(self, *_args, **_kwargs):
            return None

        def distinct(self, *_args, **_kwargs):
            return []

    class FakeDb:
        def list_collection_names(self):
            return ["Condition"]

        def command(self, *_args, **_kwargs):
            return {"objects": 1}

        def __getitem__(self, name):
            assert name == "Condition"
            return FakeCollection()

    class FakeLoader:
        def list_resources(self):
            return ["Condition"]

        def get_config(self, resource_type):
            return {"resourceType": resource_type, "indexes": []}

    fake_ctx = type(
        "FakeMqlCtx",
        (),
        {
            "config_loader": FakeLoader(),
            "db": FakeDb(),
            "compartment_definitions_dir": None,
        },
    )()
    with patch(
        "kehrnel.engine.strategies.fhir.resource_store.stats.bridge.build_mql_context",
        return_value=fake_ctx,
    ), patch(
        "kehrnel.engine.strategies.fhir.resource_store.stats.bridge.close_mql_context"
    ), patch(
        "kehrnel.engine.strategies.fhir.resource_store.stats.bridge.known_generation_resource_types",
        return_value={"Condition"},
    ):
        result = await fhir_stats_mod.fhir_stats(
            _ctx(), {"resource_types": ["Condition"]}
        )

    row = result["collections"][0]
    assert result["data_target_digest"] == DATA_TARGET_DIGEST
    assert row["collection_exists"] is True
    assert row["canonical_projection_mismatch_count"] == 0
    assert any("$expr" in query for query in observed_filters)
    receipt = row["projection_readiness_receipt"]
    assert receipt["contract"] == "fhir-resource-store-projection-readiness/v1"
    assert receipt["state"] == "ready"
    assert receipt["receiptDigest"].startswith("sha256:")


@pytest.mark.asyncio
async def test_fhir_operational_probe_reports_projection_and_index_drift():
    stats = {
        "ok": True,
        "collections": [
            {
                "resource_type": "Observation",
                "collection_exists": True,
                "document_count": 2,
                "contract_current_count": 1,
                "resource_projection_version": "sha256:" + "a" * 64,
                "search_index_present": False,
                "projection_readiness_receipt": {"state": "not_ready"},
            }
        ],
    }
    with patch.object(
        fhir_stats_mod,
        "fhir_stats",
        new_callable=AsyncMock,
        return_value=stats,
    ) as stats_mock:
        result = await fhir_stats_mod.fhir_operational_probe(
            _ctx(), {"resource_types": ["Observation"]}
        )

    stats_mock.assert_awaited_once_with(
        ANY,
        {"resource_types": ["Observation"]},
    )
    assert result["state"] == "degraded"
    assert result["schemaState"] == "drifted"
    assert result["indexState"] == "drifted"
    assert result["reconciliationDivergenceCount"] == 1
    assert result["reconciliationDivergenceCount"] == 1
    assert result["reconciliationDivergenceCount"] == 1
    assert result["reconciliationDivergenceCount"] == 1
    assert {check["checkId"] for check in result["checks"]} == {
        "runtime_connectivity",
        "required_collections",
        "storage_schema",
        "managed_indexes",
        "derived_projection",
        "data_freshness",
        "reconciliation_integrity",
        "data_freshness",
        "reconciliation_integrity",
        "data_freshness",
        "reconciliation_integrity",
        "data_freshness",
        "reconciliation_integrity",
        "data_freshness",
        "reconciliation_integrity",
    }


@pytest.mark.asyncio
async def test_fhir_operational_probe_refuses_an_unbounded_collection_scan():
    result = await fhir_stats_mod.fhir_operational_probe(_ctx(), {})

    assert result["state"] == "unavailable"
    assert result["schemaState"] == "unavailable"
    assert result["indexState"] == "unavailable"
    assert all(check["state"] == "unavailable" for check in result["checks"])


@pytest.mark.asyncio
async def test_fhir_operational_probe_reports_projection_and_index_drift():
    stats = {
        "ok": True,
        "collections": [
            {
                "resource_type": "Observation",
                "collection_exists": True,
                "document_count": 2,
                "contract_current_count": 1,
                "resource_projection_version": "sha256:" + "a" * 64,
                "search_index_present": False,
                "projection_readiness_receipt": {"state": "not_ready"},
            }
        ],
    }
    with patch.object(
        fhir_stats_mod,
        "fhir_stats",
        new_callable=AsyncMock,
        return_value=stats,
    ) as stats_mock:
        result = await fhir_stats_mod.fhir_operational_probe(
            _ctx(), {"resource_types": ["Observation"]}
        )

    stats_mock.assert_awaited_once_with(
        ANY,
        {"resource_types": ["Observation"]},
    )
    assert result["state"] == "degraded"
    assert result["schemaState"] == "drifted"
    assert result["indexState"] == "drifted"
    assert {check["checkId"] for check in result["checks"]} == {
        "runtime_connectivity",
        "required_collections",
        "storage_schema",
        "managed_indexes",
        "derived_projection",
    }


@pytest.mark.asyncio
async def test_fhir_operational_probe_refuses_an_unbounded_collection_scan():
    result = await fhir_stats_mod.fhir_operational_probe(_ctx(), {})

    assert result["state"] == "unavailable"
    assert result["schemaState"] == "unavailable"
    assert result["indexState"] == "unavailable"
    assert all(check["state"] == "unavailable" for check in result["checks"])


@pytest.mark.asyncio
async def test_fhir_operational_probe_reports_projection_and_index_drift():
    stats = {
        "ok": True,
        "collections": [
            {
                "resource_type": "Observation",
                "collection_exists": True,
                "document_count": 2,
                "contract_current_count": 1,
                "resource_projection_version": "sha256:" + "a" * 64,
                "search_index_present": False,
                "projection_readiness_receipt": {"state": "not_ready"},
            }
        ],
    }
    with patch.object(
        fhir_stats_mod,
        "fhir_stats",
        new_callable=AsyncMock,
        return_value=stats,
    ) as stats_mock:
        result = await fhir_stats_mod.fhir_operational_probe(
            _ctx(), {"resource_types": ["Observation"]}
        )

    stats_mock.assert_awaited_once_with(
        ANY,
        {"resource_types": ["Observation"]},
    )
    assert result["state"] == "degraded"
    assert result["schemaState"] == "drifted"
    assert result["indexState"] == "drifted"
    assert {check["checkId"] for check in result["checks"]} == {
        "runtime_connectivity",
        "required_collections",
        "storage_schema",
        "managed_indexes",
        "derived_projection",
    }


@pytest.mark.asyncio
async def test_fhir_operational_probe_refuses_an_unbounded_collection_scan():
    result = await fhir_stats_mod.fhir_operational_probe(_ctx(), {})

    assert result["state"] == "unavailable"
    assert result["schemaState"] == "unavailable"
    assert result["indexState"] == "unavailable"
    assert all(check["state"] == "unavailable" for check in result["checks"])


@pytest.mark.asyncio
async def test_fhir_operational_probe_reports_projection_and_index_drift():
    stats = {
        "ok": True,
        "collections": [
            {
                "resource_type": "Observation",
                "collection_exists": True,
                "document_count": 2,
                "contract_current_count": 1,
                "resource_projection_version": "sha256:" + "a" * 64,
                "search_index_present": False,
                "projection_readiness_receipt": {"state": "not_ready"},
            }
        ],
    }
    with patch.object(
        fhir_stats_mod,
        "fhir_stats",
        new_callable=AsyncMock,
        return_value=stats,
    ) as stats_mock:
        result = await fhir_stats_mod.fhir_operational_probe(
            _ctx(), {"resource_types": ["Observation"]}
        )

    stats_mock.assert_awaited_once_with(
        ANY,
        {"resource_types": ["Observation"]},
    )
    assert result["state"] == "degraded"
    assert result["schemaState"] == "drifted"
    assert result["indexState"] == "drifted"
    assert {check["checkId"] for check in result["checks"]} == {
        "runtime_connectivity",
        "required_collections",
        "storage_schema",
        "managed_indexes",
        "derived_projection",
    }


@pytest.mark.asyncio
async def test_fhir_operational_probe_refuses_an_unbounded_collection_scan():
    result = await fhir_stats_mod.fhir_operational_probe(_ctx(), {})

    assert result["state"] == "unavailable"
    assert result["schemaState"] == "unavailable"
    assert result["indexState"] == "unavailable"
    assert all(check["state"] == "unavailable" for check in result["checks"])


@pytest.mark.asyncio
async def test_fhir_operational_probe_reports_projection_and_index_drift():
    stats = {
        "ok": True,
        "collections": [
            {
                "resource_type": "Observation",
                "collection_exists": True,
                "document_count": 2,
                "contract_current_count": 1,
                "resource_projection_version": "sha256:" + "a" * 64,
                "search_index_present": False,
                "projection_readiness_receipt": {"state": "not_ready"},
            }
        ],
    }
    with patch.object(
        fhir_stats_mod,
        "fhir_stats",
        new_callable=AsyncMock,
        return_value=stats,
    ) as stats_mock:
        result = await fhir_stats_mod.fhir_operational_probe(
            _ctx(), {"resource_types": ["Observation"]}
        )

    stats_mock.assert_awaited_once_with(
        ANY,
        {"resource_types": ["Observation"]},
    )
    assert result["state"] == "degraded"
    assert result["schemaState"] == "drifted"
    assert result["indexState"] == "drifted"
    assert {check["checkId"] for check in result["checks"]} == {
        "runtime_connectivity",
        "required_collections",
        "storage_schema",
        "managed_indexes",
        "derived_projection",
    }


@pytest.mark.asyncio
async def test_fhir_operational_probe_refuses_an_unbounded_collection_scan():
    result = await fhir_stats_mod.fhir_operational_probe(_ctx(), {})

    assert result["state"] == "unavailable"
    assert result["schemaState"] == "unavailable"
    assert result["indexState"] == "unavailable"
    assert all(check["state"] == "unavailable" for check in result["checks"])


@pytest.mark.asyncio
async def test_fhir_stats_does_not_treat_a_missing_empty_collection_as_initialized():
    class MissingCollection:
        def count_documents(self, _filter):
            raise AssertionError("a missing collection must not be queried")

    class FakeDb:
        def list_collection_names(self):
            return []

        def command(self, *_args, **_kwargs):
            return {"objects": 0}

        def __getitem__(self, _name):
            return MissingCollection()

    class FakeLoader:
        def list_resources(self):
            return ["Condition"]

        def get_config(self, resource_type):
            return {"resourceType": resource_type, "indexes": []}

    fake_ctx = type(
        "FakeMqlCtx",
        (),
        {
            "config_loader": FakeLoader(),
            "db": FakeDb(),
            "compartment_definitions_dir": None,
        },
    )()
    with patch(
        "kehrnel.engine.strategies.fhir.resource_store.stats.bridge.build_mql_context",
        return_value=fake_ctx,
    ), patch(
        "kehrnel.engine.strategies.fhir.resource_store.stats.bridge.close_mql_context"
    ), patch(
        "kehrnel.engine.strategies.fhir.resource_store.stats.bridge.known_generation_resource_types",
        return_value={"Condition"},
    ):
        result = await fhir_stats_mod.fhir_stats(
            _ctx(), {"resource_types": ["Condition"]}
        )

    row = result["collections"][0]
    assert row["collection_exists"] is False
    assert row["canonical_projection_mismatch_count"] is None
    assert row["projection_readiness_receipt"]["state"] == "not_ready"


@pytest.mark.asyncio
async def test_strategy_run_op_fhir_search_explain_only():
    strat = FHIRResourceStoreStrategy(MANIFEST)
    result = await strat.run_op(
        _ctx(),
        "fhir_search",
        {"resource_type": "Patient", "criteria": {"gender": "female"}, "explain_only": True},
    )
    assert result["explain_only"] is True


@pytest.mark.asyncio
async def test_strategy_run_op_fhir_list_search_params():
    strat = FHIRResourceStoreStrategy(MANIFEST)
    result = await strat.run_op(_ctx(), "fhir_list_search_params", {"resource_type": "Patient"})
    assert result["parameter_count"] > 0


@pytest.mark.asyncio
async def test_strategy_rejects_unadvertised_natural_language_search_op():
    strat = FHIRResourceStoreStrategy(MANIFEST)
    with pytest.raises(ValueError, match="not supported"):
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
