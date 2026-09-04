"""Contract tests for fhir.clinical_cdr fhir_ensure_indexes op."""

from __future__ import annotations

import os

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.indexes import (
    fhir_ensure_indexes,
    fhir_index_manifest,
    search_auto_index_enabled,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import (
    FHIRClinicalCDRStrategy,
    MANIFEST,
)


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


def test_search_auto_index_defaults_true():
    assert search_auto_index_enabled({}) is True
    assert search_auto_index_enabled({"search": {"auto_index": False}}) is True


@pytest.mark.asyncio
async def test_fhir_ensure_indexes_dry_run_patient():
    result = await fhir_ensure_indexes(
        _ctx(),
        {"resource_types": ["Patient"], "dry_run": True},
    )
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["indexes"]
    assert result["indexes"][0]["collection"] == "Patient"
    assert result["indexes"][0]["status"] == "planned"
    assert result["index_manifest"]["within_budget"] is True


def test_fhir_index_manifest_is_deterministic_and_includes_identity_index():
    first = fhir_index_manifest(_ctx(), {"resource_types": ["Patient"]})
    second = fhir_index_manifest(_ctx(), {"resource_types": ["Patient"]})

    assert first["digest"] == second["digest"]
    assert first["within_budget"] is True
    assert first["collections"][0]["indexes"][0] == {
        "name": "id_unique",
        "fields": [["id", 1]],
        "unique": True,
        "source": "fhir-identity",
    }


@pytest.mark.asyncio
async def test_fhir_ensure_indexes_rejects_a_plan_over_budget():
    with pytest.raises(Exception) as exc_info:
        await fhir_ensure_indexes(
            _ctx(
                config={
                    "database": "fhir_test",
                    "index_policy": {"max_managed_indexes_per_collection": 1},
                }
            ),
            {"resource_types": ["Patient"], "dry_run": True},
        )

    assert getattr(exc_info.value, "code", None) == "FHIR_INDEX_BUDGET_EXCEEDED"


@pytest.mark.asyncio
async def test_fhir_ensure_indexes_all_resources_when_omitted():
    result = await fhir_ensure_indexes(_ctx(), {"dry_run": True})
    assert result["ok"] is True
    assert len(result["indexes"]) > 0


@pytest.mark.asyncio
async def test_strategy_run_op_fhir_ensure_indexes_dry_run():
    strat = FHIRClinicalCDRStrategy(MANIFEST)
    result = await strat.run_op(
        _ctx(),
        "fhir_ensure_indexes",
        {"resource_types": ["Patient"], "dry_run": True},
    )
    assert result["ok"] is True


@pytest.mark.skipif(
    not os.getenv("FHIR_INDEX_INTEGRATION"),
    reason="Set FHIR_INDEX_INTEGRATION=1 to verify created vs exists on MongoDB",
)
@pytest.mark.asyncio
async def test_fhir_ensure_indexes_idempotent_on_mongo():
    db_name = os.getenv("FHIR_TEST_DB", "fhir_kehrnel_index_test")
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    ctx = _ctx(
        config={"database": db_name},
        bindings={"db": {"provider": "mongodb", "uri": uri, "database": db_name}},
    )

    first = await fhir_ensure_indexes(ctx, {"resource_types": ["Patient"]})
    second = await fhir_ensure_indexes(ctx, {"resource_types": ["Patient"]})

    assert first["indexes"]
    assert any(entry.get("status") == "created" for entry in first["indexes"]) or all(
        entry.get("status") == "exists" for entry in first["indexes"]
    )
    assert second["indexes"]
    assert all(entry.get("status") == "exists" for entry in second["indexes"])
