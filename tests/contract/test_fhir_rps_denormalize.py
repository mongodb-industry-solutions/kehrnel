"""Contract tests for fhir.rps_canonical fhir_denormalize op."""

from __future__ import annotations

import os

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.rps_canonical.denormalize import fhir_denormalize
from kehrnel.engine.strategies.fhir.rps_canonical.strategy import FHIRRPSCanonicalStrategy, MANIFEST


def _ctx(*, config: dict | None = None, bindings: dict | None = None) -> StrategyContext:
    return StrategyContext(
        environment_id="test-env",
        config=config or {"database": "fhir_test"},
        bindings=bindings
        or {
            "db": {
                "provider": "mongodb",
                "uri": "mongodb://localhost:27017",
                "database": "fhir_test",
            }
        },
        manifest=MANIFEST,
    )


@pytest.mark.asyncio
async def test_fhir_denormalize_requires_resource_types():
    with pytest.raises(KehrnelError) as exc:
        await fhir_denormalize(_ctx(), {})
    assert exc.value.code == "INVALID_INPUT"


@pytest.mark.asyncio
async def test_fhir_denormalize_dry_run_patient():
    result = await fhir_denormalize(
        _ctx(),
        {"resource_types": ["Patient"], "dry_run": True},
    )
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert "Patient" in result["denormalized"]
    assert result["denormalized"]["Patient"]["dry_run"] is True


@pytest.mark.asyncio
async def test_fhir_denormalize_skips_unknown_config():
    result = await fhir_denormalize(
        _ctx(),
        {"resource_types": ["Patient", "NotInMqlConfigXYZ"], "dry_run": True},
    )
    assert "Patient" in result["denormalized"]
    assert "NotInMqlConfigXYZ" in result.get("skipped", [])


@pytest.mark.asyncio
async def test_strategy_run_op_fhir_denormalize_dry_run():
    strat = FHIRRPSCanonicalStrategy(MANIFEST)
    result = await strat.run_op(
        _ctx(),
        "fhir_denormalize",
        {"resource_types": ["Patient"], "dry_run": True},
    )
    assert result["ok"] is True


@pytest.mark.skipif(
    not os.getenv("FHIR_DENORM_INTEGRATION"),
    reason="Set FHIR_DENORM_INTEGRATION=1 after seeding Patient docs in MongoDB",
)
@pytest.mark.asyncio
async def test_fhir_denormalize_updates_search_fields():
    from pymongo import MongoClient

    db_name = os.getenv("FHIR_TEST_DB", "fhir_kehrnel_denorm_test")
    uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)
    coll = client[db_name]["Patient"]
    if coll.count_documents({}) == 0:
        pytest.skip("No Patient documents in test database; run synthetic job first")

    result = await fhir_denormalize(
        _ctx(
            config={"database": db_name},
            bindings={"db": {"provider": "mongodb", "uri": uri, "database": db_name}},
        ),
        {"resource_types": ["Patient"], "batch_size": 50, "limit": 5},
    )
    assert result["denormalized"]["Patient"]["processed"] >= 1
    doc = coll.find_one({"_search": {"$exists": True}})
    assert doc is not None
    client.close()
