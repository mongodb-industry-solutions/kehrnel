"""Contract tests for FHIR synthetic watermarks and job progress phases."""

from __future__ import annotations

import os

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr import watermark
from kehrnel.engine.strategies.fhir.clinical_cdr.generation import synthetic_generate_batch
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import MANIFEST


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


def test_apply_watermark_adds_meta_tag_and_extension():
    doc = {"resourceType": "Patient", "id": "p1"}
    marked = watermark.apply_watermark(doc)
    tags = marked["meta"]["tag"]
    assert any(t.get("code") == "synthetic" for t in tags)
    assert any(
        e.get("url") == watermark.KEHRNEL_SYNTHETIC_EXTENSION_URL and e.get("valueCode") == "generated"
        for e in marked["meta"]["extension"]
    )
    assert watermark.has_synthetic_watermark(marked)


def test_apply_watermark_idempotent():
    doc = watermark.apply_watermark({"resourceType": "Patient", "id": "p1"})
    again = watermark.apply_watermark(doc)
    assert len(again["meta"]["tag"]) == len(doc["meta"]["tag"])


def test_watermark_enabled_defaults_true():
    assert watermark.watermark_enabled({}) is True
    assert watermark.watermark_enabled({"generation": {"watermark": {"enabled": False}}}) is False


@pytest.mark.asyncio
async def test_generation_emits_queued_generating_completed_phases():
    phases: list[str] = []

    async def progress_cb(*, progress=None, phase=None, stats=None):
        if phase:
            phases.append(str(phase))

    await synthetic_generate_batch(
        _ctx(),
        {"resources": {"Patient": 1}, "dry_run": True, "seed": 11},
        progress_cb=progress_cb,
    )
    assert phases[0] == "queued"
    assert "generating" in phases
    assert phases[-1] == "completed"


@pytest.mark.asyncio
async def test_fhir_denormalize_emits_denormalizing_phase():
    from kehrnel.engine.strategies.fhir.clinical_cdr.denormalize import fhir_denormalize

    phases: list[str] = []

    async def progress_cb(*, progress=None, phase=None, stats=None):
        if phase:
            phases.append(str(phase))

    await fhir_denormalize(
        _ctx(),
        {"resource_types": ["Patient"], "dry_run": True},
        progress_cb=progress_cb,
    )
    assert "denormalizing" in phases


@pytest.mark.asyncio
async def test_generation_watermark_applied_flag():
    enabled = await synthetic_generate_batch(
        _ctx(),
        {"resources": {"Patient": 1}, "dry_run": True},
    )
    disabled = await synthetic_generate_batch(
        _ctx(config={"database": "fhir_test", "generation": {"watermark": {"enabled": False}}}),
        {"resources": {"Patient": 1}, "dry_run": True},
    )
    assert enabled["watermark_applied"] is True
    assert disabled["watermark_applied"] is False


@pytest.mark.skipif(
    not os.getenv("FHIR_WATERMARK_INTEGRATION"),
    reason="Set FHIR_WATERMARK_INTEGRATION=1 to verify MongoDB meta.tag after save",
)
@pytest.mark.asyncio
async def test_saved_patient_has_synthetic_tag_in_mongo():
    from pymongo import MongoClient

    db_name = os.getenv("FHIR_TEST_DB", "fhir_kehrnel_watermark_test")
    await synthetic_generate_batch(
        _ctx(
            config={"database": db_name},
            bindings={
                "db": {
                    "provider": "mongodb",
                    "uri": os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
                    "database": db_name,
                }
            },
        ),
        {"resources": {"Patient": 1}, "seed": 77, "store_canonical": True},
    )
    doc = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"))[db_name]["Patient"].find_one()
    assert doc is not None
    assert any(t.get("code") == "synthetic" for t in doc.get("meta", {}).get("tag", []))
