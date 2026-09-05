"""Contract tests for fhir.clinical_cdr synthetic_generate_batch."""

from __future__ import annotations

import os

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.generation import synthetic_generate_batch
from kehrnel.engine.strategies.fhir.clinical_cdr.strategy import FHIRClinicalCDRStrategy, MANIFEST


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
async def test_synthetic_generate_batch_plan_only_clinical_dev_recipe():
    result = await synthetic_generate_batch(
        _ctx(),
        {"recipe": "clinical_dev", "plan_only": True},
    )
    assert result["ok"] is True
    assert result["recipe"] == "clinical_dev"
    assert result["planned"]["Patient"] == 50
    assert len(result["planned"]) == 10


@pytest.mark.asyncio
async def test_synthetic_generate_batch_plan_only():
    result = await synthetic_generate_batch(
        _ctx(),
        {"resources": {"Patient": 2, "Observation": 5}, "plan_only": True},
    )
    assert result["ok"] is True
    assert result["plan_only"] is True
    assert result["planned"]["Patient"] == 2
    assert "Patient" in result["generation_order"]


@pytest.mark.asyncio
async def test_synthetic_generate_batch_dry_run_generates_without_insert():
    result = await synthetic_generate_batch(
        _ctx(),
        {"resources": {"Patient": 1}, "dry_run": True, "seed": 42},
    )
    assert result["ok"] is True
    assert result["generated"]["Patient"] >= 1
    assert result["inserted"] == {}
    assert result["total_documents"] >= 1


@pytest.mark.asyncio
async def test_synthetic_generate_batch_cancel_raises():
    cancel = {"flag": False}

    def should_cancel() -> bool:
        return cancel["flag"]

    cancel["flag"] = True
    with pytest.raises(KehrnelError) as exc:
        await synthetic_generate_batch(
            _ctx(),
            {"resources": {"Patient": 10}, "seed": 1},
            should_cancel=should_cancel,
        )
    assert exc.value.code == "JOB_CANCELED"


@pytest.mark.asyncio
async def test_synthetic_generate_batch_cannot_disable_projection():
    with pytest.raises(KehrnelError) as exc:
        await synthetic_generate_batch(
            _ctx(),
            {"resources": {"Patient": 1}, "denormalize_after": False},
        )
    assert exc.value.code == "FHIR_PERSISTENCE_INVARIANT_REQUIRED"


@pytest.mark.asyncio
async def test_strategy_run_op_synthetic_generate_batch_dry_run():
    strat = FHIRClinicalCDRStrategy(MANIFEST)
    result = await strat.run_op(
        _ctx(),
        "synthetic_generate_batch",
        {"resources": {"Patient": 1}, "dry_run": True, "seed": 7},
    )
    assert result["ok"] is True
    assert result["generated"]["Patient"] >= 1


@pytest.mark.parametrize(
    ("release", "recipe", "expected_omissions"),
    [
        ("R5", "clinical_dev", 0),
        ("R5", "clinical_full84", 0),
        ("R6", "clinical_dev", 0),
        ("R6", "clinical_full84", 6),
    ],
)
@pytest.mark.asyncio
async def test_every_release_compatible_recipe_document_conforms_to_base_schema(
    release: str, recipe: str, expected_omissions: int
):
    result = await synthetic_generate_batch(
        _ctx(config={"database": "fhir_test", "schema_version": release}),
        {"recipe": recipe, "dry_run": True, "seed": 42},
    )

    assert result["generation_conformance"]["passed"] is True
    assert result["generation_conformance"]["resources_checked"] == result["total_documents"]
    assert len(result["omitted_recipe_resource_types"]) == expected_omissions


@pytest.mark.asyncio
async def test_recipe_filter_never_silently_drops_an_explicit_unsupported_type():
    with pytest.raises(KehrnelError) as exc:
        await synthetic_generate_batch(
            _ctx(config={"database": "fhir_test", "schema_version": "R6"}),
            {
                "recipe": "clinical_full84",
                "resources": {"DeviceDispense": 1},
                "dry_run": True,
            },
        )

    assert exc.value.code == "INVALID_INPUT"


@pytest.mark.skipif(
    not os.getenv("FHIR_GENERATION_INTEGRATION"),
    reason="Set FHIR_GENERATION_INTEGRATION=1 to persist to local MongoDB",
)
@pytest.mark.asyncio
async def test_synthetic_generate_batch_writes_to_mongo():
    db_name = os.getenv("FHIR_TEST_DB", "fhir_kehrnel_gen_test")
    result = await synthetic_generate_batch(
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
        {
            "resources": {"Patient": 2, "Observation": 3},
            "seed": 99,
            "store_canonical": True,
        },
    )
    assert result["inserted"].get("Patient", 0) >= 2
    assert result["inserted"].get("Observation", 0) >= 3
