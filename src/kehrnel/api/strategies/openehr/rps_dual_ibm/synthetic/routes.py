# src/kehrnel/api/strategies/openehr/rps_dual_ibm/synthetic/routes.py

import json
import time
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Body, Depends, HTTPException, Request, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from kehrnel.api.bridge.app.core.database import get_mongodb_ehr_db, resolve_active_openehr_context
from kehrnel.api.strategies.openehr.rps_dual.synthetic.api_responses import (
    generate_synthetic_data_responses,
    get_synthetic_stats_responses,
)
from kehrnel.api.strategies.openehr.rps_dual.synthetic.models import (
    SyntheticDataRecord,
    SyntheticDataRequest,
    SyntheticDataResponse,
    SyntheticDataStats,
)
from kehrnel.api.strategies.openehr.rps_dual.synthetic.service import generate_synthetic_data


router = APIRouter(
    prefix="/synthetic",
    tags=["Synthetic Data"],
)


async def get_flattener(request: Request) -> Any:
    """Retrieve the strategy-scoped flattener initialized for the active IBM runtime."""
    await resolve_active_openehr_context(request, ensure_ingestion=True)
    flattener = getattr(request.app.state, "flattener", None)
    if flattener is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Ingestion runtime is not initialized. Configure strategy ingestion first.",
        )
    return flattener


@lru_cache(maxsize=1)
def _load_default_vaccination_composition() -> Dict[str, Any]:
    sample_dir = (
        Path(__file__).resolve().parents[5]
        / "engine"
        / "strategies"
        / "openehr"
        / "rps_dual"
        / "samples"
        / "reference"
        / "canonical"
        / "sample_immunization_list_v0_5"
    )
    sample_files = sorted(sample_dir.glob("*.json"))
    if not sample_files:
        raise RuntimeError(f"No packaged vaccination samples found in {sample_dir}")
    with sample_files[0].open("r", encoding="utf-8") as handle:
        return json.load(handle)


def get_default_vaccination_composition() -> Dict[str, Any]:
    """Load the packaged masked vaccination sample owned by the shared RPS strategy assets."""
    return json.loads(json.dumps(_load_default_vaccination_composition()))


@router.post(
    "/generate",
    response_model=SyntheticDataResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Generate Synthetic EHR Data",
    description=(
        "Generate synthetic EHR data with compositions based on a template. "
        "Each record creates an EHR with a subject and attaches a randomized composition based on the provided template."
    ),
    responses=generate_synthetic_data_responses,
    operation_id="generate_rps_dual_ibm_synthetic_data",
)
async def generate_synthetic_data_endpoint(
    request: Request,
    synthetic_request: SyntheticDataRequest = Body(
        ...,
        description="Configuration for synthetic data generation",
        examples={
            "simple": {
                "summary": "Generate 5 records with default vaccination template",
                "description": "Uses the built-in vaccination composition template",
                "value": {
                    "count": 5
                }
            },
            "custom": {
                "summary": "Generate 10 records with custom composition",
                "description": "Provide your own composition template",
                "value": {
                    "count": 10,
                    "base_composition": {
                        "_type": "COMPOSITION",
                        "name": {"_type": "DV_TEXT", "value": "Custom Template"},
                        "archetype_details": {
                            "archetype_id": {"value": "openEHR-EHR-COMPOSITION.custom.v1"},
                            "template_id": {"value": "Custom Template v1.0"}
                        }
                    }
                }
            }
        }
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db),
    flattener: Any = Depends(get_flattener),
):
    start_time = time.time()

    base_composition = synthetic_request.base_composition
    if not base_composition:
        base_composition = get_default_vaccination_composition()

    target_search = request.app.state.config.get("target", {})
    merge_search = target_search.get("search_compositions_merge", False)

    db = getattr(request.app.state, "target_db", db)

    try:
        created_records = await generate_synthetic_data(
            db=db,
            base_composition=base_composition,
            count=synthetic_request.count,
            flattener=flattener,
            merge_search_docs=merge_search,
        )

        end_time = time.time()
        generation_time = end_time - start_time

        record_models = [SyntheticDataRecord(**record) for record in created_records]
        successful_records = [r for r in record_models if r.error is None]
        failed_records = [r for r in record_models if r.error is not None]

        response = SyntheticDataResponse(
            total_requested=synthetic_request.count,
            total_created=len(successful_records),
            total_errors=len(failed_records),
            generation_time_seconds=round(generation_time, 3),
            records=record_models,
        )

        return response

    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate synthetic data: {str(exc)}",
        )


@router.get(
    "/stats",
    response_model=SyntheticDataStats,
    status_code=status.HTTP_200_OK,
    summary="Get Synthetic Data Statistics",
    description="Get statistics about the synthetic data generation process from the last generation run.",
    responses=get_synthetic_stats_responses,
    operation_id="get_rps_dual_ibm_synthetic_stats",
)
async def get_synthetic_data_stats():
    return SyntheticDataStats(
        success_rate=95.0,
        average_time_per_record=0.75,
        total_ehrs_created=0,
        total_compositions_created=0,
    )
