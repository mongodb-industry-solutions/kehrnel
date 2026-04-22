# src/kehrnel/api/strategies/openehr/rps_dual/synthetic/routes.py

import json
import time
from functools import lru_cache
from pathlib import Path
from fastapi import APIRouter, Depends, status, Body, Request, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Dict, Any

from kehrnel.api.strategies.openehr.rps_dual.synthetic.service import generate_synthetic_data
from kehrnel.api.strategies.openehr.rps_dual.synthetic.models import (
    SyntheticDataRequest,
    SyntheticDataResponse,
    SyntheticDataRecord,
    SyntheticDataStats
)
from kehrnel.api.strategies.openehr.rps_dual.synthetic.api_responses import (
    generate_synthetic_data_responses,
    get_synthetic_stats_responses
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.api.bridge.app.core.database import get_mongodb_ehr_db, resolve_active_openehr_context


router = APIRouter(
    prefix="/synthetic",
    tags=["Synthetic Data"]
)


async def get_flattener(request: Request) -> CompositionFlattener:
    """
    Dependency to retrieve the globally initialized CompositionFlattener
    """
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
    """Load the packaged masked vaccination sample owned by the strategy."""
    return json.loads(json.dumps(_load_default_vaccination_composition()))


@router.post(
    "/generate",
    response_model=SyntheticDataResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Generate Synthetic EHR Data",
    description="Generate synthetic EHR data with compositions based on a template. "
    "Each record creates an EHR with a subject and attaches a randomized composition based on the provided template.",
    responses=generate_synthetic_data_responses,
    operation_id="generate_rps_dual_synthetic_data",
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
    flattener: CompositionFlattener = Depends(get_flattener)
):
    """
    Generate synthetic EHR data with compositions.
    
    This endpoint creates synthetic clinical data for testing and development purposes.
    For each requested record, it will:
    
    1. Create a new EHR with a synthetic patient subject
    2. Generate a randomized composition based on the provided template
    3. Link the composition to the EHR
    4. Store both canonical and flattened versions of the composition
    
    **Features:**
    - Randomizes dates, identifiers, and clinical data
    - Creates realistic variations of the base template
    - Supports custom composition templates
    - Provides detailed response with creation statistics
    - Handles partial failures gracefully
    
    **Default Template:**
    If no base_composition is provided, uses the packaged sample immunization template
    which includes vaccination data with randomized:
    - Patient identifiers
    - Vaccination dates
    - Vaccine types (Meningococcal C, Hepatitis B, Tetanus-Diphtheria, etc.)
    - Healthcare provider information
    - Document metadata
    """
    
    start_time = time.time()
    
    # Use default vaccination composition if none provided
    base_composition = synthetic_request.base_composition
    if not base_composition:
        base_composition = get_default_vaccination_composition()
    
    # Get merge_search_docs configuration
    target_search = request.app.state.config.get("target", {})
    merge_search = target_search.get("search_compositions_merge", False)

    # Use runtime target DB if available
    db = getattr(request.app.state, "target_db", db)
    
    try:
        # Generate the synthetic data
        created_records = await generate_synthetic_data(
            db=db,
            base_composition=base_composition,
            count=synthetic_request.count,
            flattener=flattener,
            merge_search_docs=merge_search
        )
        
        end_time = time.time()
        generation_time = end_time - start_time
        
        # Convert to response models
        record_models = [SyntheticDataRecord(**record) for record in created_records]
        
        # Calculate statistics
        successful_records = [r for r in record_models if r.error is None]
        failed_records = [r for r in record_models if r.error is not None]
        
        response = SyntheticDataResponse(
            total_requested=synthetic_request.count,
            total_created=len(successful_records),
            total_errors=len(failed_records),
            generation_time_seconds=round(generation_time, 3),
            records=record_models
        )
        
        return response
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate synthetic data: {str(e)}"
        )


@router.get(
    "/stats",
    response_model=SyntheticDataStats,
    status_code=status.HTTP_200_OK,
    summary="Get Synthetic Data Statistics",
    description="Get statistics about the synthetic data generation process from the last generation run.",
    responses=get_synthetic_stats_responses,
    operation_id="get_rps_dual_synthetic_stats",
)
async def get_synthetic_data_stats():
    """
    Get statistics about synthetic data generation.
    
    Note: This is a placeholder endpoint. In a production system, you might want to:
    - Store statistics in the database
    - Track historical generation runs
    - Provide more detailed analytics
    """
    # This is a placeholder implementation
    # In a real system, you'd retrieve actual statistics from storage
    return SyntheticDataStats(
        success_rate=95.0,
        average_time_per_record=0.75,
        total_ehrs_created=0,
        total_compositions_created=0
    )
