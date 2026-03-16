"""FHIR domain preview routes."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body
from pydantic import BaseModel, Field


router = APIRouter(prefix="/api/domains/fhir", tags=["FHIR"])


class FhirSearchRequest(BaseModel):
    resource_type: str = Field(default="Patient", description="FHIR resource type (e.g., Patient, Observation).")
    criteria: dict[str, Any] = Field(default_factory=dict, description="FHIR search criteria for preview purposes.")
    limit: int = Field(default=20, ge=1, le=1000, description="Maximum number of preview results.")


@router.post("/search")
async def search_fhir_preview(payload: FhirSearchRequest = Body(default_factory=FhirSearchRequest)):
    """
    Preview-only FHIR Search endpoint.

    This provides a stable API surface while full FHIR strategy-backed search
    is being implemented.
    """
    return {
        "status": "todo",
        "message": "FHIR search preview endpoint. Full FHIR strategy search is not implemented yet.",
        "query": payload.model_dump(),
        "results": [],
        "total": 0,
    }

