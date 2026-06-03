"""Request/response models for FHIR domain search API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FhirSearchRequest(BaseModel):
    """FHIR search parameters routed to fhir.rps_canonical compile + execute."""

    resource_type: str = Field(
        default="Patient",
        description="FHIR resource type (e.g. Patient, Observation).",
    )
    criteria: dict[str, Any] = Field(
        default_factory=dict,
        description="FHIR search criteria as param → value map.",
    )
    limit: int = Field(
        default=20,
        ge=1,
        le=1000,
        description="Maximum number of results (maps to FHIR _count).",
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Result offset (maps to FHIR _offset).",
    )
    fhir_search: str | None = Field(
        default=None,
        description="Optional FHIR search URL or ResourceType?params form.",
    )
