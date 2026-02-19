# src/kehrnel/api/ingest/models.py

from pydantic import BaseModel, Field, RootModel, model_validator
from typing import Dict, Any

# --- Request Models ---

class FilePathRequest(BaseModel):
    """Model for the request body of the /from-file endpoint."""
    file_path: str = Field(
        ...,
        json_schema_extra={"example": "/app/data/composition-example.json"},
        description="Absolute path to the canonical composition JSON file on the server.",
    )

class EhrIdRequest(BaseModel):
    """Model for the request body of the /from-db endpoint."""
    ehr_id: str = Field(
        ...,
        json_schema_extra={"example": "a3b7e7a7-fe7c-4e8a-8a5b-6a0b9a4b2c15"},
        description="The ehr_id to find the canonical composition in the source database.",
    )

class CanonicalCompositionPayload(RootModel[Dict[str, Any]]):
    """
    A model that accepts a raw dictionary as its payload, which is expected
    to be a valid canonical composition document as stored in the source DB.

    This is used for the /composition endpoint which ingests from the request body.
    """
    @model_validator(mode="after")
    def check_structure(self):
        # With RootModel, 'v' is the entire dictionary.
        v = self.root
        if not all(k in v for k in ["_id", "ehr_id", "canonicalJSON"]):
            raise ValueError("Payload must contain '_id', 'ehr_id', and 'canonicalJSON'.")

        canonical_json = v.get("canonicalJSON", {})
        if not isinstance(canonical_json, dict):
            raise ValueError("'canonicalJSON' must be a dictionary.")

        if canonical_json.get("_type") != "COMPOSITION":
            raise ValueError("The 'canonicalJSON' object must have _type: 'COMPOSITION'")

        return self

# --- Response Models ---

class IngestionSuccessResponse(BaseModel):
    """Standard successful response for all ingestion endpoints."""
    status: str = "success"
    message: str
    flattened_composition_id: str

class ErrorResponse(BaseModel):
    """Standard error response, consistent with the EHR API."""
    detail: str
