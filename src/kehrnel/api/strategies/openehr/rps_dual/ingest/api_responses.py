# src/kehrnel/api/legacy/v1/ingest/api_responses.py

from fastapi import status
from kehrnel.api.strategies.openehr.rps_dual.ingest.models import IngestionSuccessResponse, ErrorResponse

# --- Example Payloads for Documentation ---

ingest_from_body_example = {
    "summary": "Example of a full canonical composition document",
    "description": "This is the structure expected from the source database, containing metadata and the 'canonicalJSON' object.",
    "value": {
        "_id": "d3c6c8d7-b8ea-4a3e-b8d4-f3c7e7b7f1c1",
        "ehr_id": "a3b7e7a7-fe7c-4e8a-8a5b-6a0b9a4b2c15",
        "template_id": '72c38a74-c7fa-4836-9b7f-34882612aa9c',
        "template_name": 'EII-Control_v2',
        "composition_date":'2024-11-11T07:05:54.435Z',
        "composition_version": '1',
        "archetype_node_id": 'openEHR-EHR-COMPOSITION.self_reported_data.v1',
        "last_processed": '2025-03-14T10:00:19.024Z',
        "canonicalJSON": {
            "_type": "COMPOSITION",
            "archetype_node_id": "openEHR-EHR-COMPOSITION.report.v1",
            "name": {"_type": "DV_TEXT", "value": "Example Report"},
            "archetype_details": {
                "archetype_id": {"value": "openEHR-EHR-COMPOSITION.report.v1"},
                "template_id": {"value": "Example.v1"},
                "rm_version": "1.0.4"
            },
            "language": {"terminology_id": {"value": "ISO_639-1"}, "code_string": "en"},
            "territory": {"terminology_id": {"value": "ISO_3166-1"}, "code_string": "US"},
            "category": {"defining_code": {"terminology_id": {"value": "openehr"}, "code_string": "433"}},
            "composer": {"_type": "PARTY_IDENTIFIED", "name": "Dr. Smith"},
            "context": {},
            "content": []
        }
    }
}

# --- Response Definitions for API Endpoints ---

# Shared error responses to avoid duplication
shared_ingestion_error_responses = {
    status.HTTP_422_UNPROCESSABLE_ENTITY: {
        "description": "Transformation Error: The composition could not be processed by the flattener.",
        "model": ErrorResponse,
        "content": {
            "application/json": {"example": {"detail": "Transformation Error: Unknown code 'at9999' for domain 'at'"}}
        }
    },
    status.HTTP_500_INTERNAL_SERVER_ERROR: {
        "description": "An unexpected internal error occurred.",
        "model": ErrorResponse,
        "content": {
            "application/json": {"example": {"detail": "Internal Server Error: [specific error message]"}}
        }
    }
}

ingest_from_payload_responses = {
    status.HTTP_201_CREATED: {
        "description": "Composition from payload ingested and stored successfully.",
        "model": IngestionSuccessResponse,
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The request body is malformed or missing required fields.",
        "model": ErrorResponse,
        "content": {
            "application/json": {"example": {"detail": "Payload must contain '_id', 'ehr_id', and 'canonicalJSON'."}}
        }
    },
    **shared_ingestion_error_responses  # Merge shared responses
}

ingest_from_file_responses = {
    status.HTTP_201_CREATED: {
        "description": "Composition from local file ingested and stored successfully.",
        "model": IngestionSuccessResponse,
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified file path could not be found on the server.",
        "model": ErrorResponse,
        "content": {
            "application/json": {"example": {"detail": "The specified file was not found: /path/to/nonexistent.json"}}
        }
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The file is not a valid JSON or the contained data is malformed.",
        "model": ErrorResponse,
        "content": {
            "application/json": {"example": {"detail": "The file is not a valid JSON: /path/to/bad.json"}}
        }
    },
    **shared_ingestion_error_responses
}

ingest_from_db_responses = {
    status.HTTP_201_CREATED: {
        "description": "Composition from source database ingested and stored successfully.",
        "model": IngestionSuccessResponse,
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "No canonical composition was found for the given ehr_id.",
        "model": ErrorResponse,
        "content": {
            "application/json": {"example": {"detail": "No canonical composition found for ehr_id: ... "}}
        }
    },
    **shared_ingestion_error_responses
}