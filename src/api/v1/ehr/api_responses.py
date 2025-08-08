from fastapi import APIRouter, Depends, status, Body, Query, HTTPException, Response
from src.api.v1.ehr.models import EHRCreationResponse, EHRStatus, ErrorResponse, EHR, Composition
from typing import Optional, List

# Dictionary of examples for the request body for the create_ehr_endpoint:
ehr_status_example = {
    "with_subject": {
        "summary": "Create EHR with a subject",
        "description": "Provide a full EHR_STATUS object to create an EHR for a known subject.",
        "value": {
            "_type": "EHR_STATUS",
            "subject": {
                "id": "12345",
                "namespace": "my.patient.id.space"
            },
            "is_modifiable": True,
            "is_queryable": True
        }
    },
    "no_subject": {
        "summary": "Create a subject-less EHR",
        "description": "Send an empty request body `{}`. The system will auto-generate a temporary subject.",
        "value": None
    }
}

# Responses for swagger documentation for the create_ehr_endpoint
create_ehr_api_responses = {
    status.HTTP_201_CREATED: {
        "description": "EHR created successfully.",
        "model": EHRCreationResponse,
    },
    status.HTTP_409_CONFLICT: {
        "description": "An EHR for the given subject already exists.",
        "model": ErrorResponse,
        "content": {
            "application/json": {
                "example": {"detail": "An EHR with subjectId '12345' already exists."}
            }
        },
    },
    status.HTTP_422_UNPROCESSABLE_ENTITY: {
        "description": "The request body is invalid (e.g., missing a required field).",
        "model": ErrorResponse,
    }
}

get_ehr_by_id_responses = {
    status.HTTP_200_OK: {
        "description": "EHR found and returned successfully",
        "model": EHR,
        "content": {
            "application/json": {
                "example": {
                    "ehr_id": "a1b2c3d4-e4f5-a6b7-c8d9-e0f1a2b3c4d5",
                    "system_id": "my-openehr-server",
                    "time_created": "2023-10-27T12:00:00.123456+00:00",
                    "ehr_status": {
                        "uid": "f1g2h3i4-j5k6-l7m8-n9o0-p1q2r3s4t5u6::my-openehr-server::1",
                        "_type": "EHR_STATUS",
                        "subject": {
                            "id": "patient-123",
                            "namespace": "hospital.main.ids"
                        },
                        "is_modifiable": True,
                        "is_queryable": True
                    },
                    "contributions": [
                        "c1a2b3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6"
                    ],
                    "compositions": [],
                    "directory_id": None
                }
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found.",
        "model": ErrorResponse
    }
}

update_ehr_status_responses = {
    status.HTTP_200_OK: {"description": "EHR_STATUS updated successfully."},
    status.HTTP_404_NOT_FOUND: {"description": "EHR not found.", "model": ErrorResponse},
    status.HTTP_412_PRECONDITION_FAILED: {"description": "The If-Match header does not match the latest version.", "model": ErrorResponse},
}


get_ehr_list_responses = {
    status.HTTP_200_OK: {
        "description": "A list of EHRs retrieved successfully.",
        "model": List[EHR],
        "content": {
            "application/json": {
                "example": [
                    {
                        "ehr_id": "a1b2c3d4-e4f5-a6b7-c8d9-e0f1a2b3c4d5",
                        "system_id": "my-openehr-server",
                        "time_created": "2023-10-27T12:00:00.123456+00:00",
                        "ehr_status": {
                            "uid": "f1g2h3i4-j5k6-l7m8-n9o0-p1q2r3s4t5u6::my-openehr-server::1",
                            "_type": "EHR_STATUS",
                            "subject": {
                                "id": "patient-123",
                                "namespace": "hospital.main.ids"
                            },
                            "is_modifiable": True,
                            "is_queryable": True
                        },
                        "contributions": [
                            "c1a2b3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6"
                        ],
                        "compositions": [],
                        "directory_id": None
                    }
                ]
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {"description": "No EHRs found in the database.", "model": ErrorResponse}
}

# Response definitions for the create composition endpoint
create_composition_responses = {
    status.HTTP_201_CREATED: {
        "description": "Composition successfully created and added to the EHR",
        "model": Composition,
        "headers": {
            "Location": {
                "description": "The path to the newly created composition resource",
                "schema": {"type": "string"},
                "example": "/v1/ehr/{ehr_id}/composition/{composition_uid}"
            },
            "ETag": {
                "description": "The ETag of the new composition version (its UID)",
                "schema": {"type": "string"},
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found",
        "model": ErrorResponse,
        "content": {
            "application/json": {
                "example": {
                    "detail": "EHR with id '...' not found"
                }
            }
        }
    },
    status.HTTP_422_UNPROCESSABLE_ENTITY: {
        "description": "The request body is invalid (e.g. missing a required field like 'template_id')",
        "model": ErrorResponse
    }
}

# Response definitions for the get composition endpoint
get_composition_responses = {
    status.HTTP_200_OK: {
        "description": "Composition found and returned successfully. The body contains the raw canonical JSON of the compositions",
        "content": {
            "application/json": {
                "example": {
                    "_type": "COMPOSITION",
                    "name": {"value": "Problem/Diagnosis"},
                    "archetype_details": {
                        "template_id": {"value": "T-IGR-TUMOUR-SUMMARY"}
                    },
                    # ... canonical composition fields
                }
            }
        },
        "headers": {
            "ETag": {
                "description": "The ETag of the composition version (its UID).",
                "schema": {"type": "string"},
            },
            "Location": {
                "description": "The path to the retrieved composition resource.",
                "schema": {"type": "string"},
            },
            "Last-Modified": {
                "description": "The timestamp of when this version of the composition was created.",
                "schema": {"type": "string", "format": "date-time"},
            },
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified EHR or the Composition within it was not found",
        "model": ErrorResponse
    }
}