from fastapi import status
from kehrnel.api.legacy.v1.ehr.models import EHRCreationResponse, EHR
from kehrnel.api.legacy.v1.common.models import ErrorResponse, RevisionHistory
from typing import List


get_ehr_by_id_responses = {
    status.HTTP_200_OK: {
        "description": "EHR found and returned successfully.",
        "model": EHR,
        "content": {
            "application/json": {
                "example": {
                    "ehr_id": {"value": "a1b2c3d4-e4f5-a6b7-c8d9-e0f1a2b3c4d5"},
                    "system_id": {"value": "my-openehr-server"},
                    "time_created": {"value": "2024-01-01T10:00:00.000Z"},
                    "ehr_status": {
                        "uid": {
                            "value": "f1g2h3i4::my-openehr-server::1",
                            "_type": "OBJECT_VERSION_ID"
                        },
                        "_type": "EHR_STATUS",
                        "archetype_node_id": "openEHR-EHR-EHR_STATUS.generic.v1",
                        "name": {"value": "EHR status"},
                        "subject": {
                            "_type": "PARTY_SELF",
                            "external_ref": {
                                "id": {"value": "patient-123"},
                                "namespace": "hospital.main.ids",
                                "type": "PERSON"
                            }
                        },
                        "is_modifiable": True,
                        "is_queryable": True
                    },
                    "ehr_access": {
                        "id": {"value": "b2c3d4e5-f6a7-b8c9-d0e1-f2a3b4c5d6e7"},
                        "namespace": "local",
                        "type": "EHR_ACCESS"
                    },
                    "contributions": [],
                    "compositions": [],
                    "directory": None
                }
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found.",
        "model": ErrorResponse
    }
}

# Dictionary of examples for the request body for the create_ehr_endpoint:
get_ehr_by_subject_responses = {
    status.HTTP_200_OK: {
        "description": "EHR for the specified subject found and returned successfully.",
        "model": EHR,
        "content": get_ehr_by_id_responses[status.HTTP_200_OK]["content"]
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "No EHR was found for the specified `subject_id` and `subject_namespace`.",
        "model": ErrorResponse,
    }
}

ehr_status_example = {
    "with_subject": {
        "summary": "Create EHR with a subject",
        "description": "Provide a full EHR_STATUS object to create an EHR for a known subject.",
        "value": {
            "_type": "EHR_STATUS",
            "subject": {
                "_type": "PARTY_SELF",
                "external_ref": {
                    "id": {"value": "12345"},
                    "namespace": "my.patient.id.space",
                    "type": "PERSON"
                }
            },
            "is_modifiable": True,
            "is_queryable": True
        }
    },
    "no_subject": {
        "summary": "Create a subject-less EHR",
        "description": "Send an empty request body `{}`. The system will auto-generate a subject.",
        "value": None
    }
}

# Responses for swagger documentation for the create_ehr_endpoint
create_ehr_api_responses = {
    status.HTTP_201_CREATED: {
        "description": "EHR created successfully. Body is returned if 'Prefer: return=representation' header is set.",
        "model": EHRCreationResponse,
        "content": {
            "application/json": {
                "example": {
                    "ehr_id": {"value": "a1b2c3d4-e4f5-a6b7-c8d9-e0f1a2b3c4d5"},
                    "ehr_status": {
                        "id": {"value": "f1g2h3i4::my-openehr-server::1"},
                        "namespace": "local",
                        "type": "EHR_STATUS"
                    },
                    "system_id": {"value": "my-openehr-server"},
                    "time_created": {"value": "2024-01-01T10:00:00.000Z"},
                    "ehr_access": {
                        "id": {"value": "b2c3d4e5-f6a7-b8c9-d0e1-f2a3b4c5d6e7"},
                        "namespace": "local",
                        "type": "EHR_ACCESS"
                    }
                }
            }
        }
    },
    status.HTTP_409_CONFLICT: {
        "description": "An EHR for the given subject or with the given ehr_id already exists.",
        "model": ErrorResponse,
    }
}


get_ehr_list_responses = {
    status.HTTP_200_OK: {
        "description": "A list of EHRs retrieved successfully.",
        "model": List[EHR],
        "content": {
            "application/json": {
                "example": [
                    get_ehr_by_id_responses[status.HTTP_200_OK]["content"]["application/json"]["example"]
                ]
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {"description": "No EHRs found in the database.", "model": ErrorResponse}
}


get_revision_history_responses = {
    status.HTTP_200_OK: {
        "description": "Revision history retrieved successfully.",
        "model": RevisionHistory,
        "content": {
            "application/json": {
                "example": {
                    "items": [
                        {
                            "versionId": {
                                "value": "a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6::my-openehr-server::1",
                                "_type": "OBJECT_VERSION_ID"
                            },
                            "audit": {
                                "system_id": "my-openehr-server",
                                "committer_name": "System",
                                "time_committed": "2024-05-20T10:00:00.000Z",
                                "change_type": "creation"
                            }
                        },
                        {
                            "versionId": {
                                "value": "a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6::my-openehr-server::2",
                                "_type": "OBJECT_VERSION_ID"
                            },
                            "audit": {
                                "system_id": "my-openehr-server",
                                "committer_name": "Dr. Alice",
                                "time_committed": "2024-05-21T14:30:00.000Z",
                                "change_type": "modification"
                            }
                        }
                    ]
                }
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified EHR or Composition was not found.",
        "model": ErrorResponse
    }
}