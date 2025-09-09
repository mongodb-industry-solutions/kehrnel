from fastapi import status
from src.api.v1.ehr.models import EHRCreationResponse, ErrorResponse, EHR, Composition, EHRStatus
from typing import List
from src.app.core.models import Contribution

get_contribution_responses = {
    status.HTTP_200_OK: {
        "description": "Contribution found and returned successfully",
        "model": Contribution,
        "content": {
            "application/json": {
                "example": {
                    "_id": "c1d2e3f4-a5b6-c7d8-e9f0-a1b2c3d4e5f6",
                    "ehr_id": "a1b2c3d4-e4f5-a6b7-c8d9-e0f1a2b3c4d5",
                    "versions": [
                        {
                            "_type": "EHR_STATUS",
                            "uid": {
                                "value": "f1g2h3i4::my-openehr-server::1",
                                "_type": "OBJECT_VERSION_ID"
                            }
                        }
                    ],
                    "audit": {
                        "system_id": "my-openehr-server",
                        "committer_name": "System",
                        "time_committed": "2024-01-01T10:00:00.000Z",
                        "change_type": "creation"
                    }
                }
            }
        },
        "headers": {
            "ETag": {
                "description": "The ETag of the contribution (its UID)",
                "schema": {
                    "type": "string"
                }
            },
            "Location": {
                "description": "The path to the retrieved contribution resource.",
                "schema": {
                    "type": "string"
                }
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified EHR or the Contribution within it was not found.",
        "model": ErrorResponse
    }
}

get_ehr_status_responses = {
    status.HTTP_200_OK: {
        "description": "EHR_STATUS retrieved successfully",
        "model": EHRStatus,
        "content": {
            "application/json": {
                "example": {
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
                            "id": {
                                "value": "patient-123",
                                "namespace": "hospital.main.ids",
                                "type": "Person"
                            }
                        }
                    },
                    "is_modifiable": True,
                    "is_queryable": True
                }
            }
        },
        "headers": {
            "ETag": {
                "description": "The ETag of the current EHR_STATUS version (its UID)",
                "schema": {
                    "type": "string"
                }
            },
            "Location": {
                "description": "The path to this specific version of the EHR_STATUS resource.",
                "schema": {"type": "string"},
            },
            "Last-Modified": {
                "description": "The timestamp of when this version of the EHR_STATUS was created.",
                "schema": {"type": "string", "format": "date-time"},
            },
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found",
        "model": ErrorResponse
    }
}


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
                    get_ehr_by_id_responses[status.HTTP_200_OK]["content"]["application/json"]["example"]
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

update_composition_responses = {
    status.HTTP_200_OK: {
        "description": "Composition updated successfully by creating a new version. The new version is returned",
        "content": {
            "application/json": {
                "example": {
                    "_type": "COMPOSITION",
                    "name": {
                        "value": "Updated Problem/Diagnosis",
                        # All canonical composition fields ...
                    }
                }
            }
        },
        "headers": {
            "ETag": {
                "description": "The ETag of the new composition version (its UID)",
                "schema": {
                    "type": "string"
                }
            },
            "Location": {
                "description": "The path to the newly created version of the composition resource",
                "schema": {
                    "type": "string"
                }
            },
            "Last-Modified": {
                "description": "The timestamp of when this new version was created",
                "schema": {
                    "type": "string", "format": "date-time"
                }
            },
        },
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The `If-Match` header does not match the `preceding_version_uid` in the URL",
        "model": ErrorResponse,
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified EHR or the Composition to be updated was not found",
        "model": ErrorResponse
    },
    status.HTTP_422_UNPROCESSABLE_ENTITY: {
        "description": "The request body is not a valid COMPOSITION object",
        "model": ErrorResponse
    }
}

delete_composition_responses = {
    status.HTTP_204_NO_CONTENT: {
       "description": "Composition successfully marked as deleted. The response has no body",
       "headers": {
           "ETag": {
               "description": "The ETaf of the new 'deleted' audit entry version (its UID)",
               "schema": {
                   "type": "string"
               }
           },
           "Location": {
               "description": "The path to the 'versioned composition' resource which has been modified ",
               "schema": {
                   "type": "string"
               }
           },
           "Last-Modified": {
               "description": "The timestamp of when the deletion was committed",
               "schema": {
                   "type": "string",
                   "format": "date-time"
               }
           }
       }
    },
    status.HTTP_409_CONFLICT: {
        "description": "The specified version of the composition has already been deleted.",
        "model": ErrorResponse,
        "content": {
            "application/json": {
                "example": {
                    "detail": "Version '{version_uid}' has already been deleted."
                }
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified EHR or the composition to be deleted was not found",
        "model": ErrorResponse
    },
    status.HTTP_412_PRECONDITION_FAILED: {
        "description": "The `If-Match` header does not match the `preceding_version_uid`, indicating a concurrency conflict",
        "model": ErrorResponse
    }
}