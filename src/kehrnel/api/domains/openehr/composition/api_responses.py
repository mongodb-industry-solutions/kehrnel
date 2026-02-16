from fastapi import status
from kehrnel.api.domains.openehr.composition.models import Composition, VersionedComposition
from kehrnel.api.common.models import ErrorResponse, OriginalVersionResponse
from kehrnel.api.domains.openehr.ehr.api_responses import get_revision_history_responses


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
               "description": "The ETag of the new 'deleted' audit entry version (its UID)",
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


get_versioned_composition_responses = {
    status.HTTP_200_OK: {
        "description": "Versioned Composition metadata retrieved successfully.",
        "model": VersionedComposition, # Assuming you create this model
        "content": {
            "application/json": {
                "example": {
                    "_type": "VERSIONED_COMPOSITION",
                    "uid": {
                        "value": "a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6"
                    },
                    "ownerId": {
                        "id": {"value": "e1f2a3b4-c5d6-e7f8-a9b0-c1d2e3f4a5b6"},
                        "namespace": "local",
                        "type": "EHR"
                    },
                    "timeCreated": {
                        "value": "2024-05-22T10:00:00.000Z"
                    }
                }
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified EHR or Composition was not found.",
        "model": ErrorResponse
    }
}


get_composition_version_at_time_responses = {
    status.HTTP_200_OK: {
        "description": "The requested version of the composition has been successfully retrieved.",
        "model": OriginalVersionResponse,
        "content": {
            "application/json": {
                "example": {
                    "_type": "ORIGINAL_VERSION",
                    "uid": {
                        "value": "a1b2c3d4-e5f6::my-openehr-server::1",
                        "_type": "OBJECT_VERSION_ID"
                    },
                    "precedingVersionUid": None,
                    "data": {
                        "_type": "COMPOSITION",
                        "name": {"value": "Problem/Diagnosis"},
                        "archetype_details": {
                            "template_id": {"value": "T-IGR-TUMOUR-SUMMARY"}
                        },
                    },
                    "commitAudit": {
                        "system_id": "my-openehr-server",
                        "committer_name": "System",
                        "time_committed": "2024-05-20T10:00:00.000Z",
                        "change_type": "creation"
                    },
                    "contribution": {
                        "id": {"value": "c1d2e3f4-a5b6-c7d8-e9f0-a1b2c3d4e5f6"},
                        "namespace": "local",
                        "type": "CONTRIBUTION"
                    }
                }
            }
        },
        "headers": {
            "ETag": {
                "description": "The ETag of the returned version (its UID).",
                "schema": {"type": "string"}
            },
            "Location": {
                "description": "The canonical URL of the returned version.",
                "schema": {"type": "string"}
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR, Composition, or a version at the specified time was not found.",
        "model": ErrorResponse
    }
}