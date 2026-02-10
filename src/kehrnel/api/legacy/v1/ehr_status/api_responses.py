from fastapi import status
from kehrnel.api.legacy.v1.ehr_status.models import EHRStatus, VersionedEHRStatus
from kehrnel.api.legacy.v1.common.models import ErrorResponse, RevisionHistory, OriginalVersionResponse

get_ehr_status_by_version_id_responses = {
    status.HTTP_200_OK: {
        "description": "The specified version of the EHR_STATUS was found and returned.",
        "model": EHRStatus,
        "content": {
            "application/json": {
                "example": {
                    "uid": {
                        "value": "f1g2h3i4::my-openehr-server::2",
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
                    "is_modifiable": False,
                    "is_queryable": True
                }
            }
        },
        "headers": {
            "ETag": {
                "description": "The ETag of this specific EHR_STATUS version (its UID).",
                "schema": {
                    "type": "string"
                }
            },
            "Location": {
                "description": "The path to this specific version of the EHR_STATUS resource.",
                "schema": {"type": "string"},
            },
            "Last-Modified": {
                "description": "The timestamp of when this version of the EHR_STATUS was committed.",
                "schema": {"type": "string", "format": "date-time"},
            },
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found, or the `version_uid` does not exist.",
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


update_ehr_status_responses = {
    status.HTTP_200_OK: {"description": "EHR_STATUS updated successfully."},
    status.HTTP_404_NOT_FOUND: {"description": "EHR not found.", "model": ErrorResponse},
    status.HTTP_412_PRECONDITION_FAILED: {"description": "The If-Match header does not match the latest version.", "model": ErrorResponse},
}


get_versioned_ehr_status_responses = {
    status.HTTP_200_OK: {
        "description": "Versioned EHR_STATUS metadata retrieved successfully.",
        "model": VersionedEHRStatus,
        "content": {
            "application/json": {
                "example": {
                    "_type": "VERSIONED_EHR_STATUS",
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
        "description": "The specified EHR was not found.",
        "model": ErrorResponse
    }
}


get_ehr_status_revision_history_responses = {
    status.HTTP_200_OK: {
        "description": "EHR_STATUS revision history retrieved successfully.",
        "model": RevisionHistory,
        "content": {
            "application/json": {
                "example": {
                    "items": [
                        {
                            "versionId": {
                                "value": "f1g2h3i4::my-openehr-server::1",
                                "_type": "OBJECT_VERSION_ID"
                            },
                            "audit": {
                                "system_id": "my-openehr-server",
                                "committer_name": "System",
                                "time_committed": "2024-01-01T10:00:00.000Z",
                                "change_type": "creation"
                            }
                        },
                        {
                            "versionId": {
                                "value": "f1g2h3i4::my-openehr-server::2",
                                "_type": "OBJECT_VERSION_ID"
                            },
                            "audit": {
                                "system_id": "my-openehr-server",
                                "committer_name": "System",
                                "time_committed": "2024-05-23T11:00:00.000Z",
                                "change_type": "modification"
                            }
                        }
                    ]
                }
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The specified EHR was not found.",
        "model": ErrorResponse
    }
}


get_ehr_status_version_at_time_responses = {
    status.HTTP_200_OK: {
        "description": "The requested version of the EHR_STATUS has been successfully retrieved.",
        "model": OriginalVersionResponse,
        "content": {
            "application/json": {
                "example": {
                    "_type": "ORIGINAL_VERSION",
                    "uid": {
                        "value": "a1b2c3d4::my-openehr-server::2",
                        "_type": "OBJECT_VERSION_ID"
                    },
                    "precedingVersionUid": {
                        "value": "a1b2c3d4::my-openehr-server::1",
                        "_type": "OBJECT_VERSION_ID"
                    },
                    "data": {
                        "_type": "EHR_STATUS",
                        "uid": {
                            "value": "a1b2c3d4::my-openehr-server::2"
                        },
                        "subject": {
                            "_type": "PARTY_SELF"
                        },
                        "is_modifiable": False,
                        "is_queryable": True
                    },
                    "commitAudit": {
                        "system_id": "my-openehr-server",
                        "committer_name": "Dr. Alice",
                        "time_committed": "2024-05-23T14:30:00.000Z",
                        "change_type": "modification"
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
                "description": "The canonical URL of the returned EHR_STATUS version.",
                "schema": {"type": "string"}
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR or a version at the specified time was not found.",
        "model": ErrorResponse
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The `version_at_time` parameter is not a valid ISO 8601 timestamp.",
        "model": ErrorResponse
    }
}


get_ehr_status_version_by_id_responses = {
    status.HTTP_200_OK: {
        "description": "The specified version of the EHR_STATUS has been successfully retrieved.",
        "model": OriginalVersionResponse,
        "content": get_ehr_status_version_at_time_responses[status.HTTP_200_OK]["content"] # Re-use example
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR or the specified version_uid was not found.",
        "model": ErrorResponse
    }
}