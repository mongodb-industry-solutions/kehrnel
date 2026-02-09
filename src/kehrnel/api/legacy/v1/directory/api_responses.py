# src/kehrnel/api/legacy/v1/directory/api_responses.py

from fastapi import status
from kehrnel.api.legacy.v1.common.models import ErrorResponse
from kehrnel.api.legacy.v1.directory.models import Folder

create_directory_responses = {
    status.HTTP_201_CREATED: {
        "description": "Directory created successfully. Body is returned if 'Prefer: return=representation' header is set.",
        "model": Folder,
        "content": {
            "application/json": {
                "example": {
                    "_type": "FOLDER",
                    "archetype_node_id": "openEHR-EHR-FOLDER.directory.v1",
                    "name": {"value": "Directory", "_type": "DV_TEXT"},
                    "uid": {
                        "value": "8849182c-82ad-4088-a07f-48ead4180515::my-openehr-server::1",
                        "_type": "OBJECT_VERSION_ID",
                    },
                    "items": [],
                    "folders": [],
                }
            }
        },
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The request body is invalid.",
        "model": ErrorResponse,
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found.",
        "model": ErrorResponse,
    },
    status.HTTP_409_CONFLICT: {
        "description": "A directory for this EHR already exists.",
        "model": ErrorResponse,
    },
}

update_directory_responses = {
    status.HTTP_200_OK: {
        "description": "Directory updated successfully. Body is returned if 'Prefer: return=representation' header is set.",
        "model": Folder,
        "content": create_directory_responses[status.HTTP_201_CREATED]["content"], # Reuse the same example
    },
    status.HTTP_204_NO_CONTENT: {
        "description": "Directory updated successfully. Body is empty if 'Prefer: return=minimal' header is set.",
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The request body or If-Match header is invalid.",
        "model": ErrorResponse,
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found, or it does not have a directory.",
        "model": ErrorResponse,
    },
    status.HTTP_412_PRECONDITION_FAILED: {
        "description": "The `If-Match` header does not match the current version of the directory.",
        "model": ErrorResponse,
    }
}

get_directory_responses = {
    status.HTTP_200_OK: {
        "description": "Directory retrieved successfully.",
        "model": Folder,
        "content": create_directory_responses[status.HTTP_201_CREATED]["content"], # Reuse example
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The `version_at_time` parameter is not a valid ISO 8601 timestamp.",
        "model": ErrorResponse,
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found, it does not have a directory, or the specified `path` or `version_at_time` does not exist.",
        "model": ErrorResponse,
    },
}

get_directory_by_version_id_responses = {
    status.HTTP_200_OK: {
        "description": "Directory version retrieved successfully.",
        "model": Folder,
        "content": create_directory_responses[status.HTTP_201_CREATED]["content"], # Reuse example
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found, or a directory with the specified `version_uid` does not exist for that EHR, or the `path` is invalid.",
        "model": ErrorResponse,
    },
}

delete_directory_responses = {
    status.HTTP_204_NO_CONTENT: {
        "description": "Directory logically deleted successfully.",
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The If-Match header is missing or invalid.",
        "model": ErrorResponse,
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "The EHR with the specified `ehr_id` was not found, or it does not have a directory.",
        "model": ErrorResponse,
    },
    status.HTTP_412_PRECONDITION_FAILED: {
        "description": "The `If-Match` header does not match the current version of the directory.",
        "model": ErrorResponse,
    }
}