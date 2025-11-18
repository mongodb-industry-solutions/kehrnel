# src/api/v1/directory/api_responses.py

from fastapi import status
from src.api.v1.common.models import ErrorResponse
from src.api.v1.directory.models import Folder

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