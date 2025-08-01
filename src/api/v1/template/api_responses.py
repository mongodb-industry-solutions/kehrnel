from fastapi import status
from src.api.v1.ehr.models import ErrorResponse

create_template_responses = {
    status.HTTP_201_CREATED: {
        "description": "Template successfully uploaded. The response body is empty, but headers `Location` and `ETag` are set.",
        "headers": {
            "Location": {
                "description": "The path to the newly created template resource.",
                "schema": {
                    "type": "string"
                },
                "example": "/v1/template/T-IGR-PMSI-EXTRACT"
            },
            "Etag": {
                "description": "The Etag of the created template, which is a hash of its content",
                "schema": {
                    "type": "string",
                },
                "example": "a1b2c3d4e5f6..."
            }
        }
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The request body is not a valid OPERATIONAL_TEMPLATE (e.g, invalid XML or missing template_id)",
        "model": ErrorResponse
    },
    status.HTTP_409_CONFLICT: {
        "description": "A template with the same template_id already exists",
        "model": ErrorResponse
    }
}