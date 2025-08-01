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

get_template_responses = {
    status.HTTP_200_OK: {
        "description": "Template found and returned successfully. The body contains the raw XML content.",
        "content": {
            "application/xml": {
                "schema": {
                    "type": "string",
                    "format": "binary"
                },
                "example": "<?xml version='1.0' encoding='UTF-8'?><template>...</template>"
            }
        },
        "headers": {
            "ETag": {
                "description": "The ETag of the template, which is a hash of its content",
                "schema": {"type": "string"},
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "A template with the specified `template_id` does not exist.",
        "model": ErrorResponse,
        "content": {
            "application/json": {
                "example": {
                    "detail": "Template with ID 'NON_EXISTENT_ID' not found."
                }
            }
        }
    }
}