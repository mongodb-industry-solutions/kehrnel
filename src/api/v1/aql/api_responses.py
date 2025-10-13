from fastapi import status
from src.api.v1.common.models import ErrorResponse

# TODO: Add the API responses for the AQL query execution

stored_query_responses = {
    status.HTTP_201_CREATED: {
        "description": "Stored query was created successfully"
    },
    status.HTTP_200_OK: {
        "description": "Stored query was updated successfully or retrieved",
        "content": {
            "text/plain": {
                "example": "SELECT c FROM EHR e CONTAINS COMPOSITION c"
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "A stored query with the specified name doesn't exist",
        "model": ErrorResponse
    }
}