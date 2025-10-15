from fastapi import status
from src.api.v1.common.models import ErrorResponse
from .models import AQLtoMQLDebugErrorResponse, AQLtoMQLDebugResponse

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

aql_to_mql_debug_responses = {
    status.HTTP_200_OK: {
        "description": "The AQL query was successfully translated.",
        "model": AQLtoMQLDebugResponse,
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The provided AQL query has a syntax error or is otherwise invalid.",
        "model": AQLtoMQLDebugErrorResponse,
    },
    status.HTTP_500_INTERNAL_SERVER_ERROR: {
        "description": "An unexpected server error occurred during translation.",
        "model": ErrorResponse
    }
}