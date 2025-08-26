from fastapi import status
from src.api.v1.ehr.models import ErrorResponse
from src.api.v1.aql.models import QueryResponse

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

execute_query_responses = {
    status.HTTP_200_OK: {
        "description": "Query executed successfully.",
        "model": QueryResponse
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "The AQL query is invalid or contains errors.",
        "model": ErrorResponse
    }
}