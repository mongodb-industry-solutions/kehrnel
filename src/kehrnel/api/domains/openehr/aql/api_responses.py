from fastapi import status
from kehrnel.api.common.models import ErrorResponse
from .models import AQLtoMQLDebugErrorResponse, AQLtoMQLDebugResponse, QueryResponse

# API responses for AQL query execution
aql_query_responses = {
    status.HTTP_200_OK: {
        "description": "AQL query executed successfully",
        "model": QueryResponse,
        "content": {
            "application/json": {
                "example": {
                    "meta": {
                        "created": "2024-01-01T10:00:00.000Z",
                        "type": "AQL_QUERY_RESULT",
                        "generator": "KEHRNEL_OPENEHR_SERVER",
                        "executed_aql": "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c"
                    },
                    "q": "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c",
                    "columns": [
                        {
                            "name": "uid",
                            "path": "c/uid/value"
                        }
                    ],
                    "rows": [
                        {
                            "uid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890::local::1"
                        },
                        {
                            "uid": "b2c3d4e5-f6g7-8901-bcde-f23456789012::local::1"
                        }
                    ]
                }
            }
        }
    },
    status.HTTP_422_UNPROCESSABLE_CONTENT: {
        "description": "Validation Error",
        "model": ErrorResponse
    }
}

stored_query_responses = {
    status.HTTP_201_CREATED: {
        "description": "Stored query was created successfully"
    },
    status.HTTP_200_OK: {
        "description": "Stored query was updated successfully or retrieved",
        "content": {
            "text/plain": {
                "example": "SELECT c/uid/value as uid FROM EHR e CONTAINS COMPOSITION c"
            }
        }
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "A stored query with the specified name doesn't exist",
        "model": ErrorResponse
    }
}

# API responses for AQL validation
aql_validation_responses = {
    status.HTTP_200_OK: {
        "description": "AQL query validation completed",
        "content": {
            "application/json": {
                "example": {
                    "valid": True,
                    "message": "AQL query syntax is valid",
                    "query": "SELECT c/uid/value FROM EHR e CONTAINS COMPOSITION c",
                    "warnings": []
                }
            }
        }
    },
    status.HTTP_400_BAD_REQUEST: {
        "description": "AQL query has syntax errors",
        "content": {
            "application/json": {
                "example": {
                    "valid": False,
                    "message": "Syntax error in AQL query", 
                    "query": "SELECT c/uid/value FROM INVALID_SYNTAX",
                    "errors": [
                        "Expected 'EHR' after 'FROM' at position 25"
                    ]
                }
            }
        }
    }
}

# API responses for listing all stored queries
list_stored_queries_responses = {
    status.HTTP_200_OK: {
        "description": "List of stored queries retrieved successfully",
        "content": {
            "application/json": {
                "example": [
                    {
                        "name": "org.openehr::compositions/basic",
                        "description": "Basic composition query",
                        "created_at": "2024-01-01T10:00:00.000Z"
                    },
                    {
                        "name": "org.openehr::ehr/patient-list", 
                        "description": "List all patients",
                        "created_at": "2024-01-02T14:30:00.000Z"
                    }
                ]
            }
        }
    }
}

# API responses for deleting stored queries
delete_stored_query_responses = {
    status.HTTP_204_NO_CONTENT: {
        "description": "Stored query deleted successfully"
        # No content/example for 204 - this is correct
    },
    status.HTTP_404_NOT_FOUND: {
        "description": "A stored query with the specified name doesn't exist",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Stored query 'org.openehr::nonexistent/query' not found"
                }
            }
        }
    },
    status.HTTP_422_UNPROCESSABLE_CONTENT: {
        "description": "Validation Error",
        "content": {
            "application/json": {
                "example": {
                    "detail": [
                        {
                            "type": "value_error",
                            "loc": ["path", "name"],
                            "msg": "Invalid query name format",
                            "input": "invalid//name"
                        }
                    ]
                }
            }
        }
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