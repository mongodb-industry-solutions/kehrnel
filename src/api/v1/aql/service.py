# src/api/v1/aql/service.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError
from typing import List

from src.api.v1.aql.repository import (
    save_stored_query,
    find_stored_query_by_name,
    delete_stored_query_by_name,
    execute_aql_query,
    find_all_stored_queries
)

from src.api.v1.aql.models import StoredQuery, QueryRequest, QueryResponse, StoredQuerySummary

async def create_or_update_stored_query(name: str, aql_query: str, db: AsyncIOMotorDatabase) -> None:
    """
    Handles the logic for creating or updating a stored query.
    """
    try:
        await save_stored_query(name=name, aql_query=aql_query, db=db)
    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error while saving stored query '{name}': {e}"
        )

async def retrieve_stored_query(name: str, db: AsyncIOMotorDatabase) -> StoredQuery:
    """
    Handles the logic for retrieving a stored query by its name.
    """
    query_doc = await find_stored_query_by_name(name, db)
    if not query_doc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Stored query with name '{name}' not found."
        )
    return StoredQuery.model_validate(query_doc)

async def list_all_stored_queries(db: AsyncIOMotorDatabase) -> List[StoredQuerySummary]:
    """Handles the logic for listing all stored queries."""
    try:
        query_docs = await find_all_stored_queries(db)
        return [StoredQuerySummary.model_validate(doc) for doc in query_docs]
    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error while listing stored queries: {e}"
        )

async def remove_stored_query(name: str, db: AsyncIOMotorDatabase) -> None:
    """
    Handles the logic for deleting a stored query.
    """
    try:
        deleted_count = await delete_stored_query_by_name(name, db)
        if deleted_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stored query with name '{name}' not found for deletion."
            )
    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error while deleting stored query '{name}': {e}"
        )

async def process_aql_query(request: QueryRequest, request_path: str, db: AsyncIOMotorDatabase) -> QueryResponse:
    """
    Processes an AQL query execution request by calling the repository's execution engine.
    """
    try:
        # Pass the full request model to the repository
        result_dict = await execute_aql_query(
            request_body=request.model_dump(by_alias=True, exclude_none=True),
            db=db
        )

        # Build the final response object
        response_data = {
            "meta": {
                "href": request_path,
                "executed_aql": request.query,
            },
            "q": result_dict["q"],
            "columns": result_dict["columns"],
            "rows": result_dict["rows"]
        }

        return QueryResponse.model_validate(response_data)
    
    except ValueError as e:
        # This will catch errors from the future AQL engine
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid AQL query: {e}"
        )