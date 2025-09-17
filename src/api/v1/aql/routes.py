# src/api/v1/aql/routes.py

from fastapi import APIRouter, Depends, status, Body, Response, Request
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import List, Dict, Any

from src.app.core.database import get_mongodb_ehr_db
from src.api.v1.aql.service import (
    create_or_update_stored_query,
    retrieve_stored_query,
    remove_stored_query,
    list_all_stored_queries,
    process_aql_query
)
from src.api.v1.aql.models import StoredQuerySummary, QueryResponse
from src.api.v1.aql.api_responses import stored_query_responses
from src.api.v1.aql.aql_transformer import AQLtoMQLTransformer

router = APIRouter(
    prefix="/query",
    tags=["AQL"]
)

@router.post(
    "/aql",
    summary="Execute AQL Query",
    description="Executes an AQL query provided in the request body and returns the results.",
    response_model=QueryResponse
)
async def execute_query(
    request: Request,
    aql: str = Body(..., media_type="text/plain", description="The AQL query string."),
    ehr_id: str = None,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Accepts an AQL query as plain text, executes it, and returns the result set.
    Optional ehr_id parameter can be provided to filter results for a specific EHR.
    """
    response = await process_aql_query(aql_query=aql, request_url=request.url, db=db, ehr_id=ehr_id)
    return response


@router.post(
    "/ast",
    summary="Execute AQL AST Query (Testing)",
    description="Executes an AQL query from AST structure (for testing LET clauses and other features)."
)
async def execute_ast_query(
    request: Request,
    ast_data: Dict[str, Any] = Body(..., description="The AQL AST structure."),
    ehr_id: str = None,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Accepts an AQL query as AST structure, converts it to MongoDB pipeline, executes it, and returns the result set.
    This endpoint is primarily for testing LET clause functionality and other advanced features.
    """
    try:
        # Create transformer with AST
        transformer = AQLtoMQLTransformer(ast_data, ehr_id=ehr_id)
        
        # Generate MongoDB pipeline
        pipeline = transformer.build_pipeline()
        
        # Execute the pipeline
        collection = db["sm_compositions3"]  # Use the composition collection from config
        cursor = collection.aggregate(pipeline)
        results = await cursor.to_list(length=1000)  # Limit to 1000 results for testing
        
        # Return simple response for testing
        return {
            "ast": ast_data,
            "pipeline": pipeline,
            "resultCount": len(results),
            "results": results[:10] if results else [],  # Show first 10 results only
            "letVariables": list(transformer.let_variables.keys())
        }
        
    except Exception as e:
        # Return error details for debugging
        return {
            "ast": ast_data,
            "pipeline": [],
            "error": str(e),
            "errorType": type(e).__name__
        }


@router.get(
    "",
    summary="List Stored Queries",
    description="Lists all available stored queries.",
    response_model=List[StoredQuerySummary],
    response_model_by_alias=True
)
async def get_all_stored_queries(db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)):
    """
    Returns a list of summaries for all stored queries.
    """
    return await list_all_stored_queries(db=db)

@router.put(
    "/{name:path}",
    summary="Create/Update Stored Query",
    description="Creates a new stored query or updates an existing one with the given `name`. The `name` must be a reverse domain name (e.g., `org.openehr::compositions/v1`). The body must be the raw AQL string.",
    status_code=status.HTTP_201_CREATED,
    responses=stored_query_responses
)
async def put_stored_query(
    name: str,
    response: Response,
    aql: str = Body(..., media_type="text/plain", description="The AQL query string."),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Stores or updates an AQL query. The name can contain slashes.
    If the query `name` is new, it returns 201 Created.
    If it exists, it's updated. (We return 201 for simplicity).
    """
    await create_or_update_stored_query(name=name, aql_query=aql, db=db)
    return Response(status_code=status.HTTP_201_CREATED)


@router.get(
    "/{name:path}",
    summary="Get Stored Query",
    description="Retrieves a stored query by its `name`.",
    response_class=Response, # Return raw text
    responses=stored_query_responses
)
async def get_stored_query(
    name: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Returns the AQL string of the stored query as plain text. The name can contain slashes.
    """
    stored_query = await retrieve_stored_query(name=name, db=db)
    return Response(content=stored_query.query, media_type="text/plain")


@router.delete(
    "/{name:path}",
    summary="Delete Stored Query",
    description="Deletes a stored query by its `name`.",
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_stored_query(
    name: str,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Deletes the stored query and returns 204 No Content on success. The name can contain slashes.
    """
    await remove_stored_query(name=name, db=db)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

