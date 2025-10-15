# src/api/v1/aql/routes.py

from starlette.responses import JSONResponse
from fastapi import APIRouter, Depends, status, Body, Response, Request, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import List, Dict, Any

from src.app.core.database import get_mongodb_ehr_db
from src.api.v1.aql.service import (
    create_or_update_stored_query,
    retrieve_stored_query,
    remove_stored_query,
    list_all_stored_queries,
    process_aql_query,
    process_aql_ast_query
)
from src.api.v1.aql.models import StoredQuerySummary, QueryResponse, AQLtoMQLDebugResponse, AQLtoMQLDebugErrorResponse
from src.api.v1.aql.api_responses import stored_query_responses, aql_to_mql_debug_responses
from src.aql_parser.validator import validate_aql_syntax

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
    "/aql/validate",
    summary="Validate AQL Query",
    description="Validates an AQL query syntax without executing it."
)
async def validate_aql_query(
    aql: str = Body(..., media_type="text/plain", description="The AQL query string to validate.")
):
    """
    Validates an AQL query syntax and returns validation results including errors and warnings.
    This endpoint can be used to check query syntax before execution.
    """
    try:
        validation_result = validate_aql_syntax(aql)
        return validation_result
    except Exception as e:
        return {
            "success": False,
            "message": "Validation process failed.",
            "errors": [str(e)],
            "warnings": []
        }


@router.post(
    "/aql/parse",
    summary="Parse AQL to AST",
    description="Converts an AQL query to AST structure without executing it."
)
async def parse_aql_to_ast_endpoint(
    aql: str = Body(..., media_type="text/plain", description="The AQL query string to parse.")
):
    """
    Parses an AQL query string and returns the corresponding AST structure.
    This endpoint can be used to understand how queries are interpreted or for debugging.
    """
    try:
        from src.aql_parser.parser import AQLParser
        
        parser = AQLParser(aql)
        ast = parser.parse()
        
        return {
            "success": True,
            "message": "AQL parsed successfully",
            "original_query": aql,
            "ast": ast
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Failed to parse AQL: {str(e)}",
            "original_query": aql,
            "ast": None,
            "error": str(e)
        }


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
    This endpoint is primarily for testing
    """
    try:
        
        response = await process_aql_ast_query(ast_query= ast_data, request_url=request.url, db=db, ehr_id=ehr_id)
        return response
        
    except Exception as e:
        return {
            "query": ast_data,
            "columns": [],
            "rows": [],
            "error": str(e),
            "errorType": type(e).__name__
        }


@router.post(
    "/aql/mql",
    summary="Translate AQL to MongoDB Query Language (MQL)",
    description=(
        "Parses an AQL query and translates it into the corresponding MongoDB Aggregation Pipeline (MQL). "
        "Allows users to understand how their AQL queries are being interpreted and executed against the underlying MongoDB database"
        "It does NOT execute the query."
    ),
    response_model=AQLtoMQLDebugResponse,
    responses=aql_to_mql_debug_responses
)
async def debug_aql_to_mql_query(
    request: Request,
    aql: str = Body(..., media_type="text/plain", description="The AQL query string to translate.", example="SELECT c/uid/value as composition_uid, c/name/value as name FROM COMPOSITION c"),
    ehr_id: str = Query(
        None,
        description="Optional EHR ID to scope the query. This will add a `$match` stage for the `ehr_id` in the generated pipeline",
        example="a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6"
    ),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Translates an AQL query into its corresponding MongoDB Aggregation Pipeline.

    This endpoint provides a transparent view of the translation layer:
    - **aql_query**: The original query sent.
    - **ast**: The intermediate Abstract Syntax Tree representation.
    - **mql_pipeline**: The final MongoDB pipeline ready for execution.
    """
    try:
        from src.aql_parser.parser import AQLParser
        from .service import build_aql_pipeline
        
        parser = AQLParser(aql)
        ast_data = parser.parse()

        # aql = Original query
        # ast_data = Parsed query
        # mql_pipeline = MongoDB Aggregation Pipeline
        
        mql_pipeline = await build_aql_pipeline(ast_data, db, ehr_id)
    
        return AQLtoMQLDebugResponse(
            success=True,
            aql_query=aql,
            ast=ast_data,
            mql_pipeline=mql_pipeline
        )
        
    except Exception as e:
        error_response_body = AQLtoMQLDebugErrorResponse(
            message=f"Failed to process AQL: {str(e)}",
            original_query=aql,
            error=str(e)
        ).model_dump()

        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=error_response_body
        )

@router.post(
    "/ast/debug",
    summary="Debug AQL AST Query Pipeline",
    description="Returns the generated MongoDB pipeline for debugging purposes."
)
async def debug_ast_query(
    request: Request,
    ast_data: Dict[str, Any] = Body(..., description="The AQL AST structure."),
    ehr_id: str = None,
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Returns the generated MongoDB aggregation pipeline for debugging purposes.
    """
    try:
        # Import here to avoid circular imports
        from .service import build_aql_pipeline
        
        pipeline = await build_aql_pipeline(ast_data, db, ehr_id)
        
        return {
            "query": ast_data,
            "pipeline": pipeline,
            "pipeline_count": len(pipeline)
        }
        
    except Exception as e:
        return {
            "query": ast_data,
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

