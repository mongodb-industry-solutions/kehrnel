# src/kehrnel/api/aql/routes.py

from starlette.responses import JSONResponse
from fastapi import APIRouter, Depends, status, Body, Response, Request, Query, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import List, Dict, Any

from kehrnel.api.bridge.app.core.database import get_mongodb_ehr_db
from kehrnel.api.domains.openehr.aql.service import (
    create_or_update_stored_query,
    retrieve_stored_query,
    remove_stored_query,
    list_all_stored_queries,
    process_aql_query,
    process_aql_ast_query
)
from kehrnel.api.domains.openehr.aql.models import StoredQuerySummary, QueryResponse, AQLtoMQLDebugResponse, AQLtoMQLDebugErrorResponse
from kehrnel.api.domains.openehr.aql.api_responses import stored_query_responses, aql_to_mql_debug_responses, aql_query_responses, list_stored_queries_responses, delete_stored_query_responses, aql_validation_responses
from kehrnel.engine.domains.openehr.aql.validator import validate_aql_syntax

router = APIRouter(
    prefix="/query",
    tags=["AQL"]
)

@router.post(
    "/aql",
    summary="Execute AQL Query",
    description="Executes an AQL query provided in the request body and returns the results. Uses dual-query strategy: $match for EHR-specific queries, $search for cross-EHR queries.",
    response_model=QueryResponse,
    responses=aql_query_responses
)
async def execute_query(
    request: Request,
    aql: str = Body(..., media_type="text/plain", description="The AQL query string."),
    ehr_id: str = Query(None, description="Optional EHR ID. If provided, uses $match strategy on flatten_compositions. If not provided, uses $search strategy on search collection."),
    force_search: bool = Query(False, description="Force the use of search strategy for testing purposes."),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Accepts an AQL query as plain text, executes it, and returns the result set.

    Query Strategy:
    - With ehr_id: Uses $match on flatten_compositions (targeted, efficient)
    - Without ehr_id: Uses $search on search collection (cross-EHR, indexed)
    - force_search=true: Forces $search strategy regardless of ehr_id
    """
    from kehrnel.api.bridge.app.core.config import settings

    # Temporarily override force_search_strategy if requested
    original_force_search = settings.search_config.force_search_strategy
    if force_search:
        settings.search_config.force_search_strategy = True

    try:
        response = await process_aql_query(aql_query=aql, request_url=request.url, db=db, ehr_id=ehr_id)
        return response
    finally:
        # Restore original setting
        settings.search_config.force_search_strategy = original_force_search


@router.post(
    "/aql/validate",
    summary="Validate AQL Query",
    description="Validates an AQL query syntax without executing it.",
    responses=aql_validation_responses
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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Validation process failed: {e}",
        )


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
        from kehrnel.engine.domains.openehr.aql.parser import AQLParser

        parser = AQLParser(aql)
        ast = parser.parse()

        return {
            "success": True,
            "message": "AQL parsed successfully",
            "original_query": aql,
            "ast": ast
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse AQL: {e}",
        )


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
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to execute AST query: {e}",
        )


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
    aql: str = Body(
        ...,
        media_type="text/plain",
        description="The AQL query string to translate.",
        examples={
            "sample": {
                "summary": "Simple composition UID query",
                "value": "SELECT c/uid/value as uid FROM EHR e CONTAINS COMPOSITION c",
            }
        },
    ),
    ehr_id: str = Query(
        None,
        description="Optional EHR ID to scope the query. This will add a `$match` stage for the `ehr_id` in the generated pipeline",
        examples={"sample": {"value": "a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6"}},
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
        from kehrnel.engine.domains.openehr.aql.parser import AQLParser
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
    response_model_by_alias=True,
    responses=list_stored_queries_responses
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
    status_code=status.HTTP_204_NO_CONTENT,
    responses=delete_stored_query_responses
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


@router.get(
    "/strategy/info",
    summary="Get Query Strategy Information",
    description="Returns information about the dual-query strategy configuration and decision logic."
)
async def get_strategy_info(
    ehr_id: str = Query(None, description="Test EHR ID to see which strategy would be used"),
    force_search: bool = Query(False, description="Test force search parameter"),
    db: AsyncIOMotorDatabase = Depends(get_mongodb_ehr_db)
):
    """
    Returns detailed information about the dual-query strategy and configuration.
    """
    from kehrnel.api.bridge.app.core.config import settings
    from kehrnel.api.domains.openehr.aql.transformers.aql_transformer import AQLtoMQLTransformer

    # Create a minimal AST for testing strategy decision
    test_ast = {
        "select": {"columns": {}},
        "contains": {"rmType": "COMPOSITION", "alias": "c"}
    }

    transformer = AQLtoMQLTransformer(
        test_ast,
        ehr_id=ehr_id,
        search_index_name=settings.search_config.search_index_name
    )

    would_use_search = transformer.should_use_search_strategy(ehr_id, force_search)

    # Get collection counts for diagnostics
    try:
        flatten_count = await db[settings.search_config.flatten_collection].estimated_document_count()
        search_count = await db[settings.search_config.search_collection].estimated_document_count()
    except Exception as e:
        flatten_count = f"Error: {e}"
        search_count = f"Error: {e}"

    return {
        "strategy_config": {
            "dual_strategy_enabled": settings.search_config.enable_dual_strategy,
            "search_collection": settings.search_config.search_collection,
            "flatten_collection": settings.search_config.flatten_collection,
            "search_index_name": settings.search_config.search_index_name,
            "search_compositions_merge": settings.search_config.search_compositions_merge,
            "force_search_strategy": settings.search_config.force_search_strategy
        },
        "decision_logic": {
            "test_ehr_id": ehr_id,
            "test_force_search": force_search,
            "would_use_search_strategy": would_use_search,
            "strategy_reasoning": (
                "Search strategy (Atlas Search on search collection)" if would_use_search
                else "Match strategy (Standard aggregation on flatten collection)"
            )
        },
        "collection_diagnostics": {
            "flatten_collection_count": flatten_count,
            "search_collection_count": search_count
        },
        "strategy_benefits": {
            "match_strategy": "Highly efficient for single EHR queries, uses indexed $match operations",
            "search_strategy": "Optimized for cross-EHR queries, leverages Atlas Search full-text indexing"
        }
    }
