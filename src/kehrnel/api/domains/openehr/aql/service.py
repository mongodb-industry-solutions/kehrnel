# src/kehrnel/api/aql/service.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError
from typing import List, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)

from kehrnel.api.domains.openehr.aql.repository import (
    save_stored_query,
    find_stored_query_by_name,
    delete_stored_query_by_name,
    find_all_stored_queries,
    execute_aql_query,
    detect_collection_format
)
from kehrnel.api.bridge.app.core.config import settings

from kehrnel.api.domains.openehr.aql.models import StoredQuery, StoredQuerySummary, QueryResponse, MetaData
from kehrnel.api.domains.openehr.aql.transformers import AQLtoMQLTransformer
from kehrnel.engine.domains.openehr.aql.parser import AQLParser
from kehrnel.persistence import get_default_layout


def _safe_error_message(message: str) -> str:
    debug_enabled = os.getenv("KEHRNEL_DEBUG", "false").lower() in ("1", "true", "yes")
    return message if debug_enabled else "Query execution failed"


async def build_aql_pipeline(ast_query: Dict[str, Any], db: AsyncIOMotorDatabase, ehr_id: str = None) -> List[Dict[str, Any]]:
    """
    Builds the MongoDB aggregation pipeline from an AST query.
    Used for debugging purposes.
    """
    # Detect collection format
    collection_format = await detect_collection_format(db)
    
    # Configure schema based on detected format
    schema_config = {
        'composition_array': 'cn',  # Both formats use cn array
        'path_field': 'p',  # Both formats use p field
        'data_field': 'data',
        'format': collection_format
    }
    
    transformer = AQLtoMQLTransformer(
        ast_query, 
        ehr_id=ehr_id, 
        schema_config=schema_config, 
        db=db,
        search_index_name=settings.search_config.search_index_name,
        strategy=get_default_layout(),
    )
    
    # Determine which pipeline to build based on strategy
    if settings.search_config.enable_dual_strategy and transformer.should_use_search_strategy(ehr_id, settings.search_config.force_search_strategy):
        pipeline = await transformer.build_search_pipeline()
    else:
        pipeline = await transformer.build_pipeline()
    
    return pipeline


async def process_aql_ast_query(ast_query: Dict[str, Any], request_url: str, db: AsyncIOMotorDatabase, ehr_id: str = None) -> QueryResponse:
    """
    Handles the lifecycle of executing an AST query.
    1. Transforms AST to MQL.
    2. Executes MQL against the database.
    3. Formats the results into the standard response model.
    """

    try:
        # Detect collection format
        collection_format = await detect_collection_format(db)
        
        # Configure schema based on detected format
        schema_config = {
            'composition_array': 'cn',  # Both formats use cn array
            'path_field': 'p',  # Both formats use p field
            'data_field': 'data',
            'format': collection_format
        }
        
        transformer = AQLtoMQLTransformer(
            ast_query, 
            ehr_id=ehr_id, 
            schema_config=schema_config, 
            db=db,
            search_index_name=settings.search_config.search_index_name,
            strategy=get_default_layout(),
        )
        
        # Determine which strategy to use
        use_search_strategy = (settings.search_config.enable_dual_strategy and 
                             transformer.should_use_search_strategy(ehr_id, settings.search_config.force_search_strategy))
        
        logger.info(f"Query strategy decision: {'SEARCH' if use_search_strategy else 'MATCH'} "
                   f"(ehr_id={'provided' if ehr_id else 'none'}, dual_strategy={settings.search_config.enable_dual_strategy}, "
                   f"force_search={settings.search_config.force_search_strategy})")
        
        if use_search_strategy:
            pipeline = await transformer.build_search_pipeline()
            logger.info(f"Built search pipeline with {len(pipeline)} stages, targeting collection: {settings.search_config.search_collection}")
        else:
            pipeline = await transformer.build_pipeline()
            logger.info(f"Built standard pipeline with {len(pipeline)} stages, targeting collection: {settings.search_config.flatten_collection}")

        # Execute the query via the repository
        results = await execute_aql_query(
            pipeline=pipeline, 
            db=db, 
            collection_format=collection_format,
            use_search_collection=use_search_strategy
        )


        # Extract column names and paths from the project stage for the response model
        columns = []
        project_stage = next((stage for stage in pipeline if '$project' in stage), None)

        if project_stage:
            # This is a simplified way to get column names; a more robust method
            # would map back to the original AST select columns.
            columns = [{"name": key, "path": f"/{key}"} for key in project_stage['$project'] if key != '_id']

        # Ensure results is properly formatted and serializable
        if not isinstance(results, list):
            results = [results] if results else []

        # Convert datetime objects to ISO strings for JSON serialization
        def convert_datetime_objects(obj):
            if isinstance(obj, dict):
                return {k: convert_datetime_objects(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime_objects(item) for item in obj]
            elif hasattr(obj, 'isoformat'):  # datetime objects
                return obj.isoformat()
            else:
                return obj
            
        serializable_results = [convert_datetime_objects(result) for result in results]
        return JSONResponse(
            content={
                "query": ast_query,
                "columns": columns,
                "rows": serializable_results
            },
            status_code=200
        )
    except PyMongoError:
        logger.error("AST query execution failed", exc_info=False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_safe_error_message("Database error during query execution")
        )
    except (ValueError, NotImplementedError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error processing AST query: {e}"
        )


async def process_aql_query(aql_query: str, request_url: str, db: AsyncIOMotorDatabase, ehr_id: str = None) -> QueryResponse:
    """
    Handles the full lifecycle of executing an AQL query.
    1. Parses AQL to AST.
    2. Transforms AST to MQL.
    3. Executes MQL against the database.
    4. Formats the results into the standard response model.
    """

    try:
        # Step 1: Parse AQL into AST
        parser = AQLParser(aql_query)
        ast = parser.parse()

        # Step 2: Detect collection format
        collection_format = await detect_collection_format(db)
        
        # Configure schema based on detected format
        schema_config = {
            'composition_array': 'cn',  # Both formats use cn array
            'path_field': 'p',  # Both formats use p field
            'data_field': 'data',
            'format': collection_format
        }
        
        # Step 3: Transform AST into MQL Aggregation Pipeline
        transformer = AQLtoMQLTransformer(
            ast, 
            ehr_id=ehr_id, 
            schema_config=schema_config, 
            db=db,
            search_index_name=settings.search_config.search_index_name,
            strategy=get_default_layout(),
        )
        
        # Determine which strategy to use
        use_search_strategy = (settings.search_config.enable_dual_strategy and 
                             transformer.should_use_search_strategy(ehr_id, settings.search_config.force_search_strategy))
        
        logger.info(f"AST Query strategy decision: {'SEARCH' if use_search_strategy else 'MATCH'} "
                   f"(ehr_id={'provided' if ehr_id else 'none'}, dual_strategy={settings.search_config.enable_dual_strategy}, "
                   f"force_search={settings.search_config.force_search_strategy})")
        
        if use_search_strategy:
            pipeline = await transformer.build_search_pipeline()
            logger.info(f"Built search pipeline with {len(pipeline)} stages, targeting collection: {settings.search_config.search_collection}")
        else:
            pipeline = await transformer.build_pipeline()
            logger.info(f"Built standard pipeline with {len(pipeline)} stages, targeting collection: {settings.search_config.flatten_collection}")

        # Execute the query via the repository
        results = await execute_aql_query(
            pipeline=pipeline, 
            db=db, 
            collection_format=collection_format,
            use_search_collection=use_search_strategy
        )

        # Step 4: Format the response
        meta = MetaData(
            href=str(request_url),
            executed_aql=aql_query
        )

        # Extract column names and paths from the project stage for the response model
        columns = []
        project_stage = next((stage for stage in pipeline if '$project' in stage), None)
        if project_stage:
            # This is a simplified way to get column names; a more robust method
            # would map back to the original AST select columns.
            columns = [{"name": key, "path": f"/{key}"} for key in project_stage['$project'] if key != '_id']

        # Ensure results is properly formatted and serializable
        if not isinstance(results, list):
            results = [results] if results else []
        
        # Convert datetime objects to ISO strings for JSON serialization
        def convert_datetime_objects(obj):
            if isinstance(obj, dict):
                return {k: convert_datetime_objects(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime_objects(item) for item in obj]
            elif hasattr(obj, 'isoformat'):  # datetime objects
                return obj.isoformat()
            else:
                return obj
        
        serializable_results = [convert_datetime_objects(result) for result in results]
        
        return QueryResponse(meta=meta, q=aql_query, columns=columns, rows=serializable_results)
    
    except PyMongoError:
        logger.error("AQL query execution failed", exc_info=False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_safe_error_message("Database error during query execution")
        )
    except (ValueError, NotImplementedError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error processing AQL query: {e}"
        )



async def create_or_update_stored_query(name: str, aql_query: str, db: AsyncIOMotorDatabase) -> None:
    """
    Handles the logic for creating or updating a stored query.
    """
    try:
        await save_stored_query(name=name, aql_query=aql_query, db=db)
    except PyMongoError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_safe_error_message(f"Database error while saving stored query '{name}'")
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
    except PyMongoError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_safe_error_message("Database error while listing stored queries")
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
    except PyMongoError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_safe_error_message(f"Database error while deleting stored query '{name}'")
        )
