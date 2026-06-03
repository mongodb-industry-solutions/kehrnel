# src/kehrnel/api/compatibility/v1/aql/service.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError
from typing import List, Dict, Any, Optional, Tuple
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
from kehrnel.api.bridge.app.core.database import resolve_active_openehr_context

from kehrnel.api.domains.openehr.aql.models import StoredQuery, StoredQuerySummary, QueryResponse, MetaData
from kehrnel.engine.domains.openehr.aql.parser import AQLParser
from kehrnel.engine.strategies.openehr.rps_dual.config import build_schema_config, normalize_config
from kehrnel.engine.strategies.openehr.rps_dual.query.compiler import build_runtime_strategy
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers import AQLtoMQLTransformer


def _safe_error_message(message: str) -> str:
    debug_enabled = os.getenv("KEHRNEL_DEBUG", "false").lower() in ("1", "true", "yes")
    return message if debug_enabled else "Query execution failed"


def _dictionary_doc_id(raw_cfg: Dict[str, Any], key: str, fallback: str) -> str:
    collections = raw_cfg.get("collections") if isinstance(raw_cfg, dict) else {}
    collections = collections if isinstance(collections, dict) else {}
    dictionaries = raw_cfg.get("dictionaries") if isinstance(raw_cfg, dict) else {}
    dictionaries = dictionaries if isinstance(dictionaries, dict) else {}
    coding = raw_cfg.get("coding") if isinstance(raw_cfg, dict) else {}
    coding = coding if isinstance(coding, dict) else {}

    coll_cfg = collections.get(key) if isinstance(collections.get(key), dict) else {}
    for field_name in ("doc_id", "docId"):
        value = coll_cfg.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()

    if key == "codes":
        for value in (
            coll_cfg.get("arcodes_doc_id"),
            ((dictionaries.get("arcodes") or {}).get("doc_id") if isinstance(dictionaries.get("arcodes"), dict) else None),
            ((coding.get("archetype_ids") or {}).get("dictionary") if isinstance(coding.get("archetype_ids"), dict) else None),
        ):
            if isinstance(value, str) and value.strip():
                return value.strip()
    elif key == "shortcuts":
        for value in (
            coll_cfg.get("shortcuts_doc_id"),
            ((dictionaries.get("shortcuts") or {}).get("doc_id") if isinstance(dictionaries.get("shortcuts"), dict) else None),
        ):
            if isinstance(value, str) and value.strip():
                return value.strip()

    return fallback


async def _load_shortcut_map(
    db: AsyncIOMotorDatabase,
    *,
    collection: str,
    doc_id: str,
) -> Dict[str, str]:
    doc = await db[collection].find_one({"_id": doc_id}) or {}
    merged: Dict[str, str] = {}
    for key in ("items", "keys", "values"):
        value = doc.get(key)
        if isinstance(value, dict):
            merged.update({str(k): str(v) for k, v in value.items()})
    return merged


async def _detect_collection_format_for(
    db: AsyncIOMotorDatabase,
    collection_name: str,
) -> str:
    try:
        sample_collection = db[collection_name]
        count = await sample_collection.count_documents({})
        if count <= 0:
            return "full"
        sample = await sample_collection.find_one({})
        if sample and "cn" in sample:
            first_cn_element = sample["cn"][0] if sample["cn"] else {}
            p_value = first_cn_element.get("p", "")
            return "shortened" if len(p_value) < 20 and not str(p_value).startswith("at") else "full"
        if sample and "data" in sample and "cn" not in sample:
            return "shortened"
    except Exception as exc:
        logger.info("Falling back to legacy collection format detection: %s", exc)
    return await detect_collection_format(db)


async def _resolve_transformer_inputs(
    db: AsyncIOMotorDatabase,
    request: Optional[Request] = None,
) -> Tuple[str, Dict[str, Any], Dict[str, Any], Dict[str, str], Any]:
    raw_cfg: Dict[str, Any] = {}
    strategy_cfg = normalize_config({})
    try:
        if request is not None:
            context = await resolve_active_openehr_context(request, ensure_ingestion=False)
            raw_cfg = getattr(context.get("activation"), "config", {}) or {}
    except Exception as exc:
        logger.info("Falling back to default AQL transformer configuration: %s", exc)

    if raw_cfg:
        strategy_cfg = normalize_config(raw_cfg)
        schema_cfgs = build_schema_config(strategy_cfg)
        schema_config = dict(schema_cfgs["composition"])
        search_schema_config = dict(schema_cfgs["search"])
    else:
        schema_config = {
            "composition_array": "cn",
            "path_field": "p",
            "data_field": "data",
            "archetype_path": "ap",
            "ehr_id": "ehr_id",
            "comp_id": "comp_id",
            "template_id": "tid",
            "time_committed": "time_c",
            "collection": settings.search_config.flatten_collection,
            "codes_collection": settings.search_config.codes_collection,
            "shortcuts_collection": settings.search_config.shortcuts_collection,
            "codes_doc_id": "ar_code",
            "shortcuts_doc_id": "shortcuts",
            "separator": ".",
        }
        search_schema_config = {
            "composition_array": "sn",
            "path_field": "p",
            "data_field": "data",
            "archetype_path": "ap",
            "ehr_id": "ehr_id",
            "comp_id": "comp_id",
            "template_id": "tid",
            "time_committed": "sort_time",
            "sort_time": "sort_time",
            "collection": settings.search_config.search_collection,
            "lookup_from": settings.search_config.flatten_collection,
            "lookup_as": "full_composition",
            "codes_collection": settings.search_config.codes_collection,
            "shortcuts_collection": settings.search_config.shortcuts_collection,
            "codes_doc_id": "ar_code",
            "shortcuts_doc_id": "shortcuts",
            "separator": ".",
        }

    collection_format = await _detect_collection_format_for(
        db,
        schema_config.get("collection") or settings.search_config.flatten_collection,
    )
    schema_config["format"] = collection_format
    search_schema_config["format"] = collection_format
    schema_config.setdefault("archetype_path", "ap")
    search_schema_config.setdefault("archetype_path", "ap")
    schema_config.setdefault("codes_collection", settings.search_config.codes_collection)
    schema_config.setdefault("shortcuts_collection", settings.search_config.shortcuts_collection)
    search_schema_config.setdefault("codes_collection", settings.search_config.codes_collection)
    search_schema_config.setdefault("shortcuts_collection", settings.search_config.shortcuts_collection)

    codes_doc_id = _dictionary_doc_id(raw_cfg, "codes", schema_config.get("codes_doc_id", "ar_code"))
    shortcuts_doc_id = _dictionary_doc_id(raw_cfg, "shortcuts", schema_config.get("shortcuts_doc_id", "shortcuts"))
    schema_config["codes_doc_id"] = codes_doc_id
    schema_config["shortcuts_doc_id"] = shortcuts_doc_id
    search_schema_config["codes_doc_id"] = codes_doc_id
    search_schema_config["shortcuts_doc_id"] = shortcuts_doc_id

    shortcut_map = await _load_shortcut_map(
        db,
        collection=schema_config["shortcuts_collection"],
        doc_id=shortcuts_doc_id,
    )
    runtime_strategy = build_runtime_strategy(strategy_cfg)
    return collection_format, schema_config, search_schema_config, shortcut_map, runtime_strategy


async def build_aql_pipeline(
    ast_query: Dict[str, Any],
    db: AsyncIOMotorDatabase,
    ehr_id: str = None,
    request: Optional[Request] = None,
) -> List[Dict[str, Any]]:
    """
    Builds the MongoDB aggregation pipeline from an AST query.
    Used for debugging purposes.
    """
    _, schema_config, search_schema_config, shortcut_map, runtime_strategy = await _resolve_transformer_inputs(
        db,
        request=request,
    )
    
    transformer = AQLtoMQLTransformer(
        ast_query, 
        ehr_id=ehr_id, 
        schema_config=schema_config, 
        search_schema_config=search_schema_config,
        db=db,
        search_index_name=settings.search_config.search_index_name,
        strategy=runtime_strategy,
        shortcut_map=shortcut_map,
    )
    
    # Determine which pipeline to build based on strategy
    if settings.search_config.enable_dual_strategy and transformer.should_use_search_strategy(ehr_id, settings.search_config.force_search_strategy):
        pipeline = await transformer.build_search_pipeline()
    else:
        pipeline = await transformer.build_pipeline()
    
    return pipeline


async def process_aql_ast_query(
    ast_query: Dict[str, Any],
    request_url: str,
    db: AsyncIOMotorDatabase,
    ehr_id: str = None,
    request: Optional[Request] = None,
) -> QueryResponse:
    """
    Handles the lifecycle of executing an AST query.
    1. Transforms AST to MQL.
    2. Executes MQL against the database.
    3. Formats the results into the standard response model.
    """

    try:
        collection_format, schema_config, search_schema_config, shortcut_map, runtime_strategy = await _resolve_transformer_inputs(
            db,
            request=request,
        )
        
        transformer = AQLtoMQLTransformer(
            ast_query, 
            ehr_id=ehr_id, 
            schema_config=schema_config, 
            search_schema_config=search_schema_config,
            db=db,
            search_index_name=settings.search_config.search_index_name,
            strategy=runtime_strategy,
            shortcut_map=shortcut_map,
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


async def process_aql_query(
    aql_query: str,
    request_url: str,
    db: AsyncIOMotorDatabase,
    ehr_id: str = None,
    request: Optional[Request] = None,
) -> QueryResponse:
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

        # Step 2: Resolve request-scoped schema and dictionaries
        collection_format, schema_config, search_schema_config, shortcut_map, runtime_strategy = await _resolve_transformer_inputs(
            db,
            request=request,
        )
        
        # Step 3: Transform AST into MQL Aggregation Pipeline
        transformer = AQLtoMQLTransformer(
            ast, 
            ehr_id=ehr_id, 
            schema_config=schema_config, 
            search_schema_config=search_schema_config,
            db=db,
            search_index_name=settings.search_config.search_index_name,
            strategy=runtime_strategy,
            shortcut_map=shortcut_map,
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
