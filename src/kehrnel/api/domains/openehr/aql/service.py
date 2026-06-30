# src/kehrnel/api/compatibility/v1/aql/service.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError
from typing import List, Dict, Any, Optional, Tuple
import logging
import math
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
from kehrnel.engine.domains.openehr.aql.aql_to_ast import ParseError
from kehrnel.engine.strategies.openehr.rps_dual.config import build_schema_config, normalize_config
from kehrnel.engine.strategies.openehr.rps_dual.query.compiler import build_runtime_strategy
from kehrnel.engine.strategies.openehr.rps_dual.strategy import _bind_query_params
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers import AQLtoMQLTransformer

PARITY_AQL_FEATURE_MODE = "parity"
EXTENDED_AQL_FEATURE_MODE = "extended"
SUPPORTED_AQL_FEATURE_MODES = (PARITY_AQL_FEATURE_MODE, EXTENDED_AQL_FEATURE_MODE)
EXTENDED_ONLY_AQL_OPERATORS = frozenset({"EXISTS", "NOT EXISTS"})
EXTENDED_ONLY_CONTAINS_FEATURES = frozenset({"NOT CONTAINS"})
EXTENDED_ONLY_AQL_FEATURES = frozenset({"PATH-TO-PATH COMPARISON"})


def _safe_error_message(message: str) -> str:
    debug_enabled = os.getenv("KEHRNEL_DEBUG", "false").lower() in ("1", "true", "yes")
    return message if debug_enabled else "Query execution failed"


def _is_atlas_search_unavailable(exc: PyMongoError) -> bool:
    message = str(exc).lower()
    return "$search stage is only allowed on mongodb atlas" in message


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
    feature_mode: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Builds the MongoDB aggregation pipeline from an AST query.
    Used for debugging purposes.
    """
    enforce_aql_feature_mode(ast_query, feature_mode)

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


def _extract_columns_from_ast(ast_query: Dict[str, Any]) -> List[Dict[str, Any]]:
    columns = ast_query.get("select", {}).get("columns", {})
    extracted: List[Dict[str, Any]] = []

    for col_data in columns.values():
        if not isinstance(col_data, dict):
            continue

        value_spec = col_data.get("value", {}) if isinstance(col_data.get("value"), dict) else {}
        alias = col_data.get("alias")
        value_type = value_spec.get("type")
        path: Optional[str] = None

        if value_type == "dataMatchPath":
            path = value_spec.get("path")
        elif value_type == "variable":
            variable_name = value_spec.get("name")
            path = variable_name if isinstance(variable_name, str) and variable_name.strip() else None
        elif value_type == "aggregateFunctionCall":
            path = None

        if not alias:
            if value_type == "variable":
                alias = str(value_spec.get("name", "")).lstrip("$")
            elif path:
                alias = path.split("/")[-1] or path.replace("/", "_")
            elif value_type == "aggregateFunctionCall":
                function = value_spec.get("function") if isinstance(value_spec.get("function"), dict) else {}
                function_name = str(function.get("name", "result")).strip().lower() or "result"
                alias = f"{function_name}Result"

        if not alias:
            continue

        extracted.append({"name": alias, "path": path})

    return extracted


def _convert_datetime_objects(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _convert_datetime_objects(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_datetime_objects(item) for item in obj]
    if isinstance(obj, float):
        if math.isfinite(obj):
            return float(format(obj, ".15g"))
        return obj
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return obj


def normalize_aql_feature_mode(feature_mode: Optional[str]) -> str:
    normalized = (feature_mode or PARITY_AQL_FEATURE_MODE).strip().lower() or PARITY_AQL_FEATURE_MODE
    if normalized not in SUPPORTED_AQL_FEATURE_MODES:
        supported = ", ".join(SUPPORTED_AQL_FEATURE_MODES)
        raise ValueError(
            f"Unsupported AQL feature mode '{feature_mode}'. Supported modes: {supported}."
        )
    return normalized


def enforce_aql_feature_mode(ast_query: Dict[str, Any], feature_mode: Optional[str]) -> str:
    normalized_mode = normalize_aql_feature_mode(feature_mode)
    if normalized_mode == EXTENDED_AQL_FEATURE_MODE:
        return normalized_mode

    operators = _collect_extended_only_operators(ast_query)
    if operators:
        raise NotImplementedError(
            "AQL feature(s) require X-AQL-Feature-Mode: extended: "
            + ", ".join(sorted(operators))
        )

    return normalized_mode


def _collect_extended_only_operators(node: Any) -> set[str]:
    found: set[str] = set()
    _collect_extended_only_operators_recursive(node, found)
    return found


def _collect_extended_only_operators_recursive(node: Any, found: set[str]) -> None:
    if isinstance(node, dict):
        operator = node.get("operator")
        if isinstance(operator, str):
            normalized_operator = operator.strip().upper()
            if normalized_operator in EXTENDED_ONLY_AQL_OPERATORS:
                found.add(normalized_operator)
            if _is_path_to_path_comparison(node):
                found.update(EXTENDED_ONLY_AQL_FEATURES)
        if node.get("containsNegated"):
            found.update(EXTENDED_ONLY_CONTAINS_FEATURES)
        for value in node.values():
            _collect_extended_only_operators_recursive(value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_extended_only_operators_recursive(item, found)


def _is_path_to_path_comparison(node: Dict[str, Any]) -> bool:
    operator = str(node.get("operator") or "").strip().upper()
    if operator not in {"=", "!=", "<>", ">", ">=", "<", "<="}:
        return False
    path = node.get("path")
    value = node.get("value")
    return (
        isinstance(path, str)
        and "/" in path
        and isinstance(value, dict)
        and value.get("type") == "dataMatchPath"
        and isinstance(value.get("path"), str)
        and "/" in value["path"]
    )


def _apply_query_runtime_options(
    ast_query: Dict[str, Any],
    *,
    parameters: Optional[Dict[str, Any]] = None,
    fetch: Optional[int] = None,
    offset: Optional[int] = None,
) -> Dict[str, Any]:
    if parameters is not None:
        missing_params: set[str] = set()
        effective_ast = _bind_query_params(ast_query, parameters, missing_params)
        if missing_params:
            raise ValueError(
                "Missing query parameter values for: "
                + ", ".join(sorted(missing_params))
            )
    else:
        effective_ast = dict(ast_query)

    if offset is not None and fetch is None and effective_ast.get("limit") is None:
        raise ValueError(
            "offset without fetch/LIMIT is not supported yet for the shared runtime contract."
        )

    if fetch is not None:
        effective_ast["limit"] = fetch
    if offset is not None:
        effective_ast["offset"] = offset

    return effective_ast


async def execute_aql_query_payload(
    *,
    ast_query: Dict[str, Any],
    request_url: str,
    db: AsyncIOMotorDatabase,
    ehr_id: str = None,
    request: Optional[Request] = None,
    executed_aql: Optional[str] = None,
    include_executed_aql: bool = True,
    include_explain: bool = False,
    query_parameters: Optional[Dict[str, Any]] = None,
    fetch: Optional[int] = None,
    offset: Optional[int] = None,
    timeout_ms: Optional[int] = None,
    feature_mode: Optional[str] = None,
) -> Dict[str, Any]:
    collection_format, schema_config, search_schema_config, shortcut_map, runtime_strategy = await _resolve_transformer_inputs(
        db,
        request=request,
    )

    effective_ast = _apply_query_runtime_options(
        ast_query,
        parameters=query_parameters,
        fetch=fetch,
        offset=offset,
    )
    normalized_feature_mode = enforce_aql_feature_mode(effective_ast, feature_mode)

    transformer = AQLtoMQLTransformer(
        effective_ast,
        ehr_id=ehr_id,
        schema_config=schema_config,
        search_schema_config=search_schema_config,
        db=db,
        search_index_name=settings.search_config.search_index_name,
        strategy=runtime_strategy,
        shortcut_map=shortcut_map,
    )

    use_search_strategy = (
        settings.search_config.enable_dual_strategy
        and transformer.should_use_search_strategy(ehr_id, settings.search_config.force_search_strategy)
    )

    logger.info(
        "AQL strategy decision: %s (ehr_id=%s, dual_strategy=%s, force_search=%s)",
        "SEARCH" if use_search_strategy else "MATCH",
        "provided" if ehr_id else "none",
        settings.search_config.enable_dual_strategy,
        settings.search_config.force_search_strategy,
    )

    if use_search_strategy:
        pipeline = await transformer.build_search_pipeline()
        target_collection = settings.search_config.search_collection
    else:
        pipeline = await transformer.build_pipeline()
        target_collection = settings.search_config.flatten_collection

    logger.info(
        "Built %s pipeline with %s stages, targeting collection: %s",
        "search" if use_search_strategy else "standard",
        len(pipeline),
        target_collection,
    )

    try:
        results = await execute_aql_query(
            pipeline=pipeline,
            db=db,
            collection_format=collection_format,
            use_search_collection=use_search_strategy,
        )
    except PyMongoError as exc:
        if use_search_strategy and _is_atlas_search_unavailable(exc):
            logger.warning(
                "Atlas Search is unavailable for this MongoDB target; retrying AQL with the standard match pipeline."
            )
            pipeline = await transformer.build_pipeline()
            target_collection = settings.search_config.flatten_collection
            use_search_strategy = False
            results = await execute_aql_query(
                pipeline=pipeline,
                db=db,
                collection_format=collection_format,
                use_search_collection=False,
            )
        else:
            raise

    rows = results if isinstance(results, list) else ([results] if results else [])
    serializable_rows = [_convert_datetime_objects(result) for result in rows]
    columns = _extract_columns_from_ast(effective_ast)

    meta = {
        "href": str(request_url),
        "type": "RESULTSET",
        "schemaVersion": "1.0.4",
        "generator": "PythonEHRBase/1.0.0",
        "rowCount": len(serializable_rows),
        "strategy": "search" if use_search_strategy else "match",
    }
    if include_executed_aql and executed_aql:
        meta["executedAql"] = executed_aql
    if timeout_ms is not None:
        meta["timeoutMsRequested"] = timeout_ms

    debug = None
    if include_explain:
        debug = {
            "ast": effective_ast,
            "pipeline": pipeline,
            "strategy": meta["strategy"],
            "collection": target_collection,
            "parameters": query_parameters or {},
            "ehrId": ehr_id,
            "fetch": fetch,
            "offset": offset,
            "featureMode": normalized_feature_mode,
        }

    return {
        "columns": columns,
        "rows": serializable_rows,
        "meta": meta,
        "debug": debug,
    }


async def process_aql_ast_query(
    ast_query: Dict[str, Any],
    request_url: str,
    db: AsyncIOMotorDatabase,
    ehr_id: str = None,
    request: Optional[Request] = None,
    feature_mode: Optional[str] = None,
) -> QueryResponse:
    """
    Handles the lifecycle of executing an AST query.
    1. Transforms AST to MQL.
    2. Executes MQL against the database.
    3. Formats the results into the standard response model.
    """

    try:
        payload = await execute_aql_query_payload(
            ast_query=ast_query,
            request_url=request_url,
            db=db,
            ehr_id=ehr_id,
            request=request,
            include_executed_aql=False,
            feature_mode=feature_mode,
        )
        return JSONResponse(
            content={
                "query": ast_query,
                "columns": payload["columns"],
                "rows": payload["rows"],
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
    query_parameters: Optional[Dict[str, Any]] = None,
    fetch: Optional[int] = None,
    offset: Optional[int] = None,
    include_explain: bool = False,
    timeout_ms: Optional[int] = None,
    feature_mode: Optional[str] = None,
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
        ast = parser.parse_with_method("handwritten")
        payload = await execute_aql_query_payload(
            ast_query=ast,
            request_url=request_url,
            db=db,
            ehr_id=ehr_id,
            request=request,
            executed_aql=aql_query,
            include_executed_aql=True,
            include_explain=include_explain,
            query_parameters=query_parameters,
            fetch=fetch,
            offset=offset,
            timeout_ms=timeout_ms,
            feature_mode=feature_mode,
        )

        meta = MetaData(
            href=str(request_url),
            executed_aql=aql_query
        )
        return QueryResponse(meta=meta, q=aql_query, columns=payload["columns"], rows=payload["rows"])
    
    except PyMongoError:
        logger.error("AQL query execution failed", exc_info=False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_safe_error_message("Database error during query execution")
        )
    except ParseError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"AQL parsing failed: {e}",
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
