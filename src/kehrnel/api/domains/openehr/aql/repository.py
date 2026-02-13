# src/kehrnel/api/legacy/v1/aql/repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from kehrnel.api.legacy.app.core.config import settings

logger = logging.getLogger(__name__)

STORED_QUERY_COLL_NAME = "stored_queries"


def _max_query_results() -> int:
    try:
        return max(1, int(os.getenv("KEHRNEL_MAX_QUERY_RESULTS", "1000")))
    except Exception:
        return 1000


def _max_query_time_ms() -> int:
    try:
        return max(100, int(os.getenv("KEHRNEL_MAX_QUERY_TIME_MS", "15000")))
    except Exception:
        return 15000


def _max_stored_query_list() -> int:
    try:
        return max(1, int(os.getenv("KEHRNEL_MAX_STORED_QUERY_LIST", "500")))
    except Exception:
        return 500


def _ehr_coll() -> str:
    return settings.EHR_COLL_NAME


def _composition_coll() -> str:
    return settings.COMPOSITIONS_COLL_NAME


def _flatten_coll() -> str:
    return settings.FLAT_COMPOSITIONS_COLL_NAME


def _codes_coll() -> str:
    return settings.search_config.codes_collection


async def detect_collection_format(db: AsyncIOMotorDatabase) -> str:
    """
    Detects which collection format to use by checking for data structure.
    Returns either 'shortened' or 'full' based on the format found.
    """
    try:
        # Check flatten collection first since it's the preferred collection
        flatten_coll = _flatten_coll()
        shorten_count = await db[flatten_coll].count_documents({})
        if shorten_count > 0:
            # Sample a document to determine the format within flatten collection
            sample = await db[flatten_coll].find_one({})
            if sample and 'cn' in sample:
                # Check if this is shortened format (short p paths like '7') or full format (long archetype paths)
                first_cn_element = sample['cn'][0] if sample['cn'] else {}
                p_value = first_cn_element.get('p', '')
                
                # If p value is short (like '7', '-4.7', etc.) it's shortened format
                # If p value is long (like 'at0021/at0017/openEHR-EHR-ACTION...') it's full format
                if len(p_value) < 20 and not p_value.startswith('at'):  # Short path = shortened format
                    logger.info(f"Using shortened format from collection: {flatten_coll}")
                    return 'shortened'
                else:  # Long archetype path = full format
                    logger.info(f"Using full format from collection: {flatten_coll}")
                    return 'full'
            elif sample and 'data' in sample and 'cn' not in sample:
                # Direct nested structure without cn array = shortened format
                logger.info(f"Using shortened format from collection: {flatten_coll}")
                return 'shortened'
        
        # Fallback to check compositionsFullPath collection
        full_count = await db[flatten_coll].count_documents({})
        if full_count > 0:
            sample = await db[flatten_coll].find_one({})
            if sample and 'cn' in sample:
                logger.info(f"Using full format from collection: {flatten_coll}")
                return 'full'
        
        # Default to full format if unclear
        logger.warning("Could not detect collection format, defaulting to full format")
        return 'full'
        
    except PyMongoError as e:
        logger.error(f"Error detecting collection format: {e}")
        return 'full'  # Default fallback


async def execute_aql_query(
    pipeline: List[Dict[str, Any]],
    db: AsyncIOMotorDatabase,
    collection_format: str = None,
    use_search_collection: bool = False,
    max_results: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Executes a MongoDB aggregation pipeline against the appropriate compositions collection.
    
    Args:
        pipeline: MongoDB aggregation pipeline
        db: Database connection
        collection_format: Format detection result (for backward compatibility)
        use_search_collection: If True, use search collection; if False, use standard collection
    """
    try:
        if use_search_collection:
            # Use search collection for Atlas Search queries
            from kehrnel.api.legacy.app.core.config import settings
            collection_name = settings.search_config.search_collection
            logger.info(f"Executing search query against collection: {collection_name}")
        else:
            # Use standard collection selection logic
            # Auto-detect format if not specified
            if collection_format is None:
                collection_format = await detect_collection_format(db)
            
            # Determine which collection to use based on what's available
            # Check FLATTEN_COMPOSITION_COLL_NAME first since it's the preferred collection
            flatten_coll = _flatten_coll()
            shorten_count = await db[flatten_coll].count_documents({})
            if shorten_count > 0:
                collection_name = flatten_coll
                logger.info(f"Executing standard query against collection: {collection_name}")
            else:
                # Fallback to compositionsFullPath
                collection_name = flatten_coll
                logger.info(f"Executing standard query against collection: {collection_name}")
        
        safe_max_results = min(max_results or _max_query_results(), _max_query_results())
        cursor = db[collection_name].aggregate(pipeline, maxTimeMS=_max_query_time_ms())
        doc_result = await cursor.to_list(length=safe_max_results)
        return doc_result
    except PyMongoError as e:
        logger.error(f"AQL query execution failed in repository: {e}")
        raise e


async def save_stored_query(name: str, aql_query: str, db: AsyncIOMotorDatabase) -> None:
    query_doc = {"query": aql_query, "created_timestamp": datetime.now(timezone.utc)}
    try:
        await db[STORED_QUERY_COLL_NAME].update_one({"_id": name}, {"$set": query_doc, "$setOnInsert": {"_id": name}}, upsert=True)
    except PyMongoError as e:
        raise e

async def find_stored_query_by_name(name: str, db: AsyncIOMotorDatabase) -> Optional[Dict[str, Any]]:
    try:
        return await db[STORED_QUERY_COLL_NAME].find_one({"_id": name})
    except PyMongoError as e:
        raise e

async def find_all_stored_queries(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    try:
        return await db[STORED_QUERY_COLL_NAME].find({}).to_list(length=_max_stored_query_list())
    except PyMongoError as e:
        raise e

async def delete_stored_query_by_name(name: str, db: AsyncIOMotorDatabase) -> int:
    try:
        result = await db[STORED_QUERY_COLL_NAME].delete_one({"_id": name})
        return result.deleted_count
    except PyMongoError as e:
        raise e
