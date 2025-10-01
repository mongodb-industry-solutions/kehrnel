# src/api/v1/aql/repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

STORED_QUERY_COLL_NAME = "stored_queries"
EHR_COLL_NAME = "ehr"
COMPOSITION_COLL_NAME = "compositions"
FLATTEN_COMPOSITION_COLL_NAME = "sm_compositions3"
SHORTEN_COMPOSITION_COLL_NAME = "sm_compositions3"
CODES_COLL_NAME = "_codes"


async def detect_collection_format(db: AsyncIOMotorDatabase) -> str:
    """
    Detects which collection format to use by checking for data structure.
    Returns either 'shortened' or 'full' based on the format found.
    """
    try:
        # Check sm_compositions3 collection first since it's the preferred collection
        shorten_count = await db[SHORTEN_COMPOSITION_COLL_NAME].count_documents({})
        if shorten_count > 0:
            # Sample a document to determine the format within sm_compositions3
            sample = await db[SHORTEN_COMPOSITION_COLL_NAME].find_one({})
            if sample and 'cn' in sample:
                # Check if this is shortened format (short p paths like '7') or full format (long archetype paths)
                first_cn_element = sample['cn'][0] if sample['cn'] else {}
                p_value = first_cn_element.get('p', '')
                
                # If p value is short (like '7', '-4.7', etc.) it's shortened format
                # If p value is long (like 'at0021/at0017/openEHR-EHR-ACTION...') it's full format
                if len(p_value) < 20 and not p_value.startswith('at'):  # Short path = shortened format
                    logger.info(f"Using shortened format from collection: {SHORTEN_COMPOSITION_COLL_NAME}")
                    return 'shortened'
                else:  # Long archetype path = full format
                    logger.info(f"Using full format from collection: {SHORTEN_COMPOSITION_COLL_NAME}")
                    return 'full'
            elif sample and 'data' in sample and 'cn' not in sample:
                # Direct nested structure without cn array = shortened format
                logger.info(f"Using shortened format from collection: {SHORTEN_COMPOSITION_COLL_NAME}")
                return 'shortened'
        
        # Fallback to check compositionsFullPath collection
        full_count = await db[FLATTEN_COMPOSITION_COLL_NAME].count_documents({})
        if full_count > 0:
            sample = await db[FLATTEN_COMPOSITION_COLL_NAME].find_one({})
            if sample and 'cn' in sample:
                logger.info(f"Using full format from collection: {FLATTEN_COMPOSITION_COLL_NAME}")
                return 'full'
        
        # Default to full format if unclear
        logger.warning("Could not detect collection format, defaulting to full format")
        return 'full'
        
    except PyMongoError as e:
        logger.error(f"Error detecting collection format: {e}")
        return 'full'  # Default fallback


async def execute_aql_query(pipeline: List[Dict[str, Any]], db: AsyncIOMotorDatabase, collection_format: str = None) -> List[Dict[str, Any]]:
    """
    Executes a MongoDB aggregation pipeline against the appropriate compositions collection.
    """
    try:
        # Auto-detect format if not specified
        if collection_format is None:
            collection_format = await detect_collection_format(db)
        
        # Determine which collection to use based on what's available
        # Check sm_compositions3 first since it's the preferred collection
        shorten_count = await db[SHORTEN_COMPOSITION_COLL_NAME].count_documents({})
        if shorten_count > 0:
            collection_name = SHORTEN_COMPOSITION_COLL_NAME
            logger.info(f"Executing query against collection: {collection_name}")
        else:
            # Fallback to compositionsFullPath
            collection_name = FLATTEN_COMPOSITION_COLL_NAME
            logger.info(f"Executing query against collection: {collection_name}")
        
        cursor = db[collection_name].aggregate(pipeline)
        doc_result = await cursor.to_list(length=None)
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
        return await db[STORED_QUERY_COLL_NAME].find({}).to_list(length=None)
    except PyMongoError as e:
        raise e

async def delete_stored_query_by_name(name: str, db: AsyncIOMotorDatabase) -> int:
    try:
        result = await db[STORED_QUERY_COLL_NAME].delete_one({"_id": name})
        return result.deleted_count
    except PyMongoError as e:
        raise e