from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

STORED_QUERY_COLL_NAME = "stored_queries"

async def save_stored_query(name: str, aql_query: str, db: AsyncIOMotorDatabase) -> None:
    """
    Saves or updates a stored query in the database using its name as the unique ID.
    This is an 'upsert' operation. The field is named 'query'.
    """
    query_doc = {
        "query": aql_query,
        "created_timestamp": datetime.now(timezone.utc)
    }
    try:
        await db[STORED_QUERY_COLL_NAME].update_one(
            {"_id": name},
            {"$set": query_doc, "$setOnInsert": {"_id": name}},
            upsert=True
        )
        logger.info(f"Stored query '{name}' saved successfully.")
    except PyMongoError as e:
        logger.error(f"Error saving stored query '{name}': {e}")
        raise e

async def find_stored_query_by_name(name: str, db: AsyncIOMotorDatabase) -> Optional[Dict[str, Any]]:
    """
    Retrieves a single stored query document by its name (_id).
    """
    try:
        return await db[STORED_QUERY_COLL_NAME].find_one({"_id": name})
    except PyMongoError as e:
        logger.error(f"Error finding stored query '{name}': {e}")
        raise e

async def find_all_stored_queries(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    """Retrieves all stored queries from the database."""
    try:
        cursor = db[STORED_QUERY_COLL_NAME].find({})
        return await cursor.to_list(length=None)
    except PyMongoError as e:
        logger.error(f"Error retrieving all stored queries: {e}")
        raise e

async def delete_stored_query_by_name(name: str, db: AsyncIOMotorDatabase) -> int:
    """
    Deletes a stored query by its name.
    Returns the number of documents deleted (0 or 1).
    """
    try:
        result = await db[STORED_QUERY_COLL_NAME].delete_one({"_id": name})
        logger.info(f"Deletion attempted for stored query '{name}'. Count: {result.deleted_count}")
        return result.deleted_count
    except PyMongoError as e:
        logger.error(f"Error deleting stored query '{name}': {e}")
        raise e

async def execute_aql_query(request_body: Dict[str, Any], db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    """
    **PLACEHOLDER**
    This function will eventually contain the logic to parse the AQL, build a
    MongoDB aggregation pipeline, and execute it.

    For now, it returns a mocked, hardcoded result matching the QueryResponse model.
    """
    query = request_body.get("q")
    logger.info(f"Executing AQL query (MOCKED): {query}")
    if request_body.get("ehr_id"):
        logger.info(f"Query scoped to ehr_id: {request_body.get('ehr_id')}")

    # MOCKED RESPONSE that matches the new QueryResponse structure
    return {
        "meta": {
            "href": "/v1/query/aql", # This will be set dynamically in service layer
            "executed_aql": query,
        },
        "q": query,
        "columns": [
            {"name": "ehr_id", "path": "/ehr_id/value"},
            {"name": "Systolic", "path": "/data[at0001]/items[at0004]/value/magnitude"}
        ],
        "rows": [
            ["a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6", 120],
            ["f1g2h3i4-j5k6-l7m8-n9o0-p1q2r3s4t5u6", 125]
        ]
    }