# src/api/v1/aql/repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

STORED_QUERY_COLL_NAME = "stored_queries"
EHR_COLL_NAME = "ehr"

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


def _parse_simple_ehr_query(aql: str) -> Dict[str, Any]:
    """
    A simple regex-based parser for a limited AQL subset.
    Parses queries like: SELECT e/path/value, e/path2/value from EHR e
    """

    # Regex to capture the SELECT clause from a simple EHR Query
    match = re.match(r"\s*SELECT\s+(.+?)\s+FROM\s+EHR\s+e\s*$", aql, re.IGNORECASE)
    if not match:
        raise ValueError("Invalid or unsupported AQL query structure for simple parser")
    
    select_clause = match.group(1)

    # Split fields by comma, trim whitespace
    fields = [field.strip() for field in select_clause.split(',')]

    parsed_fields = []
    for field in fields:
        path_match = re.match(r"e/([\w_]+)/value", field)
        # AQL paths are like 'e/ehr_id/value'. We need to extract 'ehr_id'
        if not path_match:
            raise ValueError(f"Unsupported field format in SELECT clause: {field}")
        
        # Example: "e/ehr_id/value"
        aql_path = path_match.group(0)
        # Example: "ehr_id"
        mongo_field = path_match.group(1)

        # Map AQL field names to MongoDB document fields
        if mongo_field == "ehr_id":
            mongo_field = "_id"

        parsed_fields.append({
            "aql_path": aql_path,
            "column_name": path_match.group(1),
            "mongo_field": mongo_field
        })

    return {
        "select_fields": parsed_fields
    }


async def execute_aql_query(request_body: Dict[str, Any], db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    """
    Parses a simple AQL query, builds a MongoDB Aggregation Pipeline,
    executes it, and formats the result.
    """
    aql_query = request_body.get("q")
    ehr_id = request_body.get("ehr_id")

    # Parse the AQL query:
    try:
        parsed_aql = _parse_simple_ehr_query(aql_query)
    except ValueError as e:
        logger.error(f"AQL Parsing Error: {e}")
        raise

    # Build the MongoDB Agg Pipeline
    pipeline = []

    # $match stage (Filter by the ehr_id if provided)
    if ehr_id:
        pipeline.append({
            "$match": {
                "_id": ehr_id
            }
        })

    # $project stage, modify the output to show the selected fields
    projection = {
        "_id": 0
    }

    for field in parsed_aql["select_fields"]:
        projection[field["column_name"]] = f"${field['mongo_field']}"

    pipeline.append({
        "$project": projection
    })

    # Add offset and limit for pagination
    if request_body.get("offset"):
        pipeline.append({"$skip": request_body["offset"]})
    if request_body.get("fetch"):
        pipeline.append({"$limit": request_body["fetch"]})

    
    logger.info(f"Generated MongoDB Aggregation Pipeline: {pipeline}")

    # Execute the pipeline
    try:
        cursor = db[EHR_COLL_NAME].aggregate(pipeline)
        results = await cursor.to_list(length=None)
    except PyMongoError as e:
        logger.error(f"Database error during AQL execution: {e}")
        raise

    # Format the results into the required QueryResponse structure
    columns = []
    for field in parsed_aql["select_fields"]:
        columns.append({
            "name": field["column_name"],
            "path": field["aql_path"]
        })

    rows = []
    # The order of columns is determined by the parsed_aql list
    column_names_in_order = [field["column_name"] for field in parsed_aql["select_fields"]]
    for doc in results:
        row = [doc.get(col_name) for col_name in column_names_in_order]
        rows.append(row)

    return {
        "q": aql_query,
        "columns": columns,
        "rows": rows
    }