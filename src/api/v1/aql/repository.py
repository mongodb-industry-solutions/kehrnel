# src/api/v1/aql/repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from pathlib import Path

# Import Lark for parsing
from lark import Lark, Transformer, v_args

logger = logging.getLogger(__name__)

STORED_QUERY_COLL_NAME = "stored_queries"
EHR_COLL_NAME = "ehr"
COMPOSITION_COLL_NAME = "compositions"

# Load the AQL grammar from the .lark file
try:
    grammar_path = Path(__file__).parent / "aql_grammar.lark"
    with open(grammar_path, "r") as f:
        aql_grammar = f.read()
    aql_parser = Lark(aql_grammar, start='query', parser='lalr')
except FileNotFoundError:
    logger.error("AQL grammar file 'aql_grammar.lark' not found.")
    aql_parser = None

@v_args(inline=True)
class AqlMongoTransformer(Transformer):
    """
    Transforms the Lark AST into a dictionary representing a MongoDB pipeline plan
    """
    def _translate_path(self, alias, segments):
        """Translates an AQL path into a MongoDB field path"""
        # AQL: e/ehr_id/value -> Mongo: _id

        if alias == 'e' and segments[0] == 'ehr_id':
            return "_id"
        # AQL: c/uid/value -> Mongo: composition_doc.uid.value
        elif alias == 'c':
            mongo_path = "composition_doc"
            # The last segment 'value' in AQL is often implicit in the document structure
            # We will map 'c/path/to/value' to 'composition_doc.path.to'
            field_path = ".".join(s.value for s in segments if s.value != 'value')
            return f"{mongo_path}.{field_path}"
        # AQL: e/some/path -> Mongo: some.path
        else:
            return ".".join(s.value for s in segments)
    
    def path(self, alias, *segments):
        aql_path_str = f"{alias.value}/{'/'.join(s.value for s in segments)}"
        mongo_field = self._translate_path(alias.value, segments)
        return {"aql_path": aql_path_str, "mongo_field": mongo_field}
    
    def comparison_op (self, op):
        return {
            "=": "$eq", "!=": "$ne", ">": "$gt",
            "<": "$lt", ">=": "$gte", "<=": "$le"
        }.get(op.value, "$eq")
    
    def value(self, val):
        return val.strip('"\'')
    
    def comparison(self, path, op, value):
        return {path["mongo_field"]: {op: value}}
    
    def where_clause(self, comparison)
        return {"$match": comparison}
    
    def contains_clause(self, _):
        return {"contains_composition": True}
    
    def aliased_path(self, path, alias=None):
        column_name = alias.value if alias else path["aql_path"].split('/')[-2]
        return {
            "name": column_name,
            "path": path["aql_path"],
            "mongo_field": path["mongo_field"]
        }
    
    def select_clause(self, *select_exprs):
        return list(select_exprs)
    
    def query(self, select_clause, from_clause, contains_clause = None, where_clause=None):
        return{
            "select": select_clause,
            "contains": contains_clause or {},
            "where": where_clause or {}
        }
    
def _build_pipeline_from_plan(plan: Dict[str, Any], request_body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Constructs the MongoDB aggregation pipeline form the transformed query plan
    """

    pipeline= []

    # Initial $match stage (from WHERE clause and ehr_id parameter)
    match_stage = {}
    if plan.get("where", {}).get("$match"):
        match_stage.update(plan["where"]["$match"])
    if request_body.get("ehr_id"):
        match_stage["_id"] = request_body["ehr_id"]
    if match_stage:
        pipeline.append({"$match": match_stage})

    # $lookup and $unwind for CONTAINS COMPOSITION
    if plan.get("contains", {}).get("contains_composition"):
        pipeline.append({
            "$lookup": {
                "from": COMPOSITION_COLL_NAME,
                "localField": "_id",
                "foreignField": "ehr_id",
                "as": "composition_doc"
            }
        })
        pipeline.append({"$unwind": "$composition_doc"})

    # $project stage (from SELECT clause)
    projection = {"_id": 0}
    for col in plan["select"]:
        projection[col["name"]] = f"${col['mongo_field']}"
    pipeline.append({"$project": projection})

    # Pagination stages ($skip, $limit)
    if request_body.get("offset"):
        pipeline.append({"$skip": request_body["offset"]})
    if request_body.get("fetch"):
        pipeline.append({"$limit": request_body["fetch"]})

    return pipeline


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


# def _parse_simple_ehr_query(aql: str) -> Dict[str, Any]:
#     """
#     A simple regex-based parser for a limited AQL subset.
#     Parses queries like: SELECT e/path/value, e/path2/value from EHR e
#     """

#     # Regex to capture the SELECT clause from a simple EHR Query
#     match = re.match(r"\s*SELECT\s+(.+?)\s+FROM\s+EHR\s+e\s*$", aql, re.IGNORECASE)
#     if not match:
#         raise ValueError("Invalid or unsupported AQL query structure for simple parser")
    
#     select_clause = match.group(1)

#     # Split fields by comma, trim whitespace
#     fields = [field.strip() for field in select_clause.split(',')]

#     parsed_fields = []
#     for field in fields:
#         path_match = re.match(r"e/([\w_]+)/value", field)
#         # AQL paths are like 'e/ehr_id/value'. We need to extract 'ehr_id'
#         if not path_match:
#             raise ValueError(f"Unsupported field format in SELECT clause: {field}")
        
#         # Example: "e/ehr_id/value"
#         aql_path = path_match.group(0)
#         # Example: "ehr_id"
#         mongo_field = path_match.group(1)

#         # Map AQL field names to MongoDB document fields
#         if mongo_field == "ehr_id":
#             mongo_field = "_id"

#         parsed_fields.append({
#             "aql_path": aql_path,
#             "column_name": path_match.group(1),
#             "mongo_field": mongo_field
#         })

#     return {
#         "select_fields": parsed_fields
#     }


async def execute_aql_query(request_body: Dict[str, Any], db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    """
    Parses an AQL query using Lark, builds a MongoDB Aggregation Pipeline,
    executes it, and formats the result.
    """
    aql_query = request_body.get("q")
    if not aql_parser:
        raise RuntimeError("AQL parser is not initialized. Check for grammar file.")

    # Parse the AQL query into AST
    try:
        ast = aql_parser.parse(aql_query)
    except Exception as e:
        logger.error(f"AQL Parsing Error: {e}")
        raise ValueError(f"Invalid AQL syntax: {e}")

     # Transform the AST into a query plan
    query_plan = AqlMongoTransformer().transform(ast)
    logger.info(f"Generated Query Plan: {query_plan}")

    # Build the MongoDB Aggregation Pipeline
    pipeline = _build_pipeline_from_plan(query_plan, request_body)
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
    for field in query_plan["select"]:
        columns.append({
            "name": field["column_name"],
            "path": field["path"]
        })

    rows = []
    # The order of columns is determined by the parsed_aql list
    column_names_in_order = [field["name"] for field in query_plan["select"]]
    for doc in results:
        row = [doc.get(col_name) for col_name in column_names_in_order]
        rows.append(row)

    return {
        "q": aql_query,
        "columns": columns,
        "rows": rows
    }