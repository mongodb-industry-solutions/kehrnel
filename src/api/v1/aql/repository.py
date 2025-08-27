# src/api/v1/aql/repository.py

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from pathlib import Path

# Import Lark for parsing
from lark import Lark, Transformer, v_args, Token

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
    Transforms the Lark AST into a dictionary representing a MongoDB pipeline plan.
    """
    def _translate_path(self, alias, segments):
        """Translates an AQL path into a MongoDB field path."""
        # AQL: e/ehr_id/value -> Mongo: _id
        if alias == 'e' and segments[0].value == 'ehr_id':
            return "_id"

        # AQL: c/uid/value is a special path referring to the composition's version UID.
        # This correctly maps to the document's primary key `_id`.
        if alias == 'c' and segments[0].value == 'uid':
            return "_id"

        # AQL: c/some/other/path -> Mongo: data.some.other.path
        elif alias == 'c':
            field_path = ".".join(s.value for s in segments)
            return f"data.{field_path}"
        
        # AQL: e/some/path -> Mongo: some.path
        else:
            return ".".join(s.value for s in segments)

    def path_segment(self, cname):
        return cname

    def path(self, alias, *segments):
        aql_path_str = f"{alias.value}/{'/'.join(s.value for s in segments)}"
        mongo_field = self._translate_path(alias.value, segments)
        return {"aql_path": aql_path_str, "mongo_field": mongo_field}

    def comparison_op(self, *args):
        if len(args) == 1:
            op = args[0]
            op_value = op.value if hasattr(op, 'value') else str(op)
        elif len(args) == 0:
            op_value = "="
        else:
            op = args[0]
            op_value = op.value if hasattr(op, 'value') else str(op)
        
        return {"=": "$eq", "!=": "$ne", ">": "$gt", "<": "$lt", ">=": "$gte", "<=": "$le"}.get(op_value, "$eq")

    def value(self, val_token: Token):
        if val_token.type == 'ESCAPED_STRING' or val_token.type == 'SINGLE_QUOTED_STRING':
            return val_token.value.strip('"\'')
        elif val_token.type == 'SIGNED_NUMBER':
            num = float(val_token.value)
            return int(num) if num.is_integer() else num
        return val_token.value

    def comparison(self, path, op, value):
        return {path["mongo_field"]: {op: value}}

    def where_clause(self, comparison):
        return {"$match": comparison}

    def contains_clause(self, *args):
        return {"contains_composition": True}
    
    # This is the single, correct method for handling the ORDER BY clause.
    # The old `ordering()` method has been removed.
    def order_by_clause(self, path, direction_token=None):
        """
        Creates the sorting dictionary for the plan.
        'direction_token' will be a Token for 'ASC' or 'DESC', or None if omitted.
        """
        direction = -1 if direction_token and direction_token.value.upper() == 'DESC' else 1
        return {"$sort": {path["mongo_field"]: direction}}

    def aliased_path(self, path, alias=None):
        column_name = alias.value if alias else path["aql_path"].split('/')[-2]
        return {"name": column_name, "path": path["aql_path"], "mongo_field": path["mongo_field"]}

    def select_clause(self, *select_exprs):
        return list(select_exprs)

    def query(self, select_clause, from_clause, contains_clause=None, where_clause=None, order_by_clause=None):
        return {
            "select": select_clause,
            "contains": contains_clause or {},
            "where": where_clause or {},
            "orderby": order_by_clause or {}
        }


def _build_pipeline_from_plan(plan: Dict[str, Any], request_body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Constructs the MongoDB aggregation pipeline from the transformed query plan."""
    pipeline = []

    has_composition = plan.get("contains", {}).get("contains_composition")
    is_ehr_join = has_composition and any(col["path"].startswith("e/") for col in plan["select"])
    
    def get_mongo_path(plan_path: str) -> str:
        """Determines the correct MongoDB field path based on the query context."""
        # Handles case where e/ehr_id/value -> _id is selected after a join from compositions
        if is_ehr_join and plan_path == "_id" and any(c["mongo_field"] == "_id" and c["path"].startswith("e/") for c in plan["select"]):
            return f"ehr_doc.{plan_path}"
        return plan_path

    if has_composition:
        match_stage = {}
        if plan.get("where", {}).get("$match"):
            match_stage.update(plan["where"]["$match"])
        if request_body.get("ehr_id"):
            match_stage["ehr_id"] = request_body["ehr_id"]
        if match_stage:
            pipeline.append({"$match": match_stage})
        
        if is_ehr_join:
            pipeline.append({
                "$lookup": { "from": EHR_COLL_NAME, "localField": "ehr_id", "foreignField": "_id", "as": "ehr_doc" }
            })
            pipeline.append({"$unwind": "$ehr_doc"})
    else: # EHR-only query
        match_stage = {}
        if plan.get("where", {}).get("$match"):
            match_stage.update(plan["where"]["$match"])
        if request_body.get("ehr_id"):
            match_stage["_id"] = request_body["ehr_id"]
        if match_stage:
            pipeline.append({"$match": match_stage})

    # Add the $sort stage if 'orderby' exists in the plan
    if plan.get("orderby"):
        sort_plan = plan["orderby"]["$sort"]
        original_sort_field = list(sort_plan.keys())[0]
        sort_direction = sort_plan[original_sort_field]
        
        final_sort_field = get_mongo_path(original_sort_field)
        pipeline.append({"$sort": {final_sort_field: sort_direction}})

    # Build the final projection
    projection = {"_id": 0}
    for col in plan["select"]:
        plan_field = col["mongo_field"]
        final_field_path = get_mongo_path(plan_field)
        projection[col["name"]] = f"${final_field_path}"
    
    pipeline.append({"$project": projection})

    # Add pagination
    if request_body.get("offset"):
        pipeline.append({"$skip": request_body["offset"]})
    if request_body.get("fetch"):
        pipeline.append({"$limit": request_body["fetch"]})

    return pipeline


async def execute_aql_query(request_body: Dict[str, Any], db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    aql_query = request_body.get("q")
    if not aql_parser:
        raise RuntimeError("AQL parser is not initialized. Check for grammar file.")

    try:
        ast = aql_parser.parse(aql_query)
    except Exception as e:
        logger.error(f"AQL Parsing Error: {e}")
        raise ValueError(f"Invalid AQL syntax: {e}")

    query_plan = AqlMongoTransformer().transform(ast)
    logger.info(f"Generated Query Plan: {query_plan}")
    pipeline = _build_pipeline_from_plan(query_plan, request_body)
    logger.info(f"Generated MongoDB Aggregation Pipeline: {pipeline}")

    try:
        collection_name = COMPOSITION_COLL_NAME if query_plan.get("contains") else EHR_COLL_NAME
        collection = db[collection_name]
        
        cursor = collection.aggregate(pipeline)
        results = await cursor.to_list(length=None)
    except PyMongoError as e:
        logger.error(f"Database error during AQL execution: {e}")
        raise

    columns = [{"name": field["name"], "path": field["path"]} for field in query_plan["select"]]
    rows = []
    column_names_in_order = [col["name"] for col in columns]
    for doc in results:
        row = [doc.get(col_name) for col_name in column_names_in_order]
        rows.append(row)

    return {"q": aql_query, "columns": columns, "rows": rows}


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