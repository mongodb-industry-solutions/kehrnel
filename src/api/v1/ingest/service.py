# src/api/v1/ingest/service.py

from pymongo.errors import BulkWriteError
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from src.transform.flattener_g import CompositionFlattener


async def _safe_insert(collection, docs):
    """Helper to insert documents and gracefully handle duplicate key errors."""
    if not docs:
        return 0
    try:
        result = await collection.insert_many(docs, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as bwe:
        # Filter for duplicate key errors (code 11000)
        dup = sum(1 for e in bwe.details.get("writeErrors", []) if e["code"] == 11000)
        # Check for any other errors
        other_errors = [e for e in bwe.details.get("writeErrors", []) if e["code"] != 11000]
        if other_errors:
            # Re-raise if there are errors other than duplicates
            raise
        return len(docs) - dup


class IngestionService:
    @staticmethod
    async def process_and_store_composition(
        raw_composition_doc: dict,
        flattener: CompositionFlattener,
        db: AsyncIOMotorDatabase,
        config: dict,
    ) -> str:
        """
        Uses the flattener to transform a composition and stores it in the database.
        Returns the new composition's ID.
        """
        # 1. Use the flattener to transform the composition (this part is synchronous)
        base_doc, search_doc = flattener.transform_composition(raw_composition_doc)

        # 2. Assign a new BSON ObjectId for the flattened documents
        new_id = ObjectId()
        base_doc["_id"] = new_id

        has_search_data = search_doc and search_doc.get("sn")
        if has_search_data:
            search_doc["_id"] = new_id

        # 3. Get collection names from config
        compositions_collection_name = config["target"]["compositions_collection"]
        search_collection_name = config["target"]["search_collection"]

        comp_collection = db[compositions_collection_name]
        search_collection = db[search_collection_name]

        # 4. Insert into target collections asynchronously
        await _safe_insert(comp_collection, [base_doc])
        if has_search_data:
            await _safe_insert(search_collection, [search_doc])

        return str(new_id)