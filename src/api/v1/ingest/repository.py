# src/api/v1/ingest/repository.py

from typing import Any, Dict, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import PyMongoError
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)

class IngestionRepository:
    """
    Handles all database operations related to the ingestion and flattening process.
    This class based approach is used to cleanly manage both the 'db' and 'config' dependencies required for these operations
    """

    def __init__(self, db: AsyncIOMotorDatabase, config: Dict[str, Any]):
        self.db = db
        self.client = db.client
        self.config = config

        # Target collections for flattened data
        self.flat_compositions_coll = db[config["target"]["compositions_collection"]]
        self.search_compositions_coll = db[config["target"]["search_collection"]]

        # Source collection for canonical data 
        source_collection_name = config["source"]["canonical_compositions_collection"]
        self.source_compositions_coll = db[source_collection_name]

    async def find_canonical_composition_by_ehr_id(self, ehr_id: str) -> Optional[Dict[str, Any]]:
        """
        Finds the most recent canonical composition for a given ehr_id from the source collection
        """

        try:
            return await self.source_compositions_coll.find_one({"ehr_id": ehr_id})
        except PyMongoError as e:
            logger.error(f"Error finding canonical composition for ehr_id '{ehr_id}': {e} ")
            raise

    async def insert_flattened_composition_in_transaction(
        self,
        base_doc: Dict[str, Any],
        search_doc: Dict[str, Any]
    ) -> str:
        """
        Atomically inserts the flattened base and search documents into their respective collections within a single transaction.
        Returns the string of the new document ID
        """
        new_id = ObjectId()
        base_doc["_id"] = new_id

        has_search_data = search_doc and search_doc.get("sn")
        if has_search_data:
            search_doc["_id"] = new_id
        
        async with await self.client.start_session() as session:
            async with session.start_transaction():
                try:
                    # 1. Insert the main flattened composition document
                    await self.flat_compositions_coll.insert_one(base_doc, session=session)

                    # 2. Insert the search document if it contains data
                    if has_search_data:
                        await self.search_compositions_coll.insert_one(search_doc, session=session)
                except PyMongoError as e:
                    logger.error(f"Flattened composition insertion transaction failed: {e}")
                    # Re-raise for the service layer to handle
                    raise
        return str(new_id)
        