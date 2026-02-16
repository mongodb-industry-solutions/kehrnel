# src/kehrnel/api/compatibility/v1/ingest/service.py

import json
from typing import Dict, Any

from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.api.strategies.openehr.rps_dual.ingest.repository import IngestionRepository
from kehrnel.engine.strategies.openehr.rps_dual.ingest.exceptions_g import FlattenerError

class IngestionService:
    def __init__(self, flattener: CompositionFlattener, repository: IngestionRepository):
        self.flattener = flattener
        self.repository = repository
    
    async def _process_and_store(self, raw_composition_doc: Dict[str, Any]) -> str:
        """
        Private helper to run the core transformation and storage loc
        This avoids code duplication across different ingestion methods
        """

        # Validate the structure of the input document
        if not all(k in raw_composition_doc for k in ["_id", "ehr_id", "canonicalJSON"]):
            raise ValueError("Source document must contain '_id', 'ehr_id', and 'canonicalJSON'.")
        
        # Use the flattener to transform the composition
        try:
            base_doc, search_doc = self.flattener.transform_composition(raw_composition_doc)
        except FlattenerError as e:
            raise e
        
        # Use the repository to store the flatten documents 
        new_comp_id = await self.repository.insert_flattened_composition_in_transaction(
            base_doc,
            search_doc,
            raw_canonical_doc=raw_composition_doc,
        )

        return new_comp_id
    
    async def ingest_from_payload(self, raw_composition_doc: Dict[str, Any]) -> str:
        """
        Processes a composition provided directly in the request body
        """
        return await self._process_and_store(raw_composition_doc)
    
    
    async def ingest_from_local_file(self, file_path: str) -> str:
        """
        Reads a canonical composition from a local JSON file, the processes it
        """

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_composition_doc = json.load(f)
        except FileNotFoundError:
            # Avoid leaking server filesystem layout through API error details.
            raise FileNotFoundError("The specified file was not found")
        except json.JSONDecodeError:
            raise ValueError("The file is not valid JSON")
        
        return await self._process_and_store(raw_composition_doc)
    

    async def ingest_from_database(self, ehr_id: str) -> str:
        """
        Fetches a canonical composition from the source database collection using
        an ehr_id, then processes it.
        """
        raw_composition_doc = await self.repository.find_canonical_composition_by_ehr_id(ehr_id)
        
        if not raw_composition_doc:
            raise ValueError(f"No canonical composition found for ehr_id: {ehr_id}")

        # The _id from MongoDB is an ObjectId, but the flattener expects a string. Let's ensure it is.
        # This also matches the structure of a doc loaded from JSON.
        raw_composition_doc["_id"] = str(raw_composition_doc["_id"])
            
        return await self._process_and_store(raw_composition_doc)
