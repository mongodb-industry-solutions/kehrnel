"""Atlas Search adapter (best-effort)."""
from __future__ import annotations

from typing import Any, Dict, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from kehrnel.persistence.mongodb.base import TextSearchAdapter


class MongoAtlasSearchAdapter(TextSearchAdapter):
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def ensure_search_index(self, collection: str, index_name: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        warnings = []
        try:
            existing = await self.list_search_indexes(collection)
            existing_names = {
                str(doc.get("name"))
                for doc in existing
                if isinstance(doc, dict) and doc.get("name") is not None
            }
            if index_name in existing_names:
                await self.db.command(
                    {
                        "updateSearchIndex": collection,
                        "name": index_name,
                        "definition": definition,
                    }
                )
                return {"created": [], "updated": [index_name], "warnings": warnings}

            await self.db.command(
                {
                    "createSearchIndexes": collection,
                    "indexes": [
                        {
                            "name": index_name,
                            "definition": definition,
                        }
                    ],
                }
            )
            return {"created": [index_name], "updated": [], "warnings": warnings}
        except Exception as exc:
            warnings.append(f"Search index not ensured: {exc}")
            return {"created": [], "updated": [], "warnings": warnings}

    async def list_search_indexes(self, collection: str) -> List[Dict[str, Any]]:
        try:
            cursor = self.db[collection].list_search_indexes()
            return [doc async for doc in cursor]
        except Exception:
            return []
