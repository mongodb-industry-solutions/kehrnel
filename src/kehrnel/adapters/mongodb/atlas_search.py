"""Atlas Search adapter (best-effort)."""
from __future__ import annotations

from typing import Any, Dict, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from ..base import TextSearchAdapter


class MongoAtlasSearchAdapter(TextSearchAdapter):
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def ensure_search_index(self, collection: str, index_name: str, definition: Dict[str, Any]) -> Dict[str, Any]:
        warnings = []
        try:
            cmd = {
                "createSearchIndexes": collection,
                "indexes": [
                    {
                        "name": index_name,
                        "definition": definition,
                    }
                ],
            }
            await self.db.command(cmd)
            return {"created": [index_name], "warnings": warnings}
        except Exception as exc:
            warnings.append(f"Search index not ensured: {exc}")
            return {"created": [], "warnings": warnings}

    async def list_search_indexes(self, collection: str) -> List[Dict[str, Any]]:
        try:
            cursor = self.db[collection].list_search_indexes()
            return [doc async for doc in cursor]
        except Exception:
            return []
