"""Index administration adapter."""
from __future__ import annotations

from typing import Any, Dict, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from kehrnel.adapters.base import IndexAdminAdapter


class MongoIndexAdminAdapter(IndexAdminAdapter):
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def ensure_collection(self, name: str) -> None:
        # listCollections lazily creates on insert; here we just touch it
        await self.db[name].insert_one({"_touch": True})
        await self.db[name].delete_one({"_touch": True})

    async def ensure_indexes(self, collection: str, index_specs: List[Dict[str, Any]]) -> Dict[str, Any]:
        created = []
        warnings = []
        for spec in index_specs or []:
            keys = spec.get("keys")
            opts = spec.get("options", {})
            if not keys:
                warnings.append("Missing keys in index spec")
                continue
            try:
                name = await self.db[collection].create_index(keys, **opts)
                created.append(name)
            except Exception as exc:
                warnings.append(str(exc))
        return {"created": created, "warnings": warnings}
