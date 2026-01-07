"""Mongo storage adapter."""
from __future__ import annotations

from typing import Any, Dict, Iterable, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from kehrnel.adapters.base import StorageAdapter


class MongoStorageAdapter(StorageAdapter):
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def insert_one(self, collection: str, doc: Dict[str, Any]) -> Any:
        return await self.db[collection].insert_one(doc)

    async def insert_many(self, collection: str, docs: Iterable[Dict[str, Any]]) -> Any:
        return await self.db[collection].insert_many(list(docs))

    async def find_one(self, collection: str, flt: Dict[str, Any], projection: Dict[str, Any] | None = None) -> Dict[str, Any] | None:
        return await self.db[collection].find_one(flt, projection)

    async def aggregate(self, collection: str, pipeline: List[Dict[str, Any]], allow_disk_use: bool = True) -> List[Dict[str, Any]]:
        cursor = self.db[collection].aggregate(pipeline, allowDiskUse=allow_disk_use)
        return [doc async for doc in cursor]
