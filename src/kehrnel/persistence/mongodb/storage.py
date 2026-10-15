"""Mongo storage adapter."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from motor.motor_asyncio import AsyncIOMotorDatabase

from kehrnel.persistence.mongodb.base import StorageAdapter


class MongoStorageAdapter(StorageAdapter):
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def insert_one(self, collection: str, doc: Dict[str, Any]) -> Any:
        return await self.db[collection].insert_one(doc)

    async def insert_many(self, collection: str, docs: Iterable[Dict[str, Any]]) -> Any:
        return await self.db[collection].insert_many(list(docs))

    async def replace_one(
        self,
        collection: str,
        flt: Dict[str, Any],
        doc: Dict[str, Any],
        upsert: bool = False,
    ) -> Any:
        return await self.db[collection].replace_one(flt, doc, upsert=upsert)

    async def update_one(
        self,
        collection: str,
        flt: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> Any:
        return await self.db[collection].update_one(flt, update, upsert=upsert)

    async def update_many(
        self,
        collection: str,
        flt: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> Any:
        return await self.db[collection].update_many(flt, update, upsert=upsert)

    async def replace_many(
        self, collection: str, docs: Iterable[Dict[str, Any]]
    ) -> Any:
        """Idempotently replace documents by ``_id`` using one bulk write."""
        from pymongo import ReplaceOne

        operations = []
        for doc in docs:
            if "_id" not in doc:
                raise ValueError("replace_many requires every document to contain _id")
            operations.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        if not operations:
            return None
        return await self.db[collection].bulk_write(operations, ordered=False)

    async def delete_many(self, collection: str, flt: Dict[str, Any]) -> Any:
        return await self.db[collection].delete_many(flt)

    async def find_one(
        self,
        collection: str,
        flt: Dict[str, Any],
        projection: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        return await self.db[collection].find_one(flt, projection)

    async def aggregate(
        self,
        collection: str,
        pipeline: List[Dict[str, Any]],
        allow_disk_use: bool = True,
        hint: str | None = None,
    ) -> List[Dict[str, Any]]:
        options: Dict[str, Any] = {"allowDiskUse": allow_disk_use}
        if hint:
            options["hint"] = hint
        cursor = self.db[collection].aggregate(pipeline, **options)
        return [doc async for doc in cursor]
