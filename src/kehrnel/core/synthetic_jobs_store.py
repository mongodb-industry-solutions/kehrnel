"""Persistence backend for synthetic job history."""
from __future__ import annotations

from typing import Any, Dict, List

import certifi
from pymongo import MongoClient


class MongoSyntheticJobStore:
    """Simple MongoDB-backed store for synthetic job records."""

    def __init__(self, *, uri: str, database: str, collection: str = "synthetic_data_jobs"):
        self._uri = uri
        self._database = database
        self._collection = collection
        self._client: MongoClient | None = None

    @property
    def _coll(self):
        if self._client is None:
            self._client = MongoClient(self._uri, tlsCAFile=certifi.where())
        return self._client[self._database][self._collection]

    def upsert(self, rec: Dict[str, Any]) -> None:
        job_id = rec.get("job_id")
        if not job_id:
            return
        self._coll.replace_one({"job_id": job_id}, rec, upsert=True)

    def patch(self, job_id: str, patch: Dict[str, Any]) -> None:
        if not job_id:
            return
        self._coll.update_one({"job_id": job_id}, {"$set": patch}, upsert=False)

    def get(self, job_id: str) -> Dict[str, Any] | None:
        if not job_id:
            return None
        rec = self._coll.find_one({"job_id": job_id}, {"_id": 0})
        return rec if isinstance(rec, dict) else None

    def list(self, *, env_id: str | None = None, domain: str | None = None) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {}
        if env_id:
            q["env_id"] = env_id
        if domain:
            q["domain"] = str(domain).lower()
        cursor = self._coll.find(q, {"_id": 0}).sort("created_at", -1)
        return list(cursor)

