"""MongoDB persistence for generated FHIR resources."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError

from ..config import settings
from ..schema.registry import registry

logger = logging.getLogger(__name__)

_CLIENT_CACHE: dict[str, MongoClient] = {}


def _shared_mongo_client(uri: str) -> MongoClient:
    """Reuse one client per URI (required for mongomock; safe for pymongo)."""
    if uri not in _CLIENT_CACHE:
        _CLIENT_CACHE[uri] = MongoClient(uri)
    return _CLIENT_CACHE[uri]


def _utc_now() -> datetime:
    return datetime.now(UTC)


class FHIRMongoStore:
    """
    MongoDB persistence for FHIR resources.
    One collection per resource type. Name is ``{prefix}{ResourceType}`` when
    ``mongodb_collection_prefix`` is set, otherwise the resource type alone (e.g. ``Patient``).

    Does not create MongoDB indexes on save — use fhir-mql (``fhir_ensure_indexes`` /
    ``indexes`` CLI) after denormalization for search indexes.
    """

    def __init__(
        self,
        uri: str | None = None,
        db_name: str | None = None,
        client: MongoClient | None = None,
        collection_prefix: str | None = None,
    ) -> None:
        resolved_uri = uri or settings.mongodb_uri
        if client is None:
            self._client = _shared_mongo_client(resolved_uri)
            self._owns_client = False
        else:
            self._client = client
            self._owns_client = True
        self._db = self._client[db_name or settings.mongodb_db]
        if collection_prefix is None:
            collection_prefix = settings.mongodb_collection_prefix
        self._prefix = collection_prefix or ""

    def collection_name(self, resource_type: str) -> str:
        """MongoDB collection name for a FHIR resource type."""
        return f"{self._prefix}{resource_type}"

    def _collection(self, resource_type: str) -> Collection:
        return self._db[self.collection_name(resource_type)]

    def _resource_type_from_collection(self, collection_name: str) -> str | None:
        if self._prefix:
            if not collection_name.startswith(self._prefix):
                return None
            return collection_name[len(self._prefix) :]
        if collection_name in registry.all_resources():
            return collection_name
        return None

    def _managed_collection_names(self) -> list[str]:
        names = self._db.list_collection_names()
        return [
            name
            for name in names
            if self._resource_type_from_collection(name) is not None
        ]

    def save(self, resource: dict[str, Any]) -> str:
        """Upsert a single resource. Returns the resource id."""
        rtype = resource.get("resourceType")
        rid = resource.get("id")
        if not rtype or not rid:
            raise ValueError("Resource must have resourceType and id")

        doc = {**resource, "_fhir_resource_type": rtype, "_stored_at": _utc_now()}
        self._collection(rtype).replace_one({"id": rid}, doc, upsert=True)
        return rid

    def save_many(
        self,
        resources: list[dict[str, Any]],
        batch_size: int = 500,
    ) -> dict[str, int]:
        """
        Bulk upsert resources grouped by type.
        Returns {resource_type: count} dict.
        """
        by_type: dict[str, list[dict[str, Any]]] = {}
        for resource in resources:
            rtype = resource.get("resourceType")
            if rtype:
                by_type.setdefault(rtype, []).append(resource)

        counts: dict[str, int] = {}
        stored_at = _utc_now()
        for rtype, docs in by_type.items():
            col = self._collection(rtype)
            for i in range(0, len(docs), batch_size):
                batch = docs[i : i + batch_size]
                ops = [
                    UpdateOne(
                        {"id": doc["id"]},
                        {
                            "$set": {
                                **doc,
                                "_fhir_resource_type": rtype,
                                "_stored_at": stored_at,
                            },
                        },
                        upsert=True,
                    )
                    for doc in batch
                ]
                try:
                    result = col.bulk_write(ops, ordered=False)
                    counts[rtype] = counts.get(rtype, 0) + (
                        result.upserted_count + result.modified_count
                    )
                except BulkWriteError as exc:
                    logger.error("Bulk write error for %s: %s", rtype, exc.details)
                except TypeError:
                    # mongomock may not support newer pymongo bulk_write kwargs
                    for doc in batch:
                        self.save(doc)
                    counts[rtype] = counts.get(rtype, 0) + len(batch)
        return counts

    def find(
        self,
        resource_type: str,
        query: dict[str, Any] | None = None,
        limit: int = 100,
        skip: int = 0,
    ) -> list[dict[str, Any]]:
        """Search resources by MongoDB query dict."""
        col = self._collection(resource_type)
        projection = {"_id": 0, "_fhir_resource_type": 0, "_stored_at": 0}
        cursor = col.find(query or {}, projection)
        return list(cursor.skip(skip).limit(limit))

    def find_by_reference(self, reference: str) -> dict[str, Any] | None:
        """Find a resource by FHIR reference string e.g. 'Patient/abc123'."""
        parts = reference.split("/")
        if len(parts) != 2:
            return None
        rtype, rid = parts
        return self.get(rtype, rid)

    def get(self, resource_type: str, resource_id: str) -> dict[str, Any] | None:
        """Get a single resource by type and id."""
        projection = {"_id": 0, "_fhir_resource_type": 0, "_stored_at": 0}
        return self._collection(resource_type).find_one({"id": resource_id}, projection)

    def search_patient(
        self,
        family: str | None = None,
        given: str | None = None,
        birthdate: str | None = None,
        identifier: str | None = None,
        gender: str | None = None,
    ) -> list[dict[str, Any]]:
        """FHIR-style patient search."""
        query: dict[str, Any] = {}
        if family:
            query["name.family"] = {"$regex": family, "$options": "i"}
        if given:
            query["name.given"] = {"$regex": given, "$options": "i"}
        if birthdate:
            query["birthDate"] = birthdate
        if identifier:
            query["identifier.value"] = identifier
        if gender:
            query["gender"] = gender
        return self.find("Patient", query)

    def search_observations_for_patient(
        self,
        patient_id: str,
        code: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """Observations for a patient, optionally filtered by LOINC code."""
        query: dict[str, Any] = {"subject.reference": f"Patient/{patient_id}"}
        if code:
            query["code.coding.code"] = code
        if status:
            query["status"] = status
        return self.find("Observation", query)

    def search_conditions_for_patient(
        self,
        patient_id: str,
        clinical_status: str | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"subject.reference": f"Patient/{patient_id}"}
        if clinical_status:
            query["clinicalStatus.coding.code"] = clinical_status
        return self.find("Condition", query)

    def search_encounters_for_patient(
        self,
        patient_id: str,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {"subject.reference": f"Patient/{patient_id}"}
        if status:
            query["status"] = status
        return self.find("Encounter", query)

    def count(self, resource_type: str, query: dict[str, Any] | None = None) -> int:
        return self._collection(resource_type).count_documents(query or {})

    def delete_all(self, resource_type: str | None = None) -> None:
        """Delete all documents, or only one resource type collection."""
        if resource_type:
            self._collection(resource_type).drop()
        else:
            for name in self._managed_collection_names():
                self._db[name].drop()

    def list_resource_types(self) -> list[str]:
        types = [
            self._resource_type_from_collection(name)
            for name in self._managed_collection_names()
        ]
        return sorted(t for t in types if t)

    def stats(self) -> dict[str, int]:
        """Return document count per resource type."""
        return {rt: self.count(rt) for rt in self.list_resource_types()}

    def close(self) -> None:
        if getattr(self, "_owns_client", True):
            self._client.close()
