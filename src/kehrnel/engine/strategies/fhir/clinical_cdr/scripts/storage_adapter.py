"""FHIR storage-adapter seam.

The adapter owns canonical↔stored mapping so the REST and migration paths never
write MongoDB directly. Canonical FHIR fields remain at the document root while
all Kehrnel control metadata is grouped under ``_kehrnel``. Search and compartment
projections remain root siblings because the bundled MQL/index contract targets
those paths directly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.document_contract import (
    OPERATIONAL_METADATA_FIELD,
    ProjectionVersions,
    STORED_DOCUMENT_SCHEMA_VERSION,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import (
    canonical_resource,
    canonical_resources,
)


@runtime_checkable
class FHIRStorageAdapter(Protocol):
    """persist / read / serialize seam for FHIR resources."""

    def serialize(self, stored_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Stored document → canonical FHIR resource (operational fields removed)."""
        ...

    def serialize_many(self, stored_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ...

    def read(self, resource_type: str, resource_id: str) -> Optional[Dict[str, Any]]:
        """Return the canonical current version of a resource, or None."""
        ...

    def persist(self, resource_type: str, resource: Dict[str, Any]) -> Dict[str, Any]:
        """Idempotently upsert one canonical resource by logical id."""
        ...

    def persist_many(
        self,
        resources: List[Dict[str, Any]],
        *,
        mode: str = "upsert",
    ) -> Dict[str, Any]:
        """Persist a heterogeneous batch and return bounded write statistics."""
        ...


class MongoFHIRStorageAdapter:
    """Concrete adapter over the per-resource-type Mongo collections.

    ``db`` is a pymongo-style database handle; ``collection_prefix`` matches the
    strategy config. Every write must already carry its mandatory search and
    compartment projections plus versioned Kehrnel metadata.
    """

    def __init__(
        self,
        db: Any,
        collection_prefix: str = "",
        *,
        projection_versions: ProjectionVersions | None = None,
    ):
        self._db = db
        self._prefix = collection_prefix or ""
        self._projection_versions = projection_versions

    def _collection_name(self, resource_type: str) -> str:
        return f"{self._prefix}{resource_type}"

    @staticmethod
    def _ensure_logical_id_index(collection: Any) -> str:
        """Accept an existing equivalent unique index regardless of its name."""
        for info in collection.list_indexes():
            key = list(dict(info.get("key") or {}).items())
            if key == [("id", 1)]:
                if not bool(info.get("unique", False)):
                    raise RuntimeError(
                        "FHIR collection already has a non-unique id index; "
                        "deduplicate logical ids before enabling writes"
                    )
                return str(info.get("name") or "id_1")
        return str(collection.create_index([("id", 1)], unique=True, name="id_unique"))

    # ── serialize ──────────────────────────────────────────────────────────
    def serialize(self, stored_doc: Dict[str, Any]) -> Dict[str, Any]:
        return canonical_resource(stored_doc)

    def serialize_many(self, stored_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return canonical_resources(stored_docs)

    # ── read ───────────────────────────────────────────────────────────────
    def read(self, resource_type: str, resource_id: str) -> Optional[Dict[str, Any]]:
        coll = self._db[self._collection_name(resource_type)]
        doc = coll.find_one({"id": resource_id})
        return self.serialize(doc) if doc else None

    # ── persist ────────────────────────────────────────────────────────────
    def persist(self, resource_type: str, resource: Dict[str, Any]) -> Dict[str, Any]:
        if resource.get("resourceType") != resource_type:
            raise ValueError("resource_type must match resource.resourceType")
        result = self.persist_many([resource], mode="upsert")
        return {**result, "resource": self.read(resource_type, str(resource.get("id") or ""))}

    def persist_many(
        self,
        resources: List[Dict[str, Any]],
        *,
        mode: str = "upsert",
    ) -> Dict[str, Any]:
        """Bulk-create or upsert resources grouped by resource type.

        Search projections already present on ``resources`` are stored atomically
        with the canonical fields.  Client-provided operational fields have been
        removed by the importer before this boundary.
        """
        if mode not in {"upsert", "create"}:
            raise ValueError("mode must be 'upsert' or 'create'")
        if self._projection_versions is None:
            raise ValueError("Current projection versions are required for FHIR writes")

        try:
            from pymongo import InsertOne, ReplaceOne
            from pymongo.errors import BulkWriteError, DuplicateKeyError
        except ImportError as exc:  # pragma: no cover - installation failure
            raise RuntimeError("pymongo is required for FHIR persistence") from exc

        grouped: dict[str, list[Dict[str, Any]]] = {}
        now = datetime.now(UTC)
        for resource in resources:
            resource_type = str(resource.get("resourceType") or "")
            resource_id = str(resource.get("id") or "")
            if not resource_type or not resource_id:
                raise ValueError("Resource must have resourceType and id")
            if not isinstance(resource.get("_search"), dict):
                raise ValueError("Resource must have a materialized _search object")
            if not isinstance(resource.get("_compartments"), dict):
                raise ValueError("Resource must have a materialized _compartments object")
            metadata = resource.get(OPERATIONAL_METADATA_FIELD)
            if not isinstance(metadata, dict):
                raise ValueError("Resource must have versioned _kehrnel metadata")
            if metadata.get("storage_schema_version") != STORED_DOCUMENT_SCHEMA_VERSION:
                raise ValueError("Resource storage schema version is missing or unsupported")
            for required in (
                "projection_contract_version",
                "resource_projection_version",
                "fhir_release",
                "projected_at",
            ):
                if not metadata.get(required):
                    raise ValueError(f"Resource _kehrnel.{required} is required")
            if metadata.get("fhir_release") != self._projection_versions.fhir_release:
                raise ValueError("Resource FHIR release does not match the active projection contract")
            if (
                metadata.get("projection_contract_version")
                != self._projection_versions.projection_contract_version
            ):
                raise ValueError("Resource projection contract version is stale")
            if (
                metadata.get("resource_projection_version")
                != self._projection_versions.for_resource(resource_type)
            ):
                raise ValueError(f"Resource projection version is stale for {resource_type}")
            stored = dict(resource)
            stored[OPERATIONAL_METADATA_FIELD] = {**metadata, "stored_at": now}
            grouped.setdefault(resource_type, []).append(stored)

        by_type: dict[str, dict[str, int]] = {}
        for resource_type, docs in grouped.items():
            collection = self._db[self._collection_name(resource_type)]
            # The logical id is the FHIR identity inside a per-type collection.
            # Creating this before writes makes concurrent upserts deterministic.
            self._ensure_logical_id_index(collection)
            operations = []
            for doc in docs:
                if mode == "create":
                    operations.append(InsertOne(doc))
                else:
                    operations.append(ReplaceOne({"id": doc["id"]}, doc, upsert=True))
            try:
                result = collection.bulk_write(operations, ordered=False)
            except (BulkWriteError, DuplicateKeyError):
                # Preserve the Mongo error for the importer to map to a conflict;
                # partial-write details remain available on BulkWriteError.details.
                raise
            inserted = int(getattr(result, "inserted_count", 0) or 0)
            upserted = int(getattr(result, "upserted_count", 0) or 0)
            matched = int(getattr(result, "matched_count", 0) or 0)
            modified = int(getattr(result, "modified_count", 0) or 0)
            by_type[resource_type] = {
                "processed": len(docs),
                "inserted": inserted + upserted,
                "matched": matched,
                "updated": modified,
                "unchanged": max(0, matched - modified),
            }

        return {
            "processed": sum(item["processed"] for item in by_type.values()),
            "inserted": sum(item["inserted"] for item in by_type.values()),
            "matched": sum(item["matched"] for item in by_type.values()),
            "updated": sum(item["updated"] for item in by_type.values()),
            "unchanged": sum(item["unchanged"] for item in by_type.values()),
            "by_resource_type": by_type,
        }
