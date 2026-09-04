"""FHIR collection diagnostics: counts, denormalization coverage, index presence."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.document_contract import (
    STORED_DOCUMENT_SCHEMA_VERSION,
    build_projection_versions,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.query import _search_paths_from_config


def _collection_has_search_index(collection: Any) -> bool:
    try:
        for info in collection.list_indexes():
            key = info.get("key") or {}
            if any(str(field).startswith(("_search", "_compartments")) for field in key):
                return True
    except Exception as exc:
        if "NamespaceNotFound" not in str(exc) and getattr(exc, "code", None) != 26:
            raise
    return False


def _resolve_resource_types(payload: dict[str, Any], search_types: set[str], gen_types: set[str]) -> list[str]:
    raw = payload.get("resource_types")
    if isinstance(raw, list) and raw:
        return sorted({str(rt).strip() for rt in raw if str(rt).strip()})
    return sorted(search_types | gen_types)


def _existing_collection_names(db: Any) -> set[str] | None:
    if not hasattr(db, "list_collection_names"):
        return None
    return set(db.list_collection_names())


def _database_document_count(db: Any) -> int | None:
    """Return the database-wide object count when the driver supports dbStats."""
    if not hasattr(db, "command"):
        return None
    try:
        stats = db.command("dbStats", scale=1)
        return int(stats.get("objects", 0))
    except Exception:
        return None


async def fhir_stats(ctx: StrategyContext, payload: dict[str, Any] | None) -> dict[str, Any]:
    """
    Per-collection document counts, ``_search`` denormalization coverage, and
    generation vs search configuration gaps.
    """
    payload = payload or {}
    summary_only = bool(payload.get("summary_only", False))
    uri, database, prefix = bridge.resolve_mongo(ctx)
    cfg = bridge.resolve_strategy_config(ctx)
    config_dir, compartment_dir = _search_paths_from_config(cfg)

    mql_ctx = bridge.build_mql_context(
        uri,
        database,
        prefix,
        config_dir=config_dir,
        compartment_dir=compartment_dir,
    )

    try:
        search_types = set(bridge.supported_search_resource_types(mql_ctx.config_loader))
        gen_types = bridge.known_generation_resource_types()
        resource_types = _resolve_resource_types(payload, search_types, gen_types)
        versions = build_projection_versions(
            mql_ctx.config_loader,
            fhir_release=str(cfg.get("schema_version") or "R5"),
            compartment_definitions_dir=mql_ctx.compartment_definitions_dir,
            resource_types=[resource_type for resource_type in resource_types if resource_type in search_types],
        )

        collections: list[dict[str, Any]] = []
        total_documents = 0
        synthetic_documents = 0
        imported_documents = 0
        synthetic_job_ids: set[str] = set()
        existing_collection_names = _existing_collection_names(mql_ctx.db)
        database_document_count = _database_document_count(mql_ctx.db)
        database_is_empty = database_document_count == 0

        def inspect_resource_type(resource_type: str) -> tuple[dict[str, Any], set[str]]:
            coll_name = bridge.collection_name(prefix, resource_type)
            collection = mql_ctx.db[coll_name]
            collection_exists = (
                existing_collection_names is None or coll_name in existing_collection_names
            )
            document_count = (
                0
                if database_is_empty or not collection_exists
                else collection.count_documents({})
            )
            denormalized_count = 0
            compartment_count = 0
            contract_current_count = 0
            synthetic_count = 0
            imported_count = 0
            latest_stored_at = None
            resource_job_ids: set[str] = set()
            if document_count:
                denormalized_count = collection.count_documents({"_search": {"$exists": True}})
                compartment_count = collection.count_documents({"_compartments": {"$exists": True}})
                if resource_type in search_types:
                    contract_current_count = collection.count_documents(
                        {
                            "_search": {"$exists": True, "$type": "object"},
                            "_compartments": {"$exists": True, "$type": "object"},
                            "_kehrnel.storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
                            "_kehrnel.resource_projection_version": versions.for_resource(resource_type),
                        }
                    )
                synthetic_markers = [
                    {"_kehrnel.provenance.source": "synthetic"},
                    {
                        "meta.tag": {
                            "$elemMatch": {
                                "system": "https://kehrnel.dev/fhir",
                                "code": "synthetic",
                            }
                        }
                    },
                    {
                        "meta.extension": {
                            "$elemMatch": {"url": "https://kehrnel.dev/fhir/synthetic"}
                        }
                    },
                ]
                synthetic_count = collection.count_documents({"$or": synthetic_markers})
                imported_count = collection.count_documents(
                    {
                        "_kehrnel.provenance.source": "import",
                        "$nor": synthetic_markers,
                    }
                )
                if hasattr(collection, "find_one"):
                    latest = collection.find_one(
                        {"_kehrnel.stored_at": {"$exists": True}},
                        {"_id": 0, "_kehrnel.stored_at": 1},
                        sort=[("_kehrnel.stored_at", -1)],
                    )
                    if latest:
                        latest_stored_at = (latest.get("_kehrnel") or {}).get("stored_at")
                if hasattr(collection, "distinct"):
                    resource_job_ids.update(
                        str(value)
                        for value in collection.distinct("_kehrnel.provenance.job_id")
                        if value
                    )

            denorm_percent = (
                round((denormalized_count / document_count) * 100.0, 2) if document_count else 0.0
            )
            search_configured = resource_type in search_types
            generation_schema = resource_type in gen_types
            has_search_index = (
                None
                if summary_only
                else collection_exists and _collection_has_search_index(collection)
            )

            return ({
                "resource_type": resource_type,
                "collection": coll_name,
                "document_count": document_count,
                "denormalized_count": denormalized_count,
                "denormalized_percent": denorm_percent,
                "compartment_count": compartment_count,
                "contract_current_count": contract_current_count,
                "synthetic_count": synthetic_count,
                "imported_count": imported_count,
                "unclassified_count": max(0, document_count - synthetic_count - imported_count),
                "latest_stored_at": latest_stored_at,
                "contract_current_percent": (
                    round((contract_current_count / document_count) * 100.0, 2)
                    if document_count
                    else 0.0
                ),
                "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
                "resource_projection_version": (
                    versions.for_resource(resource_type) if resource_type in search_types else None
                ),
                "search_configured": search_configured,
                "generation_schema": generation_schema,
                "search_index_present": has_search_index,
                "coverage_gap": generation_schema and not search_configured,
            }, resource_job_ids)

        # A store can expose hundreds of resource types. Mongo clients are thread-safe,
        # so collect independent per-collection diagnostics concurrently rather than
        # making the operational portal wait on hundreds of serial round trips.
        max_workers = min(16, max(1, len(resource_types)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            inspected = list(executor.map(inspect_resource_type, resource_types))

        for row, resource_job_ids in inspected:
            collections.append(row)
            total_documents += int(row["document_count"])
            synthetic_documents += int(row["synthetic_count"])
            imported_documents += int(row["imported_count"])
            synthetic_job_ids.update(resource_job_ids)

        indexed_types = sorted(
            row["resource_type"] for row in collections if row.get("search_index_present")
        )

        return {
            "ok": True,
            "database": database,
            "collection_prefix": prefix or "",
            "search_resource_types": sorted(search_types),
            "generation_resource_types": sorted(gen_types),
            "indexed_resource_types": indexed_types,
            "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
            "projection_contract_version": versions.projection_contract_version,
            "summary": {
                "document_count": total_documents,
                "initialized_resource_type_count": (
                    len(existing_collection_names) if existing_collection_names is not None else None
                ),
                "populated_resource_type_count": sum(
                    1 for row in collections if row.get("document_count", 0) > 0
                ),
                "synthetic_count": synthetic_documents,
                "imported_count": imported_documents,
                "unclassified_count": max(
                    0, total_documents - synthetic_documents - imported_documents
                ),
                "synthetic_job_ids": sorted(synthetic_job_ids),
            },
            "index_verification": "not_scanned" if summary_only else "live",
            "collections": [] if summary_only else collections,
            "gaps": {
                "generation_without_search": sorted(gen_types - search_types),
                "search_without_generation": sorted(search_types - gen_types),
            },
        }
    finally:
        bridge.close_mql_context(mql_ctx)
