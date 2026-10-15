"""FHIR collection diagnostics: counts, denormalization coverage, index presence."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
from typing import Any

from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.resource_store.scripts import bridge
from kehrnel.engine.strategies.fhir.resource_store.scripts.document_contract import (
    STORED_DOCUMENT_SCHEMA_VERSION,
    build_projection_versions,
)
from kehrnel.engine.strategies.fhir.resource_store.scripts.query import (
    _search_paths_from_config,
)


FHIR_PROJECTION_READINESS_RECEIPT_CONTRACT = (
    "fhir-resource-store-projection-readiness/v1"
)


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _condition_code_projection_mismatch_filter() -> dict[str, Any]:
    """Compare canonical Condition codings with the exact search-token sidecar.

    The expression mirrors the denormalizer: a Coding with a code contributes
    ``system|code`` (or ``|code`` when system is absent).  Set comparison ignores
    duplicate codings while still detecting missing, stale, or extra tokens.
    """

    canonical_tokens = {
        "$map": {
            "input": {
                "$filter": {
                    "input": {
                        "$cond": [
                            {"$isArray": "$code.coding"},
                            "$code.coding",
                            [],
                        ]
                    },
                    "as": "coding",
                    "cond": {
                        "$and": [
                            {"$eq": [{"$type": "$$coding.code"}, "string"]},
                            {"$ne": ["$$coding.code", ""]},
                        ]
                    },
                }
            },
            "as": "coding",
            "in": {
                "$concat": [
                    {"$ifNull": ["$$coding.system", ""]},
                    "|",
                    "$$coding.code",
                ]
            },
        }
    }
    return {
        "$expr": {
            "$or": [
                {
                    "$not": [
                        {
                            "$in": [
                                {"$type": "$code.coding"},
                                ["array", "missing"],
                            ]
                        }
                    ]
                },
                {
                    "$not": [
                        {
                            "$in": [
                                {"$type": "$_search.code_systemCode"},
                                ["array", "missing"],
                            ]
                        }
                    ]
                },
                {
                    "$eq": [
                        {
                            "$setEquals": [
                                canonical_tokens,
                                {
                                    "$cond": [
                                        {"$isArray": "$_search.code_systemCode"},
                                        "$_search.code_systemCode",
                                        [],
                                    ]
                                },
                            ]
                        },
                        False,
                    ]
                },
            ]
        }
    }


def _projection_readiness_receipt(
    *,
    resource_type: str,
    database: str,
    collection: str,
    collection_exists: bool | None,
    document_count: int,
    contract_current_count: int,
    canonical_projection_mismatch_count: int | None,
    projection_contract_version: str,
    resource_projection_version: str | None,
    search_configured: bool,
    search_index_present: bool | None,
    exact_code_index_present: bool | None,
) -> dict[str, Any]:
    ready = bool(
        collection_exists is True
        and search_configured
        and search_index_present is True
        and (exact_code_index_present is True if resource_type == "Condition" else True)
        and resource_projection_version
        and contract_current_count == document_count
        and (
            canonical_projection_mismatch_count == 0
            if resource_type == "Condition"
            else True
        )
    )
    payload = {
        "contract": FHIR_PROJECTION_READINESS_RECEIPT_CONTRACT,
        "state": "ready" if ready else "not_ready",
        "resourceType": resource_type,
        "database": database,
        "collection": collection,
        "collectionExists": collection_exists,
        "documentCount": document_count,
        "contractCurrentCount": contract_current_count,
        "canonicalProjectionMismatchCount": canonical_projection_mismatch_count,
        "storageSchemaVersion": STORED_DOCUMENT_SCHEMA_VERSION,
        "projectionContractVersion": projection_contract_version,
        "resourceProjectionVersion": resource_projection_version,
        "searchConfigured": search_configured,
        "searchIndexPresent": search_index_present,
        "exactCodeIndexPresent": exact_code_index_present,
    }
    return {**payload, "receiptDigest": _digest(payload)}


def _collection_has_search_index(
    collection: Any,
    *,
    required_field: str | None = None,
) -> bool:
    try:
        for info in collection.list_indexes():
            key = info.get("key") or {}
            if required_field is not None and required_field in key:
                return True
            if required_field is None and any(
                str(field).startswith(("_search", "_compartments")) for field in key
            ):
                return True
    except Exception as exc:
        if "NamespaceNotFound" not in str(exc) and getattr(exc, "code", None) != 26:
            raise
    return False


def _resolve_resource_types(
    payload: dict[str, Any], search_types: set[str], gen_types: set[str]
) -> list[str]:
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


async def fhir_stats(
    ctx: StrategyContext, payload: dict[str, Any] | None
) -> dict[str, Any]:
    """
    Per-collection document counts, ``_search`` denormalization coverage, and
    generation vs search configuration gaps.
    """
    payload = payload or {}
    summary_only = bool(payload.get("summary_only", False))
    data_target_digest = str(
        ((ctx.meta or {}).get("data_target_digest") if ctx.meta else "") or ""
    ).strip() or None
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
        search_types = set(
            bridge.supported_search_resource_types(mql_ctx.config_loader)
        )
        gen_types = bridge.known_generation_resource_types()
        resource_types = _resolve_resource_types(payload, search_types, gen_types)
        versions = build_projection_versions(
            mql_ctx.config_loader,
            fhir_release=str(cfg.get("schema_version") or "R5"),
            compartment_definitions_dir=mql_ctx.compartment_definitions_dir,
            resource_types=[
                resource_type
                for resource_type in resource_types
                if resource_type in search_types
            ],
        )

        collections: list[dict[str, Any]] = []
        total_documents = 0
        synthetic_documents = 0
        imported_documents = 0
        synthetic_job_ids: set[str] = set()
        existing_collection_names = _existing_collection_names(mql_ctx.db)
        database_document_count = _database_document_count(mql_ctx.db)
        database_is_empty = database_document_count == 0

        def inspect_resource_type(
            resource_type: str,
        ) -> tuple[dict[str, Any], set[str]]:
            coll_name = bridge.collection_name(prefix, resource_type)
            collection = mql_ctx.db[coll_name]
            collection_exists = (
                None
                if existing_collection_names is None
                else coll_name in existing_collection_names
            )
            document_count = (
                0
                if database_is_empty or collection_exists is False
                else collection.count_documents({})
            )
            denormalized_count = 0
            compartment_count = 0
            contract_current_count = 0
            synthetic_count = 0
            imported_count = 0
            latest_stored_at = None
            canonical_projection_mismatch_count: int | None = None
            resource_job_ids: set[str] = set()
            if document_count:
                denormalized_count = collection.count_documents(
                    {"_search": {"$exists": True}}
                )
                compartment_count = collection.count_documents(
                    {"_compartments": {"$exists": True}}
                )
                if resource_type in search_types:
                    contract_current_count = collection.count_documents(
                        {
                            "_search": {"$exists": True, "$type": "object"},
                            "_compartments": {"$exists": True, "$type": "object"},
                            "_kehrnel.storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
                            "_kehrnel.projection_contract_version": (
                                versions.projection_contract_version
                            ),
                            "_kehrnel.resource_projection_version": versions.for_resource(
                                resource_type
                            ),
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
                        latest_stored_at = (latest.get("_kehrnel") or {}).get(
                            "stored_at"
                        )
                if hasattr(collection, "distinct"):
                    resource_job_ids.update(
                        str(value)
                        for value in collection.distinct("_kehrnel.provenance.job_id")
                        if value
                    )

                if resource_type == "Condition":
                    canonical_projection_mismatch_count = collection.count_documents(
                        _condition_code_projection_mismatch_filter()
                    )
            elif resource_type == "Condition" and collection_exists is True:
                # An intentionally empty, initialized collection has no mismatches.
                # Missing or unverifiable collections remain ``None`` and fail the
                # governed readiness receipt below.
                canonical_projection_mismatch_count = 0

            denorm_percent = (
                round((denormalized_count / document_count) * 100.0, 2)
                if document_count
                else 0.0
            )
            search_configured = resource_type in search_types
            generation_schema = resource_type in gen_types
            has_search_index = (
                None
                if summary_only
                else (
                    _collection_has_search_index(collection)
                    if collection_exists is True
                    else False
                    if collection_exists is False
                    else None
                )
            )
            exact_code_index_present = (
                _collection_has_search_index(
                    collection,
                    required_field="_search.code_systemCode",
                )
                if resource_type == "Condition" and collection_exists is True
                else False
                if resource_type == "Condition" and collection_exists is False
                else None
            )

            resource_projection_version = (
                versions.for_resource(resource_type)
                if resource_type in search_types
                else None
            )
            readiness_receipt = _projection_readiness_receipt(
                resource_type=resource_type,
                database=database,
                collection=coll_name,
                collection_exists=collection_exists,
                document_count=document_count,
                contract_current_count=contract_current_count,
                canonical_projection_mismatch_count=(
                    canonical_projection_mismatch_count
                ),
                projection_contract_version=versions.projection_contract_version,
                resource_projection_version=resource_projection_version,
                search_configured=search_configured,
                search_index_present=has_search_index,
                exact_code_index_present=exact_code_index_present,
            )

            return (
                {
                    "resource_type": resource_type,
                    "collection": coll_name,
                    "collection_exists": collection_exists,
                    "document_count": document_count,
                    "denormalized_count": denormalized_count,
                    "denormalized_percent": denorm_percent,
                    "compartment_count": compartment_count,
                    "contract_current_count": contract_current_count,
                    "canonical_projection_mismatch_count": (
                        canonical_projection_mismatch_count
                    ),
                    "synthetic_count": synthetic_count,
                    "imported_count": imported_count,
                    "unclassified_count": max(
                        0, document_count - synthetic_count - imported_count
                    ),
                    "latest_stored_at": latest_stored_at,
                    "contract_current_percent": (
                        round((contract_current_count / document_count) * 100.0, 2)
                        if document_count
                        else 0.0
                    ),
                    "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
                    "resource_projection_version": resource_projection_version,
                    "search_configured": search_configured,
                    "generation_schema": generation_schema,
                    "search_index_present": has_search_index,
                    "exact_code_index_present": exact_code_index_present,
                    "projection_readiness_receipt": readiness_receipt,
                    "coverage_gap": generation_schema and not search_configured,
                },
                resource_job_ids,
            )

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
            row["resource_type"]
            for row in collections
            if row.get("search_index_present")
        )

        return {
            "ok": True,
            "database": database,
            "data_target_digest": data_target_digest,
            "collection_prefix": prefix or "",
            "search_resource_types": sorted(search_types),
            "generation_resource_types": sorted(gen_types),
            "indexed_resource_types": indexed_types,
            "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
            "projection_contract_version": versions.projection_contract_version,
            "summary": {
                "document_count": total_documents,
                "initialized_resource_type_count": (
                    len(existing_collection_names)
                    if existing_collection_names is not None
                    else None
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


async def fhir_operational_probe(
    ctx: StrategyContext,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    """Return payload-safe schema, projection, and index evidence for selected resources."""

    resource_types = sorted(
        {
            str(value).strip()
            for value in ((payload or {}).get("resource_types") or [])
            if str(value).strip()
        }
    )
    if not resource_types:
        detail = (
            "No FHIR resource type was established by the package runtime evidence; "
            "the probe did not scan unrelated collections."
        )
        return {
            "state": "unavailable",
            "schemaState": "unavailable",
            "indexState": "unavailable",
            "checks": [
                {"checkId": check_id, "state": "unavailable", "detail": detail}
                for check_id in (
                    "runtime_connectivity",
                    "required_collections",
                    "storage_schema",
                    "managed_indexes",
                    "derived_projection",
                    "data_freshness",
                    "reconciliation_integrity",
                )
            ],
        }

    result = await fhir_stats(ctx, {"resource_types": resource_types})
    rows = result.get("collections") or []
    collection_check = bool(rows) and all(
        row.get("collection_exists") is True for row in rows
    )
    schema_check = bool(rows) and all(
        row.get("resource_projection_version")
        and row.get("contract_current_count") == row.get("document_count")
        for row in rows
    )
    index_check = bool(rows) and all(
        row.get("search_index_present") is True
        and (
            row.get("exact_code_index_present") is True
            if row.get("resource_type") == "Condition"
            else True
        )
        for row in rows
    )
    projection_check = bool(rows) and all(
        (row.get("projection_readiness_receipt") or {}).get("state") == "ready"
        for row in rows
    )
    reconciliation_check = bool(rows) and all(
        row.get("contract_current_count") == row.get("document_count")
        and (
            row.get("canonical_projection_mismatch_count") in (None, 0)
        )
        for row in rows
    )
    reconciliation_divergence_count = sum(
        max(
            max(
                int(row.get("document_count") or 0)
                - int(row.get("contract_current_count") or 0),
                0,
            ),
            int(row.get("canonical_projection_mismatch_count") or 0),
        )
        for row in rows
    )
    freshness_target = (payload or {}).get("freshness_target_seconds")
    freshness_check = projection_check and reconciliation_check
    checks = [
        {
            "checkId": "runtime_connectivity",
            "state": "passed",
            "detail": "The active FHIR strategy reached its configured MongoDB target.",
        },
        {
            "checkId": "required_collections",
            "state": "passed" if collection_check else "failed",
            "detail": (
                "Every package-observed FHIR resource collection exists."
                if collection_check
                else "One or more package-observed FHIR resource collections are missing."
            ),
        },
        {
            "checkId": "storage_schema",
            "state": "passed" if schema_check else "failed",
            "detail": (
                "Stored resources use the active storage and projection contract."
                if schema_check
                else "Stored resources do not fully match the active projection contract."
            ),
        },
        {
            "checkId": "managed_indexes",
            "state": "passed" if index_check else "failed",
            "detail": (
                "Required FHIR search indexes are present."
                if index_check
                else "One or more required FHIR search indexes are missing."
            ),
        },
        {
            "checkId": "derived_projection",
            "state": "passed" if projection_check else "failed",
            "detail": (
                "FHIR search projections are ready for the package-observed resources."
                if projection_check
                else "FHIR search projections are stale, incomplete, or unverified."
            ),
        },
        {
            "checkId": "data_freshness",
            "state": "passed" if freshness_check else "failed",
            "detail": (
                "FHIR search fields are projected in the authoritative write; observed governed-write lag is 0 seconds"
                + (
                    f" against a {freshness_target}-second target."
                    if isinstance(freshness_target, int)
                    else "; no bounded freshness target was declared."
                )
                if freshness_check
                else "FHIR projection freshness cannot be proven while projection readiness is incomplete."
            ),
        },
        {
            "checkId": "reconciliation_integrity",
            "state": "passed" if reconciliation_check else "failed",
            "detail": (
                "All package-observed FHIR documents match the active storage and canonical projection contract."
                if reconciliation_check
                else "One or more package-observed FHIR documents diverge from the active projection contract."
            ),
        },
    ]
    all_passed = all(check["state"] == "passed" for check in checks)
    return {
        "state": "healthy" if all_passed else "degraded",
        "schemaState": "in_sync" if collection_check and schema_check else "drifted",
        "indexState": "in_sync" if index_check else "drifted",
        "freshnessTargetSeconds": (
            freshness_target if isinstance(freshness_target, int) else None
        ),
        "observedFreshnessSeconds": 0 if freshness_check else None,
        "reconciliationDivergenceCount": reconciliation_divergence_count,
        "checks": checks,
    }

