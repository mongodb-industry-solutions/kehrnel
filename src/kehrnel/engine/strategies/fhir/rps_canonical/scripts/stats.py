"""FHIR collection diagnostics: counts, denormalization coverage, index presence."""

from __future__ import annotations

from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.rps_canonical.scripts import bridge
from kehrnel.engine.strategies.fhir.rps_canonical.scripts.query import _search_paths_from_config


def _collection_has_search_index(collection: Any) -> bool:
    for info in collection.list_indexes():
        key = info.get("key") or {}
        if any(str(field).startswith("_search") for field in key):
            return True
    return False


def _resolve_resource_types(payload: dict[str, Any], search_types: set[str], gen_types: set[str]) -> list[str]:
    raw = payload.get("resource_types")
    if isinstance(raw, list) and raw:
        return sorted({str(rt).strip() for rt in raw if str(rt).strip()})
    return sorted(search_types | gen_types)


async def fhir_stats(ctx: StrategyContext, payload: dict[str, Any] | None) -> dict[str, Any]:
    """
    Per-collection document counts, ``_search`` denormalization coverage, and
    generation vs search configuration gaps.
    """
    payload = payload or {}
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

        collections: list[dict[str, Any]] = []
        for resource_type in resource_types:
            coll_name = bridge.collection_name(prefix, resource_type)
            collection = mql_ctx.db[coll_name]
            document_count = collection.count_documents({})
            denormalized_count = 0
            if document_count:
                denormalized_count = collection.count_documents({"_search": {"$exists": True}})

            denorm_percent = (
                round((denormalized_count / document_count) * 100.0, 2) if document_count else 0.0
            )
            search_configured = resource_type in search_types
            generation_schema = resource_type in gen_types
            has_search_index = _collection_has_search_index(collection) if document_count else False

            collections.append(
                {
                    "resource_type": resource_type,
                    "collection": coll_name,
                    "document_count": document_count,
                    "denormalized_count": denormalized_count,
                    "denormalized_percent": denorm_percent,
                    "search_configured": search_configured,
                    "generation_schema": generation_schema,
                    "search_index_present": has_search_index,
                    "coverage_gap": generation_schema and not search_configured,
                }
            )

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
            "collections": collections,
            "gaps": {
                "generation_without_search": sorted(gen_types - search_types),
                "search_without_generation": sorted(search_types - gen_types),
            },
        }
    finally:
        mql_ctx.client.close()
