"""Shared helpers for synthetic generation model catalogs."""
from __future__ import annotations

from typing import Any, Dict, List

from kehrnel.engine.core.errors import KehrnelError


def _as_lower(value: Any) -> str:
    return str(value or "").strip().lower()


def _extract_model_id(item: Dict[str, Any]) -> str:
    for key in ("model_id", "modelId", "_id", "id"):
        if item.get(key):
            return str(item[key]).strip()
    return ""


def _extract_catalog_template_id(doc: Dict[str, Any]) -> str:
    for key in ("template_id", "templateId", "name"):
        if doc.get(key):
            return str(doc.get(key)).strip()
    meta = doc.get("metadata") or {}
    if isinstance(meta, dict):
        for key in ("templateId", "template_id"):
            if meta.get(key):
                return str(meta.get(key)).strip()
    analytics = doc.get("analyticsTemplate") or {}
    if isinstance(analytics, dict):
        if analytics.get("templateId"):
            return str(analytics.get("templateId")).strip()
    return ""


async def _find_one_in_catalog(db: Any, collection: str, model_id: str) -> Dict[str, Any] | None:
    coll = db[collection]
    object_id = None
    try:
        from bson import ObjectId  # type: ignore

        if ObjectId.is_valid(model_id):
            object_id = ObjectId(model_id)
    except Exception:
        object_id = None
    probes = [
        {"model_id": model_id},
        {"modelId": model_id},
        {"_id": model_id},
        {"id": model_id},
        {"name": model_id},
        {"template_id": model_id},
        {"templateId": model_id},
        {"metadata.templateId": model_id},
        {"analyticsTemplate.templateId": model_id},
    ]
    if object_id is not None:
        probes.insert(0, {"_id": object_id})
    for q in probes:
        doc = await coll.find_one(q)
        if doc:
            return doc
    return None


def _get_model_catalog_db(storage: Any, model_source: Dict[str, Any] | None) -> Any | None:
    db = getattr(storage, "db", None)
    if db is None:
        return None
    db_name = (model_source or {}).get("database_name") or (model_source or {}).get("database")
    if db_name:
        client = getattr(db, "client", None)
        if client is None:
            return None
        return client[str(db_name)]
    return db


async def resolve_model_specs(
    storage: Any,
    *,
    model_source: Dict[str, Any] | None,
    requested_models: List[Dict[str, Any]],
    domain: str | None = None,
    strategy_id: str | None = None,
) -> List[Dict[str, Any]]:
    """Resolve model specs from payload + optional DB catalog documents."""
    if not isinstance(requested_models, list) or not requested_models:
        raise KehrnelError(code="INVALID_INPUT", status=400, message="models must be a non-empty array")

    catalog_collection = (model_source or {}).get("catalog_collection") or "user-data-models"
    domain_l = _as_lower(domain)
    strategy_l = _as_lower(strategy_id)
    resolved: List[Dict[str, Any]] = []
    catalog_db = _get_model_catalog_db(storage, model_source)

    for raw in requested_models:
        if not isinstance(raw, dict):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="each model entry must be an object")
        model_id = _extract_model_id(raw)
        if not model_id:
            raise KehrnelError(code="INVALID_INPUT", status=400, message="model_id is required in each model entry")

        catalog_doc = None
        if catalog_db is not None:
            catalog_doc = await _find_one_in_catalog(catalog_db, catalog_collection, model_id)

        if catalog_doc:
            doc_domain = _as_lower(catalog_doc.get("domain"))
            doc_strategy = _as_lower(catalog_doc.get("strategy_id") or catalog_doc.get("strategyId"))
            if domain_l and doc_domain and doc_domain != domain_l:
                raise KehrnelError(
                    code="MODEL_DOMAIN_MISMATCH",
                    status=400,
                    message=f"model_id={model_id} belongs to domain={doc_domain}, expected {domain_l}",
                )
            if strategy_l and doc_strategy and doc_strategy != strategy_l:
                raise KehrnelError(
                    code="MODEL_STRATEGY_MISMATCH",
                    status=400,
                    message=f"model_id={model_id} belongs to strategy={doc_strategy}, expected {strategy_l}",
                )

        min_per = int(raw.get("min_per_patient", raw.get("min", 1)))
        max_per = int(raw.get("max_per_patient", raw.get("max", min_per)))
        sample_pool_size = int(raw.get("sample_pool_size", 25))
        if min_per < 0 or max_per < min_per:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message=f"invalid range for model {model_id}: min={min_per}, max={max_per}",
            )

        resolved_template_id = str(
            raw.get("template_id")
            or raw.get("templateId")
            or _extract_catalog_template_id(catalog_doc or {})
            or model_id
        ).strip()

        resolved.append(
            {
                "model_id": model_id,
                "template_id": resolved_template_id,
                "min": min_per,
                "max": max_per,
                "weight": float(raw.get("weight", 1.0)),
                "sample_size": max(1, sample_pool_size),
                "catalog": catalog_doc or {},
            }
        )
    return resolved


async def resolve_links(
    storage: Any,
    *,
    model_source: Dict[str, Any] | None,
    model_ids: List[str],
    explicit_links: List[Dict[str, Any]] | None,
) -> List[Dict[str, Any]]:
    """Get link rules from payload or optional links collection."""
    if explicit_links is not None:
        if not isinstance(explicit_links, list):
            raise KehrnelError(code="INVALID_INPUT", status=400, message="links must be an array")
        return [l for l in explicit_links if isinstance(l, dict)]

    links_collection = (model_source or {}).get("links_collection")
    if not links_collection or not model_ids:
        return []
    links_db = _get_model_catalog_db(storage, model_source)
    if links_db is None:
        return []
    cursor = links_db[links_collection].aggregate(
        [{"$match": {"from": {"$in": model_ids}, "to": {"$in": model_ids}}}, {"$limit": 5000}]
    )
    docs = [doc async for doc in cursor]
    return [d for d in docs if isinstance(d, dict)]
