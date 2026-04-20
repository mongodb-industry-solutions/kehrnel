from __future__ import annotations

from typing import Any, Dict, List

from .models import normalize_context_definition, safe_list


async def load_catalog_definitions(storage: Any, catalog: Dict[str, Any] | None) -> List[Dict[str, Any]]:
    catalog = dict(catalog or {})
    inline = safe_list(catalog.get("definitions"))
    if inline:
        return [normalize_context_definition(item) for item in inline]

    collection = catalog.get("collection")
    if not collection:
        return []
    if storage is None:
        raise ValueError("storage adapter is required to load catalog definitions from a collection")

    include_draft = bool(catalog.get("includeDraft", False))
    match: Dict[str, Any] = {"_catalogType": "definition"}
    if not include_draft:
        match["status"] = {"$ne": "draft"}

    rows = await storage.aggregate(collection, [{"$match": match}])
    return [normalize_context_definition(item) for item in rows]
