"""Deterministic governance manifest for FHIR MongoDB indexes."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge


INDEX_MANIFEST_VERSION = "fhir-index-manifest.v1"
DEFAULT_MANAGED_INDEX_BUDGET = 63  # MongoDB's 64-index limit includes _id.


def _normalized_fields(index_spec: dict[str, Any]) -> list[list[Any]]:
    fields = index_spec.get("fields", {})
    if isinstance(fields, str):
        return [[fields, 1]]
    if isinstance(fields, dict):
        return [[str(key), value] for key, value in fields.items()]
    normalized: list[list[Any]] = []
    if isinstance(fields, list):
        for entry in fields:
            if isinstance(entry, dict):
                normalized.extend([[str(key), value] for key, value in entry.items()])
            elif isinstance(entry, (list, tuple)) and len(entry) == 2:
                normalized.append([str(entry[0]), entry[1]])
            elif isinstance(entry, str):
                normalized.append([entry, 1])
    return normalized


def _index_name(index_spec: dict[str, Any], fields: list[list[Any]]) -> str:
    options = index_spec.get("options") or {}
    if isinstance(options, dict) and options.get("name"):
        return str(options["name"])
    return "_".join(f"{field}_{direction}" for field, direction in fields) or "index"


def _manifest_index(index_spec: dict[str, Any], *, source: str) -> dict[str, Any]:
    fields = _normalized_fields(index_spec)
    options = dict(index_spec.get("options") or {})
    return {
        "name": _index_name(index_spec, fields),
        "fields": fields,
        "unique": bool(options.get("unique", False)),
        "source": source,
    }


def _fingerprint(index: dict[str, Any]) -> str:
    return json.dumps(index.get("fields") or [], separators=(",", ":"), sort_keys=False)


def build_index_manifest(
    loader: Any,
    collection_prefix: str = "",
    *,
    resource_types: Iterable[str] | None = None,
    max_managed_indexes_per_collection: int = DEFAULT_MANAGED_INDEX_BUDGET,
) -> dict[str, Any]:
    """Build a stable index manifest without connecting to MongoDB."""

    supported = set(str(value) for value in loader.list_resources())
    requested = sorted(
        set(str(value) for value in (resource_types or supported) if value)
    )
    collections: list[dict[str, Any]] = []
    skipped: list[str] = []
    violations: list[dict[str, Any]] = []
    total = 0

    for resource_type in requested:
        if resource_type not in supported:
            skipped.append(resource_type)
            continue
        config = loader.get_config(resource_type)
        declared = [
            _manifest_index(
                {"fields": {"id": 1}, "options": {"name": "id_unique", "unique": True}},
                source="fhir-identity",
            )
        ]
        declared.extend(
            _manifest_index(spec, source="fhir-mql")
            for spec in list(config.get("indexes") or [])
        )
        indexes: list[dict[str, Any]] = []
        seen: set[str] = set()
        duplicate_names: list[str] = []
        for index in declared:
            fingerprint = _fingerprint(index)
            if fingerprint in seen:
                duplicate_names.append(index["name"])
                continue
            seen.add(fingerprint)
            indexes.append(index)
        count = len(indexes)
        total += count
        within_budget = count <= max_managed_indexes_per_collection
        entry = {
            "resource_type": resource_type,
            "collection": bridge.collection_name(collection_prefix, resource_type),
            "managed_index_count": count,
            "managed_index_budget": max_managed_indexes_per_collection,
            "within_budget": within_budget,
            "deduplicated_index_names": sorted(duplicate_names),
            "indexes": indexes,
        }
        collections.append(entry)
        if not within_budget:
            violations.append(
                {
                    "resource_type": resource_type,
                    "collection": entry["collection"],
                    "managed_index_count": count,
                    "managed_index_budget": max_managed_indexes_per_collection,
                }
            )

    digest_body = {
        "version": INDEX_MANIFEST_VERSION,
        "budget": max_managed_indexes_per_collection,
        "collections": collections,
    }
    digest = hashlib.sha256(
        json.dumps(digest_body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return {
        "manifest_version": INDEX_MANIFEST_VERSION,
        "digest": digest,
        "resource_count": len(collections),
        "managed_index_count": total,
        "max_managed_indexes_per_collection": max_managed_indexes_per_collection,
        "within_budget": not violations,
        "violations": violations,
        "skipped": skipped,
        "collections": collections,
    }


def expected_fingerprints(manifest: dict[str, Any], resource_type: str) -> set[tuple]:
    """Return normalized key tuples for one resource entry in a manifest."""

    for entry in manifest.get("collections") or []:
        if entry.get("resource_type") == resource_type:
            return {
                tuple(
                    (field, direction) for field, direction in index.get("fields") or []
                )
                for index in entry.get("indexes") or []
            }
    return set()
