"""FHIR semantic-projection configuration and safe text preview.

Semantic data is a rebuildable projection, never part of canonical FHIR.
Preview never sends text externally. Materialization and search are separate,
explicit operations backed by configured adapters; canonical resources are
never mutated.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
from datetime import UTC, datetime
from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.fhir import implementation_guides
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import (
    canonical_resource,
)

SEMANTIC_CONTRACT_VERSION = "fhir-semantic-projection.v1"
_PIPELINE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{0,63}$")
_SIMPLE_PATH = re.compile(r"^[A-Za-z][A-Za-z0-9]*(?:\.[A-Za-z][A-Za-z0-9]*)*$")
_HTML_TAG = re.compile(r"<[^>]+>")


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _pipelines(config: dict[str, Any]) -> list[dict[str, Any]]:
    semantic = config.get("semantic")
    if not isinstance(semantic, dict):
        return []
    pipelines = semantic.get("pipelines") or []
    if not isinstance(pipelines, list):
        raise ValueError("semantic.pipelines must be an array")
    return [item for item in pipelines if isinstance(item, dict)]


def validate_semantic_config(config: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate pipeline invariants that JSON Schema alone cannot express."""
    pipelines = _pipelines(config)
    active_profiles = {
        str(item.get("url") or "")
        for item in implementation_guides.resolve_active_profiles(config)
    }
    ids: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for position, pipeline in enumerate(pipelines):
        pipeline_id = str(pipeline.get("id") or "").strip()
        if not _PIPELINE_ID.fullmatch(pipeline_id):
            raise ValueError(
                f"semantic.pipelines[{position}].id must be a stable identifier"
            )
        if pipeline_id in ids:
            raise ValueError(f"Duplicate semantic pipeline id: {pipeline_id}")
        ids.add(pipeline_id)
        resource_types = [
            str(value).strip()
            for value in (pipeline.get("resource_types") or [])
            if str(value).strip()
        ]
        if not resource_types:
            raise ValueError(
                f"semantic pipeline {pipeline_id!r} requires resource_types"
            )
        fields = pipeline.get("fields") or []
        if not isinstance(fields, list) or not fields:
            raise ValueError(f"semantic pipeline {pipeline_id!r} requires fields")
        for field in fields:
            path = str((field or {}).get("path") or "").strip()
            relative = (
                path.split(".", 1)[1]
                if path.split(".", 1)[0] in resource_types and "." in path
                else path
            )
            if not _SIMPLE_PATH.fullmatch(relative):
                raise ValueError(
                    f"semantic pipeline {pipeline_id!r} has unsupported path {path!r}; "
                    "use the simple dotted FHIRPath subset"
                )
        profiles = {
            str(value).strip()
            for value in (pipeline.get("profiles") or [])
            if str(value).strip()
        }
        unavailable = sorted(profiles - active_profiles)
        if unavailable:
            raise ValueError(
                f"semantic pipeline {pipeline_id!r} references profiles that are not active: "
                + ", ".join(unavailable)
            )
        chunking = pipeline.get("chunking") or {}
        maximum = int(chunking.get("max_chars") or 3000)
        overlap = int(chunking.get("overlap_chars") or 300)
        if overlap >= maximum:
            raise ValueError(
                f"semantic pipeline {pipeline_id!r} requires overlap_chars < max_chars"
            )
        normalized.append(pipeline)
    return normalized


def _walk(value: Any, parts: list[str]) -> list[Any]:
    if not parts:
        if isinstance(value, list):
            return [item for child in value for item in _walk(child, [])]
        return [value]
    if isinstance(value, list):
        return [item for child in value for item in _walk(child, parts)]
    if not isinstance(value, dict) or parts[0] not in value:
        return []
    return _walk(value[parts[0]], parts[1:])


def _as_text(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        clean = html.unescape(_HTML_TAG.sub(" ", value))
        clean = " ".join(clean.split())
        return [clean] if clean else []
    if isinstance(value, (int, float, bool)):
        return [str(value)]
    if isinstance(value, list):
        return [text for item in value for text in _as_text(item)]
    if isinstance(value, dict):
        preferred: list[str] = []
        for key in ("text", "display", "title", "value", "reference"):
            if key in value:
                preferred.extend(_as_text(value[key]))
        coding = value.get("coding")
        if isinstance(coding, list):
            for item in coding:
                if isinstance(item, dict):
                    preferred.extend(_as_text(item.get("display") or item.get("code")))
        if preferred:
            return preferred
        return [
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        ]
    return [str(value)]


def _chunks(text: str, *, maximum: int, overlap: int) -> list[str]:
    if not text:
        return []
    if len(text) <= maximum:
        return [text]
    result: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + maximum)
        if end < len(text):
            boundary = text.rfind(" ", start, end)
            if boundary > start + maximum // 2:
                end = boundary
        result.append(text[start:end].strip())
        if end >= len(text):
            break
        start = max(start + 1, end - overlap)
    return [value for value in result if value]


def describe_semantic_config(
    config: dict[str, Any], adapters: dict[str, Any] | None = None
) -> dict[str, Any]:
    semantic = (
        config.get("semantic") if isinstance(config.get("semantic"), dict) else {}
    )
    pipelines = validate_semantic_config(config)
    descriptions = []
    for pipeline in pipelines:
        normalized = json.loads(json.dumps(pipeline, sort_keys=True))
        descriptions.append(
            {
                "id": pipeline["id"],
                "enabled": bool(pipeline.get("enabled", True)),
                "resource_types": list(pipeline.get("resource_types") or []),
                "profiles": list(pipeline.get("profiles") or []),
                "trigger": pipeline.get("trigger") or "manual",
                "storage": pipeline.get("storage")
                or {
                    "mode": "sidecar",
                    "collection": "fhir_semantic_chunks",
                },
                "projection_version": _digest(
                    {"contract": SEMANTIC_CONTRACT_VERSION, "pipeline": normalized}
                ),
            }
        )
    adapters = adapters or {}
    return {
        "contract_version": SEMANTIC_CONTRACT_VERSION,
        "enabled": bool(semantic.get("enabled", False)),
        "configured_pipeline_count": len(descriptions),
        "active_pipeline_count": (
            sum(1 for item in descriptions if item["enabled"])
            if semantic.get("enabled", False)
            else 0
        ),
        "pipelines": descriptions,
        "execution": {
            "preview": True,
            "embedding_generation": "embedding" in adapters,
            "sidecar_persistence": "storage" in adapters,
            "vector_search": all(
                name in adapters for name in ("embedding", "storage", "atlas_search")
            ),
            "rebuild_jobs": False,
        },
        "canonical_resources_mutated": False,
    }


def fhir_semantic_preview(
    ctx: StrategyContext, payload: dict[str, Any] | None
) -> dict[str, Any]:
    """Render the exact text/chunks a configured pipeline would embed."""
    payload = payload or {}
    cfg = bridge.resolve_strategy_config(ctx)
    semantic_cfg = cfg.get("semantic") if isinstance(cfg.get("semantic"), dict) else {}
    if not semantic_cfg.get("enabled", False):
        raise KehrnelError(
            code="FHIR_SEMANTIC_DISABLED",
            status=409,
            message="Semantic projections are not enabled in this activation",
        )
    pipelines = validate_semantic_config(cfg)
    pipeline_id = str(payload.get("pipeline_id") or "").strip()
    pipeline = next((item for item in pipelines if item.get("id") == pipeline_id), None)
    if pipeline is None:
        raise KehrnelError(
            code="FHIR_SEMANTIC_PIPELINE_NOT_FOUND",
            status=404,
            message=f"Semantic pipeline {pipeline_id!r} is not configured",
        )
    if not pipeline.get("enabled", True):
        raise KehrnelError(
            code="FHIR_SEMANTIC_PIPELINE_DISABLED",
            status=409,
            message=f"Semantic pipeline {pipeline_id!r} is disabled",
        )
    raw_resource = payload.get("resource")
    if not isinstance(raw_resource, dict):
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="resource must be a FHIR JSON object",
        )
    resource = canonical_resource(raw_resource)
    resource_type = str(resource.get("resourceType") or "")
    if resource_type not in set(map(str, pipeline.get("resource_types") or [])):
        raise KehrnelError(
            code="FHIR_SEMANTIC_RESOURCE_NOT_SELECTED",
            status=400,
            message=f"{resource_type or 'Unknown'} is not selected by pipeline {pipeline_id!r}",
        )
    selected_profiles = set(map(str, pipeline.get("profiles") or []))
    resource_profiles = {
        str(value)
        for value in ((resource.get("meta") or {}).get("profile") or [])
        if str(value)
    }
    if selected_profiles and not selected_profiles.intersection(resource_profiles):
        raise KehrnelError(
            code="FHIR_SEMANTIC_PROFILE_NOT_SELECTED",
            status=400,
            message="Resource meta.profile does not match the semantic pipeline",
            details={"required_profiles": sorted(selected_profiles)},
        )

    extracted: list[dict[str, Any]] = []
    sections: list[str] = []
    maximum_source = int(pipeline.get("max_source_chars") or 50_000)
    for field in pipeline.get("fields") or []:
        raw_path = str(field.get("path") or "").strip()
        parts = raw_path.split(".")
        if parts and parts[0] == resource_type:
            parts = parts[1:]
        values = [text for value in _walk(resource, parts) for text in _as_text(value)]
        label = str(field.get("label") or raw_path)
        extracted.append({"path": raw_path, "label": label, "values": values})
        if values:
            sections.append(f"{label}: " + " | ".join(values))
    separator = str(pipeline.get("separator") or "\n")
    rendered = separator.join(sections)[:maximum_source]
    chunking = pipeline.get("chunking") or {}
    maximum = int(chunking.get("max_chars") or 3000)
    overlap = int(chunking.get("overlap_chars") or 300)
    chunks = _chunks(rendered, maximum=maximum, overlap=overlap)
    projection_version = _digest(
        {
            "contract": SEMANTIC_CONTRACT_VERSION,
            "pipeline": json.loads(json.dumps(pipeline, sort_keys=True)),
        }
    )
    return {
        "ok": True,
        "contract_version": SEMANTIC_CONTRACT_VERSION,
        "pipeline_id": pipeline_id,
        "projection_version": projection_version,
        "source_hash": _digest(resource),
        "resource": {
            "resource_type": resource_type,
            "id": resource.get("id"),
            "profiles": sorted(resource_profiles),
        },
        "extracted": extracted,
        "rendered_text": rendered,
        "chunks": [
            {"ordinal": index, "text": value, "characters": len(value)}
            for index, value in enumerate(chunks)
        ],
        "embedding_generated": False,
        "canonical_resource_mutated": False,
    }


def _pipeline(config: dict[str, Any], pipeline_id: str) -> dict[str, Any]:
    semantic_cfg = (
        config.get("semantic") if isinstance(config.get("semantic"), dict) else {}
    )
    if not semantic_cfg.get("enabled", False):
        raise KehrnelError(
            code="FHIR_SEMANTIC_DISABLED",
            status=409,
            message="Semantic projections are not enabled in this activation",
        )
    pipeline = next(
        (
            item
            for item in validate_semantic_config(config)
            if item.get("id") == pipeline_id
        ),
        None,
    )
    if pipeline is None:
        raise KehrnelError(
            code="FHIR_SEMANTIC_PIPELINE_NOT_FOUND",
            status=404,
            message=f"Semantic pipeline {pipeline_id!r} is not configured",
        )
    if not pipeline.get("enabled", True):
        raise KehrnelError(
            code="FHIR_SEMANTIC_PIPELINE_DISABLED",
            status=409,
            message=f"Semantic pipeline {pipeline_id!r} is disabled",
        )
    return pipeline


def _adapter(ctx: StrategyContext, name: str, method: str) -> Any:
    adapter = (ctx.adapters or {}).get(name)
    if adapter is None or not callable(getattr(adapter, method, None)):
        raise KehrnelError(
            code="FHIR_SEMANTIC_ADAPTER_UNAVAILABLE",
            status=503,
            message=f"Semantic operation requires adapter {name!r} with {method}()",
            details={"binding": name, "method": method},
        )
    return adapter


async def _materialization_resources(
    ctx: StrategyContext,
    config: dict[str, Any],
    payload: dict[str, Any],
) -> list[dict[str, Any]]:
    supplied = payload.get("resources")
    if supplied is None and isinstance(payload.get("resource"), dict):
        supplied = [payload["resource"]]
    if supplied is not None:
        if not isinstance(supplied, list) or not supplied or len(supplied) > 500:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="resources must contain between 1 and 500 FHIR resources",
            )
        if not all(isinstance(item, dict) for item in supplied):
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Every semantic source resource must be a JSON object",
            )
        resources = [canonical_resource(item) for item in supplied]
        for item in resources:
            if not str(item.get("resourceType") or "").strip() or not str(
                item.get("id") or ""
            ).strip():
                raise KehrnelError(
                    code="INVALID_INPUT",
                    status=400,
                    message="Every semantic source resource requires resourceType and id",
                )
        return resources

    targets = payload.get("targets")
    if not isinstance(targets, list) or not targets or len(targets) > 500:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Provide resources or between 1 and 500 resource targets",
        )
    storage = _adapter(ctx, "storage", "find_one")
    prefix = str(config.get("collection_prefix") or "")
    resources: list[dict[str, Any]] = []
    for target in targets:
        if not isinstance(target, dict):
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Each target must be an object",
            )
        resource_type = str(target.get("resource_type") or "").strip()
        resource_id = str(target.get("id") or "").strip()
        if not resource_type or not resource_id:
            raise KehrnelError(
                code="INVALID_INPUT",
                status=400,
                message="Each target requires resource_type and id",
            )
        stored = await storage.find_one(
            f"{prefix}{resource_type}", {"id": resource_id}, None
        )
        if stored is None:
            raise KehrnelError(
                code="FHIR_RESOURCE_NOT_FOUND",
                status=404,
                message=f"{resource_type}/{resource_id} was not found",
            )
        resources.append(canonical_resource(stored))
    return resources


def _sidecar_collection(pipeline: dict[str, Any]) -> str:
    storage = (
        pipeline.get("storage") if isinstance(pipeline.get("storage"), dict) else {}
    )
    collection = str(storage.get("collection") or "fhir_semantic_chunks").strip()
    if not collection or collection.startswith("system.") or "$" in collection:
        raise KehrnelError(
            code="FHIR_SEMANTIC_STORAGE_INVALID",
            status=400,
            message="Semantic sidecar collection is invalid",
        )
    return collection


async def fhir_semantic_materialize(
    ctx: StrategyContext, payload: dict[str, Any] | None
) -> dict[str, Any]:
    """Embed selected resources and idempotently persist rebuildable sidecars."""

    payload = payload or {}
    config = bridge.resolve_strategy_config(ctx)
    pipeline_id = str(payload.get("pipeline_id") or "").strip()
    pipeline = _pipeline(config, pipeline_id)
    resources = await _materialization_resources(ctx, config, payload)
    embedding_cfg = (
        pipeline.get("embedding") if isinstance(pipeline.get("embedding"), dict) else {}
    )
    binding = str(embedding_cfg.get("binding") or "embedding")
    embedding = _adapter(ctx, binding, "embed")
    storage = _adapter(ctx, "storage", "replace_many")
    index_admin = _adapter(ctx, "index_admin", "ensure_indexes")

    previews: list[dict[str, Any]] = []
    texts: list[str] = []
    chunk_owners: list[tuple[int, int]] = []
    for resource_index, resource in enumerate(resources):
        preview = fhir_semantic_preview(
            ctx, {"pipeline_id": pipeline_id, "resource": resource}
        )
        previews.append(preview)
        for chunk in preview["chunks"]:
            texts.append(chunk["text"])
            chunk_owners.append((resource_index, int(chunk["ordinal"])))
    if not texts:
        raise KehrnelError(
            code="FHIR_SEMANTIC_EMPTY",
            status=422,
            message="The configured fields produced no semantic text",
        )

    try:
        vectors = await embedding.embed(texts)
    except Exception as exc:
        raise KehrnelError(
            code="FHIR_EMBEDDING_FAILED",
            status=502,
            message="The configured embedding provider failed",
            details={"error": str(exc)},
        ) from exc
    if len(vectors) != len(texts):
        raise KehrnelError(
            code="FHIR_EMBEDDING_INVALID",
            status=502,
            message="Embedding provider returned a different number of vectors",
        )
    dimensions = int(embedding_cfg.get("dimensions") or len(vectors[0]))
    if any(len(vector) != dimensions for vector in vectors):
        raise KehrnelError(
            code="FHIR_EMBEDDING_DIMENSIONS_MISMATCH",
            status=502,
            message=f"Every embedding must contain {dimensions} dimensions",
        )

    collection = _sidecar_collection(pipeline)
    await index_admin.ensure_collection(collection)
    index_result = await index_admin.ensure_indexes(
        collection,
        [
            {
                "keys": [
                    ("pipeline_id", 1),
                    ("source.resource_type", 1),
                    ("source.id", 1),
                    ("ordinal", 1),
                ],
                "options": {"name": "semantic_source_chunk", "unique": True},
            },
            {
                "keys": [("pipeline_id", 1), ("projection_version", 1)],
                "options": {"name": "semantic_projection_version"},
            },
        ],
    )

    now = datetime.now(UTC)
    documents: list[dict[str, Any]] = []
    ids_by_source: dict[tuple[str, str], list[str]] = {}
    model = embedding_cfg.get("model")
    for vector, text, (resource_index, ordinal) in zip(
        vectors, texts, chunk_owners, strict=True
    ):
        preview = previews[resource_index]
        source = preview["resource"]
        resource_type = str(source.get("resource_type") or "")
        resource_id = str(source.get("id") or "")
        identity = _digest(
            {
                "environment_id": ctx.environment_id,
                "pipeline_id": pipeline_id,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "ordinal": ordinal,
            }
        )
        document = {
            "_id": identity,
            "contract_version": SEMANTIC_CONTRACT_VERSION,
            "environment_id": ctx.environment_id,
            "pipeline_id": pipeline_id,
            "projection_version": preview["projection_version"],
            "source": {
                "resource_type": resource_type,
                "id": resource_id,
                "profiles": source.get("profiles") or [],
                "hash": preview["source_hash"],
            },
            "ordinal": ordinal,
            "text": text,
            "embedding": vector,
            "embedding_model": model,
            "dimensions": dimensions,
            "materialized_at": now,
        }
        documents.append(document)
        ids_by_source.setdefault((resource_type, resource_id), []).append(identity)

    await storage.replace_many(collection, documents)
    delete_many = getattr(storage, "delete_many", None)
    if callable(delete_many):
        for (resource_type, resource_id), current_ids in ids_by_source.items():
            await delete_many(
                collection,
                {
                    "pipeline_id": pipeline_id,
                    "source.resource_type": resource_type,
                    "source.id": resource_id,
                    "_id": {"$nin": current_ids},
                },
            )

    vector_index = str(
        (pipeline.get("storage") or {}).get("vector_index") or f"{pipeline_id}-vector"
    )
    vector_index_result: dict[str, Any] = {
        "created": [],
        "updated": [],
        "warnings": ["Atlas vector-search adapter is not configured"],
    }
    atlas = (ctx.adapters or {}).get("atlas_search")
    if atlas is not None and callable(getattr(atlas, "ensure_vector_index", None)):
        vector_index_result = await atlas.ensure_vector_index(
            collection,
            vector_index,
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": dimensions,
                        "similarity": "cosine",
                    },
                    {"type": "filter", "path": "pipeline_id"},
                    {"type": "filter", "path": "source.resource_type"},
                ]
            },
        )

    return {
        "ok": True,
        "contract_version": SEMANTIC_CONTRACT_VERSION,
        "pipeline_id": pipeline_id,
        "collection": collection,
        "vector_index": vector_index,
        "resources": len(resources),
        "chunks": len(documents),
        "dimensions": dimensions,
        "projection_versions": sorted(
            {preview["projection_version"] for preview in previews}
        ),
        "index_result": index_result,
        "vector_index_result": vector_index_result,
        "canonical_resources_mutated": False,
    }


async def fhir_semantic_search(
    ctx: StrategyContext, payload: dict[str, Any] | None
) -> dict[str, Any]:
    """Embed a query and execute Atlas Vector Search over semantic sidecars."""

    payload = payload or {}
    config = bridge.resolve_strategy_config(ctx)
    pipeline_id = str(payload.get("pipeline_id") or "").strip()
    pipeline = _pipeline(config, pipeline_id)
    query = str(payload.get("query") or "").strip()
    if not query or len(query) > 20_000:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="Semantic query must contain between 1 and 20,000 characters",
        )
    limit = payload.get("limit", 10)
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        raise KehrnelError(
            code="INVALID_INPUT",
            status=400,
            message="limit must be between 1 and 100",
        )
    embedding_cfg = (
        pipeline.get("embedding") if isinstance(pipeline.get("embedding"), dict) else {}
    )
    embedding = _adapter(ctx, str(embedding_cfg.get("binding") or "embedding"), "embed")
    storage = _adapter(ctx, "storage", "aggregate")
    try:
        vectors = await embedding.embed([query])
    except Exception as exc:
        raise KehrnelError(
            code="FHIR_EMBEDDING_FAILED",
            status=502,
            message="The configured embedding provider failed",
            details={"error": str(exc)},
        ) from exc
    if len(vectors) != 1:
        raise KehrnelError(
            code="FHIR_EMBEDDING_INVALID",
            status=502,
            message="Embedding provider did not return one query vector",
        )
    dimensions = embedding_cfg.get("dimensions")
    if dimensions is not None and len(vectors[0]) != int(dimensions):
        raise KehrnelError(
            code="FHIR_EMBEDDING_DIMENSIONS_MISMATCH",
            status=502,
            message=f"Query embedding must contain {int(dimensions)} dimensions",
        )
    collection = _sidecar_collection(pipeline)
    vector_index = str(
        (pipeline.get("storage") or {}).get("vector_index") or f"{pipeline_id}-vector"
    )
    vector_stage: dict[str, Any] = {
        "index": vector_index,
        "path": "embedding",
        "queryVector": vectors[0],
        "numCandidates": min(max(limit * 10, 100), 10_000),
        "limit": limit,
        "filter": {"pipeline_id": pipeline_id},
    }
    resource_types = payload.get("resource_types")
    if isinstance(resource_types, list) and resource_types:
        vector_stage["filter"] = {
            "$and": [
                {"pipeline_id": pipeline_id},
                {"source.resource_type": {"$in": sorted(map(str, resource_types))}},
            ]
        }
    try:
        rows = await storage.aggregate(
            collection,
            [
                {"$vectorSearch": vector_stage},
                {
                    "$project": {
                        "_id": 0,
                        "score": {"$meta": "vectorSearchScore"},
                        "pipeline_id": 1,
                        "projection_version": 1,
                        "source": 1,
                        "ordinal": 1,
                        "text": 1,
                        "embedding_model": 1,
                    }
                },
            ],
        )
    except Exception as exc:
        raise KehrnelError(
            code="FHIR_SEMANTIC_SEARCH_UNAVAILABLE",
            status=503,
            message="Atlas Vector Search could not execute for this pipeline",
            details={
                "collection": collection,
                "index": vector_index,
                "error": str(exc),
            },
        ) from exc
    return {
        "ok": True,
        "contract_version": SEMANTIC_CONTRACT_VERSION,
        "pipeline_id": pipeline_id,
        "collection": collection,
        "vector_index": vector_index,
        "count": len(rows),
        "results": rows,
        "canonical_resources_mutated": False,
    }
