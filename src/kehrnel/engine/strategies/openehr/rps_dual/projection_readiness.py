"""Deterministic contracts for the governed semantic search projection."""

from __future__ import annotations

import hashlib
from typing import Any, Mapping, Sequence

from kehrnel.contracts.package_digest import canonical_json_bytes


SEMANTIC_PROJECTION_MANIFEST_CONTRACT = (
    "openehr-semantic-projection-manifest/v1"
)
SEMANTIC_PROJECTION_READINESS_CONTRACT = (
    "openehr-semantic-projection-readiness/v1"
)


def content_digest(value: Mapping[str, Any] | Sequence[Any]) -> str:
    """Return the repository-wide prefixed digest for canonical JSON content."""

    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def build_semantic_projection_manifest(
    *,
    environment_id: str,
    template_id: str,
    template_source_digest: str,
    catalog_digest: str,
    plan_digest: str,
    mapping_content: Mapping[str, Any] | Sequence[Any],
    projection_revision: str,
    required_paths: Sequence[str],
    available_paths: Sequence[str],
    atlas_index_name: str,
    atlas_index_definition: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the immutable manifest bound to one semantic plan's dependencies."""

    required = sorted(set(required_paths))
    available = set(available_paths)
    covered = [path for path in required if path in available]
    missing = [path for path in required if path not in available]
    manifest: dict[str, Any] = {
        "contract": SEMANTIC_PROJECTION_MANIFEST_CONTRACT,
        "environmentId": environment_id,
        "strategyId": "openehr.rps_dual",
        "templateId": template_id,
        "templateSourceDigest": template_source_digest,
        "catalogDigest": catalog_digest,
        "planDigest": plan_digest,
        "mappingDigest": content_digest(mapping_content),
        "projectionRevision": projection_revision,
        "requiredPaths": required,
        "coveredPaths": covered,
        "missingPaths": missing,
        "atlasIndexName": atlas_index_name,
        "atlasIndexDefinitionDigest": content_digest(atlas_index_definition),
    }
    manifest["manifestDigest"] = content_digest(manifest)
    return manifest


def atlas_index_readiness(
    indexes: Sequence[Mapping[str, Any]],
    *,
    index_name: str,
    expected_definition_digest: str,
) -> tuple[str, bool]:
    """Classify Atlas Search state without inferring readiness from configuration."""

    candidate = next(
        (item for item in indexes if str(item.get("name") or "") == index_name),
        None,
    )
    if candidate is None:
        return "missing", False

    definition = candidate.get("latestDefinition")
    if not isinstance(definition, Mapping):
        definition = candidate.get("definition")
    if not isinstance(definition, Mapping):
        return "configured_unverified", False
    if content_digest(definition) != expected_definition_digest:
        return "stale", False

    status = str(candidate.get("status") or "").strip().upper()
    queryable = candidate.get("queryable")
    if status == "READY" and queryable is True:
        return "verified_ready", True
    return "configured_unverified", False


__all__ = [
    "SEMANTIC_PROJECTION_MANIFEST_CONTRACT",
    "SEMANTIC_PROJECTION_READINESS_CONTRACT",
    "atlas_index_readiness",
    "build_semantic_projection_manifest",
    "content_digest",
]
