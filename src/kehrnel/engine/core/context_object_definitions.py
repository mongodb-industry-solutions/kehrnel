"""Load and integrity-check bundled ContextObject definition summaries."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.semantic_execution import (
    ContextObjectDefinitionCatalog,
    ContextObjectDefinitionSummary,
)


DEFAULT_CONTEXT_OBJECT_DEFINITION_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "context_object_definitions" / "catalog.json"
)


class ContextObjectDefinitionCatalogError(ValueError):
    """The bundled definition catalog is malformed or fails integrity checks."""


def context_object_definition_content_digest(
    definition: ContextObjectDefinitionSummary | dict[str, Any],
) -> str:
    payload = definition.to_wire() if isinstance(definition, ContextObjectDefinitionSummary) else deepcopy(definition)
    artifact_ref = payload.get("artifactRef") or payload.get("artifact_ref")
    if not isinstance(artifact_ref, dict):
        raise ContextObjectDefinitionCatalogError("ContextObject definition is missing artifactRef")
    artifact_ref.pop("digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def load_context_object_definition_catalog(
    path: str | Path = DEFAULT_CONTEXT_OBJECT_DEFINITION_CATALOG_PATH,
) -> ContextObjectDefinitionCatalog:
    catalog_path = Path(path)
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog = ContextObjectDefinitionCatalog.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise ContextObjectDefinitionCatalogError(
            f"invalid ContextObject definition catalog at {catalog_path}: {exc}"
        ) from exc
    for definition in catalog.definitions:
        expected = context_object_definition_content_digest(definition)
        if definition.artifact_ref.digest != expected:
            raise ContextObjectDefinitionCatalogError(
                "ContextObject definition digest mismatch for "
                f"{definition.artifact_ref.id}@{definition.artifact_ref.version}"
            )
    return catalog


def context_object_definition_index(
    catalog: ContextObjectDefinitionCatalog,
) -> dict[tuple[str, str, str], ContextObjectDefinitionSummary]:
    return {
        (item.artifact_ref.id, item.artifact_ref.version, item.artifact_ref.digest): item
        for item in catalog.definitions
    }
