"""Load and integrity-check the bundled Mapping IR catalog."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kehrnel.contracts.mapping_ir import (
    MappingCatalog,
    MappingDefinition,
    MappingSelector,
    SelectorDialect,
)
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.source_profiles import (
    SourceProfileCatalogError,
    load_source_profile_catalog,
    source_profile_index,
)


DEFAULT_MAPPING_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "mappings" / "catalog.json"
)


class MappingCatalogError(ValueError):
    """The mapping catalog is malformed or fails integrity checks."""


def _declared_source_path(selector: MappingSelector) -> str:
    if selector.dialect is SelectorDialect.DOTTED_PATH:
        return selector.path
    if not selector.path.startswith("/"):
        return selector.path
    return ".".join(
        part.replace("~1", "/").replace("~0", "~")
        for part in selector.path[1:].split("/")
    )


def mapping_content_digest(mapping: MappingDefinition | dict[str, Any]) -> str:
    """Digest a mapping while excluding its self-referential digest value."""

    if isinstance(mapping, MappingDefinition):
        payload = mapping.to_wire()
    else:
        payload = deepcopy(mapping)
    artifact_ref = payload.get("artifactRef") or payload.get("artifact_ref")
    if not isinstance(artifact_ref, dict):
        raise MappingCatalogError("mapping is missing artifactRef")
    artifact_ref.pop("digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _assert_source_contracts(catalog: MappingCatalog) -> None:
    try:
        profiles = load_source_profile_catalog()
    except SourceProfileCatalogError as exc:
        raise MappingCatalogError(str(exc)) from exc
    profile_by_ref = source_profile_index(profiles)

    for mapping in catalog.mappings:
        source_ref = mapping.source.profile_ref
        key = (source_ref.id, source_ref.version, source_ref.digest)
        profile = profile_by_ref.get(key)
        if profile is None:
            raise MappingCatalogError(
                "mapping sourceProfileRef does not resolve exactly for "
                f"{mapping.artifact_ref.id}@{mapping.artifact_ref.version}"
            )

        known_interactions = {
            interaction.interaction_id for interaction in profile.interactions
        }
        missing_interactions = sorted(
            set(mapping.source.interaction_ids) - known_interactions
        )
        if missing_interactions:
            raise MappingCatalogError(
                f"mapping references unknown source interactions: {missing_interactions}"
            )

        structures = {structure.name: structure for structure in profile.structures}
        missing_structures = sorted(set(mapping.source.structures) - set(structures))
        if missing_structures:
            raise MappingCatalogError(
                f"mapping references unknown source structures: {missing_structures}"
            )

        declared_paths = {
            f"{structure.name}.{field.path}"
            for structure in profile.structures
            for field in structure.fields
        }
        used_paths = {
            _declared_source_path(rule.source)
            for rule in mapping.rules
            if rule.source is not None
        }
        used_paths.update(
            _declared_source_path(selector)
            for selector in mapping.execution.idempotency_selectors
        )
        used_paths.add(_declared_source_path(mapping.target.document_id_source))
        if mapping.target.subject_key_source is not None:
            used_paths.add(_declared_source_path(mapping.target.subject_key_source))
        if mapping.execution.ordering_selector is not None:
            used_paths.add(_declared_source_path(mapping.execution.ordering_selector))
        for join in mapping.source.joins:
            used_paths.add(f"{join.left_structure}.{join.left_path}")
            used_paths.add(f"{join.right_structure}.{join.right_path}")
        unknown_paths = sorted(used_paths - declared_paths)
        if unknown_paths:
            raise MappingCatalogError(
                f"mapping references undeclared source fields: {unknown_paths}"
            )


def load_mapping_catalog(
    path: str | Path = DEFAULT_MAPPING_CATALOG_PATH,
) -> MappingCatalog:
    catalog_path = Path(path)
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog = MappingCatalog.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise MappingCatalogError(
            f"invalid mapping catalog at {catalog_path}: {exc}"
        ) from exc

    for mapping in catalog.mappings:
        expected = mapping_content_digest(mapping)
        if mapping.artifact_ref.digest != expected:
            raise MappingCatalogError(
                "mapping digest mismatch for "
                f"{mapping.artifact_ref.id}@{mapping.artifact_ref.version}"
            )
    _assert_source_contracts(catalog)
    return catalog


def mapping_index(
    catalog: MappingCatalog,
) -> dict[tuple[str, str, str], MappingDefinition]:
    return {
        (
            mapping.artifact_ref.id,
            mapping.artifact_ref.version,
            mapping.artifact_ref.digest,
        ): mapping
        for mapping in catalog.mappings
    }
