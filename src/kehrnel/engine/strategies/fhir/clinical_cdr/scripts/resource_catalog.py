"""Package-backed FHIR resource catalog for Healthcare Data Lab.

The catalog is deliberately read-only and never consults MongoDB.  It joins the
FHIR JSON schema/resource index with the active fhir-mql configuration and the
Clinical CDR recipe scope, so the UI describes the same resources that the
runtime can store, search, generate, and index.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr._paths import FHIR_GEN_ROOT, FHIR_MQL_ROOT
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import bridge
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.document_contract import (
    STORED_DOCUMENT_SCHEMA_VERSION,
    build_projection_versions,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.query import (
    build_search_converter,
    fhir_list_search_params,
)

CATALOG_CONTRACT_VERSION = "fhir-resource-catalog.v1"
_SUPPORTED_RELEASES = frozenset({"R5", "R6"})


def _release(value: Any) -> str:
    normalized = str(value or "R5").strip().upper()
    if normalized not in _SUPPORTED_RELEASES:
        raise KehrnelError(
            code="FHIR_RELEASE_UNSUPPORTED",
            status=400,
            message=f"FHIR resource catalog is not available for {normalized!r}",
            details={"supported_releases": sorted(_SUPPORTED_RELEASES)},
        )
    return normalized


@lru_cache(maxsize=2)
def _schema(release: str) -> dict[str, Any]:
    suffix = "v5" if release == "R5" else "v6"
    path = Path(FHIR_GEN_ROOT) / "fhir_gen" / "schema" / f"fhir.schema.{suffix}.json"
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=2)
def _resource_index(release: str) -> dict[str, Any]:
    """Load the precomputed schema index shipped with the fhir-mql library."""
    suffix = "r5" if release == "R5" else "r6"
    path = Path(FHIR_MQL_ROOT) / "schema" / "indexes" / f"resources.{suffix}.json"
    if not path.is_file():
        raise KehrnelError(
            code="FHIR_RESOURCE_INDEX_UNAVAILABLE",
            status=500,
            message=f"Bundled {release} FHIR resource index is unavailable",
            details={"expected_file": path.name},
        )
    return json.loads(path.read_text(encoding="utf-8"))


def _property_type(prop: dict[str, Any]) -> str:
    candidate = prop
    if prop.get("type") == "array" and isinstance(prop.get("items"), dict):
        candidate = prop["items"]
    ref = candidate.get("$ref")
    if isinstance(ref, str) and ref:
        return ref.rsplit("/", 1)[-1]
    for option in candidate.get("allOf") or []:
        if isinstance(option, dict) and isinstance(option.get("$ref"), str):
            return option["$ref"].rsplit("/", 1)[-1]
    value_type = candidate.get("type")
    if isinstance(value_type, str):
        return value_type
    if "const" in candidate:
        return "code"
    return "Element"


def _fields_for_definition(
    definition_name: str,
    definitions: dict[str, Any],
    *,
    path: str,
    depth: int,
    seen: frozenset[str],
) -> list[dict[str, Any]]:
    raw = definitions.get(definition_name) or {}
    properties = raw.get("properties") or {}
    required = set(raw.get("required") or [])
    rows: list[dict[str, Any]] = []

    for name, prop in properties.items():
        # FHIR primitive-extension companions are represented on their primitive
        # field rather than as duplicate rows. Mongo operational fields are not
        # part of this schema and are described separately by the catalog.
        if name.startswith("_"):
            continue
        prop = prop if isinstance(prop, dict) else {}
        is_array = prop.get("type") == "array" or "items" in prop
        field_type = _property_type(prop)
        row: dict[str, Any] = {
            "name": name,
            "path": f"{path}.{name}",
            "type": field_type,
            "array": is_array,
            "required": name in required,
            "cardinality": f"{'1' if name in required else '0'}..{'*' if is_array else '1'}",
            "description": str(prop.get("description") or ""),
        }
        if f"_{name}" in properties:
            row["primitive_extension"] = f"_{name}"

        can_expand = (
            depth > 0
            and field_type in definitions
            and field_type not in seen
            and field_type not in {"ResourceList"}
        )
        if can_expand:
            children = _fields_for_definition(
                field_type,
                definitions,
                path=row["path"],
                depth=depth - 1,
                seen=seen | {field_type},
            )
            if children:
                row["children"] = children
        rows.append(row)
    return rows


def _normalize_index(index: Any) -> dict[str, Any] | None:
    if not isinstance(index, dict):
        return None
    fields = index.get("fields")
    normalized_fields: list[dict[str, Any]] = []
    if isinstance(fields, dict):
        normalized_fields = [{"path": str(name), "direction": direction} for name, direction in fields.items()]
    elif isinstance(fields, list):
        for item in fields:
            if isinstance(item, dict):
                normalized_fields.extend(
                    {"path": str(name), "direction": direction} for name, direction in item.items()
                )
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                normalized_fields.append({"path": str(item[0]), "direction": item[1]})
    options = index.get("options") if isinstance(index.get("options"), dict) else {}
    return {
        "name": options.get("name") or index.get("name") or "unnamed_index",
        "fields": normalized_fields,
        "unique": bool(options.get("unique", index.get("unique", False))),
        "sparse": bool(options.get("sparse", index.get("sparse", False))),
    }


def _normalize_projection(name: str, config: Any) -> dict[str, Any]:
    config = config if isinstance(config, dict) else {}
    mappings = config.get("field_mappings") if isinstance(config.get("field_mappings"), list) else []
    return {
        "name": name,
        "source": config.get("source"),
        "target": config.get("target"),
        "extractor": config.get("extractor"),
        "projected_fields": sorted(
            {
                str(mapping.get("target_field"))
                for mapping in mappings
                if isinstance(mapping, dict) and mapping.get("target_field")
            }
        ),
    }


def _catalog_context(ctx: StrategyContext) -> dict[str, Any]:
    cfg = bridge.resolve_strategy_config(ctx)
    release = _release(cfg.get("schema_version"))
    converter = build_search_converter(ctx)
    searchable = set(bridge.supported_search_resource_types(converter.config_loader))
    configured = bridge.configured_cdr_resource_types(cfg)
    schema_types = set((_resource_index(release).get("resources") or {}).keys())
    generatable = configured & bridge.known_generation_resource_types() & schema_types
    storable = configured & searchable & schema_types
    search_cfg = cfg.get("search") if isinstance(cfg.get("search"), dict) else {}
    compartment_dir = (
        search_cfg.get("compartment_definitions_dir")
        or bridge._bundled_compartment_definitions_dir()
    )
    versions = build_projection_versions(
        converter.config_loader,
        fhir_release=release,
        compartment_definitions_dir=compartment_dir,
        resource_types=storable,
    )
    return {
        "cfg": cfg,
        "release": release,
        "converter": converter,
        "configured": configured,
        "searchable": searchable,
        "generatable": generatable,
        "storable": storable,
        "schema_types": schema_types,
        "versions": versions,
    }


def _generation_recipes(state: dict[str, Any]) -> list[dict[str, Any]]:
    recipes = ((state["cfg"].get("generation") or {}).get("recipes") or {})
    if not isinstance(recipes, dict):
        return []

    result: list[dict[str, Any]] = []
    for name, value in sorted(recipes.items()):
        recipe = value if isinstance(value, dict) else {}
        configured_resources = recipe.get("resources") or {}
        if not isinstance(configured_resources, dict):
            configured_resources = {}
        supported = {
            str(resource_type): int(count)
            for resource_type, count in configured_resources.items()
            if str(resource_type) in state["generatable"]
        }
        omitted = sorted(set(map(str, configured_resources)) - set(supported))
        result.append(
            {
                "name": str(name),
                "description": str(recipe.get("description") or ""),
                "resource_count": len(supported),
                "document_count": sum(supported.values()),
                "resources": supported,
                "omitted_resource_types": omitted,
            }
        )
    return result


def _summary(resource_type: str, *, state: dict[str, Any]) -> dict[str, Any]:
    release = state["release"]
    indexed = (_resource_index(release).get("resources") or {}).get(resource_type) or {}
    resource_cfg: dict[str, Any] = {}
    if resource_type in state["searchable"]:
        resource_cfg = state["converter"].config_loader.get_config(resource_type)
    return {
        "resource_type": resource_type,
        "description": indexed.get("description") or "",
        "collection": bridge.collection_name(
            str(state["cfg"].get("collection_prefix") or ""), resource_type
        ),
        "field_count": len(indexed.get("fields") or []),
        "required_field_count": len(indexed.get("required") or []),
        "polymorphic_group_count": len(indexed.get("polymorphic") or {}),
        "backbone_element_count": len(indexed.get("backbone_elements") or []),
        "search_parameter_count": len(
            resource_cfg.get("search_parameters") or resource_cfg.get("parameters") or {}
        ),
        "index_count": len(resource_cfg.get("indexes") or []),
        "capabilities": {
            "storable": resource_type in state["storable"],
            "searchable": resource_type in state["searchable"],
            "generatable": resource_type in state["generatable"],
        },
        "resource_projection_version": state["versions"].for_resource(resource_type),
    }


def fhir_resource_catalog(
    ctx: StrategyContext, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Return catalog summaries or one detailed resource definition."""
    payload = payload or {}
    state = _catalog_context(ctx)
    release = state["release"]
    resource_type = str(payload.get("resource_type") or "").strip()

    if not resource_type:
        resource_types = sorted(state["storable"])
        omitted_configured = sorted(state["configured"] - state["storable"])
        return {
            "ok": True,
            "contract_version": CATALOG_CONTRACT_VERSION,
            "source": "kehrnel.fhir_packages",
            "database_backed": False,
            "fhir_version": release,
            "database": state["cfg"].get("database"),
            "collection_prefix": state["cfg"].get("collection_prefix") or "",
            "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
            "projection_contract_version": state["versions"].projection_contract_version,
            "resource_count": len(resource_types),
            "configured_resource_count": len(state["configured"]),
            "schema_resource_count": len((_resource_index(release).get("resources") or {})),
            "omitted_configured_resource_types": omitted_configured,
            "generation_recipes": _generation_recipes(state),
            "resources": [_summary(name, state=state) for name in resource_types],
        }

    if resource_type not in state["storable"]:
        raise KehrnelError(
            code="FHIR_RESOURCE_NOT_IN_STRATEGY_SCOPE",
            status=404,
            message=f"{resource_type} is not in the active Clinical CDR resource scope",
            details={"resource_type": resource_type, "fhir_version": release},
        )

    indexed = (_resource_index(release).get("resources") or {}).get(resource_type)
    definitions = _schema(release).get("definitions") or {}
    if not isinstance(indexed, dict) or resource_type not in definitions:
        raise KehrnelError(
            code="FHIR_RESOURCE_DEFINITION_UNAVAILABLE",
            status=500,
            message=f"Bundled definition for {resource_type} is unavailable",
            details={"resource_type": resource_type, "fhir_version": release},
        )

    resource_cfg = state["converter"].config_loader.get_config(resource_type)
    search = fhir_list_search_params(ctx, {"resource_type": resource_type})
    indexes = [
        normalized
        for value in (resource_cfg.get("indexes") or [])
        if (normalized := _normalize_index(value)) is not None
    ]
    projections = [
        _normalize_projection(name, value)
        for name, value in sorted((resource_cfg.get("denormalization") or {}).items())
    ]
    polymorphic = [
        {"name": f"{name}[x]", "variants": variants}
        for name, variants in sorted((indexed.get("polymorphic") or {}).items())
    ]
    fields = _fields_for_definition(
        resource_type,
        definitions,
        path=resource_type,
        depth=1,
        seen=frozenset({resource_type}),
    )

    return {
        "ok": True,
        "contract_version": CATALOG_CONTRACT_VERSION,
        "source": "kehrnel.fhir_packages",
        "database_backed": False,
        "fhir_version": release,
        "storage_schema_version": STORED_DOCUMENT_SCHEMA_VERSION,
        "projection_contract_version": state["versions"].projection_contract_version,
        "resource_projection_version": state["versions"].for_resource(resource_type),
        "resource": {
            **_summary(resource_type, state=state),
            "structure": {
                "root": resource_type,
                "fields": fields,
                "required": indexed.get("required") or [],
                "polymorphic": polymorphic,
                "backbone_elements": indexed.get("backbone_elements") or [],
            },
            "storage": {
                "database": state["cfg"].get("database"),
                "collection": bridge.collection_name(
                    str(state["cfg"].get("collection_prefix") or ""), resource_type
                ),
                "identity": ["resourceType", "id"],
                "canonical_location": "document root",
                "operational_fields": [
                    "_search",
                    "_compartments",
                    "_kehrnel",
                ],
                "indexes": indexes,
            },
            "search": {
                "parameter_count": search["parameter_count"],
                "parameters": search["parameters"],
                "projections": projections,
            },
        },
    }
