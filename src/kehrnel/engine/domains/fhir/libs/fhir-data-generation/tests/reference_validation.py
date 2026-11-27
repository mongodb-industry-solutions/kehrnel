"""Helpers for schema-driven FHIR reference integrity tests."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from fhir_gen.resolvers.reference import ReferenceStore
from fhir_gen.schema.reference_paths import SchemaReferenceField, reference_catalog

PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "fhir_gen" / "schema"
V5_SCHEMA_PATH = PACKAGE_ROOT / "fhir.schema.v5.json"
V6_SCHEMA_PATH = PACKAGE_ROOT / "fhir.schema.v6.json"


@dataclass
class FoundReference:
    """A concrete Reference value found in a generated resource."""

    path: str
    reference: str
    resource_type: str | None


def iter_values_at_path(node: Any, path: str) -> Iterator[Any]:
    """Yield value(s) at a dotted path; ``[]`` in path means array elements."""
    parts = path.split(".") if path else []

    def descend(current: Any, index: int) -> Iterator[Any]:
        if current is None:
            return
        if index >= len(parts):
            yield current
            return
        part = parts[index]
        if isinstance(current, dict):
            child = current.get(part)
            if isinstance(child, list):
                for item in child:
                    yield from descend(item, index + 1)
            else:
                yield from descend(child, index + 1)
        elif isinstance(current, list):
            for item in current:
                yield from descend(item, index)

    yield from descend(node, 0)


def _is_fhir_reference_element(node: dict[str, Any]) -> bool:
    if "resourceType" in node:
        return False
    return isinstance(node.get("reference"), str)


def iter_reference_objects(node: Any, path_prefix: str = "") -> Iterator[tuple[str, dict[str, Any]]]:
    """Walk a resource tree and yield ``(path, reference_dict)`` for FHIR Reference elements."""
    if isinstance(node, dict):
        if _is_fhir_reference_element(node):
            yield (path_prefix or "reference", node)
        for key, value in node.items():
            if key == "reference":
                continue
            child_prefix = f"{path_prefix}.{key}" if path_prefix else key
            yield from iter_reference_objects(value, child_prefix)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            child_prefix = f"{path_prefix}[{index}]"
            yield from iter_reference_objects(item, child_prefix)


def find_references(resource: dict[str, Any]) -> list[FoundReference]:
    """Collect all ``Type/id`` references in a resource (excludes ``urn:`` literals)."""
    found: list[FoundReference] = []
    for path, ref_obj in iter_reference_objects(resource):
        ref_str = ref_obj.get("reference")
        if not isinstance(ref_str, str) or ref_str.startswith("urn:"):
            continue
        if "/" not in ref_str:
            continue
        rtype, _, rid = ref_str.partition("/")
        if not rtype or not rid:
            continue
        found.append(
            FoundReference(
                path=path,
                reference=ref_str,
                resource_type=ref_obj.get("type") or rtype,
            )
        )
    return found


def assert_reference_resolves(
    store: ReferenceStore,
    reference: str,
    *,
    msg_suffix: str = "",
) -> None:
    assert store.reference_is_valid(reference), f"Unresolved reference: {reference}{msg_suffix}"


def build_validation_store(
    session_store: ReferenceStore,
    resource: dict[str, Any] | None = None,
) -> ReferenceStore:
    """
    Build a store for validation including session resources and inline Bundle entries.
    """
    combined = ReferenceStore()
    for entry in session_store._store.values():  # noqa: SLF001 — test helper
        for item in entry:
            combined.register(item["resource"])
    if resource and resource.get("resourceType") == "Bundle":
        for bundle_entry in resource.get("entry", []):
            if not isinstance(bundle_entry, dict):
                continue
            nested = bundle_entry.get("resource")
            if isinstance(nested, dict) and nested.get("resourceType"):
                combined.register(nested)
    return combined


def assert_all_references_resolve(
    resource: dict[str, Any],
    store: ReferenceStore,
    *,
    session_store: ReferenceStore | None = None,
) -> None:
    """Every ``ResourceType/id`` reference in the document must exist in the store."""
    validation_store = store
    if session_store is not None:
        validation_store = build_validation_store(session_store, resource)
    elif resource.get("resourceType") == "Bundle":
        validation_store = build_validation_store(store, resource)

    for item in find_references(resource):
        assert_reference_resolves(validation_store, item.reference)


def assert_schema_paths_resolve(
    resource: dict[str, Any],
    store: ReferenceStore,
    schema_paths: list[SchemaReferenceField],
    *,
    session_store: ReferenceStore | None = None,
) -> None:
    """
    For each schema Reference path, every populated value must resolve in the store.
    Optional fields may be absent; present values must be valid.
    """
    validation_store = store
    if session_store is not None:
        validation_store = build_validation_store(session_store, resource)
    elif resource.get("resourceType") == "Bundle":
        validation_store = build_validation_store(store, resource)

    for field in schema_paths:
        for value in iter_values_at_path(resource, field.path):
            if not isinstance(value, dict):
                continue
            ref_str = value.get("reference")
            if not isinstance(ref_str, str) or ref_str.startswith("urn:"):
                continue
            if "/" in ref_str:
                assert_reference_resolves(
                    validation_store,
                    ref_str,
                    msg_suffix=f" at schema path {resource.get('resourceType')}.{field.path}",
                )


def catalog_for(schema_path: Path) -> dict[str, list[SchemaReferenceField]]:
    return reference_catalog(str(schema_path))


def resources_with_reference_fields(catalog: dict[str, list[SchemaReferenceField]]) -> list[str]:
    return sorted(rt for rt, paths in catalog.items() if paths)


def total_reference_paths(catalog: dict[str, list[SchemaReferenceField]]) -> int:
    return sum(len(paths) for paths in catalog.values())
