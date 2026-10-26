"""Discover Reference-typed fields in FHIR JSON Schema definitions."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from .parser import FHIRSchemaParser

# Types that are not expanded when walking nested element definitions.
_SKIP_RECURSE_TYPES = frozenset({
    "Reference",
    "Extension",
    "Narrative",
    "Meta",
    "Element",
    "BackboneElement",
    "DataType",
    "Resource",
    "DomainResource",
})

_MAX_RECURSE_DEPTH = 12


@dataclass(frozen=True)
class SchemaReferenceField:
    """A Reference field on a resource type, including nested backbone paths."""

    path: str
    field_name: str
    is_array: bool


def collect_reference_paths(parser: FHIRSchemaParser, resource_type: str) -> list[SchemaReferenceField]:
    """
    Return all Reference fields for ``resource_type``, including nested backbone paths
    (e.g. ``participant.actor``, ``insurance.coverage``).
    """
    results: list[SchemaReferenceField] = []
    seen: set[tuple[str, str]] = set()

    def walk(type_name: str, prefix: str, stack: tuple[str, ...], depth: int) -> None:
        if depth > _MAX_RECURSE_DEPTH or type_name in stack:
            return
        if type_name not in parser._defs:  # noqa: SLF001 — schema introspection
            return

        stack = stack + (type_name,)
        res_def = parser.parse_definition(type_name)

        for fname, fdef in res_def.fields.items():
            if fname.startswith("_"):
                continue

            path = f"{prefix}.{fname}" if prefix else fname
            key = (resource_type, path)
            if key in seen:
                continue

            if fdef.ref == "Reference":
                seen.add(key)
                results.append(
                    SchemaReferenceField(
                        path=path,
                        field_name=fname,
                        is_array=fdef.is_array,
                    )
                )
                continue

            if fdef.ref and fdef.ref not in parser.PRIMITIVES and fdef.ref not in _SKIP_RECURSE_TYPES:
                if fdef.ref in parser._defs:  # noqa: SLF001
                    walk(fdef.ref, path, stack, depth + 1)

    walk(resource_type, "", (), 0)
    return sorted(results, key=lambda item: item.path)


@lru_cache(maxsize=4)
def _cached_parser(schema_path: str) -> FHIRSchemaParser:
    from pathlib import Path

    return FHIRSchemaParser(Path(schema_path))


def reference_catalog(schema_path: str) -> dict[str, list[SchemaReferenceField]]:
    """Map each resource type to its schema Reference field paths."""
    parser = _cached_parser(schema_path)
    return {
        resource_type: collect_reference_paths(parser, resource_type)
        for resource_type in parser.get_all_resources()
    }
