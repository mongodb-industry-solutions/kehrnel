"""Catalog of all schema fields per FHIR resource (including nested backbone paths)."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from .parser import FHIRSchemaParser, FieldDef

_SKIP_RECURSE = frozenset({
    "Extension",
    "Narrative",
    "Meta",
    "Element",
    "BackboneElement",
    "DataType",
    "Resource",
    "DomainResource",
})

_MAX_DEPTH = 14


@dataclass(frozen=True)
class SchemaFieldSpec:
    """One field on a resource type (dot path from resource root)."""

    path: str
    field_name: str
    ref: str | None
    is_array: bool
    is_required: bool
    description: str


def collect_field_specs(parser: FHIRSchemaParser, resource_type: str) -> list[SchemaFieldSpec]:
    """Return flattened field specs for a resource, excluding extension noise."""
    results: list[SchemaFieldSpec] = []
    seen: set[tuple[str, str]] = set()

    def walk(type_name: str, prefix: str, stack: tuple[str, ...], depth: int) -> None:
        if depth > _MAX_DEPTH or type_name in stack:
            return
        if type_name not in parser._defs:  # noqa: SLF001
            return

        stack = stack + (type_name,)
        res_def = parser.parse_definition(type_name)

        for fname, fdef in res_def.fields.items():
            if fname.startswith("_") or fname in ("extension", "modifierExtension"):
                continue

            path = f"{prefix}.{fname}" if prefix else fname
            key = (resource_type, path)
            if key in seen:
                continue
            seen.add(key)

            is_req = fname in res_def.required
            results.append(
                SchemaFieldSpec(
                    path=path,
                    field_name=fname,
                    ref=fdef.ref,
                    is_array=fdef.is_array,
                    is_required=is_req,
                    description=fdef.description or "",
                )
            )

            if (
                fdef.ref
                and fdef.ref not in parser.PRIMITIVES
                and fdef.ref not in _SKIP_RECURSE
                and fdef.ref != "Reference"
                and fdef.ref in parser._defs  # noqa: SLF001
            ):
                walk(fdef.ref, path, stack, depth + 1)

    walk(resource_type, "", (), 0)
    return sorted(results, key=lambda s: s.path)


@lru_cache(maxsize=4)
def _cached_parser(schema_path: str) -> FHIRSchemaParser:
    from pathlib import Path

    return FHIRSchemaParser(Path(schema_path))


def field_catalog(schema_path: str) -> dict[str, list[SchemaFieldSpec]]:
    parser = _cached_parser(schema_path)
    return {
        rt: collect_field_specs(parser, rt)
        for rt in parser.get_all_resources()
    }
