"""Schema-derived polymorphic (choice) variant scenarios from FHIR JSON Schema."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from ..schema.parser import FHIRSchemaParser, ResourceDef
from ..schema.registry import SchemaRegistry, registry
from .scenarios import GenerationScenario


def _poly_scenario_id(base: str, variant_key: str) -> str:
    """Stable id: ``poly_value_valueQuantity`` from base ``value`` + key ``valueQuantity``."""
    return f"poly_{base}_{variant_key}"


def poly_variant_scenarios(resource_def: ResourceDef) -> tuple[GenerationScenario, ...]:
    """One scenario per polymorphic choice (forces a single branch per choice group)."""
    entries: list[GenerationScenario] = []
    for base, keys in sorted(resource_def.poly_groups.items()):
        for key in keys:
            entries.append(
                GenerationScenario(
                    id=_poly_scenario_id(base, key),
                    description=f"Choice group «{base}» → {key}",
                    forced_poly={base: key},
                )
            )
    return tuple(entries)


def resources_with_poly_groups(parser: FHIRSchemaParser | None = None) -> list[str]:
    p = parser or _parser_from_registry()
    result: list[str] = []
    for rt in p.get_all_resources():
        if p.parse_definition(rt).poly_groups:
            result.append(rt)
    return sorted(result)


def poly_groups_for(resource_type: str, parser: FHIRSchemaParser | None = None) -> dict[str, list[str]]:
    p = parser or _parser_from_registry()
    return dict(p.parse_definition(resource_type).poly_groups)


@lru_cache(maxsize=1)
def _parser_from_registry() -> FHIRSchemaParser:
    return SchemaRegistry.get().parser()


def poly_coverage_summary() -> list[dict[str, object]]:
    """Summary row per resource that has polymorphic choice groups."""
    p = _parser_from_registry()
    rows: list[dict[str, object]] = []
    for rt in resources_with_poly_groups(p):
        groups = poly_groups_for(rt, p)
        variant_count = sum(len(v) for v in groups.values())
        rows.append({
            "resource_type": rt,
            "group_count": len(groups),
            "variant_count": variant_count,
            "groups": groups,
        })
    return rows
