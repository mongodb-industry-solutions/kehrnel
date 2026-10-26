"""
Named generation scenarios for FHIR choice groups and clinical lifecycle variants.

Ensures the corpus includes explicit examples (e.g. Patient with deceasedBoolean true
and active false) rather than relying on low-probability random fills.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable

from ..resolvers.reference import ReferenceStore
from .special_types import SpecialTypeGenerator

PrepareFn = Callable[[Any], None]


@dataclass(frozen=True)
class GenerationScenario:
    """One reproducible generation scenario for a resource type."""

    id: str
    description: str
    forced_poly: dict[str, str] = field(default_factory=dict)
    extra_fields: dict[str, Any] = field(default_factory=dict)


def _clear_deceased(resource: dict[str, Any]) -> None:
    for key in (
        "deceasedBoolean",
        "deceasedDateTime",
        "_deceasedBoolean",
        "_deceasedDateTime",
    ):
        resource.pop(key, None)


def _clear_multiple_birth(resource: dict[str, Any]) -> None:
    for key in (
        "multipleBirthBoolean",
        "multipleBirthInteger",
        "_multipleBirthBoolean",
        "_multipleBirthInteger",
    ):
        resource.pop(key, None)


def _death_after_birth(birth_date: str, rng: random.Random) -> str:
    birth = datetime.strptime(birth_date[:10], "%Y-%m-%d")
    max_age_days = max(365, (datetime.now().date() - birth.date()).days - 1)
    age_days = rng.randint(365, min(max_age_days, 95 * 365))
    death = birth + timedelta(days=age_days)
    ms = rng.randint(0, 999)
    return death.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"


def apply_patient_scenario(
    resource: dict[str, Any],
    scenario_id: str,
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    """Apply Patient lifecycle / choice scenarios after base enrichment."""
    birth = resource.get("birthDate")
    if not birth:
        days_ago = rng.randint(5 * 365, 95 * 365)
        birth = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        resource["birthDate"] = birth

    if scenario_id == "alive_active":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        resource["active"] = True

    elif scenario_id == "alive_inactive":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        resource["active"] = False

    elif scenario_id == "deceased_boolean":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        resource["deceasedBoolean"] = True
        resource["active"] = False

    elif scenario_id == "deceased_datetime":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        resource["deceasedDateTime"] = _death_after_birth(birth, rng)
        resource["active"] = False

    elif scenario_id == "multiple_birth_boolean":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        resource["multipleBirthBoolean"] = True
        resource.setdefault("active", True)

    elif scenario_id == "multiple_birth_integer":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        resource["multipleBirthInteger"] = rng.randint(2, 4)
        resource.setdefault("active", True)

    elif scenario_id == "with_photo":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        resource.setdefault("active", True)
        resource["photo"] = [t.gen_Attachment()]

    elif scenario_id == "with_link":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        resource.setdefault("active", True)
        if store.has("Patient"):
            from .field_fill import backbone_filler_for

            link_fill = backbone_filler_for("Patient_Link")
            if link_fill:
                link = link_fill(t, store, rng)
                if link:
                    resource["link"] = [link]

    elif scenario_id == "with_communication":
        _clear_deceased(resource)
        _clear_multiple_birth(resource)
        from ..codes.loader import get_system, random_code

        lang = random_code("languages", rng)
        resource["communication"] = [{
            "language": t.gen_CodeableConcept(
                system=lang.get("system") if lang else get_system("languages"),
                code=lang["code"] if lang else "en",
                display=lang.get("display") if lang else "English",
            ),
            "preferred": True,
        }]

    else:
        apply_patient_scenario(resource, pick_patient_scenario(rng), t, store, rng)

    return resource


def pick_patient_scenario(rng: random.Random) -> str:
    return rng.choices(
        [
            "alive_active",
            "alive_inactive",
            "deceased_boolean",
            "deceased_datetime",
            "multiple_birth_boolean",
            "multiple_birth_integer",
            "with_photo",
            "with_link",
            "with_communication",
        ],
        weights=[55, 8, 10, 10, 4, 3, 4, 3, 3],
        k=1,
    )[0]


def apply_practitioner_scenario(
    resource: dict[str, Any],
    scenario_id: str,
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    _ = t, store
    if scenario_id == "active":
        _clear_deceased(resource)
        resource["active"] = True
    elif scenario_id == "inactive":
        _clear_deceased(resource)
        resource["active"] = False
    elif scenario_id == "deceased_boolean":
        _clear_deceased(resource)
        resource["deceasedBoolean"] = True
        resource["active"] = False
    elif scenario_id == "deceased_datetime":
        _clear_deceased(resource)
        resource["deceasedDateTime"] = t.p.gen_instant()
        resource["active"] = False
    else:
        apply_practitioner_scenario(resource, "active", t, store, rng)
    return resource


def pick_practitioner_scenario(rng: random.Random) -> str:
    return rng.choices(
        ["active", "inactive", "deceased_boolean", "deceased_datetime"],
        weights=[70, 10, 10, 10],
        k=1,
    )[0]


def apply_person_scenario(
    resource: dict[str, Any],
    scenario_id: str,
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    return apply_practitioner_scenario(resource, scenario_id, t, store, rng)


def pick_person_scenario(rng: random.Random) -> str:
    return pick_practitioner_scenario(rng)


_SCENARIO_HANDLERS: dict[str, Any] = {
    "Patient": (apply_patient_scenario, pick_patient_scenario),
    "Practitioner": (apply_practitioner_scenario, pick_practitioner_scenario),
    "Person": (apply_person_scenario, pick_person_scenario),
}

PATIENT_SCENARIOS: tuple[GenerationScenario, ...] = (
    GenerationScenario("alive_active", "Living patient; active account"),
    GenerationScenario("alive_inactive", "Living patient; inactive account"),
    GenerationScenario(
        "deceased_boolean",
        "Deceased (boolean flag); active false",
        forced_poly={"deceased": "deceasedBoolean"},
    ),
    GenerationScenario(
        "deceased_datetime",
        "Deceased (date/time of death); active false; death after birthDate",
        forced_poly={"deceased": "deceasedDateTime"},
    ),
    GenerationScenario(
        "multiple_birth_boolean",
        "Part of a multiple birth (boolean)",
    ),
    GenerationScenario(
        "multiple_birth_integer",
        "Part of a multiple birth (birth order integer)",
    ),
    GenerationScenario("with_photo", "Patient with profile photo attachment"),
    GenerationScenario("with_link", "Patient linked to another Patient record"),
    GenerationScenario("with_communication", "Preferred communication language"),
)

PRACTITIONER_SCENARIOS: tuple[GenerationScenario, ...] = (
    GenerationScenario("active", "Active practitioner"),
    GenerationScenario("inactive", "Inactive practitioner"),
    GenerationScenario(
        "deceased_boolean",
        "Deceased practitioner (boolean)",
        forced_poly={"deceased": "deceasedBoolean"},
    ),
    GenerationScenario(
        "deceased_datetime",
        "Deceased practitioner (dateTime)",
        forced_poly={"deceased": "deceasedDateTime"},
    ),
)

PERSON_SCENARIOS: tuple[GenerationScenario, ...] = PRACTITIONER_SCENARIOS

# Hand-crafted lifecycle / administrative scenarios (not inferred from schema).
NAMED_SCENARIO_CATALOG: dict[str, tuple[GenerationScenario, ...]] = {
    "Patient": PATIENT_SCENARIOS,
    "Practitioner": PRACTITIONER_SCENARIOS,
    "Person": PERSON_SCENARIOS,
}

# Backward-compatible alias
SCENARIO_CATALOG = NAMED_SCENARIO_CATALOG


def named_scenario_catalog(resource_type: str) -> tuple[GenerationScenario, ...]:
    """Lifecycle and administrative scenarios with enricher post-processing."""
    return NAMED_SCENARIO_CATALOG.get(resource_type, ())


def scenario_catalog(
    resource_type: str,
    *,
    include_poly_variants: bool = True,
    schema_registry=None,
) -> tuple[GenerationScenario, ...]:
    """
    Full scenario list for a resource type.

    Combines named lifecycle scenarios with schema polymorphic variants
    (deduplicated when a named scenario already forces the same choice).
    """
    named = list(named_scenario_catalog(resource_type))
    if not include_poly_variants:
        return tuple(named)

    if schema_registry is None:
        from ..schema.registry import registry as schema_registry
    from .poly_catalog import poly_variant_scenarios

    try:
        resource_def = schema_registry.definition(resource_type)
    except KeyError:
        return tuple(named)

    poly = list(poly_variant_scenarios(resource_def))
    if not named:
        return tuple(poly)

    covered_poly = {
        frozenset(s.forced_poly.items())
        for s in named
        if s.forced_poly
    }
    poly_filtered = [
        p for p in poly
        if not p.forced_poly or frozenset(p.forced_poly.items()) not in covered_poly
    ]
    return tuple(named + poly_filtered)


def scenario_for_index(
    resource_type: str,
    index: int,
    *,
    schema_registry=None,
) -> GenerationScenario | None:
    catalog = scenario_catalog(resource_type, schema_registry=schema_registry)
    if not catalog:
        return None
    return catalog[index % len(catalog)]


def scenario_by_id(
    resource_type: str,
    scenario_id: str,
    *,
    include_poly_variants: bool = True,
    schema_registry=None,
) -> GenerationScenario | None:
    """Look up a scenario by id (named or ``poly_*``)."""
    for entry in scenario_catalog(
        resource_type,
        include_poly_variants=include_poly_variants,
        schema_registry=schema_registry,
    ):
        if entry.id == scenario_id:
            return entry
    return None


def apply_scenario(
    resource: dict[str, Any],
    resource_type: str,
    scenario_id: str | None,
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    """Apply a named scenario, or a weighted default for supported resource types."""
    handler = _SCENARIO_HANDLERS.get(resource_type)
    if not handler:
        return resource
    apply_fn, pick_fn = handler
    sid = scenario_id or pick_fn(rng)
    return apply_fn(resource, sid, t, store, rng)


def prepare_scenario_deps(
    gen: Any,
    resource_type: str,
    scenario: GenerationScenario,
) -> None:
    """Pre-generate dependencies required by a scenario."""
    if resource_type == "Patient" and scenario.id == "with_link":
        if not gen.store.has("Patient"):
            gen._generate_one("Patient", register=True, enrich=True, scenario="alive_active")
    if resource_type == "Patient" and scenario.id in (
        "deceased_boolean",
        "deceased_datetime",
        "with_link",
    ):
        if not gen.store.has("Organization"):
            gen._generate_one("Organization", register=True)
