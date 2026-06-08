"""Build CodeableConcept / Coding values from healthcare_codes.yaml sections."""

from __future__ import annotations

import random
from typing import Any

from .loader import get_system, random_code


def pick_code(section: str, rng: random.Random, default: str | None = None) -> str:
    """Return a code string from a YAML section (for top-level FHIR code fields)."""
    entry = random_code(section, rng)
    if entry and entry.get("code") is not None:
        return str(entry["code"])
    if default is not None:
        return default
    return ""


def concept_from_section(
    section: str,
    rng: random.Random,
    generator: Any,
) -> dict[str, Any]:
    """CodeableConcept via SpecialTypeGenerator using terminology from ``section``."""
    entry = random_code(section, rng)
    sys = get_system(section)
    if not entry or not sys:
        return generator.gen_CodeableConcept(code="unknown")
    return generator.gen_CodeableConcept(
        system=sys,
        code=entry["code"],
        display=entry.get("display") or entry["code"],
    )


def codeable_reference_from_section(
    section: str,
    rng: random.Random,
    generator: Any,
    *,
    reference: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """CodeableReference with concept arm from ``section`` and optional reference arm."""
    entry = random_code(section, rng)
    sys = get_system(section)
    kwargs: dict[str, Any] = {}
    if entry and sys:
        kwargs["system"] = sys
        kwargs["code"] = entry["code"]
        kwargs["display"] = entry.get("display") or entry["code"]
    out = generator.gen_CodeableReference(**kwargs)
    if reference is not None:
        out["reference"] = reference
    return out


def codeable_from_section(
    section: str,
    rng: random.Random,
    *,
    system: str | None = None,
    text: str | None = None,
) -> dict[str, Any]:
    """Return a CodeableConcept using a random code from ``section``."""
    entry = random_code(section, rng)
    sys = system or get_system(section)
    if not entry or not sys:
        out: dict[str, Any] = {}
        if text:
            out["text"] = text
        return out
    display = entry.get("display") or entry["code"]
    return {
        "coding": [{
            "system": sys,
            "code": entry["code"],
            "display": display,
        }],
        "text": text or display,
    }


def coding_from_section(
    section: str,
    rng: random.Random,
    *,
    system: str | None = None,
) -> dict[str, Any]:
    """Return a single Coding from ``section``."""
    entry = random_code(section, rng)
    sys = system or get_system(section)
    if not entry or not sys:
        return {}
    display = entry.get("display") or entry["code"]
    return {
        "system": sys,
        "code": entry["code"],
        "display": display,
    }
