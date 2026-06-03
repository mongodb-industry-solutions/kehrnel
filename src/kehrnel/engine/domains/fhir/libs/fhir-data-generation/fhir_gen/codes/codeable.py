"""Build CodeableConcept / Coding values from healthcare_codes.yaml sections."""

from __future__ import annotations

import random
from typing import Any

from .loader import get_system, random_code


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
