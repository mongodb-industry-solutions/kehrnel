"""Load FHIR terminology codes from fhir_gen/hl7_codes/healthcare_codes.yaml."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import yaml

from ..config import settings


@lru_cache(maxsize=1)
def load_codes() -> dict[str, Any]:
    path = settings.codes_path
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def reload_codes() -> dict[str, Any]:
    """Clear cached YAML after file changes (tests or enrichment)."""
    load_codes.cache_clear()
    return load_codes()


def get_codes(section: str) -> list[dict[str, Any]]:
    data = load_codes()
    section_data = data.get(section, {})
    if isinstance(section_data, list):
        return [_normalize_code_entry(c) for c in section_data]
    raw = section_data.get("codes", []) if isinstance(section_data, dict) else []
    return [_normalize_code_entry(c) for c in raw]


def get_system(section: str) -> str | None:
    data = load_codes()
    section_data = data.get(section, {})
    if isinstance(section_data, dict):
        return section_data.get("system")
    return None


def random_code(section: str, rng) -> dict[str, Any] | None:
    codes = get_codes(section)
    return rng.choice(codes) if codes else None


def list_sections() -> list[str]:
    """Return sorted top-level YAML section keys."""
    return sorted(load_codes().keys())


def _normalize_code_entry(entry: Any) -> dict[str, Any]:
    if isinstance(entry, dict):
        return entry
    if isinstance(entry, str):
        return {"code": entry, "display": entry}
    return {"code": str(entry), "display": str(entry)}
