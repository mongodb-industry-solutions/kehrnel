"""Validate FHIR Coding.system URLs and codes against healthcare_codes.yaml."""

from __future__ import annotations

import importlib.util
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlparse

from .loader import get_codes, get_system, list_sections, load_codes, reload_codes

# HL7 canonical CodeSystem URLs (not ValueSet URLs). condition-ver-status is correct per
# https://terminology.hl7.org/CodeSystem/condition-ver-status
CONDITION_VERIFICATION_STATUS_SYSTEM = (
    "http://terminology.hl7.org/CodeSystem/condition-ver-status"
)
CONDITION_CLINICAL_STATUS_SYSTEM = (
    "http://terminology.hl7.org/CodeSystem/condition-clinical"
)

_VALUESET_IN_SYSTEM_RE = re.compile(r"/ValueSet/", re.I)
_RELATIVE_SYSTEM_RE = re.compile(r"^CodeSystem/", re.I)

_SNOMED = "http://snomed.info/sct"
_LOINC = "http://loinc.org"
_RXNORM = "http://www.nlm.nih.gov/research/umls/rxnorm"
_CVX = "http://hl7.org/fhir/sid/cvx"

# Large or extensible terminologies: validate URI + code shape, not full enumeration.
_PATTERN_ONLY_SYSTEMS = frozenset({
    _SNOMED,
    _LOINC,
    _RXNORM,
    _CVX,
    "http://terminology.hl7.org/CodeSystem/v3-ActCode",
    "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
    "http://terminology.hl7.org/CodeSystem/audit-event-type",
    "http://terminology.hl7.org/CodeSystem/audit-event-sub-type",
    "http://terminology.hl7.org/CodeSystem/audit-source-type",
    "http://terminology.hl7.org/CodeSystem/object-role",
    "http://hl7.org/fhir/network-type",
    "http://ihe.net/fhir/ihe.formatcode.fhir/CodeSystem/formatcode",
    "http://terminology.hl7.org/CodeSystem/flag-category",
})


def _builder_path() -> Path:
    return Path(__file__).resolve().parents[1] / "hl7_codes" / "_build_healthcare_codes.py"


@lru_cache(maxsize=1)
def canonical_systems_from_builder() -> dict[str, str]:
    """Section key -> canonical system URL from fhir_gen/hl7_codes/_build_healthcare_codes.py."""
    path = _builder_path()
    if not path.exists():
        return {}
    spec = importlib.util.spec_from_file_location("healthcare_codes_builder", path)
    if spec is None or spec.loader is None:
        return {}
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sections = getattr(mod, "NEW_SECTIONS", {})
    return {
        key: value["system"]
        for key, value in sections.items()
        if isinstance(value, dict) and value.get("system")
    }


@lru_cache(maxsize=1)
def terminology_index() -> dict[str, set[str]]:
    """Map CodeSystem URL -> allowed codes from all YAML sections."""
    index: dict[str, set[str]] = {}
    for section in list_sections():
        system = get_system(section)
        if not system:
            continue
        codes = {c["code"] for c in get_codes(section) if c.get("code")}
        if not codes:
            continue
        index.setdefault(system, set()).update(codes)
    return index


def clear_terminology_cache() -> None:
    reload_codes()
    canonical_systems_from_builder.cache_clear()
    terminology_index.cache_clear()


def is_absolute_uri(uri: str) -> bool:
    parsed = urlparse(uri)
    return parsed.scheme in ("http", "https", "urn") and bool(parsed.netloc or parsed.scheme == "urn")


def validate_system_url(system: str | None) -> list[str]:
    if not system:
        return ["Coding.system is missing"]
    errors: list[str] = []
    if _RELATIVE_SYSTEM_RE.match(system.strip()):
        errors.append(f"Coding.system must be absolute, not relative: {system!r}")
    if _VALUESET_IN_SYSTEM_RE.search(system):
        errors.append(f"Coding.system must be a CodeSystem URL, not a ValueSet: {system!r}")
    if not is_absolute_uri(system):
        errors.append(f"Coding.system is not a valid absolute URI: {system!r}")
    return errors


def code_matches_system_pattern(system: str, code: str) -> bool:
    if system == _SNOMED:
        return code.isdigit()
    if system == _LOINC:
        return bool(re.match(r"^[\d.\-]+$", code))
    if system == _RXNORM:
        return code.isdigit()
    if system == _CVX:
        return code.isdigit()
    return True


def validate_coding(
    coding: dict[str, Any],
    *,
    path: str = "",
    strict_registered: bool = True,
) -> list[str]:
    """Validate one Coding element; return human-readable error strings."""
    prefix = f"{path}: " if path else ""
    errors: list[str] = []
    system = coding.get("system")
    code = coding.get("code")
    errors.extend(f"{prefix}{e}" for e in validate_system_url(system))
    if not code:
        errors.append(f"{prefix}Coding.code is missing")
        return errors
    if system and not code_matches_system_pattern(system, str(code)):
        errors.append(
            f"{prefix}code {code!r} does not match expected pattern for system {system!r}"
        )
    if system and strict_registered and system not in _PATTERN_ONLY_SYSTEMS:
        registered = terminology_index().get(system)
        if registered is not None and str(code) not in registered:
            errors.append(
                f"{prefix}code {code!r} is not in registered terminology for {system!r}"
            )
    return errors


def iter_codings(
    node: Any,
    path: str = "",
) -> Iterator[tuple[str, dict[str, Any]]]:
    """Walk a resource tree and yield (path, coding_dict) for each Coding."""
    if isinstance(node, dict):
        if "system" in node and "code" in node:
            yield path or "coding", node
            return
        for key, value in node.items():
            child = f"{path}.{key}" if path else key
            yield from iter_codings(value, child)
    elif isinstance(node, list):
        for idx, item in enumerate(node):
            yield from iter_codings(item, f"{path}[{idx}]")


def validate_resource_codings(
    resource: dict[str, Any],
    *,
    strict_registered: bool = True,
) -> list[str]:
    errors: list[str] = []
    for path, coding in iter_codings(resource):
        errors.extend(
            validate_coding(coding, path=path, strict_registered=strict_registered)
        )
    return errors


def validate_yaml_section(section: str) -> list[str]:
    """Validate one healthcare_codes.yaml section structure and canonical system."""
    errors: list[str] = []
    system = get_system(section)
    if not system:
        errors.append(f"section {section!r} has no system")
        return errors
    errors.extend(f"[{section}] {e}" for e in validate_system_url(system))
    canonical = canonical_systems_from_builder()
    expected = canonical.get(section)
    if expected and system != expected:
        errors.append(
            f"[{section}] system {system!r} != canonical {expected!r}"
        )
    codes = get_codes(section)
    if not codes:
        errors.append(f"[{section}] has no codes")
    seen: set[str] = set()
    for entry in codes:
        code = entry.get("code")
        if not code:
            errors.append(f"[{section}] entry missing code")
            continue
        if code in seen:
            errors.append(f"[{section}] duplicate code {code!r}")
        seen.add(code)
        if not code_matches_system_pattern(system, str(code)):
            errors.append(f"[{section}] code {code!r} invalid for system {system!r}")
    return errors


def validate_all_yaml_sections() -> dict[str, list[str]]:
    return {section: validate_yaml_section(section) for section in list_sections()}
