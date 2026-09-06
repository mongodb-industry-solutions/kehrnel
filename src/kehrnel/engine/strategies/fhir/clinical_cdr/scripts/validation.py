"""Bounded validation for the FHIR migration boundary.

The levels are intentionally explicit:
- ``structure`` checks safe JSON/FHIR routing invariants.
- ``base`` validates against the bundled R5 or R6 JSON Schema selected by the
  strategy activation.

R4 is a deliberately small structural-validation baseline until its generated
release schema is delivered.  Capability discovery labels that distinction.

Profile and implementation-guide validation are deliberately outside this
strategy until they are implemented as complete, separately tested packages.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from kehrnel.engine.strategies.fhir.clinical_cdr._paths import FHIR_GEN_ROOT
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.serialization import (
    OPERATIONAL_FIELDS,
    canonical_resource,
)
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts.release_support import (
    R4_MINIMAL_RESOURCE_TYPES,
    SCHEMA_BACKED_RELEASES,
    normalize_release,
)

FHIR_ID = re.compile(r"^[A-Za-z0-9\-.]{1,64}$")
RESOURCE_TYPE = re.compile(r"^[A-Z][A-Za-z0-9]{0,63}$")
SUPPORTED_LEVELS = frozenset({"structure", "base"})


def _finding(
    *,
    index: int,
    code: str,
    message: str,
    severity: str = "error",
    resource_type: str | None = None,
    resource_id: str | None = None,
    path: str | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "index": index,
        "severity": severity,
        "code": code,
        "message": message,
    }
    if resource_type:
        item["resource_type"] = resource_type
    if resource_id:
        item["resource_id"] = resource_id
    if path:
        item["path"] = path
    return item


def _unsafe_key_path(value: Any, path: str = "$") -> str | None:
    """Reject keys MongoDB cannot safely store; valid FHIR JSON never uses them."""
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str) or key.startswith("$") or "." in key:
                return f"{path}.{key}"
            found = _unsafe_key_path(child, f"{path}.{key}")
            if found:
                return found
    elif isinstance(value, list):
        for position, child in enumerate(value):
            found = _unsafe_key_path(child, f"{path}[{position}]")
            if found:
                return found
    return None


@lru_cache(maxsize=2)
def _base_schema(release: str) -> dict[str, Any]:
    normalized = normalize_release(release)
    if normalized not in SCHEMA_BACKED_RELEASES:
        raise ValueError(f"Base JSON Schema validation is not bundled for {normalized}")
    suffix = "v5" if normalized == "R5" else "v6"
    path = Path(FHIR_GEN_ROOT) / "fhir_gen" / "schema" / f"fhir.schema.{suffix}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def schema_resource_types(release: str) -> frozenset[str]:
    """Resource definitions available in the bundled schema for a release."""
    normalized = _normalized_release(release)
    if normalized == "R4":
        return R4_MINIMAL_RESOURCE_TYPES
    definitions = _base_schema(normalized).get("definitions") or {}
    return frozenset(
        name
        for name, definition in definitions.items()
        if isinstance(definition, dict)
        and isinstance(definition.get("properties", {}).get("resourceType"), dict)
        and definition["properties"]["resourceType"].get("const") is not None
    )


def _normalized_release(release: str) -> str:
    return normalize_release(release)


@lru_cache(maxsize=512)
def _base_validator(release: str, resource_type: str):
    from jsonschema import Draft6Validator

    schema = _base_schema(release)
    if resource_type not in schema.get("definitions", {}):
        return None
    return Draft6Validator(
        {
            "$schema": schema.get("$schema"),
            "$ref": f"#/definitions/{resource_type}",
            "definitions": schema["definitions"],
        }
    )


def validate_resource(
    resource: Any,
    *,
    index: int,
    level: str,
    release: str,
    supported_resource_types: set[str] | frozenset[str] | None = None,
    max_schema_findings: int = 20,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    findings: list[dict[str, Any]] = []
    if not isinstance(resource, dict):
        return None, [
            _finding(
                index=index,
                code="FHIR_RESOURCE_NOT_OBJECT",
                message="FHIR resource must be a JSON object",
            )
        ]

    client_operational = sorted(OPERATIONAL_FIELDS.intersection(resource.keys()))
    resource = canonical_resource(resource)
    resource_type = resource.get("resourceType")
    resource_id = resource.get("id")
    if not isinstance(resource_type, str) or not RESOURCE_TYPE.fullmatch(resource_type):
        findings.append(
            _finding(
                index=index,
                code="FHIR_RESOURCE_TYPE_INVALID",
                message="resourceType is required and must be a FHIR type name",
            )
        )
    elif (
        supported_resource_types is not None
        and resource_type not in supported_resource_types
    ):
        findings.append(
            _finding(
                index=index,
                code="FHIR_RESOURCE_TYPE_UNSUPPORTED",
                message=f"{resource_type} is not supported by the bundled fhir-mql resource configuration",
                resource_type=resource_type,
            )
        )
    if not isinstance(resource_id, str) or not FHIR_ID.fullmatch(resource_id):
        findings.append(
            _finding(
                index=index,
                code="FHIR_ID_INVALID",
                message="id is required and must match [A-Za-z0-9-.]{1,64}",
                resource_type=resource_type if isinstance(resource_type, str) else None,
            )
        )
    unsafe = _unsafe_key_path(resource)
    if unsafe:
        findings.append(
            _finding(
                index=index,
                code="FHIR_UNSAFE_KEY",
                message="FHIR JSON contains a MongoDB-unsafe property name",
                resource_type=resource_type if isinstance(resource_type, str) else None,
                resource_id=resource_id if isinstance(resource_id, str) else None,
                path=unsafe,
            )
        )
    if client_operational:
        findings.append(
            _finding(
                index=index,
                code="FHIR_OPERATIONAL_FIELDS_REMOVED",
                message=f"Client operational fields were removed: {', '.join(client_operational)}",
                severity="warning",
                resource_type=resource_type if isinstance(resource_type, str) else None,
                resource_id=resource_id if isinstance(resource_id, str) else None,
            )
        )

    if any(item["severity"] == "error" for item in findings):
        return None, findings

    release = str(release or "").strip().upper()
    if level == "base":
        if release not in SCHEMA_BACKED_RELEASES:
            findings.append(
                _finding(
                    index=index,
                    code="FHIR_BASE_VALIDATION_UNAVAILABLE",
                    message=f"Base JSON Schema validation is not bundled for {release}",
                    resource_type=resource_type,
                    resource_id=resource_id,
                )
            )
        else:
            validator = _base_validator(release, resource_type)
            if validator is None:
                findings.append(
                    _finding(
                        index=index,
                        code="FHIR_RESOURCE_TYPE_UNKNOWN",
                        message=f"{resource_type} is not present in the bundled {release} schema",
                        resource_type=resource_type,
                        resource_id=resource_id,
                    )
                )
            else:
                errors = sorted(
                    validator.iter_errors(resource),
                    key=lambda error: list(error.absolute_path),
                )
                for error in errors[:max_schema_findings]:
                    dotted = ".".join(str(part) for part in error.absolute_path) or "$"
                    findings.append(
                        _finding(
                            index=index,
                            code="FHIR_BASE_SCHEMA_INVALID",
                            message=error.message,
                            resource_type=resource_type,
                            resource_id=resource_id,
                            path=dotted,
                        )
                    )

    return resource, findings


def available_validation_levels(release: str) -> tuple[str, ...]:
    normalized = _normalized_release(release)
    return ("structure",) if normalized == "R4" else ("structure", "base")


def validate_level(level: str, release: str) -> str:
    normalized = str(level or "").strip().lower()
    if normalized not in SUPPORTED_LEVELS:
        raise ValueError(f"Unknown validation level: {level!r}")
    available = available_validation_levels(release)
    if normalized not in available:
        raise ValueError(
            f"Validation level {normalized!r} is not available for "
            f"{_normalized_release(release)}; choose one of {', '.join(available)}"
        )
    return normalized
