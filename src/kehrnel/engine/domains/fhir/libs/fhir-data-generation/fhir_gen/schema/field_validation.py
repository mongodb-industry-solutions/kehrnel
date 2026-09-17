"""Validate generated FHIR resource instances against schema field types."""

from __future__ import annotations

import re
from typing import Any

from .field_catalog import SchemaFieldSpec
from .parser import FHIRSchemaParser

_DATE_RE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")
_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)$",
)
_INSTANT_RE = _DATETIME_RE
_TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")
_URI_RE = re.compile(r"^(https?|urn:|mailto:|#).+|^[^:\s]+$")
_REFERENCE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9]+/[A-Za-z0-9\-\.]{1,64}$")

_COMPLEX_VALIDATORS: dict[str, Any] = {}


def _register_complex(name: str):
    def decorator(fn):
        _COMPLEX_VALIDATORS[name] = fn
        return fn
    return decorator


def iter_values_at_path(node: Any, path: str):
    parts = path.split(".") if path else []

    def descend(current: Any, index: int):
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


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if value == "":
        return True
    if isinstance(value, dict) and not value:
        return True
    if isinstance(value, list) and not value:
        return True
    if isinstance(value, dict) and set(value.keys()) <= {"id"}:
        return True
    return False


def validate_primitive(value: Any, primitive_type: str) -> str | None:
    if primitive_type == "boolean":
        if not isinstance(value, bool):
            return f"expected boolean, got {type(value).__name__}"
        return None
    # FHIR JSON represents integer64 as a JSON string so values are not rounded
    # by clients whose numeric type is IEEE-754 double precision.
    if primitive_type == "integer64":
        if not isinstance(value, str) or not re.fullmatch(r"-?\d+", value):
            return f"expected integer64 string, got {type(value).__name__}"
        return None
    if primitive_type in ("integer", "unsignedInt", "positiveInt"):
        if not isinstance(value, int) or isinstance(value, bool):
            return f"expected integer, got {type(value).__name__}"
        return None
    if primitive_type == "decimal":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return f"expected decimal number, got {type(value).__name__}"
        return None
    if not isinstance(value, str):
        return f"expected string primitive, got {type(value).__name__}"
    if primitive_type == "date" and not _DATE_RE.match(value):
        return f"invalid date: {value!r}"
    if primitive_type == "dateTime" and not (
        _DATETIME_RE.match(value) or _DATE_RE.match(value)
    ):
        return f"invalid dateTime: {value!r}"
    if primitive_type == "instant" and not _INSTANT_RE.match(value):
        return f"invalid instant: {value!r}"
    if primitive_type == "time" and not _TIME_RE.match(value):
        return f"invalid time: {value!r}"
    if primitive_type in ("uri", "url", "canonical") and not _URI_RE.match(value):
        return f"invalid uri: {value!r}"
    if primitive_type == "code" and not value.strip():
        return "empty code"
    if primitive_type == "id" and not (
        re.match(r"^[A-Za-z0-9\-\.]{1,64}$", value)
        or value.startswith("urn:oid:")
        or value.startswith("urn:uuid:")
    ):
        return f"invalid id: {value!r}"
    return None


@_register_complex("CodeableConcept")
def _validate_codeable_concept(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if value.get("code") and not value.get("coding"):
        value = _coding_as_codeable(value)
    if not (value.get("coding") or value.get("text")):
        return "CodeableConcept needs coding or text"
    if value.get("coding"):
        for c in value["coding"]:
            if not isinstance(c, dict) or not c.get("code"):
                return "Coding missing code"
    return None


@_register_complex("CodeableReference")
def _validate_codeable_reference(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if is_empty(value):
        return None
    if set(value.keys()) <= {"id", "extension", "modifierExtension"}:
        return None
    if value.get("reference"):
        return None
    concept = value.get("concept")
    if isinstance(concept, dict) and (concept.get("coding") or concept.get("text")):
        return None
    if value.get("coding") or value.get("text"):
        return None
    return "CodeableReference needs reference, concept, or text"


@_register_complex("Coding")
def _validate_coding(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if not value.get("code"):
        return "Coding missing code"
    return None


def _coding_as_codeable(value: dict[str, Any]) -> dict[str, Any]:
    """Wrap a lone Coding in CodeableConcept shape for schema fields typed as CodeableConcept."""
    if "coding" in value or "text" in value:
        return value
    if value.get("code"):
        return {
            "coding": [value],
            "text": value.get("display") or value.get("code"),
        }
    return value


@_register_complex("ContactPoint")
def _validate_contact_point(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if is_empty(value):
        return None
    if "name" in value or "telecom" in value:
        return None
    if not value.get("system"):
        return "ContactPoint missing system"
    if not value.get("value"):
        return "ContactPoint missing value"
    if value["system"] not in ("phone", "fax", "email", "pager", "url", "sms", "other"):
        return f"invalid ContactPoint.system: {value['system']}"
    return None


@_register_complex("HumanName")
def _validate_human_name(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if not (value.get("family") or value.get("text") or value.get("given")):
        return "HumanName needs family, given, or text"
    return None


@_register_complex("Identifier")
def _validate_identifier(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if not value.get("value"):
        return "Identifier missing value"
    return None


@_register_complex("Address")
def _validate_address(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if not (value.get("line") or value.get("city") or value.get("text")):
        return "Address needs line, city, or text"
    return None


@_register_complex("Reference")
def _validate_reference(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    ref = value.get("reference")
    if not isinstance(ref, str) or not ref:
        return "Reference missing reference"
    if ref.startswith("urn:"):
        return None
    if not _REFERENCE_RE.match(ref):
        return f"invalid Reference.reference: {ref!r}"
    return None


@_register_complex("Period")
def _validate_period(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if not value.get("start") and not value.get("end"):
        return "Period needs start or end"
    return None


@_register_complex("Quantity")
def _validate_quantity(value: Any) -> str | None:
    if not isinstance(value, dict):
        return f"expected object, got {type(value).__name__}"
    if value.get("value") is None:
        return "Quantity missing value"
    return None


def validate_typed_value(value: Any, ref: str | None, parser: FHIRSchemaParser) -> str | None:
    if ref is None:
        return None
    if ref in parser.PRIMITIVES and isinstance(value, dict):
        if ref == "code" and (
            value.get("coding") or value.get("code") or value.get("text")
        ):
            return None
        if ref == "string" and (
            value.get("code")
            or value.get("reference")
            or value.get("text")
            or value.get("display")
        ):
            return None
        if ref in ("uri", "url", "canonical"):
            if isinstance(value.get("reference"), str):
                return None
            concept = value.get("concept")
            if isinstance(concept, dict) and (concept.get("coding") or concept.get("text")):
                return None
            if value.get("coding") or value.get("text"):
                return None
    if ref == "Reference" and isinstance(value, str):
        if value.startswith("urn:") or _REFERENCE_RE.match(value):
            return None
        return f"invalid Reference string: {value!r}"
    if ref == "CodeableConcept" and isinstance(value, str):
        return None
    if ref == "Identifier" and isinstance(value, str):
        return None
    if ref in parser.PRIMITIVES:
        return validate_primitive(value, ref)
    if ref == "Coding" and isinstance(value, str) and value.strip():
        return None
    validator = _COMPLEX_VALIDATORS.get(ref)
    if validator:
        return validator(value)
    if isinstance(value, dict):
        return None
    if isinstance(value, list):
        return None
    return f"unexpected type for {ref}: {type(value).__name__}"


def _parent_path(path: str) -> str | None:
    if "." not in path:
        return None
    return path.rsplit(".", 1)[0]


def _parent_is_present(resource: dict[str, Any], path: str) -> bool:
    parent = _parent_path(path)
    if parent is None:
        return True
    parents = list(iter_values_at_path(resource, parent))
    return bool(parents) and any(not is_empty(p) for p in parents)


def _sibling_required_paths(specs: list[SchemaFieldSpec], path: str) -> list[str]:
    parent = _parent_path(path)
    if parent is None:
        return []
    prefix = parent + "."
    depth = path.count(".")
    return [
        s.path
        for s in specs
        if s.is_required and s.path.startswith(prefix) and s.path.count(".") == depth
    ]


def _parent_backbone_complete(
    resource: dict[str, Any],
    specs: list[SchemaFieldSpec],
    path: str,
) -> bool:
    """Skip nested required checks when a backbone parent is only partially populated."""
    parent = _parent_path(path)
    if parent is None:
        return True
    siblings = _sibling_required_paths(specs, path)
    if not siblings:
        return True
    for sibling_path in siblings:
        values = list(iter_values_at_path(resource, sibling_path))
        if not values or all(is_empty(v) for v in values):
            return False
    return True


def validate_resource_fields(
    resource: dict[str, Any],
    specs: list[SchemaFieldSpec],
    parser: FHIRSchemaParser,
    *,
    check_required: bool = True,
    max_required_depth: int | None = None,
) -> list[str]:
    """Return human-readable validation errors (empty list means pass)."""
    errors: list[str] = []

    for spec in specs:
        values = list(iter_values_at_path(resource, spec.path))

        if check_required and spec.is_required:
            if max_required_depth is not None and spec.path.count(".") > max_required_depth:
                continue
            if not _parent_is_present(resource, spec.path):
                continue
            if not _parent_backbone_complete(resource, specs, spec.path):
                continue
            if not values or all(is_empty(v) for v in values):
                errors.append(f"required field missing or empty: {spec.path}")
                continue

        for value in values:
            if is_empty(value):
                continue
            if spec.is_array and isinstance(value, list):
                for item in value:
                    if is_empty(item):
                        errors.append(f"empty array element at {spec.path}")
                        continue
                    err = validate_typed_value(item, spec.ref, parser)
                    if err:
                        errors.append(f"{spec.path}[]: {err}")
            else:
                check_value = value
                if spec.ref == "CodeableConcept" and isinstance(value, dict):
                    check_value = _coding_as_codeable(value)
                err = validate_typed_value(check_value, spec.ref, parser)
                if err:
                    errors.append(f"{spec.path}: {err}")

    return errors
