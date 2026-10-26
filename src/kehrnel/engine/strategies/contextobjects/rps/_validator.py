"""
_validator.py — ContextObjects instance validator
==================================================

Validates a ContextObject instance against its semantic model schema.

Schema format
-------------
{
  "id":    "patient-demographics",
  "nodes": {
    "co0001": {
      "label": "Full Name", "type": "string", "required": true,
      "children": {
        "co0002": {"label": "First Name", "type": "string", "required": true}
      }
    },
    "co0010": {"label": "Date of Birth", "type": "datetime", "required": true},
    "co0020": {
      "label": "Gender", "type": "coded_text",
      "bindings": [{"code": "M", "display": "Male"}, {"code": "F", "display": "Female"}]
    }
  }
}
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ValidationError:
    path:     str
    node_id:  str
    message:  str
    severity: str = "error"


@dataclass
class ValidationResult:
    ok:       bool
    errors:   list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @classmethod
    def success(cls) -> "ValidationResult":
        return cls(ok=True)

    def add_error(self, path: str, node_id: str, message: str) -> None:
        self.errors.append(ValidationError(path, node_id, message, "error"))
        self.ok = False

    def add_warning(self, path: str, node_id: str, message: str) -> None:
        self.warnings.append(ValidationError(path, node_id, message, "warning"))

    def to_dict(self) -> dict:
        return {
            "ok":       self.ok,
            "errors":   [{"path": e.path, "nodeId": e.node_id, "message": e.message, "severity": e.severity}
                         for e in self.errors],
            "warnings": [{"path": e.path, "nodeId": e.node_id, "message": e.message, "severity": e.severity}
                         for e in self.warnings],
        }


def validate(instance: dict, schema: dict) -> ValidationResult:
    """Validate *instance* against *schema*. Returns ValidationResult."""
    result = ValidationResult(ok=True)

    if not isinstance(instance, dict):
        result.add_error("/", "root", "instance must be a JSON object")
        return result

    if not instance.get("objectType"):
        result.add_error("/", "root", "objectType is required")
    if not instance.get("recordId"):
        result.add_error("/", "root", "recordId is required")
    if not isinstance(instance.get("data"), dict):
        result.add_error("/data", "root", "data must be an object")
        return result

    schema_nodes = schema.get("nodes") or {}
    if not schema_nodes:
        result.add_warning("/", "root", "Schema has no node definitions — skipping structural validation")
        return result

    schema_id = schema.get("id") or ""
    if schema_id and instance.get("objectType") != schema_id:
        result.add_error(
            "/objectType", "root",
            f"objectType '{instance['objectType']}' does not match schema id '{schema_id}'"
        )

    _validate_nodes(instance["data"], schema_nodes, "/data", result, strict_unknown=False)
    return result


def _validate_nodes(
    data:           dict,
    schema_nodes:   dict,
    path_prefix:    str,
    result:         ValidationResult,
    strict_unknown: bool,
) -> None:
    for node_id, node_schema in schema_nodes.items():
        if not isinstance(node_schema, dict):
            continue
        if node_schema.get("required") and node_id not in data:
            result.add_error(
                f"{path_prefix}[{node_id}]", node_id,
                f"Required node '{node_id}' ({node_schema.get('label', '')}) is missing",
            )

    for node_id, child in data.items():
        if not isinstance(child, dict):
            result.add_error(
                f"{path_prefix}[{node_id}]", node_id,
                f"Node value must be an object, got {type(child).__name__}",
            )
            continue

        path        = f"{path_prefix}[{node_id}]"
        node_schema = schema_nodes.get(node_id)

        if node_schema is None:
            if strict_unknown:
                result.add_error(path, node_id, f"Unknown node '{node_id}' not in schema")
            else:
                result.add_warning(path, node_id, f"Node '{node_id}' not in schema")
            continue

        _validate_node_value(child, node_schema, path, node_id, result)

        child_schema = node_schema.get("children") or {}
        child_data   = {k: v for k, v in child.items() if not k.startswith("_")}
        if child_data or child_schema:
            _validate_nodes(child_data, child_schema, path, result, strict_unknown)


def _validate_node_value(
    node:        dict,
    node_schema: dict,
    path:        str,
    node_id:     str,
    result:      ValidationResult,
) -> None:
    value    = node.get("_value")
    declared = str(node.get("_type") or "").lower().strip()
    expected = str(node_schema.get("type") or "").lower().strip()

    if value is None:
        if node_schema.get("required"):
            result.add_error(path, node_id, f"Required node '{node_id}' has no _value")
        return

    if expected and declared and expected != declared:
        result.add_warning(
            path, node_id,
            f"Declared type '{declared}' differs from schema type '{expected}'"
        )

    eff_type = declared or expected
    if eff_type:
        _check_type(value, eff_type, path, node_id, result)

    bindings = node_schema.get("bindings")
    if bindings and isinstance(bindings, list):
        _check_binding(node, bindings, path, node_id, result)

    if node_schema.get("min") is not None and isinstance(value, (int, float)):
        if value < node_schema["min"]:
            result.add_error(path, node_id, f"Value {value} is below minimum {node_schema['min']}")
    if node_schema.get("max") is not None and isinstance(value, (int, float)):
        if value > node_schema["max"]:
            result.add_error(path, node_id, f"Value {value} is above maximum {node_schema['max']}")

    if isinstance(value, str):
        if node_schema.get("maxLength") and len(value) > node_schema["maxLength"]:
            result.add_error(path, node_id, f"String length {len(value)} exceeds maxLength {node_schema['maxLength']}")
        if node_schema.get("pattern"):
            if not re.fullmatch(node_schema["pattern"], value):
                result.add_error(path, node_id, f"Value does not match pattern '{node_schema['pattern']}'")


def _check_type(
    value: Any,
    expected_type: str,
    path: str,
    node_id: str,
    result: ValidationResult,
) -> None:
    ok = True
    if expected_type in ("string", "text"):
        ok = isinstance(value, str)
    elif expected_type == "integer":
        ok = isinstance(value, int) and not isinstance(value, bool)
    elif expected_type in ("number", "decimal", "float"):
        ok = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif expected_type == "boolean":
        ok = isinstance(value, bool)
    elif expected_type == "coded_text":
        ok = isinstance(value, (str, dict))
    elif expected_type == "quantity":
        ok = isinstance(value, (int, float, dict))
    elif expected_type == "datetime":
        ok = isinstance(value, str) and _is_datetime_str(value)

    if not ok:
        result.add_error(
            path, node_id,
            f"Type mismatch: expected '{expected_type}', got {type(value).__name__} ({value!r})"
        )


def _check_binding(
    node: dict,
    bindings: list,
    path: str,
    node_id: str,
    result: ValidationResult,
) -> None:
    code = node.get("_code")
    if code is None:
        return
    valid_codes = {str(b.get("code") or "") for b in bindings if isinstance(b, dict)}
    if valid_codes and code not in valid_codes:
        result.add_warning(
            path, node_id,
            f"Code '{code}' is not in the defined value set {sorted(valid_codes)}"
        )


def _is_datetime_str(s: str) -> bool:
    import datetime as _dt
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            _dt.datetime.strptime(s[:26], fmt[:len(fmt)])
            return True
        except ValueError:
            pass
    return False
