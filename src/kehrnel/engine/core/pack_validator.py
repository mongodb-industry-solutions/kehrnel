"""Validator for strategy pack portability and completeness."""
from __future__ import annotations

import ast
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Any

import jsonschema
from pydantic import ValidationError

from kehrnel.contracts.manifest import StrategyManifest, StrategyManifestSource
from kehrnel.contracts.package_digest import PackageLayoutError, validate_package_layout
from kehrnel.contracts.schema_registry import PUBLISHED_SCHEMA_DIR


@dataclass(frozen=True)
class ValidationDiagnostic:
    code: str
    message: str
    path: str = "/"
    severity: str = "error"

    def as_dict(self) -> Dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "path": self.path,
            "severity": self.severity,
        }


class StrategyPackValidator:
    def __init__(self, manifest_data: Dict[str, Any], base_path: Path):
        self._manifest_is_object = isinstance(manifest_data, dict)
        self.manifest = manifest_data if self._manifest_is_object else {}
        self.base_path = base_path
        self.domain = str(self.manifest.get("domain") or "").lower()

    @property
    def pack_root(self) -> Path:
        """Directory containing runtime code (parent when manifest lives under specification/)."""
        if self.base_path.name == "specification":
            return self.base_path.parent
        return self.base_path

    def validate(self) -> List[str]:
        try:
            validate_package_layout(self.pack_root)
        except PackageLayoutError as exc:
            return [f"package layout invalid: {exc}"]
        if not self._manifest_is_object:
            return ["manifest.json must contain a JSON object"]

        errors: List[str] = []
        errors.extend(self._validate_required_fields())
        errors.extend(self._validate_manifest_model())
        errors.extend(self._validate_runtime_identity_fields())
        errors.extend(self._validate_ops())
        errors.extend(self._validate_defaults_and_schema())
        errors.extend(self._validate_entrypoint())
        errors.extend(self._validate_cross_domain_imports())
        errors.extend(self._validate_spec())
        if self.domain.startswith("openehr"):
            if not self.manifest.get("default_config") and not (self.base_path / "defaults.json").exists():
                errors.append("MISSING_DEFAULTS")
            if self.manifest.get("config_schema") and not (self.manifest.get("config_schema") or {}).get("properties"):
                errors.append("EMPTY_SCHEMA_PROPERTIES")
        return errors

    def validate_diagnostics(self) -> List[ValidationDiagnostic]:
        """Return stable, machine-readable diagnostics without breaking v1 callers."""

        return [self._to_diagnostic(message) for message in self.validate()]

    @staticmethod
    def _to_diagnostic(message: str) -> ValidationDiagnostic:
        op_match = re.match(r"op\[(\d+)\](.*)", message)
        if op_match:
            index, remainder = op_match.groups()
            leaf = "output_schema" if "output_schema" in remainder else "input_schema"
            return ValidationDiagnostic(
                code="OP_SCHEMA_INVALID",
                message=message,
                path=f"/ops/{index}/{leaf}",
            )
        spec_match = re.search(r"spec\.json validation error at '([^']+)'", message)
        if spec_match:
            return ValidationDiagnostic(
                code="SPEC_SCHEMA_INVALID",
                message=message,
                path="/spec/" + spec_match.group(1),
            )
        manifest_match = re.match(r"manifest validation error at '([^']+)'", message)
        if manifest_match:
            return ValidationDiagnostic(
                code="MANIFEST_SCHEMA_INVALID",
                message=message,
                path="/" + manifest_match.group(1),
            )
        missing_match = re.match(r"missing required field '([^']+)'", message)
        if missing_match:
            return ValidationDiagnostic(
                code="MANIFEST_FIELD_REQUIRED",
                message=message,
                path="/" + missing_match.group(1),
            )
        if "entrypoint" in message:
            return ValidationDiagnostic("ENTRYPOINT_INVALID", message, "/entrypoint")
        if message == "manifest.json must contain a JSON object":
            return ValidationDiagnostic("MANIFEST_SCHEMA_INVALID", message, "/")
        if message.startswith("spec.json"):
            return ValidationDiagnostic("SPEC_INVALID", message, "/spec")
        if "DEFAULT" in message or "defaults.json" in message:
            return ValidationDiagnostic("DEFAULTS_INVALID", message, "/default_config")
        if "SCHEMA" in message or "schema.json" in message:
            return ValidationDiagnostic("CONFIG_SCHEMA_INVALID", message, "/config_schema")
        return ValidationDiagnostic("PACK_INVALID", message)

    def _validate_manifest_model(self) -> List[str]:
        contract = self.manifest.get("manifestContract")
        if contract and contract != "strategy-manifest/v1":
            return [f"unsupported manifestContract '{contract}'"]
        try:
            model = (
                StrategyManifestSource
                if contract == "strategy-manifest/v1"
                else StrategyManifest
            )
            model.model_validate(self.manifest)
        except ValidationError as exc:
            errors: List[str] = []
            for item in exc.errors(include_url=False):
                path = "/".join(str(part) for part in item.get("loc") or ()) or "root"
                errors.append(
                    f"manifest validation error at '{path}': {item.get('msg', 'invalid value')}"
                )
            return errors
        return []

    def _validate_runtime_identity_fields(self) -> List[str]:
        forbidden = {
            "packageDigest",
            "package_digest",
            "contractDigest",
            "contract_digest",
            "pack_spec",
        }
        authored = sorted(forbidden.intersection(self.manifest))
        if not authored:
            return []
        return [
            "runtime-computed fields must not be authored: " + ", ".join(authored)
        ]

    def _validate_required_fields(self) -> List[str]:
        errors: List[str] = []
        required_fields = ("id", "version", "domain")
        for field in required_fields:
            if not self.manifest.get(field):
                errors.append(f"missing required field '{field}'")
        return errors

    def _validate_ops(self) -> List[str]:
        errors: List[str] = []
        ops = self.manifest.get("ops")
        if ops is None or not isinstance(ops, list):
            errors.append("ops must be a list with input_schema")
            return errors
        for idx, op in enumerate(ops):
            if not isinstance(op, dict):
                errors.append(f"op[{idx}] must be an object")
                continue
            schema = op.get("input_schema")
            if schema is None:
                errors.append(f"op[{idx}] missing input_schema")
            elif not isinstance(schema, dict):
                errors.append(f"op[{idx}] input_schema must be a dict")
            else:
                try:
                    jsonschema.Draft7Validator.check_schema(schema)
                except Exception as exc:
                    errors.append(f"op[{idx}] input_schema invalid JSON schema: {exc}")
            output_schema = op.get("output_schema")
            if output_schema is not None and not isinstance(output_schema, dict):
                errors.append(f"op[{idx}] output_schema must be a dict")
            elif output_schema is not None:
                try:
                    jsonschema.Draft7Validator.check_schema(output_schema)
                except Exception as exc:
                    errors.append(f"op[{idx}] output_schema invalid JSON schema: {exc}")
        return errors

    def _validate_defaults_and_schema(self) -> List[str]:
        errors: List[str] = []
        defaults_path = self.base_path / "defaults.json"
        if not self.manifest.get("default_config") and not defaults_path.exists():
            if self.domain.startswith("openehr"):
                errors.append("defaults.json missing and default_config empty")
        if defaults_path.exists():
            try:
                defaults_data = json.loads(defaults_path.read_text(encoding="utf-8"))
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"defaults.json invalid JSON: {exc}")
            else:
                if not isinstance(defaults_data, dict):
                    errors.append("defaults.json must contain a JSON object")

        schema_path = self.base_path / "schema.json"
        config_schema = self.manifest.get("config_schema")
        if (not config_schema) and not schema_path.exists():
            if self.domain.startswith("openehr"):
                errors.append("schema.json missing and config_schema empty")
        if schema_path.exists():
            try:
                schema_data = json.loads(schema_path.read_text(encoding="utf-8"))
                jsonschema.Draft7Validator.check_schema(schema_data)
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"schema.json invalid JSON schema: {exc}")
            else:
                if not schema_data.get("properties") and self.domain.startswith("openehr"):
                    errors.append("EMPTY_SCHEMA_PROPERTIES")
        return errors

    def _validate_entrypoint(self) -> List[str]:
        errors: List[str] = []
        entrypoint = self.manifest.get("entrypoint")
        if not entrypoint:
            errors.append("entrypoint is required for strategy discovery")
            return errors
        if not isinstance(entrypoint, str) or entrypoint.count(":") != 1:
            errors.append("entrypoint must use the form 'module:attribute'")
            return errors
        mod, attribute = entrypoint.split(":", 1)
        if not mod or not attribute:
            errors.append("entrypoint must use the form 'module:attribute'")
            return errors
        source = self._find_module_source_without_import(mod)
        if source is None:
            errors.append(f"entrypoint module not found: {mod}")
            return errors
        if not source.resolve().is_relative_to(self.pack_root.resolve()):
            errors.append(f"entrypoint module outside strategy pack: {mod}")
            return errors
        try:
            module_ast = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        except (OSError, SyntaxError, UnicodeError) as exc:
            errors.append(f"entrypoint module is not valid Python: {mod}: {exc}")
            return errors
        if not self._module_declares_attribute(module_ast, attribute):
            errors.append(f"entrypoint attribute not found: {mod}:{attribute}")
        return errors

    @staticmethod
    def _module_declares_attribute(module_ast: ast.Module, attribute: str) -> bool:
        """Recognize statically declared or imported top-level attributes."""

        for node in module_ast.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name == attribute:
                    return True
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    exposed = alias.asname or alias.name.split(".")[0]
                    if exposed == attribute:
                        return True
            elif isinstance(node, ast.Assign):
                if any(
                    isinstance(target, ast.Name) and target.id == attribute
                    for target in node.targets
                ):
                    return True
            elif isinstance(node, ast.AnnAssign):
                if isinstance(node.target, ast.Name) and node.target.id == attribute:
                    return True
        return False

    @staticmethod
    def _find_module_source_without_import(module: str) -> Path | None:
        """Resolve a filesystem module using sys.path without importing parents.

        ``importlib.util.find_spec('package.module')`` imports ``package`` and can
        execute untrusted pack code during discovery. Strategy packs are
        filesystem packages, so deterministic path lookup is sufficient here.
        The first matching sys.path entry mirrors Python's shadowing behavior.
        """

        parts = module.split(".")
        if not parts or any(not re.fullmatch(r"[A-Za-z_]\w*", part) for part in parts):
            return None
        relative = Path(*parts)
        for raw_root in sys.path:
            try:
                search_root = Path(raw_root or os.getcwd())
                module_path = search_root / relative
                candidates = (module_path / "__init__.py", module_path.with_suffix(".py"))
                for candidate in candidates:
                    if candidate.is_file():
                        return candidate
            except OSError:
                continue
        return None

    def _validate_cross_domain_imports(self) -> List[str]:
        errors: List[str] = []
        if not self.domain:
            return errors
        pattern = "kehrnel.engine.strategies."
        for path in self.pack_root.rglob("*.py"):
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeError) as exc:
                errors.append(f"cannot inspect Python source {path}: {exc}")
                continue
            start = 0
            while True:
                idx = text.find(pattern, start)
                if idx == -1:
                    break
                after = text[idx + len(pattern) :]
                domain_part = ""
                for ch in after:
                    if ch.isalnum() or ch == "_":
                        domain_part += ch
                        continue
                    break
                if domain_part and domain_part.lower() != self.domain and not domain_part.lower().startswith("common"):
                    errors.append(f"cross-domain import '{pattern}{domain_part}' in {path}")
                start = idx + len(pattern)
        return errors

    def _validate_spec(self) -> List[str]:
        """Validate optional strategy-pack spec payload when pack_format requests it."""
        errors: List[str] = []
        pack_format = str(self.manifest.get("pack_format") or "").lower()
        if pack_format != "strategy-pack/v1":
            return errors
        spec_ref = self.manifest.get("spec") or {}
        spec_path = ""
        if isinstance(spec_ref, dict):
            spec_path = spec_ref.get("path") or "spec.json"
        elif isinstance(spec_ref, str):
            spec_path = spec_ref
        candidate = (self.base_path / (spec_path or "spec.json")).resolve()
        if not candidate.is_relative_to(self.pack_root.resolve()):
            errors.append("spec.json path escapes the strategy pack")
            return errors
        if not candidate.exists():
            errors.append("spec.json missing for pack_format strategy-pack/v1")
            return errors
        try:
            spec_data = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"spec.json invalid JSON: {exc}")
            return errors
        schema_path = PUBLISHED_SCHEMA_DIR / "strategy-pack-spec.schema.json"
        if not schema_path.exists():
            errors.append("strategy-pack spec schema is unavailable")
            return errors
        try:
            schema_data = json.loads(schema_path.read_text(encoding="utf-8"))
            validator = jsonschema.Draft202012Validator(schema_data)
            validation_errors = sorted(
                validator.iter_errors(spec_data),
                key=lambda item: (-len(item.absolute_path), list(item.absolute_path)),
            )
            if validation_errors:
                exc = validation_errors[0]
                path_str = "/".join(str(item) for item in exc.absolute_path)
                message = (
                    f"spec.json validation error at '{path_str}': {exc.message}"
                    if path_str
                    else f"spec.json validation error: {exc.message}"
                )
                errors.append(message)
                return errors
        except Exception as exc:
            errors.append(f"strategy-pack spec schema invalid: {exc}")
            return errors
        # Note: bundle file references in spec.json are informational only;
        # missing bundle files don't block strategy loading
        return errors
