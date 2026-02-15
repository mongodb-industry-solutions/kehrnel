"""Validator for strategy pack portability and completeness."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Dict, List, Any

import jsonschema


class StrategyPackValidator:
    def __init__(self, manifest_data: Dict[str, Any], base_path: Path):
        self.manifest = manifest_data or {}
        self.base_path = base_path
        self.domain = str(self.manifest.get("domain") or "").lower()

    def validate(self) -> List[str]:
        errors: List[str] = []
        errors.extend(self._validate_required_fields())
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
        return errors

    def _validate_defaults_and_schema(self) -> List[str]:
        errors: List[str] = []
        defaults_path = self.base_path / "defaults.json"
        if not self.manifest.get("default_config") and not defaults_path.exists():
            if self.domain.startswith("openehr"):
                errors.append("defaults.json missing and default_config empty")
        if defaults_path.exists():
            try:
                json.loads(defaults_path.read_text(encoding="utf-8"))
            except Exception as exc:  # pragma: no cover - defensive
                errors.append(f"defaults.json invalid JSON: {exc}")

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
        mod = entrypoint.split(":")[0]
        try:
            spec = importlib.util.find_spec(mod)
            if spec is None:
                errors.append(f"entrypoint module not found: {mod}")
            else:
                base = self.base_path.resolve().as_posix()
                if spec.origin:
                    if not Path(spec.origin).resolve().as_posix().startswith(base):
                        errors.append(f"entrypoint module outside strategy pack: {mod}")
                elif spec.submodule_search_locations:
                    # Namespace packages have origin=None; ensure *all* search locations are within the pack.
                    for loc in spec.submodule_search_locations:
                        if not Path(loc).resolve().as_posix().startswith(base):
                            errors.append(f"entrypoint module outside strategy pack: {mod}")
                            break
                else:
                    errors.append(f"entrypoint module location unknown: {mod}")
        except Exception:
            errors.append(f"entrypoint module not found: {mod}")
        return errors

    def _validate_cross_domain_imports(self) -> List[str]:
        errors: List[str] = []
        if not self.domain:
            return errors
        pattern = "kehrnel.engine.strategies."
        for path in self.base_path.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
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
        candidate = self.base_path / (spec_path or "spec.json")
        if not candidate.exists():
            errors.append("spec.json missing for pack_format strategy-pack/v1")
            return errors
        try:
            spec_data = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"spec.json invalid JSON: {exc}")
            return errors
        # load metamodel schema (optional - don't fail if schema file doesn't exist)
        schema_path = Path(__file__).resolve().parents[3] / "docs" / "strategy-pack-v1" / "spec.schema.json"
        if schema_path.exists():
            try:
                schema_data = json.loads(schema_path.read_text(encoding="utf-8"))
                jsonschema.Draft7Validator(schema_data).validate(spec_data)
            except jsonschema.ValidationError as exc:
                path_str = "/".join([str(x) for x in exc.absolute_path])
                msg = f"spec.json validation error at '{path_str}': {exc.message}" if path_str else f"spec.json validation error: {exc.message}"
                errors.append(msg)
                return errors
            except Exception:
                pass  # Schema loading failed, skip validation
        # Note: bundle file references in spec.json are informational only;
        # missing bundle files don't block strategy loading
        return errors
