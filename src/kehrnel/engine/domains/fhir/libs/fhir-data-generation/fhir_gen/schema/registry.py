"""Cached access to parsed FHIR schema definitions."""

from __future__ import annotations

from pathlib import Path

from ..config import settings
from .parser import FHIRSchemaParser, ResourceDef


class SchemaRegistry:
    _instance: SchemaRegistry | None = None

    def __init__(self, schema_path: Path | None = None) -> None:
        path = schema_path or settings.resolved_schema_path
        self._parser = FHIRSchemaParser(path)
        self._cache: dict[str, ResourceDef] = {}

    @classmethod
    def get(cls) -> SchemaRegistry:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def definition(self, name: str) -> ResourceDef:
        if name not in self._cache:
            self._cache[name] = self._parser.parse_definition(name)
        return self._cache[name]

    def all_resources(self) -> list[str]:
        return list(self._parser.get_all_resources())

    def references_for(self, name: str) -> list[str]:
        return self._parser.get_references_for(name)

    def parser(self) -> FHIRSchemaParser:
        return self._parser

    @classmethod
    def reload(cls, schema_path: Path | None = None) -> SchemaRegistry:
        if schema_path is not None:
            settings.schema_path = Path(schema_path)
        cls._instance = cls(settings.resolved_schema_path)
        global registry  # noqa: PLW0603 — keep module alias in sync after reload
        registry = cls._instance
        return cls._instance


registry = SchemaRegistry.get()
