"""Schema version resolution (R5 / R6)."""

from pathlib import Path

import pytest

from fhir_gen.config import SCHEMA_PATH, V6_SCHEMA_PATH, settings
from fhir_gen.schema.versions import normalize_schema_version, resolve_schema_path


def test_default_is_r5():
    assert resolve_schema_path() == SCHEMA_PATH


def test_r6_aliases():
    for label in ("R6", "r6", "6", "v6"):
        assert resolve_schema_path(schema_version=label) == V6_SCHEMA_PATH


def test_unknown_version_raises():
    with pytest.raises(ValueError, match="Unknown"):
        resolve_schema_path(schema_version="R4")


def test_path_override_wins():
    custom = SCHEMA_PATH
    assert resolve_schema_path(schema_version="R6", schema_path=custom) == custom


def test_settings_resolved_path_uses_version():
    original_version = settings.schema_version
    original_path = settings.schema_path
    try:
        settings.schema_version = "R6"
        settings.schema_path = None
        assert settings.resolved_schema_path == V6_SCHEMA_PATH
        assert settings.fhir_version == "R6"
    finally:
        settings.schema_version = original_version
        settings.schema_path = original_path


def test_normalize_schema_version():
    assert normalize_schema_version("r5") == "R5"
