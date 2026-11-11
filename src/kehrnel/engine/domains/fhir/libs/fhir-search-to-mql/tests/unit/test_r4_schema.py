"""R4 schema file validation tests."""

import json
from pathlib import Path

import pytest


def _schema_path() -> Path:
    return Path(__file__).parent.parent.parent / "schema" / "fhir.schema.v4.json"


def test_r4_schema_is_valid_json():
    path = _schema_path()
    if not path.exists():
        pytest.skip("R4 schema file not yet copied")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    assert "definitions" in data


def test_r4_schema_has_core_resources():
    path = _schema_path()
    if not path.exists():
        pytest.skip("R4 schema file not yet copied")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    defs = data["definitions"]
    for resource in ["Patient", "Observation", "Encounter", "Condition", "Practitioner"]:
        assert resource in defs, f"R4 schema missing {resource} definition"


def test_r4_schema_encounter_has_period_not_actual_period():
    """R4 Encounter uses 'period', not 'actualPeriod' (R5 field)."""
    path = _schema_path()
    if not path.exists():
        pytest.skip("R4 schema file not yet copied")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    enc_props = data["definitions"]["Encounter"]["properties"]
    assert "period" in enc_props, "R4 Encounter must have 'period' field"
    assert "actualPeriod" not in enc_props, "R4 Encounter must not have 'actualPeriod' (R5 field)"


def test_r4_schema_no_integer64():
    """R4 schema must not have integer64 as a primitive type definition."""
    path = _schema_path()
    if not path.exists():
        pytest.skip("R4 schema file not yet copied")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    assert "integer64" not in data.get("definitions", {}), \
        "R4 schema must not have integer64 (R5+ type)"
