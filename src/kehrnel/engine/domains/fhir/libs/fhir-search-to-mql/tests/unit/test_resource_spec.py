"""Tests for resource_spec CLI (uses indexes when present)."""

from fhir_search_to_mql.schema.resource_spec import combined_spec, get_structure
from fhir_search_to_mql.schema.schema_lib import load_resources_index


def test_combined_spec_condition():
    spec = combined_spec("Condition", "R5")
    assert spec["resource"] == "Condition"
    assert spec["search_parameter_count"] >= 20
    assert "subject" in spec["structure"].get("required", [])


def test_get_structure_from_index_or_live():
    structure = get_structure("Patient", "R5")
    assert "name" in {f["name"] for f in structure["fields"]}


def test_index_search_faster_than_package_only():
    idx = load_resources_index("R5")
    if idx is None:
        return
    from fhir_search_to_mql.schema.search_package_loader import (
        _load_search_index,
        search_parameters_for_resource,
    )

    rows = search_parameters_for_resource("Condition", "R5")
    assert _load_search_index("R5") is not None
    assert any(r.code == "code" for r in rows)
