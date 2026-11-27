"""Tests for schema_lib resource structure parsing."""

import pytest

from fhir_search_to_mql.schema.schema_lib import (
    build_brief,
    get_resource_from_index,
    list_fhir_resources,
    load_resources_index,
)


def test_condition_has_polymorphic_onset():
    brief = build_brief("Condition", "R5")
    assert any("onset" in x for v in brief.polymorphic.values() for x in v)


def test_condition_required_includes_subject():
    brief = build_brief("Condition", "R5")
    assert "subject" in brief.required


def test_unknown_resource_raises():
    with pytest.raises(KeyError):
        build_brief("NotARealResource_xyz", "R5")


def test_list_fhir_resources_nonempty():
    names = list_fhir_resources("R5")
    assert "Patient" in names
    assert "Condition" in names
    assert len(names) > 100


def test_resources_index_used_when_present():
    idx = load_resources_index("R5")
    if idx is None:
        pytest.skip("indexes not built — run build_indexes")
    row = get_resource_from_index("Condition", "R5")
    assert row is not None
    assert row["resource"] == "Condition"
