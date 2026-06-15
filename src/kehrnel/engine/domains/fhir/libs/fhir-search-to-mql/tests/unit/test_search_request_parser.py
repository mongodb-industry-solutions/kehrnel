"""Unit tests for unified FHIR search request parsing."""

import pytest

from fhir_search_to_mql.core.exceptions import ParsingError
from fhir_search_to_mql.parser.search_request_parser import (
    criteria_dict_to_query_string,
    parse_fhir_search,
    parse_fhir_search_parts,
)


def test_criteria_dict_to_query_string():
    assert criteria_dict_to_query_string({"gender": "female"}) == "gender=female"
    assert "family=Smith" in criteria_dict_to_query_string(
        {"family": "Smith", "birthdate": "gt1990-01-01"},
    )


def test_parse_fhir_search_resource_question_form():
    rt, qs = parse_fhir_search_parts("Patient?gender=female&active=true")
    assert rt == "Patient"
    assert "gender=female" in qs


def test_parse_fhir_search_compartment_rest_path():
    parsed = parse_fhir_search(
        "Patient/p-123/Observation?category=vital-signs&status=final",
    )
    assert parsed["resource_type"] == "Observation"
    assert parsed["compartment"] == {"type": "Patient", "id": "p-123"}
    assert "category=vital-signs" in parsed["query_string"]


def test_parse_fhir_search_compartment_full_url():
    parsed = parse_fhir_search(
        "http://localhost/fhir/Patient/p-123/Observation?status=final",
    )
    assert parsed["resource_type"] == "Observation"
    assert parsed["compartment"]["type"] == "Patient"
    assert parsed["compartment"]["id"] == "p-123"


def test_parse_fhir_search_empty_raises():
    with pytest.raises(ParsingError, match="non-empty"):
        parse_fhir_search("")
