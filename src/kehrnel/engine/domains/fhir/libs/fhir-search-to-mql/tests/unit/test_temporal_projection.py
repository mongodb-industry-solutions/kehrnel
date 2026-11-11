from datetime import UTC, datetime

import pytest

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from fhir_search_to_mql.temporal import (
    build_date_projections,
    parse_fhir_temporal_range,
)


def test_partial_fhir_dates_become_half_open_ranges():
    assert parse_fhir_temporal_range("2024") == (
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2025, 1, 1, tzinfo=UTC),
    )
    assert parse_fhir_temporal_range("2024-02") == (
        datetime(2024, 2, 1, tzinfo=UTC),
        datetime(2024, 3, 1, tzinfo=UTC),
    )


def test_form_decoded_offset_is_same_instant_as_explicit_offset():
    # parse_qs decodes ``+`` in a query value as a space.
    assert parse_fhir_temporal_range("1990-05-15T10:30:00 05:30") == (
        parse_fhir_temporal_range("1990-05-15T10:30:00+05:30")
    )


def test_denormalizer_preserves_canonical_date_and_adds_bson_interval():
    resource = {
        "resourceType": "Patient",
        "id": "p1",
        "birthDate": "1990-05-01",
    }
    stored = ResourceDenormalizer().denormalize(resource)

    assert stored["birthDate"] == "1990-05-01"
    assert stored["_search"]["_dates"]["birthdate"] == [
        {
            "start": datetime(1990, 5, 1, tzinfo=UTC),
            "end": datetime(1990, 5, 2, tzinfo=UTC),
        }
    ]


def test_date_search_targets_interval_projection():
    query = FHIRSearchConverter().convert("Patient", "birthdate=ge1990-01-01")
    assert query == {
        "_search._dates.birthdate": {
            "$elemMatch": {"end": {"$gt": datetime(1990, 1, 1, tzinfo=UTC)}}
        }
    }


def _interval_overlaps(
    query: dict, stored_start: datetime, stored_end: datetime
) -> bool:
    """Evaluate a compiled ``$elemMatch`` overlap against one stored interval."""

    condition = query["$elemMatch"]
    return (
        stored_start < condition["start"]["$lt"]
        and stored_end > condition["end"]["$gt"]
    )


@pytest.mark.parametrize(
    ("query_value", "stored_value"),
    [
        ("2024-01-01T10:30:00Z", "2024-01-01T10:30:00.500Z"),
        ("2024-01-01T10:30Z", "2024-01-01T10:30:45Z"),
        ("2024-01-01T10Z", "2024-01-01T10:59:30Z"),
        ("2024-01-01", "2024-01-01T14:00:00Z"),
    ],
)
def test_last_updated_equality_matches_inside_precision_window(
    query_value: str, stored_value: str
):
    query = FHIRSearchConverter().convert("Patient", f"_lastUpdated={query_value}")
    start, end = parse_fhir_temporal_range(stored_value)

    assert _interval_overlaps(
        query["_search._dates._lastUpdated"], start, end
    )


def test_last_updated_equality_rejects_value_outside_precision_window():
    query = FHIRSearchConverter().convert(
        "Patient", "_lastUpdated=2024-01-01T10:30:00Z"
    )
    start, end = parse_fhir_temporal_range("2024-01-01T10:31:00.500Z")

    assert not _interval_overlaps(
        query["_search._dates._lastUpdated"], start, end
    )


def test_date_equality_matches_value_inside_day_precision_window():
    query = FHIRSearchConverter().convert("Patient", "birthdate=1990-05-01")
    start, end = parse_fhir_temporal_range("1990-05-01T14:00:00Z")

    assert _interval_overlaps(query["_search._dates.birthdate"], start, end)


def test_last_updated_query_matches_denormalized_projection():
    resource = {
        "resourceType": "Patient",
        "id": "p1",
        "meta": {"lastUpdated": "2024-01-01T10:30:00.500Z"},
    }
    stored = ResourceDenormalizer().denormalize(resource)
    query = FHIRSearchConverter().convert(
        "Patient", "_lastUpdated=2024-01-01T10:30:00Z"
    )
    interval = stored["_search"]["_dates"]["_lastUpdated"][0]

    assert _interval_overlaps(
        query["_search._dates._lastUpdated"], interval["start"], interval["end"]
    )


def test_last_updated_projection_materialized_when_config_omits_it():
    config = {
        "search_parameters": {
            "birthdate": {
                "type": "date",
                "fields": [{"field": "birthDate", "type": "date"}],
            }
        }
    }
    projections = build_date_projections(
        {
            "resourceType": "Patient",
            "id": "p1",
            "birthDate": "1990-05-01",
            "meta": {"lastUpdated": "2024-01-01T10:30:00Z"},
        },
        config,
    )

    assert projections["_lastUpdated"] == [
        {
            "start": datetime(2024, 1, 1, 10, 30, tzinfo=UTC),
            "end": datetime(2024, 1, 1, 10, 30, 1, tzinfo=UTC),
        }
    ]


def test_contains_is_literal_substring_not_whole_value_equality():
    query = FHIRSearchConverter().convert("Patient", "family:contains=e.c")
    assert query == {
        "_search.familyName_lower": {"$regex": r"e\.c", "$options": "i"}
    }


def test_forward_chain_builds_executable_one_hop_plan():
    query = FHIRSearchConverter().convert(
        "Observation", "subject:Patient.gender=female"
    )
    plan = query["_multi_step"][0]
    assert plan["target_field"] == "_search.subjectId"
    assert plan["target_constraints"] == {"_search.subjectType": "Patient"}
    assert plan["steps"][0]["resource_type"] == "Patient"
    assert plan["steps"][0]["query"] == {"gender": "female"}


def test_reverse_chain_compiles_target_search_and_reference_projection():
    query = FHIRSearchConverter().convert(
        "Patient", "_has:Observation:subject:status=final"
    )
    plan = query["_multi_step"][0]
    assert plan["target_field"] == "id"
    assert plan["steps"][0]["resource_type"] == "Observation"
    assert plan["steps"][0]["extract_field"] == "_search.subjectId"
    assert {"status": "final"} in plan["steps"][0]["query"]["$and"]
