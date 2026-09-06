from datetime import UTC, datetime

from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from fhir_search_to_mql.temporal import parse_fhir_temporal_range


def test_partial_fhir_dates_become_half_open_ranges():
    assert parse_fhir_temporal_range("2024") == (
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2025, 1, 1, tzinfo=UTC),
    )
    assert parse_fhir_temporal_range("2024-02") == (
        datetime(2024, 2, 1, tzinfo=UTC),
        datetime(2024, 3, 1, tzinfo=UTC),
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
            "$elemMatch": {"end": {"$gt": datetime(1990, 1, 1)}}
        }
    }


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
