"""
Cross-resource denormalization: R4 flat CodeableConcept vs R5 CodeableReference.

Configs use R5 paths; path_resolver expands legacy branches automatically.
"""

from __future__ import annotations

import pytest

from fhir_search_to_mql import ResourceDenormalizer


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.mark.parametrize(
    "resource_type,legacy_resource,expected_search_key,expected_value",
    [
        (
            "Slot",
            {
                "resourceType": "Slot",
                "id": "s1",
                "status": "free",
                "start": "2024-07-15T09:00:00Z",
                "end": "2024-07-15T09:30:00Z",
                "schedule": {"reference": "Schedule/sched-1"},
                "serviceType": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/service-type",
                                "code": "533",
                            }
                        ]
                    }
                ],
            },
            "serviceType_codes",
            "533",
        ),
        (
            "Schedule",
            {
                "resourceType": "Schedule",
                "id": "sch1",
                "active": True,
                "actor": [{"reference": "Practitioner/p1"}],
                "serviceType": [{"coding": [{"system": "http://x", "code": "124"}]}],
            },
            "serviceType_codes",
            "124",
        ),
        (
            "ServiceRequest",
            {
                "resourceType": "ServiceRequest",
                "id": "sr1",
                "status": "active",
                "intent": "order",
                "subject": {"reference": "Patient/p1"},
                "code": {"coding": [{"system": "http://snomed.info/sct", "code": "71388002"}]},
            },
            "codeConcept_codes",
            "71388002",
        ),
        (
            "Device",
            {
                "resourceType": "Device",
                "id": "d1",
                "status": "active",
                "definition": {"coding": [{"system": "http://snomed.info/sct", "code": "706767009"}]},
            },
            "code_codes",
            "706767009",
        ),
        (
            "Condition",
            {
                "resourceType": "Condition",
                "id": "c1",
                "subject": {"reference": "Patient/p1"},
                "code": {"coding": [{"code": "44054006"}]},
                "evidence": [{"coding": [{"code": "ev-1"}]}],
            },
            "evidence_codes",
            "ev-1",
        ),
    ],
)
def test_legacy_codeable_concept_denormalizes(
    denormalizer,
    resource_type: str,
    legacy_resource: dict,
    expected_search_key: str,
    expected_value: str,
):
    search = denormalizer.denormalize(legacy_resource).get("_search", {})
    assert expected_value in search.get(expected_search_key, [])
