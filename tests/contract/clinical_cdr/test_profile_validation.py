from __future__ import annotations

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.clinical_cdr.scripts import profile_validation

PROFILE_URL = "https://example.test/fhir/StructureDefinition/customer-patient"


class _Validator:
    def __init__(self, findings: list[dict] | None = None):
        self.findings = findings or []
        self.calls: list[dict] = []

    async def validate(self, *, snapshot, datasets, options):
        self.calls.append(
            {"snapshot": snapshot, "datasets": datasets, "options": options}
        )
        return {"findings": self.findings}


def _config(mode: str = "required") -> dict:
    return {
        "schema_version": "R5",
        "implementation_guides": {
            "active_profiles": [PROFILE_URL],
            "profile_validation": {
                "mode": mode,
                "binding": "validation_engine",
                "fail_on_warning": False,
            },
        },
    }


def _ctx(config: dict, adapter=None) -> StrategyContext:
    return StrategyContext(
        environment_id="profile-test",
        config=config,
        adapters={"validation_engine": adapter} if adapter else {},
    )


@pytest.fixture(autouse=True)
def _profile_catalog(monkeypatch):
    profiles = [{"url": PROFILE_URL, "type": "Patient", "package": "example"}]
    monkeypatch.setattr(
        profile_validation.implementation_guides,
        "resolve_active_profiles",
        lambda config: (
            profiles
            if (config.get("implementation_guides") or {}).get("active_profiles")
            else []
        ),
    )
    monkeypatch.setattr(
        profile_validation.implementation_guides,
        "inspect_configured_implementation_guides",
        lambda config: [{"package": {"name": "example", "version": "1.0.0"}}],
    )


def test_profile_enforcement_requires_an_adapter():
    with pytest.raises(ValueError, match="adapter is not available"):
        profile_validation.validate_profile_config(_config(), {})


@pytest.mark.asyncio
async def test_profile_enforcement_requires_meta_profile_and_calls_adapter():
    adapter = _Validator()
    resources = [
        {
            "resourceType": "Patient",
            "id": "selected",
            "meta": {"profile": [PROFILE_URL]},
        },
        {"resourceType": "Patient", "id": "missing"},
        {"resourceType": "Observation", "id": "core-only"},
    ]

    report = await profile_validation.validate_profiles(
        _ctx(_config(), adapter), _config(), resources
    )

    assert report["enforced"] is True
    assert report["checked"] == 2
    assert report["passed"] == 1
    assert report["failed"] == 1
    assert report["findings"][0]["code"] == "FHIR_ACTIVE_PROFILE_NOT_DECLARED"
    assert len(adapter.calls) == 1
    assert len(adapter.calls[0]["datasets"]) == 2


@pytest.mark.asyncio
async def test_adapter_findings_are_mapped_back_to_import_indexes():
    adapter = _Validator(
        [
            {
                "dataset_index": 0,
                "severity": "error",
                "code": "FHIR_PROFILE_INVALID",
                "message": "Required identifier is missing",
                "location": "Patient.identifier",
            }
        ]
    )
    resources = [
        {"resourceType": "Observation", "id": "core-only"},
        {
            "resourceType": "Patient",
            "id": "profiled",
            "meta": {"profile": [PROFILE_URL]},
        },
    ]

    report = await profile_validation.validate_profiles(
        _ctx(_config(), adapter), _config(), resources
    )

    assert report["failed"] == 1
    assert report["findings"] == [
        {
            "index": 1,
            "severity": "error",
            "code": "FHIR_PROFILE_INVALID",
            "message": "Required identifier is missing",
            "resource_type": "Patient",
            "resource_id": "profiled",
            "source": "profile-validator",
            "path": "Patient.identifier",
        }
    ]


@pytest.mark.asyncio
async def test_disabled_profile_validation_is_explicitly_not_enforced():
    report = await profile_validation.validate_profiles(
        _ctx(_config("disabled")),
        _config("disabled"),
        [{"resourceType": "Patient", "id": "patient"}],
    )
    assert report["enforced"] is False
    assert report["checked"] == 0
    assert report["findings"] == []
