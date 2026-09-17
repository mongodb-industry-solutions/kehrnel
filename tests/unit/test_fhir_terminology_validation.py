import json

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.resource_store.scripts.terminology_validation import (
    _bound_codings,
    validate_config,
    validate_resources,
)


class _TerminologyAdapter:
    def __init__(self, valid_codes=None):
        self.valid_codes = set(valid_codes or [])
        self.calls = []

    async def execute(self, operation, payload):
        self.calls.append((operation, payload))
        return {
            "ok": True,
            "valid": payload["code"] in self.valid_codes,
            "message": None if payload["code"] in self.valid_codes else "Unknown code",
            "provider": {"id": "snomed-local", "type": "kehrnel-strategy"},
            "releaseId": "20260131",
        }


def _config(mode="required"):
    return {
        "terminology_validation": {
            "mode": mode,
            "systems": ["http://snomed.info/sct"],
        }
    }


def _profile_config(tmp_path, mode="required"):
    package = tmp_path / "condition-ig" / "package"
    package.mkdir(parents=True)
    (package / "package.json").write_text(
        json.dumps(
            {
                "name": "example.condition.ig",
                "version": "1.0.0",
                "fhirVersions": ["5.0.0"],
            }
        ),
        encoding="utf-8",
    )
    profile_url = "https://example.test/fhir/StructureDefinition/condition"
    (package / "StructureDefinition-condition.json").write_text(
        json.dumps(
            {
                "resourceType": "StructureDefinition",
                "url": profile_url,
                "version": "1.0.0",
                "type": "Condition",
                "kind": "resource",
                "derivation": "constraint",
                "differential": {
                    "element": [
                        {
                            "id": "Condition.code",
                            "path": "Condition.code",
                            "binding": {
                                "strength": "required",
                                "valueSet": "https://example.test/fhir/ValueSet/condition-codes|1.0.0",
                            },
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    return {
        "schema_version": "R5",
        "implementation_guides": {
            "compiled_root": str(tmp_path / "compiled"),
            "packages": [{"source": str(package.parent)}],
            "active_profiles": [profile_url],
        },
        "terminology_validation": {
            "mode": mode,
            "systems": ["http://snomed.info/sct"],
        },
    }


@pytest.mark.asyncio
async def test_required_fhir_terminology_validation_fails_only_configured_systems():
    adapter = _TerminologyAdapter(valid_codes={"73211009"})
    ctx = StrategyContext(
        environment_id="tenant-a",
        config=_config(),
        adapters={"terminology": adapter},
    )
    resources = [
        {
            "resourceType": "Condition",
            "id": "condition-1",
            "code": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "73211009",
                        "display": "Diabetes mellitus",
                    },
                    {"system": "http://loinc.org", "code": "ignored"},
                ]
            },
        },
        {
            "resourceType": "Condition",
            "id": "condition-2",
            "code": {
                "coding": [
                    {"system": "http://snomed.info/sct", "code": "not-a-code"}
                ]
            },
        },
    ]

    report = await validate_resources(ctx, _config(), resources)

    assert report["checked"] == 2
    assert report["valid"] == 1
    assert report["invalid"] == 1
    assert report["failed_resource_indexes"] == [1]
    assert report["findings"][0]["path"] == "code.coding[0]"
    assert report["findings"][0]["severity"] == "error"
    assert len(adapter.calls) == 2


@pytest.mark.asyncio
async def test_advisory_fhir_terminology_validation_does_not_reject_resource():
    adapter = _TerminologyAdapter()
    ctx = StrategyContext(
        environment_id="tenant-a",
        config=_config("advisory"),
        adapters={"terminology": adapter},
    )
    resource = {
        "resourceType": "Condition",
        "id": "condition-1",
        "code": {
            "coding": [{"system": "http://snomed.info/sct", "code": "missing"}]
        },
    }

    report = await validate_resources(ctx, _config("advisory"), [resource])

    assert report["enforced"] is False
    assert report["failed_resource_indexes"] == [0]
    assert report["findings"][0]["severity"] == "warning"


def test_enabled_validation_requires_explicit_system_scope():
    with pytest.raises(ValueError, match="at least one canonical system"):
        validate_config({"terminology_validation": {"mode": "required"}})


def test_profile_binding_path_supports_primitive_code_values():
    assert _bound_codings(
        {"resourceType": "Patient", "gender": "female"},
        {
            "resource_type": "Patient",
            "path": "Patient.gender",
            "value_set": "http://hl7.org/fhir/ValueSet/administrative-gender",
        },
        systems={"http://snomed.info/sct"},
        limit=10,
    ) == [{"code": "female", "path": "gender"}]


@pytest.mark.asyncio
async def test_required_profile_binding_accepts_when_any_coding_is_in_value_set(tmp_path):
    config = _profile_config(tmp_path)
    adapter = _TerminologyAdapter(valid_codes={"member"})
    ctx = StrategyContext(
        environment_id="tenant-a",
        config=config,
        adapters={"terminology": adapter},
    )
    resource = {
        "resourceType": "Condition",
        "id": "condition-1",
        "meta": {"profile": ["https://example.test/fhir/StructureDefinition/condition"]},
        "code": {
            "coding": [
                {"system": "http://snomed.info/sct", "code": "not-member"},
                {"system": "http://snomed.info/sct", "code": "member"},
            ]
        },
    }

    report = await validate_resources(ctx, config, [resource])

    assert report["binding_checks"] == 1
    assert report["binding_passed"] == 1
    assert report["binding_failed"] == 0
    assert report["failed_resource_indexes"] == []
    assert {call[1]["url"] for call in adapter.calls} == {
        "https://example.test/fhir/ValueSet/condition-codes"
    }
    assert {call[1]["value_set_version"] for call in adapter.calls} == {"1.0.0"}


@pytest.mark.asyncio
async def test_required_profile_binding_rejects_when_no_coding_is_in_value_set(tmp_path):
    config = _profile_config(tmp_path)
    adapter = _TerminologyAdapter()
    ctx = StrategyContext(
        environment_id="tenant-a",
        config=config,
        adapters={"terminology": adapter},
    )
    resource = {
        "resourceType": "Condition",
        "id": "condition-1",
        "code": {
            "coding": [
                {"system": "http://snomed.info/sct", "code": "not-member"}
            ]
        },
    }

    report = await validate_resources(ctx, config, [resource])

    assert report["binding_failed"] == 1
    assert report["failed_resource_indexes"] == [0]
    assert report["findings"][0]["code"] == "FHIR_TERMINOLOGY_BINDING_NOT_SATISFIED"
    assert report["findings"][0]["value_set"] == "https://example.test/fhir/ValueSet/condition-codes"
