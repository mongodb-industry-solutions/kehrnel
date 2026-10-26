from __future__ import annotations

from copy import deepcopy

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.resource_store.scripts import authoritative_write
from kehrnel.engine.strategies.fhir.resource_store.scripts.strategy import (
    FHIRResourceStoreStrategy,
)


def _context() -> StrategyContext:
    return StrategyContext(
        environment_id="tenant-a",
        config={"database": "tenant_a_fhir", "schema_version": "R4"},
        bindings={"db": {"database": "tenant_a_fhir"}},
    )


def _observation() -> dict:
    return {
        "resourceType": "Observation",
        "status": "final",
        "code": {"text": "Heart rate"},
        "subject": {"reference": "Patient/patient-1"},
        "valueQuantity": {"value": 72, "unit": "/min"},
    }


@pytest.mark.asyncio
async def test_governed_fhir_create_assigns_stable_identity_and_version(monkeypatch):
    captured = {}

    async def fake_read(*_args):
        return None

    async def fake_import(_ctx, payload, **kwargs):
        captured["payload"] = deepcopy(payload)
        captured["kwargs"] = kwargs
        return {"committed": True, "write": {"inserted": 1}}

    monkeypatch.setattr(authoritative_write, "_read_current", fake_read)
    monkeypatch.setattr(authoritative_write, "fhir_import_resources", fake_import)
    strategy = FHIRResourceStoreStrategy()
    request = {
        "behavior": "create",
        "idempotencyKey": "create-observation-001",
        "interactionId": "record-observation",
        "requestPayload": {"resource": _observation()},
    }

    result = await strategy.execute_authoritative_write(_context(), request)
    repeated = await strategy.execute_authoritative_write(_context(), request)

    resource = captured["payload"]["resource"]
    assert result["resourceId"] == repeated["resourceId"] == resource["id"]
    assert result["versionUid"].endswith("/_history/1")
    assert result["etag"] == 'W/"1"'
    assert result["changeType"] == "creation"
    assert resource["meta"]["versionId"] == "1"
    assert resource["meta"]["lastUpdated"].endswith("Z")
    assert captured["payload"]["mode"] == "create"
    assert captured["kwargs"]["expected_versions"] is None
    assert captured["kwargs"]["provenance"]["operation"] == "authoritative_write"


@pytest.mark.asyncio
async def test_governed_fhir_update_requires_current_version_and_advances_it(monkeypatch):
    captured = {}

    async def fake_read(*_args):
        return {
            "resourceType": "Observation",
            "id": "obs-1",
            "meta": {"versionId": "7"},
        }

    async def fake_import(_ctx, payload, **kwargs):
        captured["payload"] = deepcopy(payload)
        captured["kwargs"] = kwargs
        return {"committed": True, "write": {"updated": 1}}

    monkeypatch.setattr(authoritative_write, "_read_current", fake_read)
    monkeypatch.setattr(authoritative_write, "fhir_import_resources", fake_import)
    resource = _observation()
    resource["id"] = "obs-1"
    result = await FHIRResourceStoreStrategy().execute_authoritative_write(
        _context(),
        {
            "behavior": "update",
            "idempotencyKey": "update-observation-001",
            "requestPayload": {
                "resource": resource,
                "ifMatch": 'W/"7"',
            },
        },
    )

    assert result["resourceId"] == "obs-1"
    assert result["version"] == "8"
    assert result["changeType"] == "modification"
    assert captured["payload"]["resource"]["meta"]["versionId"] == "8"
    assert captured["kwargs"]["expected_versions"] == {
        ("Observation", "obs-1"): "7"
    }


@pytest.mark.asyncio
async def test_governed_fhir_update_rejects_stale_version(monkeypatch):
    async def fake_read(*_args):
        return {
            "resourceType": "Observation",
            "id": "obs-1",
            "meta": {"versionId": "3"},
        }

    monkeypatch.setattr(authoritative_write, "_read_current", fake_read)
    resource = _observation()
    resource["id"] = "obs-1"
    with pytest.raises(KehrnelError) as exc:
        await FHIRResourceStoreStrategy().execute_authoritative_write(
            _context(),
            {
                "behavior": "update",
                "idempotencyKey": "update-observation-001",
                "requestPayload": {
                    "resource": resource,
                    "ifMatch": 'W/"2"',
                },
            },
        )

    assert exc.value.code == "FHIR_VERSION_CONFLICT"
    assert exc.value.status == 409
