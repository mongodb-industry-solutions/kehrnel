"""Behavioral regressions for corruption discovered during WIP recovery."""
from collections import Counter
from copy import deepcopy
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pymongo.errors import DuplicateKeyError

from kehrnel.api.domains.openehr.composition import service
from kehrnel.api.domains.openehr.composition.models import CompositionCreate
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy

admin = import_module("kehrnel.api.core.admin.routes")


def test_control_api_registers_each_route_once():
    counts = Counter(
        (route.path, method)
        for route in admin.router.routes
        for method in route.methods
    )
    assert all(count == 1 for count in counts.values())


@pytest.mark.asyncio
async def test_catalog_instantiation_executes_for_matching_reference(monkeypatch):
    catalog = object()
    result = object()
    body = SimpleNamespace(reference_architecture_ref=SimpleNamespace(id="architecture-1"))
    monkeypatch.setattr(admin, "load_bundled_industry_outcome_catalog", lambda: catalog)
    instantiate = lambda request, *, catalog: result
    monkeypatch.setattr(admin, "instantiate_reference_architecture", instantiate)

    assert await admin.instantiate_catalog_reference_architecture("architecture-1", body) is result


@pytest.mark.asyncio
async def test_operational_probe_dispatch_forwards_freshness_payload():
    strategy = RPSDualStrategy()
    strategy._operational_probe = AsyncMock(return_value={"state": "healthy"})
    context = StrategyContext(environment_id="sandbox", config={}, adapters={})
    payload = {"freshness_target_seconds": 30}

    assert await strategy.run_op(context, "operational_probe", payload) == {"state": "healthy"}
    assert strategy._operational_probe.await_args.args[2] is payload


@pytest.mark.asyncio
async def test_unavailable_probe_includes_unique_freshness_checks():
    strategy = RPSDualStrategy()
    context = StrategyContext(environment_id="sandbox", config={}, adapters={})

    result = await strategy.run_op(context, "operational_probe", {})
    checks = [check["checkId"] for check in result["checks"]]
    assert len(checks) == len(set(checks))
    assert {"data_freshness", "reconciliation_integrity"} <= set(checks)


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_content_matches", [True, False])
async def test_idempotent_correction_replays_matching_content_and_rejects_conflict(
    monkeypatch, stored_content_matches
):
    uid = "composition-1::my-openehr-server::2"
    payload = {
        "_type": "COMPOSITION",
        "archetype_details": {"template_id": {"value": "test-template"}},
        "name": {"_type": "DV_TEXT", "value": "Example"},
    }
    expected = service._materialize_versioned_composition_payload(payload, uid)
    stored = {
        "_id": uid,
        "time_created": "2026-10-04T00:00:00Z",
        "data": expected if stored_content_matches else {"_type": "COMPOSITION"},
    }
    collection = SimpleNamespace(find_one=AsyncMock(return_value=deepcopy(stored)))
    db = {"compositions": collection}
    monkeypatch.setattr(service, "retrieve_composition", AsyncMock(return_value=object()))
    insert = AsyncMock()
    monkeypatch.setattr(service, "insert_composition_contribution_and_update_ehr", insert)

    async def correct():
        return await service.update_composition(
            ehr_id="ehr-1",
            preceding_version_uid="composition-1::my-openehr-server::1",
            if_match='"composition-1::my-openehr-server::1"',
            new_composition_data=CompositionCreate.model_validate(payload),
            db=db,
            config=SimpleNamespace(compositions="compositions"),
            flattener=object(),
            idempotency_key="correction-1",
        )

    if stored_content_matches:
        result = await correct()
        assert result.uid == uid
        assert result.data == expected
    else:
        with pytest.raises(HTTPException) as error:
            await correct()
        assert error.value.status_code == 409
    insert.assert_not_awaited()
    collection.find_one.assert_awaited_once_with({"_id": uid})


@pytest.mark.asyncio
async def test_concurrent_identical_correction_returns_committed_version(monkeypatch):
    uid = "composition-1::my-openehr-server::2"
    payload = {
        "_type": "COMPOSITION",
        "archetype_details": {"template_id": {"value": "test-template"}},
        "name": {"_type": "DV_TEXT", "value": "Example"},
    }
    expected = service._materialize_versioned_composition_payload(payload, uid)
    stored = {"_id": uid, "time_created": "2026-10-04T00:00:00Z", "data": expected}
    collection = SimpleNamespace(find_one=AsyncMock(side_effect=[None, stored]))
    monkeypatch.setattr(service, "retrieve_composition", AsyncMock(return_value=object()))
    monkeypatch.setattr(
        service,
        "insert_composition_contribution_and_update_ehr",
        AsyncMock(side_effect=DuplicateKeyError("concurrent correction committed")),
    )
    flattener = SimpleNamespace(transform_composition=lambda doc: ({}, {}))

    result = await service.update_composition(
        ehr_id="ehr-1",
        preceding_version_uid="composition-1::my-openehr-server::1",
        if_match='"composition-1::my-openehr-server::1"',
        new_composition_data=CompositionCreate.model_validate(payload),
        db={"compositions": collection},
        config=SimpleNamespace(compositions="compositions"),
        flattener=flattener,
        idempotency_key="correction-1",
    )
    assert result.uid == uid
    assert collection.find_one.await_count == 2


def test_compatibility_routes_share_one_documented_operation(tmp_path):
    from fastapi.testclient import TestClient
    from kehrnel.api.app import create_app

    app = create_app(str(tmp_path / "registry.json"))
    schema = app.openapi()
    operation_ids = [
        operation["operationId"]
        for path in schema["paths"].values()
        for operation in path.values()
        if isinstance(operation, dict) and "operationId" in operation
    ]
    assert len(operation_ids) == len(set(operation_ids))
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    assert client.get("/v1/health").status_code == 200


@pytest.mark.parametrize("path", ["/sources/0", "/preferredStrategyIds/0"])
def test_canonical_blueprint_patch_rejects_legacy_roots(path):
    from pydantic import ValidationError
    from kehrnel.contracts.source_architecture import BlueprintOverride

    with pytest.raises(ValidationError, match="canonical Blueprint"):
        BlueprintOverride(overrideId="remove-legacy", operation="remove", path=path, reason="Remove")


@pytest.mark.parametrize("path", ["/decisions/0", "/preferredFoundationRefs/0"])
def test_legacy_blueprint_patch_rejects_canonical_roots(path):
    from pydantic import ValidationError
    from kehrnel.contracts.source_architecture import LegacyBlueprintOverride

    with pytest.raises(ValidationError, match="legacy Blueprint"):
        LegacyBlueprintOverride(overrideId="remove-canonical", operation="remove", path=path, reason="Remove")
