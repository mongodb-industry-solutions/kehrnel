from __future__ import annotations

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from kehrnel.api.strategies.openehr.rps_dual.ingest.models import CanonicalCompositionPayload
from kehrnel.api.strategies.openehr.rps_dual.ingest import routes as ingest_routes
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener


def _payload() -> dict:
    return {
        "_id": "comp-1",
        "ehr_id": "ehr-1",
        "canonicalJSON": {
            "_type": "COMPOSITION",
            "name": {"_type": "DV_TEXT", "value": "Example"},
            "archetype_details": {
                "archetype_id": {"value": "openEHR-EHR-COMPOSITION.example.v1"},
                "template_id": {"value": "Example Template"},
                "rm_version": "1.0.4",
            },
        },
    }


@pytest.mark.asyncio
async def test_ingest_from_payload_uses_service_fast_path_without_runtime_overrides(monkeypatch):
    payload_dict = _payload()
    request = SimpleNamespace(
        query_params={},
        headers={},
        app=SimpleNamespace(state=SimpleNamespace()),
    )
    service = SimpleNamespace(
        flattener=object(),
        repository=object(),
        ingest_from_payload=AsyncMock(return_value="flattened-123"),
    )
    runtime_builder = AsyncMock(side_effect=AssertionError("runtime flattener should not be rebuilt"))
    monkeypatch.setattr(ingest_routes, "get_runtime_flattener", runtime_builder)

    response = await ingest_routes.ingest_from_payload(
        payload=CanonicalCompositionPayload.model_validate(payload_dict),
        service=service,
        request=request,
    )

    assert response.flattened_composition_id == "flattened-123"
    service.ingest_from_payload.assert_awaited_once_with(payload_dict)
    runtime_builder.assert_not_called()


class _StrategyRegistry:
    def __init__(self, manifests):
        self.manifests = manifests

    def get_manifest(self, strategy_id):
        return self.manifests.get(strategy_id)


class _StrategyRuntime:
    def __init__(self, manifests):
        self.registry = _StrategyRegistry(manifests)
        self.calls = []

    async def dispatch(self, environment_id, operation, payload):
        self.calls.append((environment_id, operation, deepcopy(payload)))
        return {
            "inserted": {"base": f"flat-{environment_id}", "search": f"search-{environment_id}"},
            "base": {"_id": f"base-{environment_id}", "ehr_id": payload["ehr_id"]},
            "search": {
                "_id": f"search-{environment_id}",
                "sn": [{"p": "at0001", "data": "value"}],
            },
            "meta": {},
        }


def _routed_request(
    runtime,
    *,
    environment_id="tenant-a",
    requested_strategy_id="openehr.rps_dual",
    active_strategy_id="openehr.rps_dual",
    extra_headers=None,
):
    activation = SimpleNamespace(
        env_id=environment_id,
        domain="openehr",
        strategy_id=active_strategy_id,
        activation_id=f"activation-{environment_id}",
        config_hash=f"config-{environment_id}",
        manifest_digest=f"manifest-{active_strategy_id}",
        bindings_hash=f"bindings-{environment_id}",
    )
    headers = {"x-strategy-id": requested_strategy_id} if requested_strategy_id else {}
    headers.update(extra_headers or {})
    request_runtime = SimpleNamespace(dispatch=AsyncMock(side_effect=AssertionError("app-global runtime must not be used")))
    return SimpleNamespace(
        query_params={},
        headers=headers,
        app=SimpleNamespace(state=SimpleNamespace(strategy_runtime=request_runtime)),
        state=SimpleNamespace(
            _openehr_context={
                "domain": "openehr",
                "env_id": environment_id,
                "runtime": runtime,
                "activation": activation,
                "data_target_digest": f"sha256:{environment_id:0<64}"[:71],
            }
        ),
    )


def _routed_service(result_id):
    return SimpleNamespace(
        flattener=SimpleNamespace(
            transform_composition=lambda _payload: (_ for _ in ()).throw(
                AssertionError("request-scoped runtime transform must be used")
            )
        ),
        repository=SimpleNamespace(
            insert_flattened_composition_in_transaction=AsyncMock(
                return_value=result_id
            )
        ),
    )


@pytest.mark.asyncio
async def test_strategy_routed_ingest_uses_request_environment_and_activation_snapshot():
    manifest = SimpleNamespace(domain="openEHR", capabilities=["ingest"])
    runtime = _StrategyRuntime({"openehr.rps_dual": manifest})
    request = _routed_request(
        runtime,
        extra_headers={"x-strategy-protocol": "openEHR"},
    )
    service = _routed_service("stored-a")

    response = await ingest_routes.ingest_from_payload(
        payload=CanonicalCompositionPayload.model_validate(_payload()),
        service=service,
        request=request,
    )

    assert response.flattened_composition_id == "base-tenant-a"
    assert len(runtime.calls) == 1
    environment_id, operation, dispatched = runtime.calls[0]
    assert environment_id == "tenant-a"
    assert operation == "ingest"
    assert dispatched["domain"] == "openehr"
    assert dispatched["strategy_id"] == "openehr.rps_dual"
    assert dispatched["strategy_protocol"] == "openehr"
    assert dispatched["__activation_precondition"] == {
        "activationId": "activation-tenant-a",
        "strategyId": "openehr.rps_dual",
        "configHash": "config-tenant-a",
        "manifestDigest": "manifest-openehr.rps_dual",
        "bindingsHash": "bindings-tenant-a",
        "dataTargetDigest": request.state._openehr_context["data_target_digest"],
    }
    service.repository.insert_flattened_composition_in_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_strategy_routed_ingest_rejects_unknown_strategy_without_fallback():
    runtime = _StrategyRuntime({})
    request = _routed_request(
        runtime,
        requested_strategy_id="openehr.unknown",
    )
    service = _routed_service("not-stored")

    with pytest.raises(KehrnelError) as caught:
        await ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(_payload()),
            service=service,
            request=request,
        )

    assert caught.value.code == "STRATEGY_NOT_FOUND"
    assert caught.value.status == 404
    assert runtime.calls == []
    service.repository.insert_flattened_composition_in_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_strategy_routed_ingest_rejects_non_active_strategy():
    manifest = SimpleNamespace(domain="openEHR", capabilities=["ingest"])
    runtime = _StrategyRuntime(
        {
            "openehr.rps_dual": manifest,
            "openehr.rps_dual_ibm": manifest,
        }
    )
    request = _routed_request(
        runtime,
        requested_strategy_id="openehr.rps_dual_ibm",
        active_strategy_id="openehr.rps_dual",
    )
    service = _routed_service("not-stored")

    with pytest.raises(KehrnelError) as caught:
        await ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(_payload()),
            service=service,
            request=request,
        )

    assert caught.value.code == "OPENEHR_INGEST_ACTIVATION_MISMATCH"
    assert caught.value.status == 409
    assert runtime.calls == []
    service.repository.insert_flattened_composition_in_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_strategy_routed_ingest_keeps_concurrent_tenants_isolated():
    manifest = SimpleNamespace(domain="openEHR", capabilities=["ingest"])
    runtime = _StrategyRuntime({"openehr.rps_dual": manifest})
    request_a = _routed_request(runtime, environment_id="tenant-a")
    request_b = _routed_request(runtime, environment_id="tenant-b")
    service_a = _routed_service("stored-a")
    service_b = _routed_service("stored-b")

    response_a, response_b = await asyncio.gather(
        ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(_payload()),
            service=service_a,
            request=request_a,
        ),
        ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(_payload()),
            service=service_b,
            request=request_b,
        ),
    )

    assert {response_a.flattened_composition_id, response_b.flattened_composition_id} == {
        "base-tenant-a",
        "base-tenant-b",
    }
    assert {call[0] for call in runtime.calls} == {"tenant-a", "tenant-b"}
    for environment_id, _operation, dispatched in runtime.calls:
        assert dispatched["__activation_precondition"]["activationId"] == (
            f"activation-{environment_id}"
        )
        assert dispatched["__activation_precondition"]["dataTargetDigest"] == (
            request_a.state._openehr_context["data_target_digest"]
            if environment_id == "tenant-a"
            else request_b.state._openehr_context["data_target_digest"]
        )
    service_a.repository.insert_flattened_composition_in_transaction.assert_not_awaited()
    service_b.repository.insert_flattened_composition_in_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_strategy_protocol_without_strategy_id_is_rejected():
    request = SimpleNamespace(
        query_params={"strategy_protocol": "canonical-json"},
        headers={},
        app=SimpleNamespace(state=SimpleNamespace()),
        state=SimpleNamespace(),
    )
    service = _routed_service("not-stored")

    with pytest.raises(KehrnelError) as caught:
        await ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(_payload()),
            service=service,
            request=request,
        )

    assert caught.value.code == "OPENEHR_INGEST_STRATEGY_REQUIRED"
    assert caught.value.status == 400


@pytest.mark.asyncio
async def test_strategy_routed_ingest_rejects_per_request_config():
    manifest = SimpleNamespace(domain="openEHR", capabilities=["ingest"])
    runtime = _StrategyRuntime({"openehr.rps_dual": manifest})
    request = _routed_request(runtime)
    service = _routed_service("not-stored")
    payload = _payload()
    payload["config"] = {"fields": {"document": {"comp_id": "other_id"}}}

    with pytest.raises(KehrnelError) as caught:
        await ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(payload),
            service=service,
            request=request,
        )

    assert caught.value.code == "OPENEHR_INGEST_CONFIG_CONFLICT"
    assert caught.value.status == 400
    assert runtime.calls == []
    service.repository.insert_flattened_composition_in_transaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_strategy_routed_ingest_rejects_unsupported_protocol():
    manifest = SimpleNamespace(domain="openEHR", capabilities=["ingest"])
    runtime = _StrategyRuntime({"openehr.rps_dual": manifest})
    request = _routed_request(
        runtime,
        extra_headers={"x-strategy-protocol": "fhir"},
    )
    service = _routed_service("not-stored")

    with pytest.raises(KehrnelError) as caught:
        await ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(_payload()),
            service=service,
            request=request,
        )

    assert caught.value.code == "OPENEHR_INGEST_PROTOCOL_UNSUPPORTED"
    assert caught.value.status == 400
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_strategy_routed_ingest_rejects_conflicting_strategy_selectors():
    manifest = SimpleNamespace(domain="openEHR", capabilities=["ingest"])
    runtime = _StrategyRuntime(
        {
            "openehr.rps_dual": manifest,
            "openehr.rps_dual_ibm": manifest,
        }
    )
    request = _routed_request(runtime)
    request.query_params = {"strategy_id": "openehr.rps_dual_ibm"}
    service = _routed_service("not-stored")

    with pytest.raises(KehrnelError) as caught:
        await ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(_payload()),
            service=service,
            request=request,
        )

    assert caught.value.code == "INGEST_ROUTING_SELECTOR_CONFLICT"
    assert caught.value.status == 400
    assert runtime.calls == []


@pytest.mark.asyncio
async def test_ingest_from_payload_preserves_http_exception_status():
    request = SimpleNamespace(
        query_params={},
        headers={},
        app=SimpleNamespace(state=SimpleNamespace()),
        state=SimpleNamespace(),
    )

    with pytest.raises(HTTPException) as caught:
        await ingest_routes.ingest_from_payload(
            payload=CanonicalCompositionPayload.model_validate(_payload()),
            service=None,
            request=request,
        )

    assert caught.value.status_code == 503


@pytest.mark.asyncio
async def test_get_ingestion_service_bootstraps_dictionaries_before_warming_runtime(monkeypatch):
    request = SimpleNamespace(
        query_params={},
        headers={},
        app=SimpleNamespace(state=SimpleNamespace()),
        state=SimpleNamespace(),
    )
    call_order = []
    context = {"activation": object(), "database_name": "tenant_db"}

    async def fake_resolve_active_openehr_context(req, ensure_ingestion=False):
        call_order.append(("resolve", ensure_ingestion))
        if ensure_ingestion:
            context["target_db"] = _FakeDb()
            context["source_db"] = _FakeDb()
            context["ingest_config"] = {
                "target": {
                    "database_name": "tenant_db",
                    "compositions_collection": "compositions_rps",
                    "search_collection": "compositions_search",
                },
                "source": {
                    "canonical_compositions_collection": "compositions",
                },
            }
            context["ingest_options"] = {"search_enabled": True}
            context["flattener"] = "request-flattener"
        return context

    async def fake_ensure_active_openehr_dictionaries(req, *, context=None):
        call_order.append(("ensure_dictionaries", context))
        return False

    monkeypatch.setattr(ingest_routes, "resolve_active_openehr_context", fake_resolve_active_openehr_context)
    monkeypatch.setattr(ingest_routes, "ensure_active_openehr_dictionaries", fake_ensure_active_openehr_dictionaries)

    service = await ingest_routes.get_ingestion_service(request)

    assert service is not None
    assert service.flattener == "request-flattener"
    assert call_order == [
        ("resolve", False),
        ("ensure_dictionaries", context),
        ("resolve", True),
    ]


@pytest.mark.asyncio
async def test_runtime_flattener_uses_request_scoped_database_and_mappings(monkeypatch):
    tenant_db = object()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                target_db="foreign-db",
                mappings_content={"mapping": "foreign"},
            )
        ),
        state=SimpleNamespace(
            _openehr_context={
                "target_db": tenant_db,
                "flattener": SimpleNamespace(
                    config={
                        "target": {
                            "codes_collection": "codes-a",
                            "shortcuts_collection": "shortcuts-a",
                            "compositions_collection": "flat-a",
                            "search_collection": "search-a",
                        }
                    }
                ),
                "mappings_path": "/tenant-a/mappings.jsonc",
                "mappings_content": {"mapping": "tenant-a"},
            }
        ),
    )
    captured = {}

    async def fake_create(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(config=kwargs["config"])

    monkeypatch.setattr(CompositionFlattener, "create", staticmethod(fake_create))

    await ingest_routes.get_runtime_flattener(
        request,
        {"target": {"search_collection": "search-a-preview"}},
    )

    assert captured["db"] is tenant_db
    assert captured["mappings_path"] == "/tenant-a/mappings.jsonc"
    assert captured["mappings_content"] == {"mapping": "tenant-a"}
    assert captured["config"]["target"] == {
        "codes_collection": "codes-a",
        "shortcuts_collection": "shortcuts-a",
        "compositions_collection": "flat-a",
        "search_collection": "search-a-preview",
    }


class _FakeCodesCollection:
    def __init__(self):
        self.replace_calls = []

    async def replace_one(self, filter_doc, replacement, upsert=False):
        self.replace_calls.append(
            {
                "filter": deepcopy(filter_doc),
                "replacement": deepcopy(replacement),
                "upsert": upsert,
            }
        )


class _FakeDb:
    def __init__(self):
        self.collections = {}
        self.client = object()

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = _FakeCodesCollection()
        return self.collections[name]


@pytest.mark.asyncio
async def test_flattener_flush_codes_skips_noop_writes():
    db = _FakeDb()
    flattener = CompositionFlattener(
        db=db,
        config={"target": {"codes_collection": "_codes"}},
        mappings_path="unused",
        mappings_content={"templates": []},
    )

    await flattener.flush_codes_to_db()
    assert db["_codes"].replace_calls == []

    flattener._alloc_code("ar_code", "openEHR-EHR-OBSERVATION.height.v1")
    await flattener.flush_codes_to_db()
    assert len(db["_codes"].replace_calls) == 1

    await flattener.flush_codes_to_db()
    assert len(db["_codes"].replace_calls) == 1
