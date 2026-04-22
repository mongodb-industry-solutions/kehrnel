from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from kehrnel.api.strategies.openehr.rps_dual.ingest.models import CanonicalCompositionPayload
from kehrnel.api.strategies.openehr.rps_dual.ingest import routes as ingest_routes
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
            req.app.state.target_db = _FakeDb()
            req.app.state.source_db = _FakeDb()
            req.app.state.config = {
                "target": {
                    "database_name": "tenant_db",
                    "compositions_collection": "compositions_rps",
                    "search_collection": "compositions_search",
                },
                "source": {
                    "canonical_compositions_collection": "compositions",
                },
            }
            req.app.state.ingest_options = {"search_enabled": True}
            req.app.state.flattener = "shared-flattener"
        return context

    async def fake_ensure_active_openehr_dictionaries(req, *, context=None):
        call_order.append(("ensure_dictionaries", context))
        return False

    monkeypatch.setattr(ingest_routes, "resolve_active_openehr_context", fake_resolve_active_openehr_context)
    monkeypatch.setattr(ingest_routes, "ensure_active_openehr_dictionaries", fake_ensure_active_openehr_dictionaries)

    service = await ingest_routes.get_ingestion_service(request)

    assert service is not None
    assert service.flattener == "shared-flattener"
    assert call_order == [
        ("resolve", False),
        ("ensure_dictionaries", context),
        ("resolve", True),
    ]


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
