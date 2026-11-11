import asyncio
from contextvars import ContextVar
from threading import Barrier, Event
from types import SimpleNamespace

import pytest

from kehrnel.engine.core.bindings_resolver import resolve_bindings
from kehrnel.engine.core.integrations.hdl import bindings_resolver as hdl
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.fhir.resource_store.scripts import bridge, stats

ARGS = dict(bindings_ref="test:env", env_id="env-1", domain="fhir", strategy_id="fhir.resource_store", op="op")


@pytest.mark.asyncio
async def test_sync_secret_lookup_keeps_event_loop_responsive_and_request_context():
    started, release = Event(), Event()
    request = ContextVar("request")
    token = request.set("owner-1")

    def resolver(**kwargs):
        started.set()
        assert release.wait(2)
        return {"owner": request.get()}

    task = asyncio.create_task(resolve_bindings(resolver, **ARGS))
    try:
        assert await asyncio.to_thread(started.wait, 1)
        await asyncio.wait_for(asyncio.sleep(0), timeout=0.2)
        assert not task.done()
    finally:
        release.set()
        request.reset(token)
    assert await task == {"owner": "owner-1"}


@pytest.mark.asyncio
async def test_native_async_and_sync_wrapped_async_resolvers_are_supported():
    async def resolver(**kwargs):
        await asyncio.sleep(0)
        return {"env": kwargs["env_id"]}

    assert await resolve_bindings(resolver, **ARGS) == {"env": "env-1"}
    assert await resolve_bindings(lambda **kw: resolver(**kw), **ARGS) == {"env": "env-1"}
    with pytest.raises(TypeError, match="dict or None"):
        await resolve_bindings(lambda **kw: "invalid", **ARGS)


def test_owner_database_checks_are_parallel_live_and_fail_closed():
    barrier = Barrier(3, timeout=2)
    names = {"teams": "team-core", "users": "user-core", "managed_environment_snapshots": "managed-core"}

    class Collection:
        def __init__(self, name): self.name = name
        def aggregate(self, pipeline):
            barrier.wait()
            return [{"database": names[self.name]}]
        def find(self, query, projection):
            barrier.wait()
            return [{"dbName": names[self.name]}]

    class Db:
        def __getitem__(self, name): return Collection(name)

    store = SimpleNamespace(db=Db())
    assert hdl._environment_database_names(store, "env-1") == set(names.values())
    names["users"] = "new-user-core"
    assert hdl._environment_database_names(store, "env-1") == set(names.values())

    class BrokenDb:
        def __getitem__(self, name): raise RuntimeError("owner lookup failed")
    with pytest.raises(RuntimeError, match="owner lookup failed"):
        hdl._environment_database_names(SimpleNamespace(db=BrokenDb()), "env-1")


@pytest.mark.parametrize("value,expected", [(True, True), (False, False), (None, False), ("true", False), (1, False)])
def test_only_strict_compile_only_operation_skips_data_store_checks(value, expected):
    assert hdl._package_only_operation({"payload": {"op": "fhir_search", "payload": {"explain_only": value}}}) is expected
    assert hdl._package_only_operation({"payload": {"op": "fhir_native_query", "payload": {"explain_only": True}}}) is False
    assert hdl._package_only_operation({"payload": {"op": "fhir_index_manifest"}}) is True


@pytest.mark.asyncio
async def test_summary_reads_only_collection_estimates_and_never_claims_readiness(monkeypatch):
    calls = []
    class Collection:
        def estimated_document_count(self): calls.append("estimate"); return 42
        def count_documents(self, *args): raise AssertionError("summary scanned documents")
        def distinct(self, *args): raise AssertionError("summary scanned provenance")
        def list_indexes(self): raise AssertionError("summary scanned indexes")
    class Db:
        def list_collection_names(self): return ["fhir_Patient", "unrelated_metadata"]
        def __getitem__(self, name):
            assert name == "fhir_Patient"
            return Collection()
    context = SimpleNamespace(db=Db(), config_loader=SimpleNamespace(list_resources=lambda: ["Patient", "Condition"]), owns_client=False)
    monkeypatch.setattr(bridge, "resolve_mongo", lambda ctx: ("unused", "fhir-data", "fhir_"))
    monkeypatch.setattr(bridge, "build_mql_context", lambda *a, **kw: context)
    monkeypatch.setattr(bridge, "known_generation_resource_types", lambda: {"Patient", "Condition"})
    monkeypatch.setattr(stats, "build_projection_versions", lambda *a, **kw: pytest.fail("summary audited projections"))
    ctx = StrategyContext(environment_id="env-1", config={"database": "fhir-data"}, bindings={}, manifest={})
    result = await stats.fhir_stats(ctx, {"summary_only": True})
    assert calls == ["estimate"]
    assert result["summary"]["document_count"] == 42
    assert result["summary"]["initialized_resource_type_count"] == 1
    assert result["summary"]["synthetic_count"] is None
    assert result["summary"]["imported_count"] is None
    assert result["count_accuracy"] == "estimated"
    assert result["index_verification"] == "not_scanned"
    assert result["collections"] == []
    assert "projection_readiness_receipt" not in result


@pytest.mark.asyncio
async def test_detailed_stats_wait_does_not_block_unrelated_requests(monkeypatch):
    started, release = Event(), Event()
    def diagnostics(ctx, payload):
        started.set()
        assert release.wait(2)
        return {"ok": True}
    monkeypatch.setattr(stats, "_fhir_stats_sync", diagnostics)
    task = asyncio.create_task(stats.fhir_stats(None, {}))
    try:
        assert await asyncio.to_thread(started.wait, 1)
        await asyncio.wait_for(asyncio.sleep(0), 0.2)
        assert not task.done()
    finally:
        release.set()
    assert await task == {"ok": True}


def test_index_manifest_does_not_construct_a_database_client(monkeypatch):
    from kehrnel.engine.strategies.fhir.resource_store.scripts import indexes
    from kehrnel.engine.strategies.fhir.resource_store.strategy import MANIFEST
    monkeypatch.setattr(bridge, "get_mongo_client", lambda *a: pytest.fail("metadata opened a data client"))
    ctx = StrategyContext(environment_id="env-1", config={"database": "fhir-data"}, bindings={"db": {"provider": "mongodb", "uri": "mongodb://localhost:27017", "database": "fhir-data"}}, manifest=MANIFEST)
    result = indexes.fhir_index_manifest(ctx, {"resource_types": ["Patient"]})
    assert result["collections"][0]["resource_type"] == "Patient"
