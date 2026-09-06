"""Adapters must not be shared across event loops.

Motor binds a client to the loop that first runs an operation on it, so a client
cached on the API loop and reused from a synthetic job's throwaway worker loop
fails with "got Future attached to a different loop".
"""
import asyncio

import pytest

from kehrnel.engine.core.runtime import StrategyRuntime


@pytest.fixture(autouse=True)
def _preserve_thread_event_loop():
    """Keep asyncio.run() here from leaking into other tests.

    asyncio.run() closes its loop and unsets the thread's event loop, which breaks
    later tests that still call the deprecated asyncio.get_event_loop().
    """
    try:
        previous = asyncio.get_event_loop_policy().get_event_loop()
    except RuntimeError:
        previous = None
    try:
        yield
    finally:
        asyncio.set_event_loop(previous)


BINDINGS = {
    "db": {
        "provider": "mongodb",
        "uri": "mongodb://localhost:27017",
        "database": "kehrnel_test",
    }
}


def _runtime() -> StrategyRuntime:
    return StrategyRuntime(registry=None)


async def _build_for(runtime: StrategyRuntime) -> dict:
    """Build adapters from inside the running loop."""
    return runtime._build_adapters("env-1", BINDINGS)


def test_adapters_are_reused_within_one_loop():
    runtime = _runtime()

    async def main():
        first = runtime._build_adapters("env-1", BINDINGS)
        second = runtime._build_adapters("env-1", BINDINGS)
        return first, second

    first, second = asyncio.run(main())
    assert first is second
    assert first["storage"].db.client is second["storage"].db.client


def test_adapters_are_isolated_by_strategy_database_within_one_environment():
    runtime = _runtime()

    async def main():
        fhir = runtime._build_adapters("env-1", BINDINGS)
        openehr_bindings = {
            "db": {**BINDINGS["db"], "database": "kehrnel_openehr"}
        }
        openehr = runtime._build_adapters("env-1", openehr_bindings)
        return fhir, openehr

    fhir, openehr = asyncio.run(main())
    assert fhir is not openehr
    assert fhir["storage"].db.name == "kehrnel_test"
    assert openehr["storage"].db.name == "kehrnel_openehr"


def test_adapters_are_not_shared_across_loops():
    runtime = _runtime()

    def build_in_new_loop():
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_build_for(runtime))
        finally:
            loop.close()

    async def main():
        api_adapters = runtime._build_adapters("env-1", BINDINGS)
        worker_adapters = await asyncio.to_thread(build_in_new_loop)
        return api_adapters, worker_adapters

    api_adapters, worker_adapters = asyncio.run(main())
    assert api_adapters is not worker_adapters
    assert api_adapters["storage"].db.client is not worker_adapters["storage"].db.client


def test_dead_loop_entries_are_pruned():
    runtime = _runtime()

    def build_in_new_loop():
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_build_for(runtime))
        finally:
            loop.close()

    async def main():
        # Two sequential jobs, each on its own short-lived loop.
        first = await asyncio.to_thread(build_in_new_loop)
        second = await asyncio.to_thread(build_in_new_loop)
        return first, second

    first, second = asyncio.run(main())
    assert first is not second
    # The first (now-closed) loop's entry must not linger in the cache.
    cache = runtime._env_cache["env-1"]["adapters_by_loop"]
    assert len(cache) == 1
