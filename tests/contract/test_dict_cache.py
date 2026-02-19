import pytest
import asyncio

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.services.shortcuts_service import get_shortcuts
from kehrnel.engine.strategies.openehr.rps_dual.services.codes_service import get_codes


class FakeStorage:
    def __init__(self, shortcuts=None, codes=None):
        self.shortcuts = shortcuts or {"items": {"value": "v"}, "_id": "shortcuts"}
        self.codes = codes or {"items": {"at0001": -1}, "_id": "codes"}
        self.calls = 0

    async def find_one(self, coll, flt, projection=None):
        self.calls += 1
        if coll == "_shortcuts":
            return self.shortcuts
        if coll == "_codes":
            return self.codes
        return None


@pytest.mark.asyncio
async def test_shortcuts_cache_hits():
    storage = FakeStorage()
    ctx = StrategyContext(environment_id="env", config={}, adapters={"storage": storage}, meta={"dict_cache": {}})
    first = await get_shortcuts(ctx)
    second = await get_shortcuts(ctx)
    assert storage.calls == 1, "second call should hit cache, not storage"
    assert first["items"] == second["items"]


@pytest.mark.asyncio
async def test_codes_cache_hits():
    storage = FakeStorage()
    ctx = StrategyContext(environment_id="env", config={}, adapters={"storage": storage}, meta={"dict_cache": {}})
    first = await get_codes(ctx)
    second = await get_codes(ctx)
    assert storage.calls == 1
    assert first["items"] == second["items"]
