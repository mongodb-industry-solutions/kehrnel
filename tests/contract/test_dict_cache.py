import pytest
import asyncio

from kehrnel.core.types import StrategyContext
from kehrnel.strategies.openehr.rps_dual.services.shortcuts_service import get_shortcuts
from kehrnel.strategies.openehr.rps_dual.services.codes_service import get_codes


class FakeStorage:
    def __init__(self, shortcuts=None, codes=None):
        self.shortcuts = shortcuts or {"items": {"value": "v"}, "_id": "shortcuts"}
        self.codes = codes or {"at": {"at0001": -1}, "_id": "ar_code"}
        self.calls = 0

    async def find_one(self, coll, flt, projection=None):
        self.calls += 1
        if coll == "tenant_shortcuts" and flt.get("_id") == "shortcuts":
            return self.shortcuts
        if coll == "tenant_codes" and flt.get("_id") == "ar_code":
            return self.codes
        return None


@pytest.mark.asyncio
async def test_shortcuts_cache_hits():
    storage = FakeStorage()
    ctx = StrategyContext(
        environment_id="env",
        config={"collections": {"shortcuts": {"name": "tenant_shortcuts"}}},
        adapters={"storage": storage},
        meta={"dict_cache": {}},
    )
    first = await get_shortcuts(ctx)
    second = await get_shortcuts(ctx)
    assert storage.calls == 1, "second call should hit cache, not storage"
    assert first["items"] == second["items"]


@pytest.mark.asyncio
async def test_codes_cache_hits():
    storage = FakeStorage()
    ctx = StrategyContext(
        environment_id="env",
        config={"collections": {"codes": {"name": "tenant_codes"}}},
        adapters={"storage": storage},
        meta={"dict_cache": {}},
    )
    first = await get_codes(ctx)
    second = await get_codes(ctx)
    assert storage.calls == 1
    assert first["items"] == second["items"]
    assert first["missing"] is False
