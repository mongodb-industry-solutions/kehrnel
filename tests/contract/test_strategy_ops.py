import pytest

from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.core.types import StrategyContext


class DummyAdapter:
    def __init__(self):
        self.collections = set()
        self.inserted = {}

    async def ensure_collection(self, name):
        self.collections.add(name)

    async def find_one(self, coll, flt, projection=None):
        return self.inserted.get((coll, flt.get("_id")))

    async def insert_one(self, coll, doc):
        self.inserted[(coll, doc.get("_id"))] = doc

    async def aggregate(self, coll, pipeline, allow_disk_use=True):
        return []


@pytest.mark.asyncio
async def test_run_op_ensure_dictionaries():
    cfg = load_json(DEFAULTS_PATH)
    adapter = DummyAdapter()
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"index_admin": adapter, "storage": adapter})
    res = await strat.run_op(ctx, "ensure_dictionaries", {})
    assert res["ok"] is True
    assert adapter.collections  # collections ensured
    # ensure placeholder docs inserted (dictionary name comes from config)
    codes_name = (cfg.get("collections", {}) or {}).get("codes", {}).get("name", "_codes")
    shortcuts_name = (cfg.get("collections", {}) or {}).get("shortcuts", {}).get("name", "_shortcuts")
    # codes seed is a list of docs (defaults include _id=ar_code + sequence)
    assert (codes_name, "ar_code") in adapter.inserted
    assert (codes_name, "sequence") in adapter.inserted
    assert (shortcuts_name, "shortcuts") in adapter.inserted


@pytest.mark.asyncio
async def test_run_op_invalid():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={})
    with pytest.raises(ValueError):
        await strat.run_op(ctx, "does_not_exist", {})
