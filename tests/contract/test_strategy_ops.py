import pytest

from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.engine.core.types import StrategyContext


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


class NativeBrowseAdapter:
    def __init__(self):
        self.aggregate_results = [
            [{"_id": "ehr-1", "time_created": "2026-01-01T00:00:00Z"}],
            [{"uid": "composition-1", "template_id": "blood_pressure", "time_created": "2026-01-02T00:00:00Z"}],
        ]
        self.aggregate_calls = []
        self.find_one_calls = []

    async def aggregate(self, coll, pipeline, allow_disk_use=True):
        self.aggregate_calls.append((coll, pipeline))
        return self.aggregate_results.pop(0)

    async def find_one(self, coll, flt, projection=None):
        self.find_one_calls.append((coll, flt, projection))
        return {
            "ehr_id": "ehr-1",
            "comp_id": "composition-1",
            "tid": "blood_pressure",
            "cn": [],
        }


@pytest.mark.asyncio
async def test_run_op_ensure_dictionaries():
    cfg = load_json(DEFAULTS_PATH)
    adapter = DummyAdapter()
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"index_admin": adapter, "storage": adapter})
    res = await strat.run_op(ctx, "ensure_dictionaries", {})
    assert res["ok"] is True
    assert adapter.collections  # collections ensured
    assert res["modes"] == {"codes": "ensure", "shortcuts": "seed"}
    codes_name = (cfg.get("collections", {}) or {}).get("codes", {}).get("name", "_codes")
    shortcuts_name = (cfg.get("collections", {}) or {}).get("shortcuts", {}).get("name", "_shortcuts")
    assert codes_name in adapter.collections
    assert shortcuts_name in adapter.collections
    assert (codes_name, "ar_code") not in adapter.inserted
    assert (shortcuts_name, "shortcuts") in adapter.inserted


@pytest.mark.asyncio
async def test_run_op_invalid():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={})
    with pytest.raises(ValueError):
        await strat.run_op(ctx, "does_not_exist", {})


@pytest.mark.asyncio
async def test_run_op_native_composition_browsing():
    cfg = load_json(DEFAULTS_PATH)
    cfg["ids"] = {"ehr_id": "string", "composition_id": "string"}
    adapter = NativeBrowseAdapter()
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": adapter})

    ehrs = await strat.run_op(ctx, "list_native_ehrs", {"limit": 20})
    compositions = await strat.run_op(
        ctx,
        "list_native_compositions",
        {"ehr_id": "ehr-1", "limit": 10},
    )
    detail = await strat.run_op(
        ctx,
        "fetch_native_composition",
        {"ehr_id": "ehr-1", "uid": "composition-1"},
    )

    assert ehrs["records"] == [
        {"ehr_id": "ehr-1", "time_created": "2026-01-01T00:00:00Z"}
    ]
    assert compositions["records"] == [
        {
            "uid": "composition-1",
            "template_id": "blood_pressure",
            "time_created": "2026-01-02T00:00:00Z",
        }
    ]
    assert detail["composition"]["comp_id"] == "composition-1"
    assert adapter.find_one_calls == [
        (
            "compositions_rps",
            {"ehr_id": "ehr-1", "comp_id": "composition-1"},
            {"_id": 0},
        )
    ]


@pytest.mark.asyncio
async def test_run_op_ensure_dictionaries_can_seed_codes_explicitly():
    cfg = load_json(DEFAULTS_PATH)
    adapter = DummyAdapter()
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"index_admin": adapter, "storage": adapter})

    res = await strat.run_op(ctx, "ensure_dictionaries", {"codes": "seed", "shortcuts": "seed"})

    codes_name = (cfg.get("collections", {}) or {}).get("codes", {}).get("name", "_codes")
    shortcuts_name = (cfg.get("collections", {}) or {}).get("shortcuts", {}).get("name", "_shortcuts")
    assert res["modes"] == {"codes": "seed", "shortcuts": "seed"}
    assert (codes_name, "ar_code") in adapter.inserted
    assert (codes_name, "sequence") in adapter.inserted
    assert (shortcuts_name, "shortcuts") in adapter.inserted
