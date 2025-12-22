import pytest

from kehrnel.core.types import StrategyContext
from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json


class FakeStorage:
    def __init__(self):
        self.data = {}

    async def find_one(self, coll, flt, projection=None):
        return next((doc for doc in self.data.get(coll, []) if all(doc.get(k) == v for k, v in flt.items())), None)

    async def insert_one(self, coll, doc):
        self.data.setdefault(coll, []).append(doc)
        return {"inserted_id": len(self.data[coll])}

    async def aggregate(self, coll, pipeline, allow_disk_use=True):
        # very naive: ignore pipeline
        return list(self.data.get(coll, []))


class FakeIndexAdmin:
    def __init__(self, storage: FakeStorage):
        self.storage = storage
        self.collections = set()

    async def ensure_collection(self, name):
        self.collections.add(name)
        self.storage.data.setdefault(name, [])

    async def ensure_indexes(self, collection, index_specs):
        return {"warnings": []}


@pytest.mark.asyncio
async def test_ensure_dictionaries_creates_collections_and_docs():
    cfg = load_json(DEFAULTS_PATH)
    storage = FakeStorage()
    index_admin = FakeIndexAdmin(storage)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage, "index_admin": index_admin})
    strat = RPSDualStrategy(MANIFEST)
    res = await strat.run_op(ctx, "ensure_dictionaries", {})
    assert res["ok"] is True
    assert index_admin.collections  # collections created
    # placeholder docs should exist
    assert storage.data
