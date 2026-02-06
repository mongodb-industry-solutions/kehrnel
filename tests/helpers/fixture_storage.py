import json
from pathlib import Path


class FixtureStorage:
    def __init__(self, fixture_dir: Path):
        self.fixture_dir = fixture_dir
        self.calls = 0
        self.collections = set()

    async def find_one(self, coll, flt, projection=None):
        self.calls += 1
        fname = "_codes.json" if "_codes" in coll else "_shortcuts.json"
        path = self.fixture_dir / fname
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    async def aggregate(self, coll, pipeline, allow_disk_use=True):
        return []

    async def insert_one(self, coll, doc):
        self.calls += 1
        return {"ok": True, "collection": coll, "doc": doc}

    async def ensure_collection(self, name):
        self.collections.add(name)

    async def ensure_indexes(self, collection, index_specs):
        return {"created": [], "skipped": []}

    async def ensure_search_index(self, collection, index_name, definition):
        return {"ok": True, "collection": collection, "index": index_name}
