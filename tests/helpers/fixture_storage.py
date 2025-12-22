import json
from pathlib import Path


class FixtureStorage:
    def __init__(self, fixture_dir: Path):
        self.fixture_dir = fixture_dir
        self.calls = 0

    async def find_one(self, coll, flt, projection=None):
        self.calls += 1
        fname = "_codes.json" if "_codes" in coll else "_shortcuts.json"
        path = self.fixture_dir / fname
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    async def aggregate(self, coll, pipeline, allow_disk_use=True):
        return []
