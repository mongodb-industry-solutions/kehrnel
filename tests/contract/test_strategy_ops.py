import pytest

from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config


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


class DummyIndexAdmin:
    def __init__(self):
        self.index_calls = []

    async def ensure_indexes(self, collection, index_specs):
        self.index_calls.append((collection, index_specs))
        return {"created": [{"collection": collection}], "warnings": []}


class DummyAtlasSearch:
    def __init__(self):
        self.calls = []

    async def ensure_search_index(self, collection, index_name, definition):
        self.calls.append((collection, index_name, definition))
        return {"created": [{"collection": collection, "name": index_name}], "warnings": []}


class _UpdateResult:
    def __init__(self, modified_count: int):
        self.modified_count = modified_count


class _Cursor:
    def __init__(self, docs):
        self._docs = list(docs)
        self._limit = None

    def limit(self, value: int):
        self._limit = value
        return self

    def __aiter__(self):
        docs = self._docs if self._limit is None else self._docs[: self._limit]
        self._iter = iter(docs)
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _Collection:
    def __init__(self, docs):
        self.docs = list(docs)

    @staticmethod
    def _matches(doc, expr):
        if not expr:
            return True
        if "$and" in expr:
            return all(_Collection._matches(doc, part) for part in expr.get("$and") or [])
        if "$or" in expr:
            return any(_Collection._matches(doc, part) for part in expr.get("$or") or [])
        for key, value in expr.items():
            if key in {"$and", "$or"}:
                continue
            has_key = key in doc
            actual = doc.get(key)
            if isinstance(value, dict):
                if "$exists" in value and bool(value["$exists"]) != has_key:
                    return False
                if "$ne" in value and actual == value["$ne"]:
                    return False
                if "$in" in value and actual not in (value.get("$in") or []):
                    return False
                continue
            if actual != value:
                return False
        return True

    async def count_documents(self, filter_expr):
        return sum(1 for doc in self.docs if self._matches(doc, filter_expr))

    def find(self, filter_expr, projection=None):
        matched = [doc for doc in self.docs if self._matches(doc, filter_expr)]
        if isinstance(projection, dict) and projection.get("_id") == 1:
            matched = [{"_id": doc.get("_id")} for doc in matched]
        return _Cursor(matched)

    async def update_many(self, filter_expr, update):
        modified = 0
        set_values = (update or {}).get("$set") or {}
        for doc in self.docs:
            if not self._matches(doc, filter_expr):
                continue
            doc.update(set_values)
            modified += 1
        return _UpdateResult(modified)


class FakeDB:
    def __init__(self, collections):
        self._collections = {name: _Collection(docs) for name, docs in collections.items()}

    def __getitem__(self, name):
        return self._collections[name]


class FakeStorage:
    def __init__(self, db):
        self.db = db


class _FakeFlattener:
    def __init__(self):
        self.shortcut_keys = {}
        self.template_fields = {
            "vitals-template": [
                {"extract": "obs.time.v", "rmType": "DV_DATE_TIME"},
                {"extract": "obs.code.v", "rmType": "DV_TEXT"},
            ]
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


@pytest.mark.asyncio
async def test_run_op_build_indexes_plan_only():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={})
    res = await strat.run_op(ctx, "build_indexes", {"include_mappings": False})
    assert res["ok"] is True
    assert res["applied"] is False
    assert res["plan"]["btree"]
    assert res["plan"]["atlas"]["definition"]["mappings"]["dynamic"] is False


def test_build_generated_atlas_definition_uses_mapping_fields():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    strategy_cfg = normalize_config(cfg)
    definition = strat._build_generated_atlas_definition(
        strategy_cfg=strategy_cfg,
        flattener=_FakeFlattener(),
        include_stored_source=True,
        num_partitions=2,
    )
    sn_name = strategy_cfg.fields.document.sn
    node_data = strategy_cfg.fields.node.data
    node_path = strategy_cfg.fields.node.p
    sn_fields = definition["mappings"]["fields"][sn_name]["fields"]
    assert sn_fields[node_path]["type"] == "token"
    obs_fields = sn_fields[node_data]["fields"]["obs"]["fields"]
    assert obs_fields["time"]["fields"]["v"]["type"] == "date"
    assert obs_fields["code"]["fields"]["v"]["type"] == "token"


@pytest.mark.asyncio
async def test_run_op_build_indexes_apply():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    index_admin = DummyIndexAdmin()
    atlas = DummyAtlasSearch()
    ctx = StrategyContext(
        environment_id="env",
        config=cfg,
        adapters={"index_admin": index_admin, "atlas_search": atlas},
    )
    res = await strat.run_op(ctx, "build_indexes", {"include_mappings": False, "apply": True})
    assert res["ok"] is True
    assert res["applied"] is True
    assert index_admin.index_calls
    assert atlas.calls
    assert res["warnings"] == []


@pytest.mark.asyncio
async def test_run_op_migrate_schema_requires_version():
    cfg = load_json(DEFAULTS_PATH)
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={})
    with pytest.raises(KehrnelError):
        await strat.run_op(ctx, "migrate_schema", {})


@pytest.mark.asyncio
async def test_run_op_migrate_schema_dry_run():
    cfg = load_json(DEFAULTS_PATH)
    comp_name = cfg["collections"]["compositions"]["name"]
    search_name = cfg["collections"]["search"]["name"]
    db = FakeDB(
        {
            comp_name: [
                {"_id": "c1"},
                {"_id": "c2", "_schema_version": "1.0"},
                {"_id": "c3", "_schema_version": "2.0"},
            ],
            search_name: [
                {"_id": "s1", "_schema_version": "1.0"},
                {"_id": "s2", "_schema_version": "2.0"},
            ],
        }
    )
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": FakeStorage(db)})
    res = await strat.run_op(ctx, "migrate_schema", {"to_version": "2.0", "dry_run": True, "batch_size": 2})
    assert res["ok"] is True
    assert res["dry_run"] is True
    assert res["total"]["matched"] == 3
    assert res["total"]["updated"] == 0


@pytest.mark.asyncio
async def test_run_op_migrate_schema_apply_and_reindex():
    cfg = load_json(DEFAULTS_PATH)
    comp_name = cfg["collections"]["compositions"]["name"]
    search_name = cfg["collections"]["search"]["name"]
    db = FakeDB(
        {
            comp_name: [
                {"_id": "c1"},
                {"_id": "c2", "_schema_version": "1.0"},
                {"_id": "c3", "_schema_version": "2.0"},
            ],
            search_name: [
                {"_id": "s1", "_schema_version": "1.0"},
                {"_id": "s2", "_schema_version": "2.0"},
            ],
        }
    )
    index_admin = DummyIndexAdmin()
    atlas = DummyAtlasSearch()
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(
        environment_id="env",
        config=cfg,
        adapters={"storage": FakeStorage(db), "index_admin": index_admin, "atlas_search": atlas},
    )
    res = await strat.run_op(
        ctx,
        "migrate_schema",
        {
            "to_version": "2.0",
            "dry_run": False,
            "batch_size": 1,
            "ensure_indexes": True,
            "reindex_payload": {"include_mappings": False},
        },
    )
    assert res["ok"] is True
    assert res["dry_run"] is False
    assert res["total"]["matched"] == 3
    assert res["total"]["updated"] == 3
    assert res["reindex"]["applied"] is True
    assert index_admin.index_calls
    assert atlas.calls
