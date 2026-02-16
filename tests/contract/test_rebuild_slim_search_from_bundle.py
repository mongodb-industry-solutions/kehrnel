import json
from pathlib import Path

import pytest

from kehrnel.strategies.openehr.rps_dual.strategy import RPSDualStrategy, MANIFEST, DEFAULTS_PATH, load_json
from kehrnel.core.types import StrategyContext
from kehrnel.core.bundle_store import BundleStore


class RecordingStorage:
    def __init__(self, docs):
        self.docs = docs
        self.inserted = []

    async def aggregate(self, coll, pipeline, allow_disk_use=True):
        return self.docs

    async def insert_one(self, coll, doc):
        self.inserted.append((coll, doc))
        return {"ok": True}


@pytest.mark.asyncio
async def test_rebuild_slim_search_uses_bundle_rules(tmp_path):
    bundle_store = BundleStore(tmp_path / "bundles")
    bundle = {
        "bundle_id": "openehr.analytics.testbundle.v1",
        "domain": "openEHR",
        "kind": "slim_search_definition",
        "version": "1.0.0",
        "payload": {
            "templates": [
                {
                    "templateId": "Sample",
                    "analytics_fields": [{"name": "code", "path": "code", "rmType": "DV_TEXT"}],
                    # Strategy expects fields copied relative to node.data (not "data.*").
                    "rules": [{"when": {"pathChain": ["admin_salut"]}, "copy": ["p", "value"]}],
                }
            ]
        },
    }
    bundle_store.save_bundle(bundle, mode="upsert")
    cfg = load_json(DEFAULTS_PATH)
    # Bundle reference is currently derived from collections.search.atlasIndex.definition.
    cfg["collections"]["search"]["atlasIndex"]["definition"] = bundle["bundle_id"]
    comp_doc = {
        "_id": "comp1",
        "ehr_id": "ehr1",
        "cn": [
            {"p": "admin_salut/items[at0007]", "data": {"value": "VAL1", "code": "C1"}},
            {"p": "other/path", "data": {"value": "VAL2", "code": "C2"}},
        ],
    }
    storage = RecordingStorage([comp_doc])
    strat = RPSDualStrategy(MANIFEST)
    ctx = StrategyContext(environment_id="env", config=cfg, adapters={"storage": storage}, manifest=MANIFEST, meta={"bundle_store": bundle_store})
    res = await strat.run_op(ctx, "rebuild_slim_search_collection", {"batch_size": 10})
    assert res["ok"] is True
    assert storage.inserted, "expected slim search docs"
    inserted_doc = storage.inserted[0][1]
    sn_field = cfg["fields"]["document"]["sn"]
    assert sn_field in inserted_doc
    search_nodes = inserted_doc[sn_field]
    assert any(node.get("p") == "admin_salut/items[at0007]" for node in search_nodes)
    assert all("other/path" not in node.get("p", "") for node in search_nodes)
