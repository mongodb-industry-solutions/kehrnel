from datetime import datetime, timezone

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual_ibm.strategy import (
    DEFAULTS_PATH,
    MANIFEST as IBM_MANIFEST,
    RPSDualIBMStrategy,
    load_json,
)


class RecordingStorage:
    def __init__(self, docs):
        self.docs = docs
        self.inserted = []
        self.db = None

    async def aggregate(self, coll, pipeline, allow_disk_use=True):
        return self.docs

    async def insert_one(self, coll, doc):
        self.inserted.append((coll, doc))
        return {"ok": True}


@pytest.mark.asyncio
async def test_rebuild_slim_search_uses_analytics_mappings_for_ibm_docs():
    cfg = load_json(DEFAULTS_PATH)
    cfg["transform"]["mappings"] = {
        "analyticsTemplate": {
            "templateId": "IBM_TEMPLATE",
            "fields": [
                {
                    "path": "/name/value",
                    "rmType": "DV_TEXT",
                }
            ],
        }
    }

    comp_doc = {
        "_id": "comp-1::ehrbase.ehrbase.org::1",
        "ehr_id": "ehr-1",
        "template": "IBM_TEMPLATE",
        "version": "comp-1::ehrbase.ehrbase.org::1",
        "creation_date": datetime(2026, 4, 23, tzinfo=timezone.utc),
        "cn": [
            {
                "p": "1",
                "data": {
                    "n": {
                        "v": "Projected root value",
                    }
                },
            }
        ],
    }

    storage = RecordingStorage([comp_doc])
    strategy = RPSDualIBMStrategy()
    ctx = StrategyContext(
        environment_id="env",
        config=cfg,
        adapters={"storage": storage},
        manifest=IBM_MANIFEST.model_copy(deep=True),
        meta={},
    )

    res = await strategy.run_op(ctx, "rebuild_slim_search_collection", {"batch_size": 10})

    assert res["ok"] is True
    assert res["inserted"] == 1
    assert storage.inserted, "expected slim search docs from analytics mappings"

    _, inserted_doc = storage.inserted[0]
    assert inserted_doc["_id"] == comp_doc["_id"]
    assert inserted_doc["template"] == "IBM_TEMPLATE"
    assert inserted_doc["sn"]
    assert inserted_doc["sn"][0]["data"]["n"]["v"] == "Projected root value"
