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
        self.deleted = []
        self.db = None

    async def aggregate(self, coll, pipeline, allow_disk_use=True):
        return self.docs

    async def insert_one(self, coll, doc):
        self.inserted.append((coll, doc))
        return {"ok": True}

    async def delete_many(self, coll, flt):
        self.deleted.append((coll, flt))
        return {"deleted_count": 0}


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
        "template_id": "template-1",
        "version": "comp-1::ehrbase.ehrbase.org::1",
        "creation_date": datetime(2026, 4, 23, tzinfo=timezone.utc),
        "metrics": {"config_name": "CONFIG_1"},
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

    res = await strategy.run_op(ctx, "rebuild_slim_search_collection", {"batch_size": 10, "clear_existing": True})

    assert res["ok"] is True
    assert res["inserted"] == 1
    assert res["cleared"] is True
    assert storage.deleted == [("compositions_search_ibm", {})]
    assert storage.inserted, "expected slim search docs from analytics mappings"

    _, inserted_doc = storage.inserted[0]
    assert inserted_doc["_id"] == comp_doc["_id"]
    assert inserted_doc["template"] == "IBM_TEMPLATE"
    assert inserted_doc["template_id"] == "template-1"
    assert "metrics" not in inserted_doc
    assert inserted_doc["sn"]
    assert inserted_doc["sn"][0]["data"]["n"]["v"] == "Projected root value"


@pytest.mark.asyncio
async def test_rebuild_slim_search_matches_ibm_compact_prefix_paths_without_at_dictionary():
    cfg = load_json(DEFAULTS_PATH)
    cfg["transform"]["mappings"] = {
        "analyticsTemplate": {
            "templateId": "IBM_TEMPLATE",
            "fields": [
                {
                    "path": (
                        "/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]"
                        "/data[at0001]/events[at0002]/data[at0003]"
                        "/items[openEHR-EHR-CLUSTER.newborn.v0]"
                        "/items[at0004]/items[at0012]/value/defining_code/code_string"
                    ),
                    "rmType": "DV_CODED_TEXT",
                }
            ],
        }
    }

    comp_doc = {
        "_id": "comp-2::ehrbase.ehrbase.org::1",
        "ehr_id": "ehr-2",
        "template": "IBM_TEMPLATE",
        "version": "comp-2::ehrbase.ehrbase.org::1",
        "creation_date": datetime(2026, 4, 23, tzinfo=timezone.utc),
        "cn": [
            {
                "p": "C12/D4/76/D3/D2/D1/3/1",
                "data": {
                    "v": {
                        "df": {
                            "cs": "248152002",
                        }
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
    _, inserted_doc = storage.inserted[0]
    assert inserted_doc["sn"][0]["p"] == "C12/D4/76/D3/D2/D1/3/1"
    assert inserted_doc["sn"][0]["data"]["v"]["df"]["cs"] == "248152002"
