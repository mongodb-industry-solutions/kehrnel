import pytest

from kehrnel.contextobjects.resolver import resolve_context_contract
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.x12.co_single.strategy import (
    DEFAULTS_PATH as X12_DEFAULTS_PATH,
    MANIFEST as X12_MANIFEST,
    X12COSingleStrategy,
    load_json as load_x12_json,
)


class DummyStorage:
    def __init__(self, definitions=None):
        self.definitions = definitions or []

    async def aggregate(self, collection, pipeline, allow_disk_use=True):
        if collection == "kehrnel_context_catalog":
            return self.definitions
        return []

    async def find_one(self, collection, flt, projection=None):
        return None

    async def insert_many(self, collection, docs):
        return None


def _catalog_definition():
    return {
        "_id": "oncology_episode_context",
        "_catalogType": "definition",
        "id": "oncology_episode_context",
        "title": "Oncology Episode Context",
        "status": "active",
        "subject_kinds": ["patient"],
        "assertion_types": ["observed", "workflow"],
        "blocks": [
            {"id": "timeline", "title": "Timeline"},
            {"id": "diagnostic_evidence", "title": "Diagnostic Evidence", "aliases": ["evidence"]},
            {"id": "protocol_bindings", "title": "Protocol Bindings"},
        ],
        "terminology": [
            {"system": "snomed", "display": "Follow-up delay", "synonyms": ["delay", "follow up delay"]},
        ],
        "resolution": {"clarification_threshold": 0.6},
    }


def test_resolve_context_contract_matches_requested_points():
    resolution = resolve_context_contract(
        {
            "utterance": "Show follow-up delay evidence for this patient",
            "request_ir": {
                "scope": "patient",
                "requested_points": ["follow-up delay", "evidence"],
                "assertion_type": "observed",
            },
        },
        [_catalog_definition()],
    )
    assert resolution["ready"] is True
    assert resolution["contextContract"] == "oncology_episode_context"
    assert set(resolution["matchedRequestedPoints"]) == {"follow-up delay", "evidence"}


@pytest.mark.asyncio
async def test_x12_strategy_compile_and_context_map_summary():
    strat = X12COSingleStrategy(X12_MANIFEST)
    cfg = load_x12_json(X12_DEFAULTS_PATH)
    ctx = StrategyContext(
        environment_id="env",
        config=cfg,
        adapters={"storage": DummyStorage([_catalog_definition()])},
    )

    compile_result = await strat.run_op(
        ctx,
        "compile_con2l",
        {
            "con2lExecutable": {
                "source_definition": "oncology_episode_context",
                "scope": "subject",
                "predicates": [{"field": "semantic.delay", "op": "exists", "value": True}],
            }
        },
    )
    assert compile_result["result"]["compiled"]["collection"] == cfg["collections"]["claims"]["name"]

    summary_result = await strat.run_op(
        ctx,
        "summarize_object_map",
        {
            "objectMap": {
                "id": "tumour_summary_map",
                "title": "Tumour Summary Context Map",
                "source_type": "cda",
                "target_definition": "oncology_episode_context",
                "rules": [
                    {"source": "source.delay", "target": "blocks.timeline", "required": True},
                    {"source": "source.evidence", "target": "blocks.diagnostic_evidence", "required": True},
                ],
            },
            "catalog": {"collection": "kehrnel_context_catalog"},
        },
    )
    summary = summary_result["result"]
    assert summary["targetMatch"]["id"] == "oncology_episode_context"
    assert "timeline" in summary["coveredBlocks"]
