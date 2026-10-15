from __future__ import annotations

import json
from pathlib import Path

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.strategy import RPSDualStrategy


REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLES_ROOT = (
    REPO_ROOT
    / "src"
    / "kehrnel"
    / "engine"
    / "strategies"
    / "openehr"
    / "rps_dual"
    / "samples"
    / "reference"
)
SAMPLE_NDJSON = SAMPLES_ROOT / "envelopes" / "sample_laboratory_v0_4.ndjson"
PROJECTION_MAPPINGS = json.loads((SAMPLES_ROOT / "projection_mappings.json").read_text(encoding="utf-8"))


class _RecordingStorage:
    def __init__(self, *, fail_search=False):
        self.insert_one_calls: list[tuple[str, dict]] = []
        self.insert_many_calls: list[tuple[str, list[dict]]] = []
        self.update_one_calls: list[tuple[str, dict, dict, bool]] = []
        self.update_many_calls: list[tuple[str, dict, dict, bool]] = []
        self.state_docs: dict[str, dict] = {}
        self.fail_search = fail_search

    async def insert_one(self, collection: str, doc: dict):
        if self.fail_search and collection == "compositions_search":
            raise RuntimeError("sidecar unavailable")
        self.insert_one_calls.append((collection, doc))
        return {"inserted_id": doc.get("_id")}

    async def insert_many(self, collection: str, docs: list[dict]):
        if self.fail_search and collection == "compositions_search":
            raise RuntimeError("sidecar unavailable")
        docs_list = list(docs)
        self.insert_many_calls.append((collection, docs_list))
        return {"inserted_ids": [doc.get("_id") for doc in docs_list]}

    async def update_one(self, collection, flt, update, upsert=False):
        self.update_one_calls.append((collection, flt, update, upsert))
        state = self.state_docs.setdefault(flt["_id"], {"_id": flt["_id"]})
        if len(state) == 1:
            state.update(update.get("$setOnInsert", {}))
        for dotted, value in update.get("$set", {}).items():
            current = state
            parts = dotted.split(".")
            for part in parts[:-1]:
                current = current.setdefault(part, {})
            current[parts[-1]] = value
        for dotted, increment in update.get("$inc", {}).items():
            state[dotted] = state.get(dotted, 0) + increment
        return {"matched_count": 1}

    async def update_many(self, collection, flt, update, upsert=False):
        self.update_many_calls.append((collection, flt, update, upsert))
        return {"matched_count": 0}

    async def find_one(self, collection, flt, projection=None):
        return self.state_docs.get(flt["_id"])


def _strategy_config() -> dict:
    return {
        "collections": {
            "compositions": {"name": "compositions_rps"},
            "search": {"name": "compositions_search", "enabled": True},
        },
        "transform": {
            "mappings": PROJECTION_MAPPINGS,
            "apply_shortcuts": False,
            "coding": {
                "arcodes": {"strategy": "literal"},
                "atcodes": {"strategy": "literal", "store_original": False},
            },
        },
    }


def _first_envelope() -> dict:
    first_line = SAMPLE_NDJSON.read_text(encoding="utf-8").splitlines()[0]
    return json.loads(first_line)


def test_runtime_routing_metadata_is_not_treated_as_composition_content():
    composition = RPSDualStrategy._payload_to_composition_object(
        {
            "_type": "COMPOSITION",
            "name": {"value": "Example"},
            "domain": "openehr",
            "strategy_id": "openehr.rps_dual",
            "strategy_protocol": "canonical-json",
            "__activation_precondition": {
                "activationId": "activation-a",
                "dataTargetDigest": "sha256:" + "a" * 64,
            },
        }
    )

    assert composition == {
        "_type": "COMPOSITION",
        "name": {"value": "Example"},
    }


@pytest.mark.asyncio
async def test_single_ingest_response_shape_remains_unchanged():
    storage = _RecordingStorage()
    strategy = RPSDualStrategy()
    ctx = StrategyContext(
        environment_id="env-single",
        config=_strategy_config(),
        adapters={"storage": storage},
    )

    result = await strategy.ingest(ctx, _first_envelope())

    assert set(result) == {"inserted", "base", "search"}
    assert result["inserted"] == {
        "base": "compositions_rps",
        "search": "compositions_search",
    }
    assert result["base"]["tid"] == "sample_laboratory_v0.4"
    assert result["search"]["tid"] == "sample_laboratory_v0.4"
    assert len(storage.insert_one_calls) == 2
    assert storage.insert_many_calls == []
    state = next(iter(storage.state_docs.values()))
    assert state["projection"]["state"] == "stale"
    assert state["projection"]["complete"] is False
    assert state["mutationSequence"] == 2
    assert state["activeMutations"] == 0


@pytest.mark.asyncio
async def test_ingest_supports_ndjson_file_path_batches(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("KEHRNEL_ALLOW_LOCAL_FILE_INPUTS", "true")
    monkeypatch.setenv("KEHRNEL_LOCAL_FILE_INPUTS_BASE_DIR", str(REPO_ROOT))

    storage = _RecordingStorage()
    strategy = RPSDualStrategy()
    ctx = StrategyContext(
        environment_id="env-batch",
        config=_strategy_config(),
        adapters={"storage": storage},
    )

    result = await strategy.ingest(
        ctx,
        {
            "file_path": str(SAMPLE_NDJSON),
            "domain": "openehr",
            "strategy_id": "openehr.rps_dual",
        },
    )

    assert result == {
        "mode": "batch",
        "source": "file",
        "processed": 5,
        "generated": {"base": 5, "search": 5},
        "inserted": {
            "base": "compositions_rps",
            "search": "compositions_search",
        },
        "inserted_counts": {"base": 5, "search": 5},
    }
    assert storage.insert_one_calls == []
    assert len(storage.insert_many_calls) == 2
    assert storage.insert_many_calls[0][0] == "compositions_rps"
    assert storage.insert_many_calls[1][0] == "compositions_search"
    assert len(storage.insert_many_calls[0][1]) == 5
    assert len(storage.insert_many_calls[1][1]) == 5


@pytest.mark.asyncio
async def test_ingest_invalidates_ready_receipt_before_failed_sidecar_write():
    storage = _RecordingStorage(fail_search=True)
    state_id = "openehr.rps_dual:env-failure:compositions_search"
    storage.state_docs[state_id] = {
        "_id": state_id,
        "mutationSequence": 7,
        "projection": {"state": "ready", "complete": True},
        "reconciliation": {"safe": True, "complete": True},
    }
    strategy = RPSDualStrategy()
    ctx = StrategyContext(
        environment_id="env-failure",
        config=_strategy_config(),
        adapters={"storage": storage},
    )

    with pytest.raises(RuntimeError, match="sidecar unavailable"):
        await strategy.ingest(ctx, _first_envelope())

    state = storage.state_docs[state_id]
    assert state["mutationSequence"] == 9
    assert state["activeMutations"] == 0
    assert state["projection"]["state"] == "stale"
    assert state["projection"]["complete"] is False
    assert state["reconciliation"]["safe"] is False
    assert [call[0] for call in storage.insert_one_calls] == ["compositions_rps"]


@pytest.mark.asyncio
async def test_ingest_file_path_requires_explicit_enablement(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("KEHRNEL_ALLOW_LOCAL_FILE_INPUTS", raising=False)
    monkeypatch.setenv("KEHRNEL_LOCAL_FILE_INPUTS_BASE_DIR", str(REPO_ROOT))

    storage = _RecordingStorage()
    strategy = RPSDualStrategy()
    ctx = StrategyContext(
        environment_id="env-disabled",
        config=_strategy_config(),
        adapters={"storage": storage},
    )

    with pytest.raises(ValueError, match="Local file ingest is disabled"):
        await strategy.ingest(ctx, {"file_path": str(SAMPLE_NDJSON)})
