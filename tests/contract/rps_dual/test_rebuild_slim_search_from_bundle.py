from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_coding_opts,
    build_flattener_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import (
    CompositionFlattener,
)
from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    DEFAULTS_PATH,
    MANIFEST,
    RPSDualStrategy,
    load_json,
)


STRATEGY_ROOT = Path(DEFAULTS_PATH).parent
PROJECTION_MAPPINGS_PATH = (
    STRATEGY_ROOT / "samples" / "reference" / "projection_mappings.json"
)
HEART_FAILURE_ENVELOPES_PATH = (
    STRATEGY_ROOT
    / "samples"
    / "reference"
    / "envelopes"
    / "sample_laboratory_v0_4_heart_failure.ndjson"
)
BUNDLED_CODES_PATH = STRATEGY_ROOT / "bundles" / "dictionaries" / "_codes.json"
SHORTCUTS_PATH = STRATEGY_ROOT / "bundles" / "shortcuts" / "shortcuts.json"
STATE_COLLECTION = "_kehrnel_projection_state"


def _get_dotted(document, path):
    current = document
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
            current = current[int(part)]
        else:
            return None
    return current


def _set_dotted(document, path, value):
    current = document
    parts = path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = copy.deepcopy(value)


def _matches(document, flt):
    for path, expected in flt.items():
        if path == "$or":
            if not any(_matches(document, clause) for clause in expected):
                return False
            continue
        if path == "$nor":
            if any(_matches(document, clause) for clause in expected):
                return False
            continue
        actual = _get_dotted(document, path)
        if isinstance(expected, dict):
            if "$ne" in expected and actual == expected["$ne"]:
                return False
            if "$gt" in expected and not (actual > expected["$gt"]):
                return False
            if "$in" in expected and actual not in expected["$in"]:
                return False
            if "$regex" in expected and not re.match(expected["$regex"], str(actual or "")):
                return False
            if "$exists" in expected and (actual is not None) is not expected["$exists"]:
                return False
        elif actual != expected:
            return False
    return True


class _Collection:
    def __init__(self, docs=None):
        self.docs = copy.deepcopy(docs or {})

    async def find_one(self, flt=None, projection=None):
        return copy.deepcopy(
            next((doc for doc in self.docs.values() if _matches(doc, flt or {})), None)
        )

    async def replace_one(self, flt, doc, upsert=False):
        existing = next(
            (item for item in self.docs.values() if _matches(item, flt)),
            None,
        )
        if existing is None and not upsert:
            return {"acknowledged": True, "matched_count": 0}
        self.docs[doc["_id"]] = copy.deepcopy(doc)
        return {
            "acknowledged": True,
            "matched_count": 1 if existing is not None else 0,
        }

    async def update_one(self, flt, update, upsert=False):
        existing = next(
            (item for item in self.docs.values() if _matches(item, flt)),
            None,
        )
        inserted = existing is None
        if inserted:
            if not upsert:
                return {"acknowledged": True, "matched_count": 0}
            existing = {"_id": flt.get("_id")}
        if inserted:
            for path, value in update.get("$setOnInsert", {}).items():
                _set_dotted(existing, path, value)
        for path, value in update.get("$set", {}).items():
            _set_dotted(existing, path, value)
        for path, increment in update.get("$inc", {}).items():
            _set_dotted(existing, path, (_get_dotted(existing, path) or 0) + increment)
        self.docs[existing["_id"]] = existing
        return {
            "acknowledged": True,
            "matched_count": 0 if inserted else 1,
        }

    async def update_many(self, flt, update, upsert=False):
        matched = 0
        for existing in self.docs.values():
            if not _matches(existing, flt):
                continue
            matched += 1
            for path, value in update.get("$set", {}).items():
                _set_dotted(existing, path, value)
            for path, increment in update.get("$inc", {}).items():
                _set_dotted(
                    existing,
                    path,
                    (_get_dotted(existing, path) or 0) + increment,
                )
        if matched == 0 and upsert:
            return await self.update_one(flt, update, upsert=True)
        return {"acknowledged": True, "matched_count": matched}


class _Database(dict):
    def __getitem__(self, name):
        if name not in self:
            self[name] = _Collection()
        return super().__getitem__(name)


class RecordingStorage:
    def __init__(self, db):
        self.db = db
        self.aggregate_calls = []
        self.replace_calls = []

    async def aggregate(self, collection, pipeline, allow_disk_use=True):
        self.aggregate_calls.append((collection, copy.deepcopy(pipeline)))
        docs = copy.deepcopy(list(self.db[collection].docs.values()))
        for stage in pipeline:
            if "$match" in stage:
                match = stage["$match"]
                docs = [doc for doc in docs if _matches(doc, match)]
            elif "$sort" in stage:
                direction = stage["$sort"]["_id"]
                docs.sort(key=lambda doc: doc["_id"], reverse=direction < 0)
            elif "$limit" in stage:
                docs = docs[: stage["$limit"]]
            elif "$lookup" in stage:
                spec = stage["$lookup"]
                foreign = list(self.db[spec["from"]].docs.values())
                for doc in docs:
                    local_value = doc.get(spec["localField"])
                    doc[spec["as"]] = [
                        candidate
                        for candidate in foreign
                        if candidate.get(spec["foreignField"]) == local_value
                    ]
            elif "$project" in stage:
                projected = []
                for doc in docs:
                    projected.append(
                        {
                            key: doc.get(key)
                            for key, include in stage["$project"].items()
                            if include and key in doc
                        }
                    )
                docs = projected
            else:  # pragma: no cover - catches accidental contract expansion
                raise AssertionError(f"Unsupported fake aggregation stage: {stage}")
        return copy.deepcopy(docs)

    async def replace_one(self, collection, flt, doc, upsert=False):
        self.replace_calls.append((collection, copy.deepcopy(flt), copy.deepcopy(doc), upsert))
        return await self.db[collection].replace_one(flt, doc, upsert=upsert)

    async def update_one(self, collection, flt, update, upsert=False):
        return await self.db[collection].update_one(flt, update, upsert=upsert)

    async def update_many(self, collection, flt, update, upsert=False):
        return await self.db[collection].update_many(flt, update, upsert=upsert)

    async def find_one(self, collection, flt, projection=None):
        return await self.db[collection].find_one(flt, projection)


class BulkRecordingStorage(RecordingStorage):
    def __init__(self, db):
        super().__init__(db)
        self.replace_many_calls = []

    async def replace_many(self, collection, docs):
        materialized = copy.deepcopy(list(docs))
        self.replace_many_calls.append((collection, materialized))
        for doc in materialized:
            await self.db[collection].replace_one(
                {"_id": doc["_id"]}, doc, upsert=True
            )


class BulkRecordingStorage(RecordingStorage):
    def __init__(self, db):
        super().__init__(db)
        self.replace_many_calls = []

    async def replace_many(self, collection, docs):
        materialized = copy.deepcopy(list(docs))
        self.replace_many_calls.append((collection, materialized))
        for doc in materialized:
            await self.db[collection].replace_one(
                {"_id": doc["_id"]}, doc, upsert=True
            )


class RecordingAtlas:
    def __init__(self, result=None):
        self.calls = []
        self.result = result or {"ok": True, "warnings": []}

    async def ensure_search_index(self, collection, name, definition):
        self.calls.append((collection, name, copy.deepcopy(definition)))
        return copy.deepcopy(self.result)

    async def list_search_indexes(self, collection):
        if not self.calls:
            return []
        _, name, definition = self.calls[-1]
        return [
            {
                "name": name,
                "status": "READY",
                "queryable": True,
                "latestDefinition": copy.deepcopy(definition),
            }
        ]


def _config() -> dict:
    config = load_json(DEFAULTS_PATH)
    config["transform"]["mappings"] = (
        "file://samples/reference/projection_mappings.json"
    )
    return config


def _codes_document() -> dict:
    payload = load_json(BUNDLED_CODES_PATH)
    return next(item for item in payload if item.get("_id") == "ar_code")


async def _runtime_fixture(source_count: int = 3):
    config = _config()
    mapping = load_json(PROJECTION_MAPPINGS_PATH)
    db = _Database(
        {
            "_codes": _Collection({"ar_code": _codes_document()}),
            "_shortcuts": _Collection({"shortcuts": load_json(SHORTCUTS_PATH)}),
            config["collections"]["compositions"]["name"]: _Collection(),
            config["collections"]["search"]["name"]: _Collection(),
            STATE_COLLECTION: _Collection(),
        }
    )
    normalized = normalize_config(config)
    flattener = await CompositionFlattener.create(
        db=db,
        config=build_flattener_config(normalized),
        mappings_path="unused",
        mappings_content=mapping,
        coding_opts=build_coding_opts(normalized),
    )
    envelopes = [
        json.loads(line)
        for line in HEART_FAILURE_ENVELOPES_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ][:source_count]

    expected_search = {}
    for position, envelope in enumerate(envelopes, start=1):
        base_doc, search_doc = flattener.transform_composition(envelope)
        base_doc["_id"] = f"source-{position:03d}"
        db[config["collections"]["compositions"]["name"]].docs[
            base_doc["_id"]
        ] = base_doc
        assert search_doc is not None
        expected_search[search_doc["_id"]] = search_doc

    storage = RecordingStorage(db)
    atlas = RecordingAtlas()
    context = StrategyContext(
        environment_id="env-rebuild",
        config=config,
        adapters={"storage": storage, "atlas_search": atlas},
        manifest=MANIFEST,
        meta={},
    )
    return context, storage, atlas, expected_search


def _nested(document: dict, dotted_path: str):
    current = document
    for part in dotted_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


@pytest.mark.asyncio
async def test_rebuild_is_bounded_resumable_and_ensures_index_only_when_complete():
    context, storage, atlas, expected_search = await _runtime_fixture(3)
    strategy = RPSDualStrategy(MANIFEST)

    first = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2},
    )

    assert first["ok"] is True
    assert first["complete"] is False
    assert first["processed"] == 2
    assert first["upserted"] == 2
    assert first["counts"] == {
        "fetched": 3,
        "processed": 2,
        "upserted": 2,
        "skippedEmpty": 0,
        "skippedUnmapped": 0,
        "mappedEmpty": 0,
    }
    assert isinstance(first["next_cursor"], str)
    assert first["projection"]["state"] == "building"
    assert first["projection"]["complete"] is False
    assert first["index"]["status"] == "deferred_until_rebuild_complete"
    assert atlas.calls == []
    assert storage.aggregate_calls[0][1] == [
        {"$sort": {"_id": 1}},
        {"$limit": 3},
    ]

    second = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2, "cursor": first["next_cursor"]},
    )

    assert second["complete"] is True
    assert second["next_cursor"] is None
    assert second["processed"] == 1
    assert second["upserted"] == 1
    assert second["watermark"]["processedCount"] == 3
    assert second["watermark"]["upsertedCount"] == 3
    assert second["projection"]["state"] == "ready"
    assert second["projection"]["complete"] is True
    assert second["projection"]["projectionAlgorithmVersion"] == "rps-dual-projector/v2"
    assert second["reconciliation"] == {
        "complete": True,
        "safe": True,
        "sourceToTargetCoverage": "verified",
        "targetToSourceCoverage": "verified",
        "missingSidecarDetected": False,
        "missingSidecarSourceId": None,
        "orphanSidecarDetected": False,
        "orphanSidecarId": None,
        "unmappedSidecarDetected": False,
        "unmappedSidecarId": None,
        "mappedEmptyCount": 0,
        "skippedUnmappedCount": 0,
        "staleEmptySidecarCount": 0,
    }
    assert second["consistency"]["model"] == "governed-writer-mutation-fence/v1"
    assert second["consistency"]["mutationSequence"] == 2
    assert second["projection"]["revision"] == first["projection"]["revision"]
    assert second["projection"]["templateIds"] == [
        "sample_immunization_list_v0.5",
        "sample_laboratory_v0.4",
    ]
    assert second["index"]["status"] == "ensured"
    assert len(atlas.calls) == 1
    assert storage.aggregate_calls[1][1][0] == {
        "$match": {"_id": {"$gt": "source-002"}}
    }
    assert storage.aggregate_calls[1][1][1:] == [
        {"$sort": {"_id": 1}},
        {"$limit": 3},
    ]

    target = storage.db[second["targetCollection"]].docs
    assert target == expected_search
    first_search_doc = next(iter(target.values()))
    nodes = first_search_doc["sn"]
    assert any(isinstance(node.get("pi"), list) for node in nodes)
    assert any(_nested(node, "data.v.df.cs") == "84114007" for node in nodes)
    assert any(
        _nested(node, "data.v.df.tid.v") == "SNOMED-CT" for node in nodes
    )
    assert any(_nested(node, "data.v.v") is not None for node in nodes)

    state = storage.db[STATE_COLLECTION].docs[second["state"]["id"]]
    assert state["contract"] == "openehr.rps-dual.search-projection-readiness/v1"
    assert state["projection"] == second["projection"]
    assert state["checkpoint"] == second["watermark"]
    assert state["index"]["status"] == "ensured"
    assert state["activeMutations"] == 0
    assert state["rebuildLease"]["active"] is False


@pytest.mark.asyncio
async def test_rebuild_uses_bulk_replacement_when_adapter_supports_it():
    context, storage, _, expected_search = await _runtime_fixture(3)
    bulk_storage = BulkRecordingStorage(storage.db)
    context.adapters["storage"] = bulk_storage

    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 3},
    )

    assert result["complete"] is True
    assert len(bulk_storage.replace_many_calls) == 1
    collection, documents = bulk_storage.replace_many_calls[0]
    assert collection == context.config["collections"]["search"]["name"]
    assert {document["_id"] for document in documents} == set(expected_search)
    assert not any(
        replacement_collection == collection
        for replacement_collection, *_ in bulk_storage.replace_calls
    )


@pytest.mark.asyncio
async def test_recover_stale_rebuild_lease_preserves_resumable_checkpoint():
    context, storage, _, _ = await _runtime_fixture(3)
    strategy = RPSDualStrategy(MANIFEST)
    first = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2},
    )
    state = next(iter(storage.db[STATE_COLLECTION].docs.values()))
    state["mutationSequence"] += 1
    state["rebuildLease"] = {
        "active": True,
        "owner": "interrupted-worker",
        "acquiredAt": "2020-01-01T00:00:00Z",
    }

    recovered = await strategy.run_op(
        context,
        "recover_search_projection_rebuild",
        {"minimum_age_seconds": 1},
    )

    assert recovered["recovered"] is True
    assert recovered["checkpoint"] == {
        "processedCount": 2,
        "hasNextCursor": True,
    }
    assert state["rebuildLease"]["active"] is False
    assert state["rebuildLease"]["releaseReason"] == "recovered_interrupted_rebuild"
    assert state["checkpoint"]["mutationSequence"] == state["mutationSequence"]

    resumed = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2, "cursor": first["next_cursor"]},
    )
    assert resumed["complete"] is True
    assert resumed["watermark"]["processedCount"] == 3


@pytest.mark.asyncio
async def test_rebuild_retry_is_idempotent_and_keeps_the_same_checkpoint():
    context, storage, atlas, _ = await _runtime_fixture(3)
    strategy = RPSDualStrategy(MANIFEST)

    first = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2},
    )
    first_target = copy.deepcopy(
        storage.db[context.config["collections"]["search"]["name"]].docs
    )
    retry = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2},
    )

    assert retry["next_cursor"] == first["next_cursor"]
    assert retry["projection"]["revision"] == first["projection"]["revision"]
    assert retry["watermark"]["processedCount"] == 2
    assert retry["watermark"]["upsertedCount"] == 2
    assert storage.db[context.config["collections"]["search"]["name"]].docs == first_target
    assert atlas.calls == []


@pytest.mark.asyncio
async def test_rebuild_never_reports_ready_when_an_orphan_sidecar_exists():
    context, storage, atlas, _ = await _runtime_fixture(1)
    search_collection = context.config["collections"]["search"]["name"]
    storage.db[search_collection].docs["orphan-composition"] = {
        "_id": "orphan-composition",
        "comp_id": "orphan-composition",
        "sn": [{"p": "orphan", "data": {"v": "stale"}}],
    }

    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )

    assert result["complete"] is True
    assert result["projection"]["state"] == "stale"
    assert result["reconciliation"]["safe"] is False
    assert result["reconciliation"]["orphanSidecarDetected"] is True
    assert result["reconciliation"]["orphanSidecarId"] == "orphan-composition"
    assert result["index"]["status"] == "deferred_until_projection_reconciled"
    assert atlas.calls == []


@pytest.mark.asyncio
async def test_rebuild_never_reports_ready_when_a_mapped_source_has_no_sidecar():
    context, storage, atlas, _ = await _runtime_fixture(1)
    source_collection = context.config["collections"]["compositions"]["name"]
    search_collection = context.config["collections"]["search"]["name"]
    original_replace = storage.replace_one

    async def lose_sidecar(collection, flt, doc, upsert=False):
        if collection == search_collection:
            return {"acknowledged": True, "matched_count": 0}
        return await original_replace(collection, flt, doc, upsert=upsert)

    storage.replace_one = lose_sidecar
    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )

    assert source_collection == "compositions_rps"
    assert result["projection"]["state"] == "stale"
    assert result["reconciliation"]["safe"] is False
    assert result["reconciliation"]["sourceToTargetCoverage"] == "mismatch"
    assert result["reconciliation"]["missingSidecarDetected"] is True
    assert result["index"]["status"] == "deferred_until_projection_reconciled"
    assert atlas.calls == []


@pytest.mark.asyncio
async def test_rebuild_write_failure_releases_lease_but_remains_stale():
    context, storage, _, _ = await _runtime_fixture(1)
    search_collection = context.config["collections"]["search"]["name"]
    original_replace = storage.replace_one

    async def fail_sidecar(collection, flt, doc, upsert=False):
        if collection == search_collection:
            raise RuntimeError("sidecar write failed")
        return await original_replace(collection, flt, doc, upsert=upsert)

    storage.replace_one = fail_sidecar
    with pytest.raises(RuntimeError, match="sidecar write failed"):
        await RPSDualStrategy(MANIFEST).run_op(
            context,
            "rebuild_slim_search_collection",
            {"batch_size": 10},
        )

    state = next(iter(storage.db[STATE_COLLECTION].docs.values()))
    assert state["projection"]["state"] == "stale"
    assert state["projection"]["complete"] is False
    assert state["rebuildLease"]["active"] is False
    assert state["rebuildLease"]["releaseReason"] == "sidecar_write_failed"


@pytest.mark.asyncio
async def test_rebuild_distinguishes_unmapped_sources_from_mapped_empty_sources():
    context, storage, _, _ = await _runtime_fixture(1)
    source_collection = context.config["collections"]["compositions"]["name"]
    mapped_empty = copy.deepcopy(next(iter(storage.db[source_collection].docs.values())))
    mapped_empty["_id"] = "source-mapped-empty"
    mapped_empty["comp_id"] = "composition-mapped-empty"
    mapped_empty["cn"] = []
    unmapped = copy.deepcopy(mapped_empty)
    unmapped["_id"] = "source-unmapped"
    unmapped["comp_id"] = "composition-unmapped"
    unmapped["tid"] = "customer_specific_unmapped_template.v1"
    storage.db[source_collection].docs[mapped_empty["_id"]] = mapped_empty
    storage.db[source_collection].docs[unmapped["_id"]] = unmapped

    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )

    assert result["projection"]["state"] == "stale"
    assert result["watermark"]["mappedEmptyCount"] == 1
    assert result["watermark"]["skippedUnmappedCount"] == 1
    assert result["reconciliation"]["mappedEmptyCount"] == 1
    assert result["reconciliation"]["missingSidecarSourceId"] == "source-mapped-empty"


@pytest.mark.asyncio
async def test_rebuild_allows_an_unmapped_source_to_remain_base_only():
    context, storage, _, _ = await _runtime_fixture(1)
    source_collection = context.config["collections"]["compositions"]["name"]
    unmapped = copy.deepcopy(next(iter(storage.db[source_collection].docs.values())))
    unmapped["_id"] = "source-unmapped"
    unmapped["comp_id"] = "composition-unmapped"
    unmapped["tid"] = "customer_specific_unmapped_template.v1"
    storage.db[source_collection].docs[unmapped["_id"]] = unmapped

    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )

    assert result["projection"]["state"] == "ready"
    assert result["reconciliation"]["safe"] is True
    assert result["watermark"]["mappedEmptyCount"] == 0
    assert result["watermark"]["skippedUnmappedCount"] == 1


@pytest.mark.asyncio
async def test_rebuild_allows_a_legacy_source_without_template_id_to_remain_base_only():
    context, storage, _, _ = await _runtime_fixture(1)
    source_collection = context.config["collections"]["compositions"]["name"]
    legacy = copy.deepcopy(next(iter(storage.db[source_collection].docs.values())))
    legacy["_id"] = "source-legacy-no-template"
    legacy["comp_id"] = "composition-legacy-no-template"
    legacy.pop("tid", None)
    legacy.pop("template_id", None)
    storage.db[source_collection].docs[legacy["_id"]] = legacy

    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )

    assert result["projection"]["state"] == "ready"
    assert result["reconciliation"]["safe"] is True
    assert result["watermark"]["skippedUnmappedCount"] == 1


@pytest.mark.asyncio
async def test_rebuild_rejects_a_sidecar_for_an_unmapped_source_without_deleting_it():
    context, storage, atlas, _ = await _runtime_fixture(1)
    source_collection = context.config["collections"]["compositions"]["name"]
    search_collection = context.config["collections"]["search"]["name"]
    unmapped = copy.deepcopy(next(iter(storage.db[source_collection].docs.values())))
    unmapped["_id"] = "source-unmapped"
    unmapped["comp_id"] = "composition-unmapped"
    unmapped["tid"] = "customer_specific_unmapped_template.v1"
    stale_sidecar = {
        "_id": "composition-unmapped",
        "comp_id": "composition-unmapped",
        "tid": "customer_specific_unmapped_template.v1",
        "sn": [{"p": "stale", "data": {"v": "stale"}}],
    }
    storage.db[source_collection].docs[unmapped["_id"]] = unmapped
    storage.db[search_collection].docs[stale_sidecar["_id"]] = copy.deepcopy(
        stale_sidecar
    )

    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )

    assert result["projection"]["state"] == "stale"
    assert result["reconciliation"]["targetToSourceCoverage"] == "mismatch"
    assert result["reconciliation"]["unmappedSidecarDetected"] is True
    assert result["reconciliation"]["unmappedSidecarId"] == "composition-unmapped"
    assert storage.db[search_collection].docs[stale_sidecar["_id"]] == stale_sidecar
    assert atlas.calls == []


@pytest.mark.asyncio
async def test_rebuild_cas_preserves_stale_state_when_ingest_crosses_publication():
    context, storage, _, _ = await _runtime_fixture(1)
    original_replace = storage.replace_one
    crossed = False

    async def cross_fence(collection, flt, doc, upsert=False):
        nonlocal crossed
        if collection == STATE_COLLECTION and not crossed:
            crossed = True
            state = storage.db[STATE_COLLECTION].docs[doc["_id"]]
            state["mutationSequence"] += 1
            state.setdefault("projection", {})["state"] = "stale"
            state["projection"]["complete"] = False
        return await original_replace(collection, flt, doc, upsert=upsert)

    storage.replace_one = cross_fence
    with pytest.raises(KehrnelError) as conflict:
        await RPSDualStrategy(MANIFEST).run_op(
            context,
            "rebuild_slim_search_collection",
            {"batch_size": 10},
        )

    assert conflict.value.code == "SEARCH_REBUILD_CONCURRENT_MUTATION"
    state = next(iter(storage.db[STATE_COLLECTION].docs.values()))
    assert state["projection"]["state"] == "stale"
    assert state["projection"]["complete"] is False


@pytest.mark.asyncio
async def test_rebuild_fails_closed_while_a_governed_writer_fence_is_open():
    context, storage, atlas, _ = await _runtime_fixture(1)
    state_id = "openehr.rps_dual:env-rebuild:compositions_search"
    storage.db[STATE_COLLECTION].docs[state_id] = {
        "_id": state_id,
        "mutationSequence": 11,
        "activeMutations": 1,
        "projection": {"state": "stale", "complete": False},
    }

    with pytest.raises(KehrnelError) as active_writer:
        await RPSDualStrategy(MANIFEST).run_op(
            context,
            "rebuild_slim_search_collection",
            {"batch_size": 10},
        )

    assert active_writer.value.code == "SEARCH_REBUILD_MUTATION_IN_PROGRESS"
    assert storage.aggregate_calls == []
    assert storage.replace_calls == []
    assert atlas.calls == []


@pytest.mark.asyncio
async def test_rebuild_publishes_building_before_sidecar_write_and_rejects_overlap():
    context, storage, _, _ = await _runtime_fixture(1)
    strategy = RPSDualStrategy(MANIFEST)
    search_collection = context.config["collections"]["search"]["name"]
    projection = await strategy._build_active_search_projection(
        context,
        normalize_config(context.config),
    )
    template_id = "sample_laboratory_v0.4"
    provenance = projection["templateProvenance"][template_id]
    request = {
        "contract": "openehr-semantic-projection-preflight/v1",
        "templateId": template_id,
        "templateSourceDigest": provenance["templateSourceDigest"],
        "catalogDigest": provenance["catalogDigest"],
        "planDigest": "sha256:" + "c" * 64,
        "requiredPaths": [projection["templatePaths"][template_id][0]],
    }
    original_replace = storage.replace_one
    observed = {}

    async def inspect_before_first_sidecar(collection, flt, doc, upsert=False):
        if collection == search_collection and not observed:
            state = next(iter(storage.db[STATE_COLLECTION].docs.values()))
            observed["state"] = copy.deepcopy(state)
            observed["preflight"] = await strategy.run_op(
                context,
                "semantic_projection_preflight",
                request,
            )
            with pytest.raises(KehrnelError) as overlap:
                await strategy.run_op(
                    context,
                    "rebuild_slim_search_collection",
                    {"batch_size": 10},
                )
            observed["overlapCode"] = overlap.value.code
        return await original_replace(collection, flt, doc, upsert=upsert)

    storage.replace_one = inspect_before_first_sidecar
    result = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )

    assert observed["state"]["projection"]["state"] == "building"
    assert observed["state"]["projection"]["complete"] is False
    assert observed["state"]["rebuildLease"]["active"] is True
    assert observed["preflight"]["ready"] is False
    assert observed["preflight"]["rebuildInProgress"] is True
    assert "projection_rebuild_in_progress" in observed["preflight"]["reasons"]
    assert observed["overlapCode"] == "SEARCH_REBUILD_ALREADY_RUNNING"
    assert result["projection"]["state"] == "ready"


@pytest.mark.asyncio
async def test_completed_rebuild_changes_sequence_seen_by_pre_and_post_preflight():
    context, _, _, _ = await _runtime_fixture(1)
    strategy = RPSDualStrategy(MANIFEST)
    projection = await strategy._build_active_search_projection(
        context,
        normalize_config(context.config),
    )
    template_id = "sample_laboratory_v0.4"
    provenance = projection["templateProvenance"][template_id]
    request = {
        "contract": "openehr-semantic-projection-preflight/v1",
        "templateId": template_id,
        "templateSourceDigest": provenance["templateSourceDigest"],
        "catalogDigest": provenance["catalogDigest"],
        "planDigest": "sha256:" + "c" * 64,
        "requiredPaths": [projection["templatePaths"][template_id][0]],
    }

    await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )
    before = await strategy.run_op(
        context,
        "semantic_projection_preflight",
        request,
    )
    assert before["ready"] is True

    await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )
    after = await strategy.run_op(
        context,
        "semantic_projection_preflight",
        request,
    )

    assert after["ready"] is True
    assert after["consistency"]["mutationSequence"] == (
        before["consistency"]["mutationSequence"] + 1
    )


@pytest.mark.asyncio
async def test_continuation_rejects_changed_already_scanned_source_generation():
    context, storage, _, _ = await _runtime_fixture(3)
    strategy = RPSDualStrategy(MANIFEST)
    first = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2},
    )
    assert first["watermark"]["mutationSequence"] == 1

    normalized = normalize_config(context.config)
    await strategy._invalidate_projection_readiness(
        context,
        normalized,
        reason="test_governed_update",
    )
    source_collection = normalized.collections.compositions.name
    storage.db[source_collection].docs["source-001"]["test_revision"] = 2
    await strategy._complete_projection_mutation(
        context,
        normalized,
        reason="test_governed_update",
    )
    aggregate_count = len(storage.aggregate_calls)

    with pytest.raises(KehrnelError) as stale:
        await strategy.run_op(
            context,
            "rebuild_slim_search_collection",
            {"batch_size": 2, "cursor": first["next_cursor"]},
        )

    assert stale.value.code == "SEARCH_REBUILD_CURSOR_STALE"
    assert stale.value.details == {
        "checkpointMutationSequence": 1,
        "currentMutationSequence": 3,
    }
    assert len(storage.aggregate_calls) == aggregate_count
    state = next(iter(storage.db[STATE_COLLECTION].docs.values()))
    assert state["projection"]["state"] == "stale"


@pytest.mark.asyncio
async def test_partial_page_failure_invalidates_its_prior_continuation():
    context, storage, _, _ = await _runtime_fixture(3)
    strategy = RPSDualStrategy(MANIFEST)
    first = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2},
    )
    search_collection = context.config["collections"]["search"]["name"]
    original_replace = storage.replace_one
    failed = False

    async def fail_next_sidecar(collection, flt, doc, upsert=False):
        nonlocal failed
        if collection == search_collection and not failed:
            failed = True
            raise RuntimeError("interrupted page")
        return await original_replace(collection, flt, doc, upsert=upsert)

    storage.replace_one = fail_next_sidecar
    with pytest.raises(RuntimeError, match="interrupted page"):
        await strategy.run_op(
            context,
            "rebuild_slim_search_collection",
            {"batch_size": 2, "cursor": first["next_cursor"]},
        )

    state = next(iter(storage.db[STATE_COLLECTION].docs.values()))
    assert state["mutationSequence"] == 2
    assert state["checkpoint"]["mutationSequence"] == 1
    assert state["projection"]["state"] == "stale"
    assert state["rebuildLease"]["active"] is False

    with pytest.raises(KehrnelError) as stale:
        await strategy.run_op(
            context,
            "rebuild_slim_search_collection",
            {"batch_size": 2, "cursor": first["next_cursor"]},
        )
    assert stale.value.code == "SEARCH_REBUILD_CURSOR_STALE"


@pytest.mark.asyncio
async def test_rebuild_does_not_report_ready_when_index_ensure_warns():
    context, storage, _, _ = await _runtime_fixture(1)
    atlas = RecordingAtlas(
        {"created": [], "updated": [], "warnings": ["index build rejected"]}
    )
    context.adapters["atlas_search"] = atlas

    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )

    assert result["complete"] is True
    assert result["reconciliation"]["safe"] is True
    assert result["projection"]["state"] == "index_pending"
    assert result["index"]["status"] == "ensure_pending"
    assert "index build rejected" in result["warnings"]


@pytest.mark.asyncio
async def test_rebuild_rejects_invalid_or_stale_cursor_before_writing():
    context, storage, atlas, _ = await _runtime_fixture(3)
    strategy = RPSDualStrategy(MANIFEST)

    with pytest.raises(KehrnelError) as malformed:
        await strategy.run_op(
            context,
            "rebuild_slim_search_collection",
            {"batch_size": 2, "cursor": "not-a-valid-cursor"},
        )
    assert malformed.value.code == "SEARCH_REBUILD_CURSOR_INVALID"
    assert storage.replace_calls == []
    assert atlas.calls == []

    first = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 2},
    )
    writes_before_stale_cursor = len(storage.replace_calls)
    changed = copy.deepcopy(context.config)
    changed["transform"]["mappings"] = {
        "templates": [
            {
                "templateId": "sample_laboratory_v0.4",
                "fields": [
                    {
                        "name": "Diagnosis (Code)",
                        "path": "/content[openEHR-EHR-SECTION.problem_list.v0]/items[openEHR-EHR-EVALUATION.problem_diagnosis.v1]/data[at0001]/items[at0002]/value/defining_code/code_string",
                        "rmType": "DV_CODED_TEXT",
                    }
                ],
            }
        ]
    }
    changed_context = StrategyContext(
        environment_id=context.environment_id,
        config=changed,
        adapters=context.adapters,
        manifest=MANIFEST,
        meta={},
    )

    with pytest.raises(KehrnelError) as stale:
        await strategy.run_op(
            changed_context,
            "rebuild_slim_search_collection",
            {"batch_size": 2, "cursor": first["next_cursor"]},
        )
    assert stale.value.code == "SEARCH_REBUILD_CURSOR_MISMATCH"
    assert len(storage.replace_calls) == writes_before_stale_cursor
    assert atlas.calls == []


@pytest.mark.asyncio
async def test_rebuild_fails_closed_on_missing_projection_or_invalid_batch_size():
    context, storage, atlas, _ = await _runtime_fixture(1)
    strategy = RPSDualStrategy(MANIFEST)
    invalid_context = StrategyContext(
        environment_id=context.environment_id,
        config=copy.deepcopy(context.config),
        adapters=context.adapters,
        manifest=MANIFEST,
        meta={"mappings": {"templates": []}},
    )

    with pytest.raises(KehrnelError) as missing_projection:
        await strategy.run_op(
            invalid_context,
            "rebuild_slim_search_collection",
            {"batch_size": 1},
        )
    assert missing_projection.value.code == "SEARCH_REBUILD_CONFIG_INVALID"

    with pytest.raises(KehrnelError) as invalid_batch:
        await strategy.run_op(
            context,
            "rebuild_slim_search_collection",
            {"batch_size": 0},
        )
    assert invalid_batch.value.code == "SEARCH_REBUILD_BATCH_INVALID"
    assert storage.aggregate_calls == []
    assert storage.replace_calls == []
    assert atlas.calls == []


@pytest.mark.asyncio
async def test_template_scoped_rebuild_uses_shared_projection_and_ignores_other_templates():
    context, storage, atlas, expected_search = await _runtime_fixture(1)
    source_collection = context.config["collections"]["compositions"]["name"]
    search_collection = context.config["collections"]["search"]["name"]
    unrelated_source = copy.deepcopy(
        next(iter(storage.db[source_collection].docs.values()))
    )
    unrelated_source.update(
        {
            "_id": "source-unrelated-empty",
            "comp_id": "composition-unrelated-empty",
            "tid": "sample_immunization_list_v0.5",
            "cn": [],
        }
    )
    storage.db[source_collection].docs[unrelated_source["_id"]] = unrelated_source
    storage.db[search_collection].docs["unrelated-orphan"] = {
        "_id": "unrelated-orphan",
        "comp_id": "unrelated-orphan",
        "tid": "sample_immunization_list_v0.5",
        "sn": [{"p": "unrelated", "data": {"v": "preserve"}}],
    }

    result = await RPSDualStrategy(MANIFEST).run_op(
        context,
        "rebuild_slim_search_collection",
        {
            "batch_size": 10,
            "template_ids": ["sample_laboratory_v0.4"],
        },
    )

    assert result["complete"] is True
    assert result["processed"] == 1
    assert result["projection"]["state"] == "ready"
    assert result["projection"]["templateIds"] == ["sample_laboratory_v0.4"]
    assert result["reconciliation"]["safe"] is True
    assert result["reconciliation"]["mappedEmptyCount"] == 0
    assert result["reconciliation"]["orphanSidecarDetected"] is False
    assert result["reconciliation"]["unmappedSidecarDetected"] is False
    assert storage.db[search_collection].docs["unrelated-orphan"]["sn"][0]["p"] == (
        "unrelated"
    )
    assert set(expected_search).issubset(storage.db[search_collection].docs)
    state = storage.db[STATE_COLLECTION].docs[result["state"]["id"]]
    assert state["scope"] == {
        "kind": "templates",
        "templateIds": ["sample_laboratory_v0.4"],
    }
    assert len(atlas.calls) == 1


@pytest.mark.asyncio
async def test_semantic_preflight_prefers_ready_template_scope_over_stale_global_scope():
    context, storage, _, _ = await _runtime_fixture(1)
    strategy = RPSDualStrategy(MANIFEST)
    search_collection = context.config["collections"]["search"]["name"]
    storage.db[search_collection].docs["global-orphan"] = {
        "_id": "global-orphan",
        "comp_id": "global-orphan",
        "tid": "sample_immunization_list_v0.5",
        "sn": [{"p": "unrelated", "data": {"v": "stale"}}],
    }
    global_result = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {"batch_size": 10},
    )
    assert global_result["projection"]["state"] == "stale"

    scoped_result = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {
            "batch_size": 10,
            "template_ids": ["sample_laboratory_v0.4"],
        },
    )
    assert scoped_result["projection"]["state"] == "ready"

    projection = await strategy._build_active_search_projection(
        context,
        normalize_config(context.config),
    )
    template_id = "sample_laboratory_v0.4"
    provenance = projection["templateProvenance"][template_id]
    result = await strategy.run_op(
        context,
        "semantic_projection_preflight",
        {
            "contract": "openehr-semantic-projection-preflight/v1",
            "templateId": template_id,
            "templateSourceDigest": provenance["templateSourceDigest"],
            "catalogDigest": provenance["catalogDigest"],
            "planDigest": "sha256:" + "d" * 64,
            "requiredPaths": [projection["templatePaths"][template_id][0]],
        },
    )

    assert result["ready"] is True
    assert result["projectionScope"] == {
        "kind": "templates",
        "templateIds": [template_id],
    }


@pytest.mark.asyncio
async def test_governed_write_invalidates_template_scoped_readiness():
    context, storage, _, _ = await _runtime_fixture(1)
    strategy = RPSDualStrategy(MANIFEST)
    rebuilt = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {
            "batch_size": 10,
            "template_ids": ["sample_laboratory_v0.4"],
        },
    )
    state_id = rebuilt["state"]["id"]
    before_sequence = storage.db[STATE_COLLECTION].docs[state_id][
        "mutationSequence"
    ]

    normalized = normalize_config(context.config)
    await strategy._invalidate_projection_readiness(
        context,
        normalized,
        reason="test_scoped_invalidation",
    )

    scoped_state = storage.db[STATE_COLLECTION].docs[state_id]
    assert scoped_state["projection"]["state"] == "stale"
    assert scoped_state["projection"]["complete"] is False
    assert scoped_state["reconciliation"]["safe"] is False
    assert scoped_state["mutationSequence"] == before_sequence + 1


@pytest.mark.asyncio
async def test_scoped_rebuild_cursor_cannot_be_reused_for_another_template_scope():
    context, _, _, _ = await _runtime_fixture(3)
    strategy = RPSDualStrategy(MANIFEST)
    first = await strategy.run_op(
        context,
        "rebuild_slim_search_collection",
        {
            "batch_size": 2,
            "template_ids": ["sample_laboratory_v0.4"],
        },
    )
    assert first["next_cursor"]

    with pytest.raises(KehrnelError) as mismatch:
        await strategy.run_op(
            context,
            "rebuild_slim_search_collection",
            {
                "batch_size": 2,
                "cursor": first["next_cursor"],
                "template_ids": ["sample_immunization_list_v0.5"],
            },
        )

    assert mismatch.value.code == "SEARCH_REBUILD_CURSOR_MISMATCH"
