from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_coding_opts,
    build_flattener_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.index_definition_builder import (
    build_search_index_definition_from_mappings,
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import (
    CompositionFlattener,
)
from kehrnel.engine.strategies.openehr.rps_dual.strategy import (
    MANIFEST,
    RPSDualStrategy,
)


STRATEGY_ROOT = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "kehrnel"
    / "engine"
    / "strategies"
    / "openehr"
    / "rps_dual"
)
SAMPLES_ROOT = STRATEGY_ROOT / "samples" / "reference"
PROJECTION_MAPPINGS_PATH = SAMPLES_ROOT / "projection_mappings.json"
PACKAGED_SEARCH_INDEX_PATH = SAMPLES_ROOT / "search_index.definition.json"
PACKAGED_ACTIVATION_CONFIG_PATH = SAMPLES_ROOT / "activation.config.json"
BUNDLED_SEARCH_INDEX_PATH = STRATEGY_ROOT / "bundles" / "searchIndex" / "searchIndex.json"
BUNDLED_CODES_PATH = STRATEGY_ROOT / "bundles" / "dictionaries" / "_codes.json"
SHORTCUTS_PATH = STRATEGY_ROOT / "bundles" / "shortcuts" / "shortcuts.json"
QUERIES_ROOT = SAMPLES_ROOT / "queries"


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = docs or {}

    async def find_one(self, flt=None, projection=None):
        if flt and "_id" in flt:
            return self.docs.get(flt["_id"])
        return next(iter(self.docs.values())) if self.docs else None

    def find(self, flt=None):
        class _Cursor:
            def __init__(self, docs):
                self._docs = docs

            def limit(self, _n):
                return self

            async def to_list(self, length=None):
                return self._docs[:length] if length is not None else self._docs

        return _Cursor(list(self.docs.values()))


class _FakeDb(dict):
    def __getitem__(self, name):
        return super().__getitem__(name)


class _FakeStorage:
    def __init__(self, db):
        self.db = db

    async def find_one(self, collection, flt):
        return await self.db[collection].find_one(flt)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _bundled_codes_doc() -> dict:
    payload = _load_json(BUNDLED_CODES_PATH)
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and item.get("_id") == "ar_code":
                return item
        raise AssertionError("Packaged _codes bundle does not contain an ar_code document")
    return payload


async def _build_packaged_sample_db() -> _FakeDb:
    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    strategy_cfg = normalize_config(
        {
            "transform": {
                "mappings": projection_mappings,
            }
        }
    )

    db = _FakeDb(
        {
            "_codes": _FakeCollection({"ar_code": _bundled_codes_doc()}),
            "_shortcuts": _FakeCollection({"shortcuts": _load_json(SHORTCUTS_PATH)}),
            "compositions_rps": _FakeCollection({}),
            "compositions_search": _FakeCollection({}),
        }
    )

    flattener = await CompositionFlattener.create(
        db=db,
        config=build_flattener_config(strategy_cfg),
        mappings_path="unused",
        mappings_content=projection_mappings,
        coding_opts=build_coding_opts(strategy_cfg),
    )

    for ndjson_path in sorted((SAMPLES_ROOT / "envelopes").glob("*.ndjson")):
        if ndjson_path.name == "all.ndjson":
            continue
        for line in ndjson_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            envelope = json.loads(line)
            base_doc, search_doc = flattener.transform_composition(envelope)
            db["compositions_rps"].docs[base_doc["comp_id"]] = base_doc
            if search_doc is not None:
                db["compositions_search"].docs[search_doc["_id"]] = search_doc

    return db


@pytest.mark.asyncio
async def test_packaged_sample_envelopes_produce_search_sidecar_docs():
    strategy_cfg = normalize_config(
        {
            "transform": {
                "apply_shortcuts": False,
                "coding": {
                    "arcodes": {"strategy": "literal"},
                    "atcodes": {"strategy": "literal"},
                },
            }
        }
    )
    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    flattener = await CompositionFlattener.create(
        db=None,
        config=build_flattener_config(strategy_cfg),
        mappings_path="unused",
        mappings_content=projection_mappings,
        coding_opts=build_coding_opts(strategy_cfg),
    )

    checked = 0
    for ndjson_path in sorted((SAMPLES_ROOT / "envelopes").glob("*.ndjson")):
        if ndjson_path.name == "all.ndjson":
            continue
        for line in ndjson_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            envelope = json.loads(line)
            checked += 1

            assert envelope["composition_version"] == "1"
            assert envelope["time_committed"]

            base_doc, search_doc = flattener.transform_composition(envelope)

            assert isinstance(base_doc.get("time_c"), datetime)
            assert search_doc is not None
            assert search_doc.get("sn")
            assert isinstance(search_doc.get("sort_time"), datetime)
            assert search_doc.get("tid") == envelope["template_id"]

    assert checked == 10


@pytest.mark.asyncio
async def test_packaged_activation_config_points_to_reference_projection_mappings():
    activation_cfg = _load_json(PACKAGED_ACTIVATION_CONFIG_PATH)
    assert activation_cfg == {
        "transform": {
            "mappings": "file://samples/reference/projection_mappings.json",
        }
    }


@pytest.mark.asyncio
async def test_packaged_search_index_definition_matches_generated_definition():
    projection_mappings = _load_json(PROJECTION_MAPPINGS_PATH)
    shortcuts = _load_json(SHORTCUTS_PATH)

    generated = await build_search_index_definition_from_mappings(
        normalize_config({}),
        projection_mappings,
        shortcuts=shortcuts,
    )

    packaged_definition = _load_json(PACKAGED_SEARCH_INDEX_PATH)
    bundled_definition = _load_json(BUNDLED_SEARCH_INDEX_PATH)

    assert generated["warnings"] == []
    assert generated["definition"] == packaged_definition
    assert packaged_definition == bundled_definition


@pytest.mark.asyncio
async def test_packaged_sample_aql_queries_compile_against_packaged_dataset():
    db = await _build_packaged_sample_db()
    ctx = StrategyContext(
        environment_id="env-samples",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()

    expectations = {
        "patient_laboratory_by_ehr.aql": {"stage0": "$match", "scope": "patient"},
        "cross_patient_laboratory_by_performing_centre.aql": {
            "stage0": "$search",
            "scope": "cross_patient",
        },
        "cross_patient_immunization_by_template.aql": {
            "stage0": "$match",
            "scope": "cross_patient",
        },
        "cross_patient_immunization_by_publishing_centre.aql": {
            "stage0": "$search",
            "scope": "cross_patient",
        },
    }

    compiled = 0
    for query_path in sorted(QUERIES_ROOT.glob("*.aql")):
        query_text = query_path.read_text(encoding="utf-8").strip()
        assert query_text, f"Sample query {query_path.name} must not be empty"

        plan = await strategy.compile_query(
            ctx,
            "openEHR",
            {
                "raw_aql": query_text,
                "debug": True,
            },
        )

        pipeline = plan.plan.get("pipeline", [])
        assert pipeline, f"Sample query {query_path.name} did not produce a pipeline"

        expected = expectations[query_path.name]
        stage0 = next(iter(pipeline[0]))
        assert stage0 == expected["stage0"]
        assert plan.explain["scope"] == expected["scope"]
        compiled += 1

    assert compiled == len(expectations)
