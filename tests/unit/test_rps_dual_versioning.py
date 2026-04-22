from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from bson import ObjectId

from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_coding_opts,
    build_flattener_config,
    build_schema_config,
    normalize_bulk_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual.query.compiler import (
    build_query_pipeline,
    build_query_pipeline_from_ast,
    build_runtime_strategy,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.search_pipeline_builder import (
    SearchPipelineBuilder,
)


def _mappings_path() -> str:
    return str(
        Path(__file__).resolve().parents[2]
        / "src"
        / "kehrnel"
        / "engine"
        / "strategies"
        / "openehr"
        / "rps_dual"
        / "ingest"
        / "config"
        / "flattener_mappings_f.jsonc"
    )


def _minimal_composition() -> dict:
    return {
        "_type": "COMPOSITION",
        "archetype_node_id": "openEHR-EHR-COMPOSITION.test.v0",
        "uid": {
            "_type": "OBJECT_VERSION_ID",
            "value": "comp-1::my-openehr-server::1",
        },
        "name": {
            "_type": "DV_TEXT",
            "value": "Example composition",
        },
        "archetype_details": {
            "archetype_id": {"value": "openEHR-EHR-COMPOSITION.test.v0"},
            "template_id": {"value": "test-template"},
            "rm_version": "1.0.4",
        },
    }


def _build_flattener(raw_config: dict | None = None) -> CompositionFlattener:
    strategy_cfg = normalize_config(raw_config or {})
    bulk_cfg = normalize_bulk_config({"role": "primary"})
    return CompositionFlattener(
        db=None,
        config=build_flattener_config(strategy_cfg, bulk_cfg),
        mappings_path=_mappings_path(),
        mappings_content={"templates": []},
        coding_opts=build_coding_opts(strategy_cfg),
    )


class _DirectFieldResolver:
    async def translate_aql_path(self, aql_path: str):
        if aql_path == "c/uid/value":
            return None, "comp_id"
        raise AssertionError(f"Unexpected path resolution request: {aql_path}")


def test_transform_composition_materializes_commit_time_for_base_and_search(monkeypatch):
    flattener = _build_flattener()
    committed_at = datetime(2026, 4, 11, 13, 42, 6, tzinfo=timezone.utc)

    monkeypatch.setattr(
        flattener,
        "_compiled_rules_for_template",
        lambda template_name: [{"_path": (), "_cont": (), "_extra": [], "copy": ["p"]}],
    )
    monkeypatch.setattr(flattener, "_rule_matches", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        flattener,
        "_apply_rule",
        lambda *args, **kwargs: {"p": "1", "data": {"name": {"value": "Example composition"}}},
    )

    base_doc, search_doc = flattener.transform_composition(
        {
            "_id": "comp-1::my-openehr-server::1",
            "ehr_id": "ehr-1",
            "composition_version": "1",
            "time_committed": committed_at,
            "canonicalJSON": _minimal_composition(),
        }
    )

    assert base_doc["tid"] == "test-template"
    assert list(base_doc.keys())[:5] == ["ehr_id", "comp_id", "v", "time_c", "tid"]
    assert base_doc["time_c"] == committed_at
    assert all("ap" not in node for node in base_doc["cn"])
    assert search_doc is not None
    assert search_doc["tid"] == "test-template"
    assert search_doc["comp_id"] == base_doc["comp_id"]
    assert search_doc["sort_time"] == committed_at
    assert all("ap" not in node for node in search_doc["sn"])


def test_transform_composition_uses_configured_search_comp_id_field_name(monkeypatch):
    flattener = _build_flattener({"fields": {"document": {"comp_id": "composition_uid"}}})

    monkeypatch.setattr(
        flattener,
        "_compiled_rules_for_template",
        lambda template_name: [{"_path": (), "_cont": (), "_extra": [], "copy": ["p"]}],
    )
    monkeypatch.setattr(flattener, "_rule_matches", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        flattener,
        "_apply_rule",
        lambda *args, **kwargs: {"p": "1", "data": {"name": {"value": "Example composition"}}},
    )

    _, search_doc = flattener.transform_composition(
        {
            "_id": "64b64c2e5f6270b5c2c2c2c2",
            "ehr_id": "ehr-1",
            "composition_version": "1",
            "time_committed": datetime(2026, 4, 11, 13, 42, 6, tzinfo=timezone.utc),
            "canonicalJSON": _minimal_composition(),
        }
    )

    assert search_doc is not None
    assert search_doc["composition_uid"] == ObjectId("64b64c2e5f6270b5c2c2c2c2")


def test_transform_composition_copies_configured_envelope_fields_after_shortcuts(monkeypatch):
    flattener = _build_flattener(
        {
            "transform": {
                "envelope": {
                    "base": {
                        "version": "version",
                        "metrics.config_name": "ops.config_name",
                    },
                    "search": {
                        "template": "source_template",
                    },
                }
            }
        }
    )

    monkeypatch.setattr(
        flattener,
        "_compiled_rules_for_template",
        lambda template_name: [{"_path": (), "_cont": (), "_extra": [], "copy": ["p"]}],
    )
    monkeypatch.setattr(flattener, "_rule_matches", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        flattener,
        "_apply_rule",
        lambda *args, **kwargs: {"p": "1", "data": {"name": {"value": "Example composition"}}},
    )

    base_doc, search_doc = flattener.transform_composition(
        {
            "_id": "comp-1::my-openehr-server::1",
            "ehr_id": "ehr-1",
            "composition_version": "1",
            "time_committed": datetime(2026, 4, 11, 13, 42, 6, tzinfo=timezone.utc),
            "canonicalJSON": _minimal_composition(),
            "_source_envelope": {
                "version": "comp-1::my-openehr-server::1",
                "template": "test-template-human",
                "metrics": {"config_name": "CONFIG_1"},
            },
        }
    )

    assert base_doc["version"] == "comp-1::my-openehr-server::1"
    assert base_doc["ops"]["config_name"] == "CONFIG_1"
    assert search_doc is not None
    assert search_doc["source_template"] == "test-template-human"


@pytest.mark.asyncio
async def test_build_query_pipeline_maps_version_commit_time_to_top_level_match_and_projection():
    cfg = normalize_config({})
    ir = AqlQueryIR(
        scope="patient",
        predicates=[
            {"path": "e/ehr_id/value", "op": "=", "value": "ehr-1"},
            {"path": "c/archetype_details/template_id/value", "op": "=", "value": "test-template"},
            {
                "path": "v/commit_audit/time_committed/value",
                "op": ">=",
                "value": "2026-04-11T00:00:00Z",
            },
        ],
        select=[
            {
                "path": "v/commit_audit/time_committed/value",
                "alias": "DataRegistre",
            },
            {
                "path": "c/archetype_details/template_id/value",
                "alias": "TemplateId",
            },
        ],
        sort={"v/commit_audit/time_committed/value": 1},
    )

    engine, pipeline, *_ = await build_query_pipeline(ir, cfg)

    assert engine == "pipeline_builder"
    match_stage = pipeline[0]["$match"]
    assert match_stage["ehr_id"] == "ehr-1"
    assert match_stage["time_c"]["$gte"] == datetime(
        2026, 4, 11, 0, 0, tzinfo=timezone.utc
    )
    assert match_stage["tid"] == "test-template"
    assert match_stage["cn"]["$elemMatch"] == {"p": {"$regex": r"^[^:]+$"}}
    assert pipeline[1]["$project"]["DataRegistre"] == "$time_c"
    assert pipeline[1]["$project"]["TemplateId"] == "$tid"
    assert pipeline[2]["$sort"] == {"DataRegistre": 1}


@pytest.mark.asyncio
async def test_build_query_pipeline_prefers_match_for_cross_patient_template_and_commit_time_filters():
    cfg = normalize_config({})
    ast = {
        "select": {
            "distinct": False,
            "columns": {
                "0": {
                    "value": {
                        "type": "dataMatchPath",
                        "path": "v/commit_audit/time_committed/value",
                    },
                    "alias": "DataRegistre",
                }
            },
        },
        "from": {"rmType": "EHR", "alias": "e", "predicate": None},
        "contains": {
            "rmType": "VERSION",
            "alias": "v",
            "predicate": None,
            "contains": {
                "rmType": "COMPOSITION",
                "alias": "c",
                "predicate": {
                    "path": "archetype_node_id",
                    "operator": "=",
                    "value": "openEHR-EHR-COMPOSITION.probs_base_composition.v0",
                },
            },
        },
        "where": {
            "operator": "AND",
            "conditions": {
                "0": {
                    "path": "c/archetype_details/template_id/value",
                    "operator": "=",
                    "value": "test-template",
                },
                "1": {
                    "path": "v/commit_audit/time_committed/value",
                    "operator": ">=",
                    "value": "2026-04-11T00:00:00Z",
                },
            },
        },
        "orderBy": {
            "columns": {
                "0": {
                    "path": "v/commit_audit/time_committed/value",
                    "direction": "DESC",
                }
            }
        },
    }

    engine, pipeline, *_ = await build_query_pipeline_from_ast(ast, cfg)

    assert engine == "pipeline_builder"
    match_stage = pipeline[0]["$match"]
    assert match_stage["tid"] == "test-template"
    assert match_stage["time_c"]["$gte"] == datetime(
        2026, 4, 11, 0, 0, tzinfo=timezone.utc
    )
    assert match_stage["cn"]["$elemMatch"] == {"p": {"$regex": r"^[^:]+$"}}
    assert pipeline[1]["$project"]["DataRegistre"] == "$time_c"
    assert pipeline[2]["$sort"] == {"DataRegistre": -1}


@pytest.mark.asyncio
async def test_build_query_pipeline_uses_configured_separator_for_root_path_regex():
    cfg = normalize_config({"paths": {"separator": ":"}})
    ast = {
        "select": {
            "distinct": False,
            "columns": {
                "0": {
                    "value": {
                        "type": "dataMatchPath",
                        "path": "v/commit_audit/time_committed/value",
                    },
                    "alias": "DataRegistre",
                }
            },
        },
        "from": {"rmType": "EHR", "alias": "e", "predicate": None},
        "contains": {
            "rmType": "VERSION",
            "alias": "v",
            "predicate": None,
            "contains": {
                "rmType": "COMPOSITION",
                "alias": "c",
                "predicate": {
                    "path": "archetype_node_id",
                    "operator": "=",
                    "value": "openEHR-EHR-COMPOSITION.probs_base_composition.v0",
                },
            },
        },
        "where": {
            "operator": "AND",
            "conditions": {
                "0": {
                    "path": "c/archetype_details/template_id/value",
                    "operator": "=",
                    "value": "test-template",
                }
            },
        },
        "orderBy": {},
    }

    engine, pipeline, *_ = await build_query_pipeline_from_ast(ast, cfg)

    assert engine == "pipeline_builder"
    assert pipeline[0]["$match"]["cn"]["$elemMatch"] == {"p": {"$regex": r"^[^:]+$"}}


@pytest.mark.asyncio
async def test_build_query_pipeline_coerces_top_level_comp_id_to_objectid():
    cfg = normalize_config({})
    oid = "64b64c2e5f6270b5c2c2c2c2"
    ast = {
        "select": {
            "distinct": False,
            "columns": {
                "0": {
                    "value": {"type": "dataMatchPath", "path": "c/uid/value"},
                    "alias": "uid",
                }
            },
        },
        "from": {"rmType": "EHR", "alias": "e", "predicate": None},
        "contains": {"rmType": "COMPOSITION", "alias": "c", "predicate": None},
        "where": {
            "operator": "AND",
            "conditions": {
                "0": {
                    "path": "e/ehr_id/value",
                    "operator": "=",
                    "value": "ehr-1",
                },
                "1": {
                    "path": "c/uid/value",
                    "operator": "=",
                    "value": oid,
                },
            },
        },
        "orderBy": {},
    }

    engine, pipeline, *_ = await build_query_pipeline_from_ast(ast, cfg)

    assert engine == "pipeline_builder"
    assert pipeline[0]["$match"]["comp_id"] == ObjectId(oid)


@pytest.mark.asyncio
async def test_search_pipeline_builder_coerces_top_level_comp_id_to_objectid():
    cfg = normalize_config({})
    schema_cfg = build_schema_config(cfg)
    builder = SearchPipelineBuilder(
        "e",
        "c",
        schema_cfg["composition"],
        _DirectFieldResolver(),
        {},
        strategy=build_runtime_strategy(cfg),
        search_index_name="search_nodes_index",
    )

    query = await builder._handle_direct_condition_search(
        {"path": "c/uid/value", "operator": "=", "value": "64b64c2e5f6270b5c2c2c2c2"}
    )

    assert query == {
        "equals": {
            "path": "comp_id",
            "value": ObjectId("64b64c2e5f6270b5c2c2c2c2"),
        }
    }
