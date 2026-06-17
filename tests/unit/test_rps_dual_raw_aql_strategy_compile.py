from __future__ import annotations

from copy import deepcopy

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual import strategy as strategy_module
from kehrnel.engine.strategies.openehr.rps_dual.query.compiler import _get_or_create_shared_archetype_resolver
from kehrnel.engine.strategies.openehr.rps_dual.strategy import MANIFEST, RPSDualStrategy


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

        docs = list(self.docs.values())
        return _Cursor(docs)


class _FakeDb(dict):
    def __getitem__(self, name):
        return super().__getitem__(name)


class _FakeStorage:
    def __init__(self, db):
        self.db = db

    async def find_one(self, collection, flt):
        return await self.db[collection].find_one(flt)


def _build_fake_lab_db() -> _FakeDb:
    return _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": -1,
                            "at0002": -2,
                            "at0003": -3,
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "report_result": {
                                "v1": 1
                            }
                        },
                        "openEHR-EHR-OBSERVATION": {
                            "laboratory_test_result": {
                                "v1": 2
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "laboratory_test_analyte": {
                                "v1": 3
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        "items": {
                            "archetype_details": "ad",
                            "context": "cx",
                            "data": "d",
                            "events": "ev",
                            "items": "i",
                            "magnitude": "m",
                            "start_time": "st",
                            "template_id": "ti",
                            "value": "v",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1", "data": {"ani": 1}}],
                    }
                }
            ),
            "compositions_search": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "sn": [{"p": "1"}],
                    }
                }
            ),
        }
    )


@pytest.mark.asyncio
async def test_compile_query_raw_aql_reuses_parsed_ast_across_parameter_changes(monkeypatch):
    parse_calls = []

    class _CountingParser:
        def __init__(self, text):
            self.text = text

        def parse(self):
            parse_calls.append(self.text)
            return {
                "from": {"alias": "e"},
                "where": {"path": "e/ehr_id/value", "operator": "=", "value": "$ehrId"},
                "select": {"distinct": False, "columns": {}},
            }

    async def _fake_get_shortcuts(_ctx):
        return {"items": {}, "source": "cache", "missing": False}

    async def _fake_get_codes(_ctx):
        return {"items": {}, "source": "cache", "missing": False}

    async def _fake_build_query_pipeline_from_ast(ast_doc, *_args, **_kwargs):
        return (
            "pipeline_builder",
            [{"$match": {"ehr_id": ast_doc["where"]["value"]}}],
            "$match",
            {
                "composition": {"collection": "compositions_rps"},
                "search": {"collection": "compositions_search"},
            },
            ast_doc,
            {
                "chosen": "pipeline_builder",
                "scope": "patient",
                "reason": "scope_patient",
                "has_ehr_id_pred": True,
                "prefer_match": False,
                "search_enabled": True,
            },
        )

    monkeypatch.setattr(strategy_module, "AQLToASTParser", _CountingParser)
    monkeypatch.setattr(strategy_module, "get_shortcuts", _fake_get_shortcuts)
    monkeypatch.setattr(strategy_module, "get_codes", _fake_get_codes)
    monkeypatch.setattr(strategy_module, "build_query_pipeline_from_ast", _fake_build_query_pipeline_from_ast)

    ctx = StrategyContext(
        environment_id="env-1",
        config=MANIFEST.default_config,
        adapters={},
        manifest=MANIFEST.model_copy(deep=True),
        meta={"query_compile_cache": {}},
    )
    strategy = RPSDualStrategy()
    raw_aql = "SELECT e/ehr_id/value AS ehrId FROM EHR e WHERE e/ehr_id/value = $ehrId"

    first = await strategy.compile_query(
        ctx,
        "openEHR",
        {"raw_aql": raw_aql, "params": {"ehrId": "ehr-1"}},
    )
    second = await strategy.compile_query(
        ctx,
        "openEHR",
        {"raw_aql": raw_aql, "params": {"ehrId": "ehr-2"}},
    )

    assert parse_calls == [raw_aql]
    assert first.plan["pipeline"][0]["$match"]["ehr_id"] == "ehr-1"
    assert second.plan["pipeline"][0]["$match"]["ehr_id"] == "ehr-2"
    assert first.plan["explain"]["builder"]["cache"]["raw_aql_ast"] == "miss"
    assert second.plan["explain"]["builder"]["cache"]["raw_aql_ast"] == "hit"


def test_shared_archetype_resolver_cache_reuses_instance():
    compile_cache = {}
    schema_cfgs = {
        "composition": {
            "codes_collection": "_codes",
            "codes_doc_id": "ar_code",
            "collection": "compositions_rps",
            "separator": "/",
            "atcode_strategy": "compact_prefix",
        },
        "search": {
            "collection": "compositions_search",
        },
    }

    resolver_one, first_status = _get_or_create_shared_archetype_resolver(
        db=object(),
        schema_cfgs=schema_cfgs,
        compile_cache=compile_cache,
    )
    resolver_two, second_status = _get_or_create_shared_archetype_resolver(
        db=object(),
        schema_cfgs=schema_cfgs,
        compile_cache=compile_cache,
    )

    assert first_status == "miss"
    assert second_status == "hit"
    assert resolver_one is resolver_two


@pytest.mark.asyncio
async def test_compile_query_raw_aql_uses_strategy_field_names_codes_and_shortcuts():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": -1,
                            "at0003": -3,
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": 1
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "health_thread": {
                                "v0": 8
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        "items": {
                            "context": "cx",
                            "start_time": "st",
                            "value": "v",
                            "other_context": "oc",
                            "items": "i",
                            "id": "id",
                            "archetype_details": "ad",
                            "template_id": "ti",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1"}],
                    }
                }
            ),
            "compositions_search": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-1",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        c/context/start_time/value AS StartTime,
        v/commit_audit/time_committed/value AS DataRegistre
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        e/ehr_id/value = 'ehr-1'
        AND c/archetype_details/template_id/value = 'PO_Obstetric_process_v0.8_FORMULARIS'
        AND v/commit_audit/time_committed/value >= '2017-04-14T17:29:47.785Z'
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    pipeline = plan.plan["pipeline"]
    match_stage = pipeline[0]["$match"]
    assert match_stage["ehr_id"] == "ehr-1"
    assert match_stage["tid"] == "PO_Obstetric_process_v0.8_FORMULARIS"
    assert "time_c" in match_stage
    assert "time_created" not in match_stage
    assert "cn" in match_stage
    assert match_stage["cn"]["$elemMatch"] == {"p": "1", "data.ani": 1}

    project_stage = pipeline[1]["$project"]
    assert project_stage["DataRegistre"] == "$time_c"
    assert project_stage["StartTime"]["$first"]["$map"]["in"] == "$$node.data.cx.st.v"
    assert project_stage["StartTime"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["input"] == "$$node.p"
    assert project_stage["StartTime"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["regex"] == "^1$"


@pytest.mark.asyncio
async def test_compile_query_raw_aql_supports_mixed_or_across_ehr_and_composition_levels():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": -1,
                            "at0005": -5,
                            "at0011": -11,
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": 1
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "admin_salut": {
                                "v0": 11
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        "items": {
                            "context": "cx",
                            "start_time": "st",
                            "value": "v",
                            "other_context": "oc",
                            "items": "i",
                            "archetype_details": "ad",
                            "template_id": "ti",
                            "defining_code": "dc",
                            "code_string": "cs",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1"}],
                    }
                }
            ),
            "compositions_search": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-1",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        e/ehr_id/value AS ehrId,
        c/context/start_time/value AS StartTime
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        c/archetype_details/template_id/value = 'PO_Obstetric_process_v0.8_FORMULARIS'
        AND (
            e/ehr_id/value = 'ehr-1'
            OR c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.admin_salut.v0]/items[at0005]/items[at0011]/value/defining_code/code_string = 'E08033260'
        )
    ORDER BY
        c/context/start_time/value DESC
    LIMIT 10
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    match_stage = plan.plan["pipeline"][0]["$match"]
    assert "$and" in match_stage
    assert any(part.get("tid") == "PO_Obstetric_process_v0.8_FORMULARIS" for part in match_stage["$and"])

    or_branch = next((part["$or"] for part in match_stage["$and"] if "$or" in part), None)
    assert or_branch is not None
    assert any(option.get("ehr_id") == "ehr-1" for option in or_branch)
    assert any("cn" in option for option in or_branch)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operator_fragment", "expected_regex"),
    [
        ("LIKE 'E08033*'", "^E08033.*$"),
        ("MATCHES {'E08033260','H17001484'}", "^(?:E08033260|H17001484)$"),
    ],
)
async def test_compile_query_raw_aql_supports_string_pattern_operators_in_patient_scope(
    operator_fragment: str,
    expected_regex: str,
):
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": -1,
                            "at0005": -5,
                            "at0011": -11,
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": 1
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "admin_salut": {
                                "v0": 11
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        "items": {
                            "context": "cx",
                            "start_time": "st",
                            "value": "v",
                            "other_context": "oc",
                            "items": "i",
                            "archetype_details": "ad",
                            "template_id": "ti",
                            "defining_code": "dc",
                            "code_string": "cs",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1"}],
                    }
                }
            ),
            "compositions_search": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-1",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = f"""
    SELECT
        e/ehr_id/value AS ehrId
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        e/ehr_id/value = 'ehr-1'
        AND c/archetype_details/template_id/value = 'PO_Obstetric_process_v0.8_FORMULARIS'
        AND c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.admin_salut.v0]/items[at0005]/items[at0011]/value/defining_code/code_string {operator_fragment}
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    match_stage = plan.plan["pipeline"][0]["$match"]
    regex_branch = next(
        part["cn"]["$elemMatch"]
        for part in match_stage["$and"]
        if part.get("cn", {}).get("$elemMatch", {}).get("data.v.dc.cs")
    )
    assert regex_branch["p"]["$regex"] == "^\\-11:\\-5:11:\\-1:1(?::[^:]+)*$"
    assert regex_branch["data.v.dc.cs"]["$regex"] == expected_regex


@pytest.mark.asyncio
async def test_compile_query_raw_aql_rejects_order_by_projection_alias():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": 1
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        "items": {
                            "archetype_details": "ad",
                            "template_id": "ti",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1"}],
                    }
                }
            ),
            "compositions_search": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-1",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        e/ehr_id/value AS ehrId
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        c/archetype_details/template_id/value = 'PO_Obstetric_process_v0.8_FORMULARIS'
    ORDER BY
        ehrId ASC
    LIMIT 10
    """

    with pytest.raises(ValueError, match="ORDER BY projection alias 'ehrId' is not supported"):
        await strategy.compile_query(
            ctx,
            "openEHR",
            {
                "raw_aql": raw_aql,
                "debug": True,
            },
        )


@pytest.mark.asyncio
async def test_compile_query_raw_aql_resolves_content_paths_and_order_by_for_match_pipeline():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": -1,
                            "at0002": -2,
                            "at0003": -3,
                            "at0004": -4,
                            "at0005": -5,
                            "at0006": -6,
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": 1
                            }
                        },
                        "openEHR-EHR-OBSERVATION": {
                            "probs_base_observation": {
                                "v0": 2
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "health_thread": {
                                "v0": 8
                            },
                            "obstetric_process_closure": {
                                "v0": 12
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        "items": {
                            "context": "cx",
                            "start_time": "st",
                            "value": "v",
                            "defining_code": "df",
                            "code_string": "cs",
                            "content": "ct",
                            "data": "data",
                            "events": "ev",
                            "items": "i",
                            "archetype_details": "ad",
                            "template_id": "ti",
                            "uid": "uid",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1", "data": {"ani": 1}}],
                    }
                }
            ),
            "compositions_search": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "sn": [
                            {"p": "-4:8:-3:-2:-1:2:1"},
                            {"p": "-5:8:-3:-2:-1:2:1"},
                            {"p": "-6:8:-3:-2:-1:2:1"},
                            {"p": "-1:12:-3:-2:-1:2:1"},
                            {"p": "-2:12:-3:-2:-1:2:1"},
                            {"p": "-3:12:-3:-2:-1:2:1"},
                        ],
                    }
                }
            ),
        }
    )

    ctx = StrategyContext(
        environment_id="env-1",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        c/context/start_time/value AS StartTime,
        c/uid/value AS compositionId,
        v/commit_audit/time_committed/value AS DataRegistre,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.health_thread.v0]/items[at0004]/value/value AS DataInici,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.obstetric_process_closure.v0]/items[at0003]/value/value AS DataHoraFiProces
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        c/archetype_details/template_id/value = 'PO_Obstetric_process_v0.8_FORMULARIS'
        AND v/commit_audit/time_committed/value >= '2017-04-14T17:29:47.785Z'
        AND v/commit_audit/time_committed/value < '2026-04-14T17:29:47.786Z'
    ORDER BY
        v/commit_audit/time_committed/value,
        c/uid/value
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    pipeline = plan.plan["pipeline"]
    match_stage = pipeline[0]["$match"]
    assert match_stage["tid"] == "PO_Obstetric_process_v0.8_FORMULARIS"
    assert match_stage["cn"]["$elemMatch"] == {"p": "1", "data.ani": 1}

    project_stage = pipeline[1]["$project"]
    data_inici_regex = project_stage["DataInici"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["regex"]
    data_hora_fi_regex = project_stage["DataHoraFiProces"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["regex"]
    assert ":2:1" in data_inici_regex
    assert ":2:1" in data_hora_fi_regex
    assert ":1:1" not in data_inici_regex

    sort_stage = pipeline[2]["$sort"]
    assert sort_stage == {"DataRegistre": 1, "compositionId": 1}


@pytest.mark.asyncio
async def test_compile_query_raw_aql_supports_configured_separator_and_compact_atcodes():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "A1",
                            "at0002": "A2",
                            "at0003": "A3",
                            "at0004": "A4",
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": 1
                            }
                        },
                        "openEHR-EHR-OBSERVATION": {
                            "probs_base_observation": {
                                "v0": 2
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "health_thread": {
                                "v0": 8
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        "items": {
                            "context": "cx",
                            "start_time": "st",
                            "value": "v",
                            "content": "ct",
                            "data": "data",
                            "events": "ev",
                            "items": "i",
                            "archetype_details": "ad",
                            "template_id": "ti",
                            "uid": "uid",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1", "data": {"ani": 1}}],
                    }
                }
            ),
            "compositions_search": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "sn": [
                            {"p": "A4:8:A3:A2:A1:2:1"},
                        ],
                    }
                }
            ),
        }
    )

    cfg = deepcopy(MANIFEST.default_config)
    cfg["paths"]["separator"] = ":"

    ctx = StrategyContext(
        environment_id="env-compact-prefix",
        config=cfg,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.health_thread.v0]/items[at0004]/value/value AS DataInici
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        c/archetype_details/template_id/value = 'PO_Obstetric_process_v0.8_FORMULARIS'
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    pipeline = plan.plan["pipeline"]
    assert pipeline[0]["$match"]["cn"]["$elemMatch"] == {"p": "1", "data.ani": 1}
    data_inici_regex = pipeline[1]["$project"]["DataInici"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["regex"]
    assert "A4:8:A3:A2:A1:2:1" in data_inici_regex
    assert ".2\\.1" not in data_inici_regex


@pytest.mark.asyncio
async def test_compile_query_raw_aql_supports_min_aggregate_on_standard_pipeline():
    db = _build_fake_lab_db()

    ctx = StrategyContext(
        environment_id="env-1",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        MIN(c/context/start_time/value) AS minStartTime
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report-result.v1]
    WHERE
        e/ehr_id/value = 'ehr-1'
        AND c/archetype_details/template_id/value = 'sample_laboratory_v0.4'
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    assert plan.plan["explain"]["builder"]["compiler_engine"] == "pipeline_builder"
    pipeline = plan.plan["pipeline"]
    match_stage = pipeline[0]["$match"]
    match_parts = match_stage.get("$and") if isinstance(match_stage.get("$and"), list) else [match_stage]
    assert any(part.get("ehr_id") == "ehr-1" for part in match_parts if isinstance(part, dict))
    assert pipeline[1]["$addFields"]["__aggregate_values"]["$filter"]["input"]["$map"]["in"] == "$$node.data.cx.st.v"
    assert pipeline[2] == {"$unwind": "$__aggregate_values"}
    assert pipeline[3]["$group"]["minStartTime"] == {"$min": "$__aggregate_values"}
    assert pipeline[4]["$project"]["minStartTime"] == 1


@pytest.mark.asyncio
async def test_compile_query_raw_aql_supports_avg_aggregate_on_cross_patient_pipeline():
    db = _build_fake_lab_db()

    ctx = StrategyContext(
        environment_id="env-1",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        AVG(o/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.laboratory_test_analyte.v1]/items[at0001]/value/magnitude) AS avgMagnitude
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report-result.v1]
            CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.laboratory_test_result.v1]
    WHERE
        c/archetype_details/template_id/value = 'sample_laboratory_v0.4'
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    assert plan.plan["explain"]["builder"]["compiler_engine"] == "pipeline_builder"
    pipeline = plan.plan["pipeline"]
    assert pipeline[0].get("$match")
    aggregate_values_stage = next(
        stage["$addFields"]["__aggregate_values"]
        for stage in pipeline
        if "$addFields" in stage and "__aggregate_values" in stage["$addFields"]
    )
    assert aggregate_values_stage["$filter"]["input"]["$map"]["in"] == "$$node.data.v.m"
    assert any(stage == {"$unwind": "$__aggregate_values"} for stage in pipeline)
    group_stage = next(stage["$group"] for stage in pipeline if "$group" in stage)
    assert group_stage["avgMagnitude"] == {"$avg": "$__aggregate_values"}
