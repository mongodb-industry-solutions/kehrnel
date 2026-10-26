from __future__ import annotations

from copy import deepcopy

import pytest

from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.archetype_resolver import (
    ArchetypeResolver,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.pipeline_builder import (
    PipelineBuilder,
)
from kehrnel.engine.strategies.openehr.rps_dual.strategy import MANIFEST, RPSDualStrategy


def _first_node_filter(expr: dict) -> dict:
    return expr["$let"]["vars"]["node"]["$first"]["$filter"]


def _first_stage(pipeline: list[dict], stage_name: str) -> dict:
    return next(stage[stage_name] for stage in pipeline if stage_name in stage)


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


class _CountingFindCollection(_FakeCollection):
    def __init__(self, docs=None):
        super().__init__(docs)
        self.find_calls = 0

    def find(self, flt=None):
        self.find_calls += 1
        return super().find(flt)


class _CountingFindOneCollection(_FakeCollection):
    def __init__(self, docs=None):
        super().__init__(docs)
        self.find_one_calls = 0

    async def find_one(self, flt=None, projection=None):
        self.find_one_calls += 1
        return await super().find_one(flt, projection)


class _FakeDb(dict):
    def __getitem__(self, name):
        return super().__getitem__(name)


class _FakeStorage:
    def __init__(self, db):
        self.db = db

    async def find_one(self, collection, flt):
        return await self.db[collection].find_one(flt)


class _CountingFormatResolver:
    archetype_resolver = None

    def __init__(self):
        self.translate_calls = 0

    async def translate_aql_path(self, aql_path):
        self.translate_calls += 1
        return "^1$", "data.value.value"

    async def get_selector_codes(self, aql_path):
        return []


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
async def test_projection_source_resolution_is_reused_across_cache_plan_and_project():
    resolver = _CountingFormatResolver()
    builder = PipelineBuilder(
        "e",
        "c",
        {
            "format": "shortened",
            "composition_array": "cn",
            "path_field": "p",
            "data_field": "data",
            "separator": ":",
        },
        resolver,
        {},
    )
    ast = {
        "select": {
            "columns": {
                "col1": {"alias": "A", "value": {"path": "x/value/value"}},
                "col2": {"alias": "B", "value": {"path": "x/value/value"}},
            }
        }
    }

    projection_cache_plan = await builder.build_projection_cache_plan(ast)
    project_stage = await builder.build_project_stage(ast, projection_cache_plan=projection_cache_plan)

    assert resolver.translate_calls == 1
    assert builder.projection_source_cache_stats() == {"entries": 1, "hits": 3, "misses": 1}
    assert projection_cache_plan["group_count"] == 1
    assert project_stage["$project"]["A"] == "$__projection_cache.c0.data.value.value"
    assert project_stage["$project"]["B"] == "$__projection_cache.c0.data.value.value"


@pytest.mark.asyncio
async def test_archetype_resolver_reuses_nested_path_pattern_discovery():
    search = _CountingFindCollection(
        {
            "sample": {
                "_id": "comp-1",
                "sn": [{"p": "D2/D1/31/ROOT"}],
            }
        }
    )
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "D1",
                            "at0002": "D2",
                        },
                        "openEHR-EHR-EVALUATION": {
                            "adverse_reaction_risk": {
                                "v2": "31",
                            }
                        },
                    }
                }
            ),
            "search": search,
        }
    )
    resolver = ArchetypeResolver(
        db,
        search_collection="search",
        separator="/",
        atcode_strategy="compact_prefix",
        data_driven_path_discovery=True,
    )
    context_map = {
        "ar": {
            "archetype_id": "openEHR-EHR-EVALUATION.adverse_reaction_risk.v2",
        }
    }
    parts = ["data[at0001]", "items[at0002]", "value", "value"]

    first = await resolver.resolve_nested_path_to_p_pattern("ar", parts, context_map)
    second = await resolver.resolve_nested_path_to_p_pattern("ar", parts, context_map)

    assert first == second
    assert first == r"^D2/D1/31(?:/[^/]+)*$"
    assert search.find_calls == 1


@pytest.mark.asyncio
async def test_archetype_resolver_uses_deterministic_nested_path_pattern_by_default():
    search = _CountingFindCollection(
        {
            "sample": {
                "_id": "comp-1",
                "sn": [{"p": "D2/D1/31/ROOT"}],
            }
        }
    )
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "D1",
                            "at0002": "D2",
                        },
                        "openEHR-EHR-EVALUATION": {
                            "adverse_reaction_risk": {
                                "v2": "31",
                            }
                        },
                    }
                }
            ),
            "search": search,
        }
    )
    resolver = ArchetypeResolver(
        db,
        search_collection="search",
        separator="/",
        atcode_strategy="compact_prefix",
    )
    context_map = {
        "ar": {
            "archetype_id": "openEHR-EHR-EVALUATION.adverse_reaction_risk.v2",
        }
    }
    parts = ["data[at0001]", "items[at0002]", "value", "value"]

    first = await resolver.resolve_nested_path_to_p_pattern("ar", parts, context_map)
    second = await resolver.resolve_nested_path_to_p_pattern("ar", parts, context_map)

    assert first == second
    assert first == r"^D2/D1/31(?:/[^/]+)*$"
    assert search.find_calls == 0


@pytest.mark.asyncio
async def test_archetype_resolver_uses_seeded_code_items_without_reloading_codes():
    codes = _CountingFindOneCollection(
        {
            "ar_code": {
                "_id": "ar_code",
                "at": {
                    "at0001": "D1",
                    "at0002": "D2",
                },
                "openEHR-EHR-EVALUATION": {
                    "adverse_reaction_risk": {
                        "v2": "31",
                    }
                },
            }
        }
    )
    db = _FakeDb({"_codes": codes})
    resolver = ArchetypeResolver(
        db,
        separator="/",
        atcode_strategy="compact_prefix",
        code_items={
            "at0001": "D1",
            "at0002": "D2",
            "openEHR-EHR-EVALUATION.adverse_reaction_risk.v2": "31",
        },
    )
    context_map = {
        "ar": {
            "archetype_id": "openEHR-EHR-EVALUATION.adverse_reaction_risk.v2",
        }
    }

    pattern = await resolver.resolve_nested_path_to_p_pattern(
        "ar",
        ["data[at0001]", "items[at0002]", "value", "value"],
        context_map,
    )

    assert pattern == r"^D2/D1/31(?:/[^/]+)*$"
    assert codes.find_one_calls == 0


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

    assert pipeline[1] == {"$limit": 100}

    project_stage = _first_stage(pipeline, "$project")
    assert project_stage["DataRegistre"] == "$time_c"
    assert project_stage["StartTime"]["$let"]["in"] == "$$node.data.cx.st.v"
    start_time_filter = _first_node_filter(project_stage["StartTime"])
    assert start_time_filter["cond"]["$regexMatch"]["input"] == {
        "$cond": [{"$eq": [{"$type": "$$node.p"}, "string"]}, "$$node.p", ""]
    }
    assert start_time_filter["cond"]["$regexMatch"]["regex"] == "^1$"


@pytest.mark.asyncio
async def test_compile_query_raw_aql_defaults_to_100_row_page_when_limit_missing():
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
        c/uid/value AS compositionId
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report_result.v1]
    WHERE
        e/ehr_id/value = 'patient-1'
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    assert plan.plan["pipeline"][1] == {"$limit": 100}
    assert "$project" in plan.plan["pipeline"][-1]
    assert plan.explain["ast"]["limit"] == 100
    assert plan.explain["effectiveAql"].strip().endswith("LIMIT 100")
    assert plan.plan["pagination"]["limitSource"] == "default"
    assert any(warning["code"] == "default_limit_applied" for warning in plan.plan["warnings"])


@pytest.mark.asyncio
async def test_compile_query_raw_aql_preserves_explicit_limit_and_offset():
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
        c/uid/value AS compositionId
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report_result.v1]
    WHERE
        e/ehr_id/value = 'patient-1'
    LIMIT 25 OFFSET 50
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    assert plan.plan["pipeline"][1:3] == [{"$skip": 50}, {"$limit": 25}]
    assert "$project" in plan.plan["pipeline"][-1]
    assert plan.explain["ast"]["limit"] == 25
    assert plan.explain["ast"]["offset"] == 50
    assert plan.plan["pagination"]["limitSource"] == "explicit"


@pytest.mark.asyncio
async def test_compile_query_raw_aql_caps_explicit_limit_above_page_size():
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
        c/uid/value AS compositionId
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report_result.v1]
    WHERE
        e/ehr_id/value = 'patient-1'
    LIMIT 1000
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    assert plan.plan["pipeline"][1] == {"$limit": 100}
    assert "$project" in plan.plan["pipeline"][-1]
    assert plan.explain["ast"]["limit"] == 100
    assert "LIMIT 100" in plan.explain["effectiveAql"]
    assert plan.plan["pagination"]["limitSource"] == "capped"
    assert any(warning["code"] == "limit_capped" for warning in plan.plan["warnings"])


@pytest.mark.asyncio
async def test_compile_query_raw_aql_search_sort_uses_sequence_token_pagination():
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
        c/uid/value AS compositionId,
        v/commit_audit/time_committed/value AS DataRegistre
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report_result.v1]
    WHERE
        c/name/value = 'Report'
    ORDER BY
        v/commit_audit/time_committed/value,
        c/uid/value
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "pageToken": "token-1",
            "debug": True,
        },
    )

    pipeline = plan.plan["pipeline"]
    assert plan.engine == "text_search_dual"
    assert pipeline[0]["$search"]["sort"] == {"sort_time": 1, "comp_id": 1}
    assert pipeline[0]["$search"]["searchAfter"] == "token-1"
    assert pipeline[1] == {"$addFields": {"__searchSequenceToken": {"$meta": "searchSequenceToken"}}}
    assert not any("$sort" in stage for stage in pipeline[2:])
    assert any(
        stage.get("$project", {}).get("__searchSequenceToken") == "$__searchSequenceToken"
        for stage in pipeline
    )
    assert pipeline[-1] == {"$limit": 100}
    assert plan.plan["pagination"]["tokens"] == {"searchAfter": "token-1"}


@pytest.mark.asyncio
async def test_compile_query_raw_aql_shortened_action_time_uses_slim_data_paths():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "openEHR-EHR-COMPOSITION": {
                            "vaccination_list": {
                                "v0": "6",
                            }
                        },
                        "openEHR-EHR-ACTION": {
                            "medication": {
                                "v1": "8",
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
                            "value": "v",
                            "archetype_details": "ad",
                            "template_id": "ti",
                            "other_participations": "op",
                            "performer": "pf",
                            "identifiers": "ids",
                            "type": "t",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection({}),
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
        a/time/value AS DataAdministracio,
        a/other_participations/performer/identifiers/id AS ProfessionalId,
        a/other_participations/performer/identifiers/type AS ProfessionalType
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.vaccination_list.v0]
                CONTAINS ACTION a[openEHR-EHR-ACTION.medication.v1]
    WHERE
        c/archetype_details/template_id/value = 'HC3 Immunization List v0.5'
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
    assert plan.explain["builder"]["reason"] == "scope_cross_patient_match_friendly"
    assert pipeline[0]["$match"]["cn"]["$elemMatch"]["data.ani"] == "8"

    project_stage = next(stage["$project"] for stage in pipeline if "$project" in stage)
    assert project_stage["DataAdministracio"] == "$__fanout_nodes.data.time.v"
    assert project_stage["ProfessionalId"] == "$__fanout_nodes.data.op.pf.ids.id"
    assert project_stage["ProfessionalType"] == "$__fanout_nodes.data.op.pf.ids.t"


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
                            {"p": "-4.8.-3.-2.-1.2.1"},
                            {"p": "-5.8.-3.-2.-1.2.1"},
                            {"p": "-6.8.-3.-2.-1.2.1"},
                            {"p": "-1.12.-3.-2.-1.2.1"},
                            {"p": "-2.12.-3.-2.-1.2.1"},
                            {"p": "-3.12.-3.-2.-1.2.1"},
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

    assert pipeline[1]["$sort"] == {"time_c": 1, "comp_id": 1}
    assert pipeline[2] == {"$limit": 100}

    project_stage = pipeline[3]["$project"]
    data_inici_regex = _first_node_filter(project_stage["DataInici"])["cond"]["$regexMatch"]["regex"]
    data_hora_fi_regex = _first_node_filter(project_stage["DataHoraFiProces"])["cond"]["$regexMatch"]["regex"]
    assert ":2:1" in data_inici_regex
    assert ":2:1" in data_hora_fi_regex
    assert ":1:1" not in data_inici_regex


@pytest.mark.asyncio
async def test_compile_query_raw_aql_contains_name_predicate_uses_match_pipeline():
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
                        "cn": [{"p": "1:2:8", "data": {"ani": 8}}],
                    }
                }
            ),
            "compositions_search": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-contains-name-predicate",
        config={"paths": {"separator": ":"}},
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        c/uid/value AS uid
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
                    CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.probs_base_observation.v0 and name/value='Nadó/Fetus 1-n']
                        CONTAINS CLUSTER cl[openEHR-EHR-CLUSTER.health_thread.v0]
    WHERE
        c/archetype_details/template_id/value = 'PO_Care_of_newborn_and_fetus_1-n_v0.13_FORMULARIS'
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
    assert plan.explain["builder"]["reason"] == "scope_cross_patient_match_friendly"
    assert "$match" in pipeline[0]
    assert "$search" not in pipeline[0]
    assert pipeline[0]["$match"]["tid"] == "PO_Care_of_newborn_and_fetus_1-n_v0.13_FORMULARIS"
    assert pipeline[0]["$match"]["cn"]["$elemMatch"] == {
        "p": {"$regex": "^8(?::[^:]+)*:2(?::[^:]+)*:1$"},
        "data.ani": 8,
    }


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
    data_inici_regex = _first_node_filter(_first_stage(pipeline, "$project")["DataInici"])["cond"]["$regexMatch"]["regex"]
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


@pytest.mark.asyncio
async def test_compile_query_raw_aql_supports_mixed_count_projection_on_standard_pipeline():
    db = _build_fake_lab_db()

    ctx = StrategyContext(
        environment_id="env-mixed-count-match",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        c/context/start_time/value AS creationDate,
        COUNT(*) AS rowCount
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

    assert plan.engine == "mongo_pipeline"
    pipeline = plan.plan["pipeline"]
    assert [next(iter(stage)) for stage in pipeline] == ["$match", "$project", "$group", "$project", "$limit"]
    group_stage = pipeline[2]["$group"]
    assert group_stage["_id"] == {"creationDate": "$creationDate"}
    assert group_stage["rowCount"] == {"$sum": 1}
    assert pipeline[3]["$project"] == {
        "_id": 0,
        "creationDate": "$_id.creationDate",
        "rowCount": 1,
    }
    assert all("__aggregate_values" not in str(stage) for stage in pipeline)


@pytest.mark.asyncio
async def test_compile_query_raw_aql_supports_mixed_count_projection_on_search_pipeline():
    db = _build_fake_lab_db()

    ctx = StrategyContext(
        environment_id="env-mixed-count-search",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        c/context/start_time/value AS creationDate,
        COUNT(c) AS compositionCount
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report-result.v1]
            CONTAINS OBSERVATION o[openEHR-EHR-OBSERVATION.laboratory_test_result.v1]
    WHERE
        o/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.laboratory_test_analyte.v1]/items[at0001]/value/magnitude > 1
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    assert plan.engine == "text_search_dual"
    assert plan.plan["explain"]["builder"]["chosen"] == "search_pipeline_builder"
    pipeline = plan.plan["pipeline"]
    assert [next(iter(stage)) for stage in pipeline][-4:] == ["$project", "$group", "$project", "$limit"]
    group_stage = next(stage["$group"] for stage in pipeline if "$group" in stage)
    assert group_stage["_id"] == {"creationDate": "$creationDate"}
    assert group_stage["compositionCount"] == {"$sum": 1}


@pytest.mark.asyncio
async def test_compile_query_raw_aql_supports_deterministic_function_projections():
    db = _build_fake_lab_db()

    ctx = StrategyContext(
        environment_id="env-function-projections",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()
    raw_aql = """
    SELECT
        LENGTH(c/context/start_time/value) AS startLength,
        CONCAT(e/ehr_id/value, ':', c/context/start_time/value) AS startLabel,
        SUBSTRING(c/context/start_time/value, 1, 10) AS startDate
    FROM
        EHR e
            CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report-result.v1]
    WHERE
        e/ehr_id/value = 'ehr-1'
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
    project_stage = next(stage["$project"] for stage in pipeline if "$project" in stage)
    assert "$strLenCP" in project_stage["startLength"]
    assert project_stage["startLabel"]["$concat"][1] == ":"
    assert project_stage["startDate"]["$substrCP"][1] == {
        "$max": [
            {"$subtract": [1, 1]},
            0,
        ]
    }
