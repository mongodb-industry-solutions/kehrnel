from __future__ import annotations

from copy import deepcopy

import pytest

from kehrnel.engine.core.types import StrategyContext
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


@pytest.mark.asyncio
async def test_compile_query_raw_aql_prefers_match_pipeline_for_linear_nested_contains():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "openEHR-EHR-COMPOSITION": {
                            "encounter": {
                                "v1": "24",
                            }
                        },
                        "openEHR-EHR-SECTION": {
                            "adverse_reaction_list": {
                                "v0": "30",
                            }
                        },
                        "openEHR-EHR-EVALUATION": {
                            "adverse_reaction_risk": {
                                "v2": "33",
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "adverse_reaction_event": {
                                "v1": "31",
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
                            "uid": "uid",
                            "value": "v",
                            "archetype_details": "ad",
                            "template_id": "ti",
                            "commit_audit": "ca",
                            "time_committed": "tc",
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
        c/uid/value AS compositionId
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
                    CONTAINS SECTION s[openEHR-EHR-SECTION.adverse_reaction_list.v0]
                        CONTAINS EVALUATION ar[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
                            CONTAINS CLUSTER ev[openEHR-EHR-CLUSTER.adverse_reaction_event.v1]
    WHERE
        c/archetype_details/template_id/value = 'air_adverse_reaction_record_v1'
        AND v/commit_audit/time_committed/value >= '2020-04-23T10:17:17.297Z'
        AND v/commit_audit/time_committed/value < '2026-04-23T10:17:17.299Z'
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

    assert plan.engine == "mongo_pipeline"
    assert plan.plan["collection"] == "compositions_rps"
    assert plan.explain["builder"]["chosen"] == "pipeline_builder"
    assert plan.explain["builder"]["reason"] == "scope_cross_patient_match_friendly"

    match_stage = plan.plan["pipeline"][0]["$match"]
    assert match_stage["tid"] == "air_adverse_reaction_record_v1"
    assert match_stage["cn"]["$elemMatch"]["data.ani"] == "31"
    assert match_stage["cn"]["$elemMatch"]["p"]["$regex"] == "^31(?::[^:]+)*:33(?::[^:]+)*:30(?::[^:]+)*:24$"


@pytest.mark.asyncio
async def test_compile_query_raw_aql_adds_row_fanout_for_deepest_selected_alias():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "-1",
                            "at0002": "-2",
                            "at0006": "-6",
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "encounter": {
                                "v1": "24",
                            }
                        },
                        "openEHR-EHR-SECTION": {
                            "adverse_reaction_list": {
                                "v0": "30",
                            }
                        },
                        "openEHR-EHR-EVALUATION": {
                            "adverse_reaction_risk": {
                                "v2": "33",
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "adverse_reaction_event": {
                                "v1": "31",
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
                            "uid": "uid",
                            "value": "v",
                            "data": "data",
                            "items": "i",
                            "archetype_details": "ad",
                            "template_id": "ti",
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
        c/uid/value AS compositionId,
        ar/data[at0001]/items[at0002]/value/value AS Substance,
        ev/items[at0006]/value/value AS Manifestacio
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
                    CONTAINS SECTION s[openEHR-EHR-SECTION.adverse_reaction_list.v0]
                        CONTAINS EVALUATION ar[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
                            CONTAINS CLUSTER ev[openEHR-EHR-CLUSTER.adverse_reaction_event.v1]
    WHERE
        c/archetype_details/template_id/value = 'air_adverse_reaction_record_v1'
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
    assert pipeline[1]["$addFields"]["__fanout_nodes"]["$filter"]["cond"]["$regexMatch"]["regex"] == "^31(?::[^:]+)*:33(?::[^:]+)*:30(?::[^:]+)*:24$"
    assert pipeline[2] == {"$unwind": "$__fanout_nodes"}
    assert pipeline[3]["$addFields"]["__fanout_paths"]["ev"] == "$__fanout_nodes.p"

    project_stage = pipeline[4]["$project"]
    assert project_stage["compositionId"] == "$comp_id"
    assert project_stage["Substance"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["regex"]["$concat"] == ["^", "-2:-1", ":", "$__fanout_paths.ar", "$"]
    assert project_stage["Manifestacio"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["regex"]["$concat"] == ["^", "-6", ":", "$__fanout_paths.ev", "$"]


@pytest.mark.asyncio
async def test_compile_query_raw_aql_projection_cache_activates_only_for_high_reuse():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "-1",
                            "at0002": "-2",
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "encounter": {
                                "v1": "24",
                            }
                        },
                        "openEHR-EHR-SECTION": {
                            "adverse_reaction_list": {
                                "v0": "30",
                            }
                        },
                        "openEHR-EHR-EVALUATION": {
                            "adverse_reaction_risk": {
                                "v2": "33",
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
                            "uid": "uid",
                            "value": "v",
                            "defining_code": "df",
                            "code_string": "cs",
                            "terminology_id": "ti",
                            "data": "data",
                            "items": "i",
                            "archetype_details": "ad",
                            "template_id": "ti",
                        },
                    }
                }
            ),
            "compositions_rps": _FakeCollection({}),
            "compositions_search": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-cache",
        config=MANIFEST.default_config,
        adapters={"storage": _FakeStorage(db)},
        manifest=MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualStrategy()

    low_reuse_aql = """
    SELECT
        ar/data[at0001]/items[at0002]/value/defining_code/code_string AS SubstanceCode,
        ar/data[at0001]/items[at0002]/value/value AS SubstanceValue
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
                    CONTAINS SECTION s[openEHR-EHR-SECTION.adverse_reaction_list.v0]
                        CONTAINS EVALUATION ar[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
    WHERE
        c/archetype_details/template_id/value = 'air_adverse_reaction_record_v1'
    """

    low_reuse_plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": low_reuse_aql,
            "debug": True,
        },
    )

    assert not any("__projection_cache" in stage.get("$addFields", {}) for stage in low_reuse_plan.plan["pipeline"])

    high_reuse_aql = """
    SELECT
        ar/data[at0001]/items[at0002]/value/defining_code/code_string AS SubstanceCode,
        ar/data[at0001]/items[at0002]/value/value AS SubstanceValue,
        ar/data[at0001]/items[at0002]/value/defining_code/terminology_id/value AS SubstanceTerminology
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
                    CONTAINS SECTION s[openEHR-EHR-SECTION.adverse_reaction_list.v0]
                        CONTAINS EVALUATION ar[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
    WHERE
        c/archetype_details/template_id/value = 'air_adverse_reaction_record_v1'
    """

    high_reuse_plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": high_reuse_aql,
            "debug": True,
        },
    )

    cache_stage = next(
        stage["$addFields"]
        for stage in high_reuse_plan.plan["pipeline"]
        if "__projection_cache" in stage.get("$addFields", {})
    )
    assert cache_stage["__projection_cache"]["c0"]["$filter"]["input"] == "$cn"

    project_stage = next(stage["$project"] for stage in high_reuse_plan.plan["pipeline"] if "$project" in stage)
    assert project_stage["SubstanceCode"]["$first"]["$map"]["input"] == "$__projection_cache.c0"
    assert project_stage["SubstanceValue"]["$first"]["$map"]["input"] == "$__projection_cache.c0"
    assert project_stage["SubstanceTerminology"]["$first"]["$map"]["input"] == "$__projection_cache.c0"


@pytest.mark.asyncio
async def test_compile_query_raw_aql_search_pipeline_keeps_row_fanout_for_selected_leaf_alias():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "-1",
                            "at0002": "-2",
                            "at0006": "-6",
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "encounter": {
                                "v1": "24",
                            }
                        },
                        "openEHR-EHR-SECTION": {
                            "adverse_reaction_list": {
                                "v0": "30",
                            }
                        },
                        "openEHR-EHR-EVALUATION": {
                            "adverse_reaction_risk": {
                                "v2": "33",
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "adverse_reaction_event": {
                                "v1": "31",
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
                            "uid": "uid",
                            "value": "v",
                            "data": "data",
                            "items": "i",
                            "archetype_details": "ad",
                            "template_id": "ti",
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
        c/uid/value AS compositionId,
        ar/data[at0001]/items[at0002]/value/value AS Substance,
        ev/items[at0006]/value/value AS Manifestacio
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
                    CONTAINS SECTION s[openEHR-EHR-SECTION.adverse_reaction_list.v0]
                        CONTAINS EVALUATION ar[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
                            CONTAINS CLUSTER ev[openEHR-EHR-CLUSTER.adverse_reaction_event.v1]
    WHERE
        c/archetype_details/template_id/value = 'air_adverse_reaction_record_v1'
        AND ev/items[at0006]/value/value = 'Rash'
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
    assert plan.engine == "text_search_dual"
    assert "$lookup" in pipeline[2]
    assert pipeline[3]["$addFields"]["__fanout_nodes"]["$filter"]["cond"]["$regexMatch"]["regex"] == "^31(?::[^:]+)*:33(?::[^:]+)*:30(?::[^:]+)*:24$"
    assert pipeline[4] == {"$unwind": "$__fanout_nodes"}
    assert pipeline[5]["$addFields"]["__fanout_paths"]["ev"] == "$__fanout_nodes.p"


@pytest.mark.asyncio
async def test_compile_query_raw_aql_adds_exact_row_match_for_descendant_where_on_match_pipeline():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "-1",
                            "at0002": "-2",
                            "at0006": "-6",
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "encounter": {
                                "v1": "24",
                            }
                        },
                        "openEHR-EHR-SECTION": {
                            "adverse_reaction_list": {
                                "v0": "30",
                            }
                        },
                        "openEHR-EHR-EVALUATION": {
                            "adverse_reaction_risk": {
                                "v2": "33",
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "adverse_reaction_event": {
                                "v1": "31",
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
                            "uid": "uid",
                            "value": "v",
                            "data": "data",
                            "items": "i",
                            "ehr_id": "ehr_id",
                            "archetype_details": "ad",
                            "template_id": "ti",
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
        c/uid/value AS compositionId,
        ar/data[at0001]/items[at0002]/value/value AS Substance
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
                    CONTAINS SECTION s[openEHR-EHR-SECTION.adverse_reaction_list.v0]
                        CONTAINS EVALUATION ar[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
                            CONTAINS CLUSTER ev[openEHR-EHR-CLUSTER.adverse_reaction_event.v1]
    WHERE
        e/ehr_id/value = 'ehr-1'
        AND c/archetype_details/template_id/value = 'air_adverse_reaction_record_v1'
        AND ev/items[at0006]/value/value = 'Rash'
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
    assert "__fanout_instances" in pipeline[3]["$addFields"]
    assert "$let" in pipeline[3]["$addFields"]["__fanout_instances"]["ar"]

    row_match_stage = pipeline[4]["$match"]["$expr"]["$and"][1]["$and"][1]
    correlation_expr = row_match_stage["$gt"][0]["$size"]["$filter"]["cond"]["$and"][1]["$and"]
    assert correlation_expr[0]["$eq"][1] == "$__fanout_paths.ar"
    assert correlation_expr[1]["$eq"][1] == "$__fanout_instances.ar"


@pytest.mark.asyncio
async def test_compile_query_raw_aql_search_pipeline_adds_exact_row_match_after_lookup():
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "-1",
                            "at0002": "-2",
                            "at0006": "-6",
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "encounter": {
                                "v1": "24",
                            }
                        },
                        "openEHR-EHR-SECTION": {
                            "adverse_reaction_list": {
                                "v0": "30",
                            }
                        },
                        "openEHR-EHR-EVALUATION": {
                            "adverse_reaction_risk": {
                                "v2": "33",
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "adverse_reaction_event": {
                                "v1": "31",
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
                            "uid": "uid",
                            "value": "v",
                            "data": "data",
                            "items": "i",
                            "archetype_details": "ad",
                            "template_id": "ti",
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
        c/uid/value AS compositionId,
        ar/data[at0001]/items[at0002]/value/value AS Substance
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.encounter.v1]
                    CONTAINS SECTION s[openEHR-EHR-SECTION.adverse_reaction_list.v0]
                        CONTAINS EVALUATION ar[openEHR-EHR-EVALUATION.adverse_reaction_risk.v2]
                            CONTAINS CLUSTER ev[openEHR-EHR-CLUSTER.adverse_reaction_event.v1]
    WHERE
        c/archetype_details/template_id/value = 'air_adverse_reaction_record_v1'
        AND ev/items[at0006]/value/value = 'Rash'
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
    assert "$lookup" in pipeline[2]
    assert "__fanout_instances" in pipeline[5]["$addFields"]
    assert "$let" in pipeline[5]["$addFields"]["__fanout_instances"]["ar"]

    row_match_stage = pipeline[6]["$match"]["$expr"]["$and"][1]
    correlation_expr = row_match_stage["$gt"][0]["$size"]["$filter"]["cond"]["$and"][1]["$and"]
    assert correlation_expr[0]["$eq"][1] == "$__fanout_paths.ar"
    assert correlation_expr[1]["$eq"][1] == "$__fanout_instances.ar"
