from __future__ import annotations

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

    project_stage = pipeline[1]["$project"]
    data_inici_regex = project_stage["DataInici"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["regex"]
    data_hora_fi_regex = project_stage["DataHoraFiProces"]["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["regex"]
    assert ".2\\.1" in data_inici_regex
    assert ".2\\.1" in data_hora_fi_regex
    assert ".1\\.1" not in data_inici_regex

    sort_stage = pipeline[2]["$sort"]
    assert sort_stage == {"DataRegistre": 1, "compositionId": 1}
