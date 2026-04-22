from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
import uuid

from bson.binary import Binary, UuidRepresentation
import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.domains.openehr.aql.ir import AqlQueryIR
from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_coding_opts,
    build_flattener_config,
    normalize_bulk_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.query.compiler import build_query_pipeline
from kehrnel.engine.strategies.openehr.rps_dual.ingest.encoding import PathCodec
from kehrnel.engine.strategies.openehr.rps_dual_ibm.ingest.flattener import IBMCompositionFlattener
from kehrnel.engine.strategies.openehr.rps_dual_ibm.ingest.unflattener import IBMCompositionUnflattener
from kehrnel.engine.strategies.openehr.rps_dual_ibm.strategy import (
    DEFAULTS_PATH,
    MANIFEST as IBM_MANIFEST,
    RPSDualIBMStrategy,
    load_json,
)


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
        if flt:
            regex = ((flt.get("sn.p") or {}).get("$regex")) if isinstance(flt.get("sn.p"), dict) else None
            if regex:
                pattern = re.compile(regex)
                docs = [
                    doc
                    for doc in docs
                    if any(pattern.search(item.get("p", "")) for item in doc.get("sn", []))
                ]
        return _Cursor(docs)

    async def replace_one(self, flt, doc, upsert=False):
        if isinstance(doc, dict) and doc.get("_id") is not None:
            self.docs[doc["_id"]] = doc

        class _Result:
            matched_count = 1
            modified_count = 1
            upserted_id = doc.get("_id") if isinstance(doc, dict) else None

        return _Result()


class _FakeDb(dict):
    def __getitem__(self, name):
        return super().__getitem__(name)


class _FakeStorage:
    def __init__(self, db):
        self.db = db

    async def find_one(self, collection, flt):
        return await self.db[collection].find_one(flt)


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


def _ibm_defaults() -> dict:
    return load_json(DEFAULTS_PATH)


def _ibm_shortcuts() -> dict:
    path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "kehrnel"
        / "engine"
        / "strategies"
        / "openehr"
        / "rps_dual_ibm"
        / "bundles"
        / "shortcuts"
        / "shortcuts.json"
    )
    return load_json(path)


def _build_ibm_flattener(raw_config: dict | None = None) -> IBMCompositionFlattener:
    merged = _ibm_defaults()
    if raw_config:
        for key, value in raw_config.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = {**merged[key], **value}
            else:
                merged[key] = value
    strategy_cfg = normalize_config(merged)
    bulk_cfg = normalize_bulk_config({"role": "primary"})
    flattener = IBMCompositionFlattener(
        db=None,
        config=build_flattener_config(strategy_cfg, bulk_cfg),
        mappings_path=_mappings_path(),
        mappings_content={"templates": []},
        coding_opts=build_coding_opts(strategy_cfg),
    )
    shortcuts = _ibm_shortcuts()
    flattener.shortcut_keys.update(shortcuts.get("keys") or {})
    flattener.shortcut_vals.update(shortcuts.get("values") or {})
    flattener._refresh_codec()
    return flattener


def _minimal_ibm_composition() -> dict:
    return {
        "_type": "COMPOSITION",
        "archetype_node_id": "openEHR-EHR-COMPOSITION.test.v0",
        "uid": {
            "_type": "OBJECT_VERSION_ID",
            "value": "c23aa47f-fa8a-4ef6-9a92-d4c50d23377a::ehrbase.ehrbase.org::1",
        },
        "name": {
            "_type": "DV_TEXT",
            "value": "Example composition",
        },
        "archetype_details": {
            "archetype_id": {"value": "openEHR-EHR-COMPOSITION.test.v0"},
            "template_id": {"value": "IBM_TEMPLATE"},
            "rm_version": "1.0.4",
        },
        "content": [
            {
                "_type": "OBSERVATION",
                "archetype_node_id": "openEHR-EHR-OBSERVATION.test.v0",
                "name": {
                    "_type": "DV_TEXT",
                    "value": "Observation A",
                },
                "archetype_details": {
                    "archetype_id": {"value": "openEHR-EHR-OBSERVATION.test.v0"},
                    "rm_version": "1.0.4",
                },
            }
        ],
    }


def test_ibm_flattener_emits_exact_shortcuts_list_indexes_and_envelope_fields(monkeypatch):
    flattener = _build_ibm_flattener()

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

    created_at = datetime(2026, 4, 21, 15, 22, 34, tzinfo=timezone.utc)
    payload = {
        "_id": "c23aa47f-fa8a-4ef6-9a92-d4c50d23377a",
        "ehr_id": "8bffb50b-990a-45c7-8da1-b56525a536c7",
        "composition_version": "1",
        "canonicalJSON": _minimal_ibm_composition(),
        "_source_envelope": {
            "_id": "c23aa47f-fa8a-4ef6-9a92-d4c50d23377a",
            "version": "c23aa47f-fa8a-4ef6-9a92-d4c50d23377a::ehrbase.ehrbase.org::1",
            "template": "IBM_TEMPLATE",
            "template_id": "649b0ad6-a453-4ece-a0fa-c534536d3ae9",
            "creation_date": created_at,
            "metrics": {"config_name": "CONFIG_1"},
        },
    }

    base_doc, search_doc = flattener.transform_composition(payload)

    assert base_doc["_id"] == Binary.from_uuid(
        uuid.UUID("c23aa47f-fa8a-4ef6-9a92-d4c50d23377a"),
        uuid_representation=UuidRepresentation.STANDARD,
    )
    assert base_doc["ehr_id"] == Binary.from_uuid(
        uuid.UUID("8bffb50b-990a-45c7-8da1-b56525a536c7"),
        uuid_representation=UuidRepresentation.STANDARD,
    )
    assert base_doc["template_id"] == Binary.from_uuid(
        uuid.UUID("649b0ad6-a453-4ece-a0fa-c534536d3ae9"),
        uuid_representation=UuidRepresentation.STANDARD,
    )
    assert base_doc["version"] == "c23aa47f-fa8a-4ef6-9a92-d4c50d23377a::ehrbase.ehrbase.org::1"
    assert base_doc["template"] == "IBM_TEMPLATE"
    assert base_doc["creation_date"] == created_at
    assert base_doc["metrics"]["config_name"] == "CONFIG_1"

    root = base_doc["cn"][0]
    assert root["p"] == "1"
    assert root["data"]["T"] == "$>C"
    assert root["data"]["ani"] == "1"
    assert root["data"]["n"]["T"] == "$>dt"
    assert root["data"]["ad"]["ai"]["v"] == "1"

    observation = base_doc["cn"][1]
    assert observation["p"] == "2/1"
    assert observation["li"] == 0
    assert not isinstance(observation["li"], list)
    assert observation["data"]["T"] == "$>O"

    assert search_doc is not None
    assert search_doc["_id"] == base_doc["_id"]
    assert search_doc["version"] == base_doc["version"]
    assert search_doc["template"] == "IBM_TEMPLATE"
    assert search_doc["template_id"] == base_doc["template_id"]


def test_ibm_unflattener_reconstructs_repeated_children_using_li_and_expands_value_refs():
    shortcuts = _ibm_shortcuts()
    codec = PathCodec(
        ar_codes={
            "openEHR-EHR-COMPOSITION.test.v0": "1",
            "openEHR-EHR-OBSERVATION.test.v0": "2",
            "openEHR-EHR-CLUSTER.test.v0": "3",
        },
        at_codes={"at0001": "A1"},
        separator="/",
        shortcuts=shortcuts.get("keys") or {},
    )
    unflattener = IBMCompositionUnflattener(
        codec=codec,
        shortcuts=shortcuts.get("keys") or {},
        value_shortcuts=shortcuts.get("values") or {},
        li_field="li",
    )

    base_doc = {
        "cn": [
            {"p": "1", "data": {"T": "$>C", "ani": "1", "n": {"T": "$>dt", "v": "Root"}}},
            {"p": "2/1", "kp": ["ct"], "li": 0, "data": {"T": "$>O", "ani": "2", "n": {"T": "$>dt", "v": "Obs"}}},
            {"p": "3/2/1", "kp": ["i"], "li": 0, "data": {"T": "$>K", "ani": "3", "n": {"T": "$>dt", "v": "Cluster A"}}},
            {
                "p": "A1/3/2/1",
                "kp": ["i"],
                "li": 0,
                "data": {"T": "$>U", "ani": "A1", "n": {"T": "$>dt", "v": "Child A"}, "v": {"T": "$>dt", "v": "A"}},
            },
            {"p": "3/2/1", "kp": ["i"], "li": 1, "data": {"T": "$>K", "ani": "3", "n": {"T": "$>dt", "v": "Cluster B"}}},
            {
                "p": "A1/3/2/1",
                "kp": ["i"],
                "li": 0,
                "data": {"T": "$>U", "ani": "A1", "n": {"T": "$>dt", "v": "Child B"}, "v": {"T": "$>dt", "v": "B"}},
            },
        ]
    }

    composition = unflattener.unflatten(base_doc)

    assert composition["_type"] == "COMPOSITION"
    assert composition["name"]["value"] == "Root"
    assert composition["content"][0]["_type"] == "OBSERVATION"
    assert composition["content"][0]["items"][0]["_type"] == "CLUSTER"
    assert composition["content"][0]["items"][0]["items"][0]["archetype_node_id"] == "at0001"
    assert composition["content"][0]["items"][0]["items"][0]["value"]["value"] == "A"
    assert composition["content"][0]["items"][1]["items"][0]["value"]["value"] == "B"


@pytest.mark.asyncio
async def test_ibm_query_pipeline_uses_ibm_document_fields():
    cfg = normalize_config(_ibm_defaults())
    ir = AqlQueryIR(
        scope="patient",
        predicates=[
            {"path": "e/ehr_id/value", "op": "=", "value": "8bffb50b-990a-45c7-8da1-b56525a536c7"},
            {"path": "c/archetype_details/template_id/value", "op": "=", "value": "IBM_TEMPLATE"},
            {
                "path": "v/commit_audit/time_committed/value",
                "op": ">=",
                "value": "2026-04-21T00:00:00Z",
            },
        ],
        select=[
            {"path": "c/uid/value", "alias": "CompositionVersion"},
            {"path": "v/commit_audit/time_committed/value", "alias": "CreationDate"},
        ],
        sort={"v/commit_audit/time_committed/value": 1},
    )

    engine, pipeline, *_ = await build_query_pipeline(ir, cfg)

    assert engine == "pipeline_builder"
    match_stage = pipeline[0]["$match"]
    assert match_stage["template"] == "IBM_TEMPLATE"
    assert match_stage["creation_date"]["$gte"] == datetime(2026, 4, 21, 0, 0, tzinfo=timezone.utc)
    assert match_stage["cn"]["$elemMatch"] == {"p": {"$regex": r"^[^/]+$"}}
    assert pipeline[1]["$project"]["CompositionVersion"] == "$version"
    assert pipeline[1]["$project"]["CreationDate"] == "$creation_date"


@pytest.mark.asyncio
async def test_ibm_raw_aql_nested_projections_resolve_against_cn_nodes():
    shortcuts = _ibm_shortcuts()
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "D1",
                            "at0002": "D2",
                            "at0003": "D3",
                            "at0005": "D5",
                            "at0007": "D7",
                            "at0008": "D8",
                            "at0009": "D9",
                            "at0011": "C11",
                            "at0012": "C12",
                            "at0013": "C13",
                            "at0015": "C15",
                            "at0016": "C16",
                            "at0018": "C18",
                            "at0019": "C19",
                            "at0021": "C21",
                            "at0022": "C22",
                            "at0023": "C23",
                            "at0024": "C24",
                            "at0025": "C25",
                            "at0026": "C26",
                            "at0027": "C27",
                            "at0028": "C28",
                            "at0029": "C29",
                            "at0030": "C30",
                            "at0031": "C31",
                            "at0041": "C41",
                            "at0042": "C42",
                            "at0043": "C43",
                            "at0071": "C71",
                            "at0079": "C79",
                            "at0080": "C80",
                            "at0126": "B126",
                            "at0127": "B127",
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": "1"
                            }
                        },
                        "openEHR-EHR-OBSERVATION": {
                            "probs_base_observation": {
                                "v0": "3"
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "admin_professional": {
                                "v0": "2"
                            },
                            "health_thread": {
                                "v0": "4"
                            },
                            "admin_salut": {
                                "v0": "25"
                            },
                            "mother_general_data": {
                                "v0": "5"
                            },
                            "mothers_pregnancy_follow_up_general_data": {
                                "v0": "20"
                            },
                            "escales": {
                                "v0": "45"
                            },
                            "pregnancy_follow_up_complementary_tests": {
                                "v0": "38"
                            },
                            "test_request_screen": {
                                "v0": "99"
                            },
                            "activitats_educatius_seguiment_mare": {
                                "v0": "18"
                            },
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        **shortcuts,
                    }
                }
            ),
            "compositions_rps_ibm": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1", "data": {"ani": "1"}}],
                    }
                }
            ),
            "compositions_search_ibm": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-ibm-raw-aql",
        config=_ibm_defaults(),
        adapters={"storage": _FakeStorage(db)},
        manifest=IBM_MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualIBMStrategy()
    raw_aql = """
    SELECT
        c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.health_thread.v0]/items[at0003]/value/id AS ProcesId,
        c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.admin_professional.v0]/items[at0013]/value/defining_code/code_string AS CatergoriaProf,
        c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.admin_professional.v0]/items[at0012]/value/value AS IdProfessional,
        c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.admin_salut.v0]/items[at0005]/items[at0011]/value/defining_code/code_string AS centre,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.mother_general_data.v0]/items[at0007]/value/magnitude AS EdatDonaInici,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.mother_general_data.v0]/items[at0008]/value/value AS DataUltimaCitologia,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.mothers_pregnancy_follow_up_general_data.v0]/items[at0001]/value/magnitude AS SetmanaGestacio,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.mothers_pregnancy_follow_up_general_data.v0]/items[at0079]/value/magnitude AS TAS,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.escales.v0]/items[at0018]/items[at0002]/value/magnitude AS tAuditPuntuacio,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.pregnancy_follow_up_complementary_tests.v0]/items[at0071]/value/defining_code/code_string AS ChlamydiaGonorrhoeaeCodi,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.activitats_educatius_seguiment_mare.v0]/items[at0011]/value/value AS HabitsHigene
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        c/archetype_details/template_id/value = 'IBM_TEMPLATE'
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    project_stage = plan.plan["pipeline"][1]["$project"]

    expected = {
        "ProcesId": ("$$node.data.v.id", "^D3/4/D1/1(?:/[^/]+)*$"),
        "CatergoriaProf": ("$$node.data.v.df.cs", "^C13/2/D1/1(?:/[^/]+)*$"),
        "IdProfessional": ("$$node.data.v.v", "^C12/2/D1/1(?:/[^/]+)*$"),
        "centre": ("$$node.data.v.df.cs", "^C11/D5/25/D1/1(?:/[^/]+)*$"),
        "EdatDonaInici": ("$$node.data.v.magnitude", "^D7/5/D3/D2/D1/3/1(?:/[^/]+)*$"),
        "DataUltimaCitologia": ("$$node.data.v.v", "^D8/5/D3/D2/D1/3/1(?:/[^/]+)*$"),
        "SetmanaGestacio": ("$$node.data.v.magnitude", "^D1/20/D3/D2/D1/3/1(?:/[^/]+)*$"),
        "TAS": ("$$node.data.v.magnitude", "^C79/20/D3/D2/D1/3/1(?:/[^/]+)*$"),
        "tAuditPuntuacio": ("$$node.data.v.magnitude", "^D2/C18/45/D3/D2/D1/3/1(?:/[^/]+)*$"),
        "ChlamydiaGonorrhoeaeCodi": ("$$node.data.v.df.cs", "^C71/38/D3/D2/D1/3/1(?:/[^/]+)*$"),
        "HabitsHigene": ("$$node.data.v.v", "^C11/18/D3/D2/D1/3/1(?:/[^/]+)*$"),
    }

    for field_name, (expected_value_path, expected_regex) in expected.items():
        assert isinstance(project_stage[field_name], dict)
        field_map = project_stage[field_name]["$first"]["$map"]
        assert field_map["in"] == expected_value_path
        assert field_map["input"]["$filter"]["cond"]["$regexMatch"]["regex"] == expected_regex


@pytest.mark.asyncio
async def test_ibm_raw_aql_nested_projections_fall_back_when_at_dictionary_is_empty():
    shortcuts = _ibm_shortcuts()
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {},
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": "1"
                            }
                        },
                        "openEHR-EHR-OBSERVATION": {
                            "probs_base_observation": {
                                "v0": "3"
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "admin_salut": {
                                "v0": "25"
                            },
                            "mother_general_data": {
                                "v0": "5"
                            },
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        **shortcuts,
                    }
                }
            ),
            "compositions_rps_ibm": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1", "data": {"ani": "1"}}],
                    }
                }
            ),
            "compositions_search_ibm": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-ibm-empty-at",
        config=_ibm_defaults(),
        adapters={"storage": _FakeStorage(db)},
        manifest=IBM_MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualIBMStrategy()
    raw_aql = """
    SELECT
        c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.admin_salut.v0]/items[at0005]/items[at0011]/value/defining_code/code_string AS centre,
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.mother_general_data.v0]/items[at0007]/value/magnitude AS EdatDonaInici
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        c/archetype_details/template_id/value = 'IBM_TEMPLATE'
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    project_stage = plan.plan["pipeline"][1]["$project"]

    centre_map = project_stage["centre"]["$first"]["$map"]
    assert centre_map["in"] == "$$node.data.v.df.cs"
    assert centre_map["input"]["$filter"]["cond"]["$regexMatch"]["regex"] == "^C11/D5/25/D1/1(?:/[^/]+)*$"

    age_map = project_stage["EdatDonaInici"]["$first"]["$map"]
    assert age_map["in"] == "$$node.data.v.magnitude"
    assert age_map["input"]["$filter"]["cond"]["$regexMatch"]["regex"] == "^D7/5/D3/D2/D1/3/1(?:/[^/]+)*$"


@pytest.mark.asyncio
async def test_ibm_raw_aql_respects_configured_codes_doc_id_for_nested_paths():
    shortcuts = _ibm_shortcuts()
    cfg = {
        **_ibm_defaults(),
        "dictionaries": {
            "arcodes": {
                "doc_id": "tenant_codes"
            }
        },
    }
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "tenant_codes": {
                        "_id": "tenant_codes",
                        "at": {},
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": "1"
                            }
                        },
                        "openEHR-EHR-OBSERVATION": {
                            "probs_base_observation": {
                                "v0": "3"
                            }
                        },
                        "openEHR-EHR-CLUSTER": {
                            "mother_general_data": {
                                "v0": "17"
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        **shortcuts,
                    }
                }
            ),
            "compositions_rps_ibm": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1", "data": {"ani": "1"}}],
                    }
                }
            ),
            "compositions_search_ibm": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-ibm-custom-codes-doc",
        config=cfg,
        adapters={"storage": _FakeStorage(db)},
        manifest=IBM_MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualIBMStrategy()
    raw_aql = """
    SELECT
        c/content[openEHR-EHR-OBSERVATION.probs_base_observation.v0]/data[at0001]/events[at0002]/data[at0003]/items[openEHR-EHR-CLUSTER.mother_general_data.v0]/items[at0023]/value/defining_code/code_string AS ResultatUltimHPVCodi
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        c/archetype_details/template_id/value = 'IBM_TEMPLATE'
    """

    plan = await strategy.compile_query(
        ctx,
        "openEHR",
        {
            "raw_aql": raw_aql,
            "debug": True,
        },
    )

    result_map = plan.plan["pipeline"][1]["$project"]["ResultatUltimHPVCodi"]["$first"]["$map"]
    assert result_map["in"] == "$$node.data.v.df.cs"
    assert result_map["input"]["$filter"]["cond"]["$regexMatch"]["regex"] == "^C23/17/D3/D2/D1/3/1(?:/[^/]+)*$"


@pytest.mark.asyncio
async def test_ibm_raw_aql_unresolved_archetyped_path_fails_instead_of_root_projection():
    shortcuts = _ibm_shortcuts()
    db = _FakeDb(
        {
            "_codes": _FakeCollection(
                {
                    "ar_code": {
                        "_id": "ar_code",
                        "at": {
                            "at0001": "D1",
                        },
                        "openEHR-EHR-COMPOSITION": {
                            "probs_base_composition": {
                                "v0": "1"
                            }
                        },
                    }
                }
            ),
            "_shortcuts": _FakeCollection(
                {
                    "shortcuts": {
                        "_id": "shortcuts",
                        **shortcuts,
                    }
                }
            ),
            "compositions_rps_ibm": _FakeCollection(
                {
                    "sample": {
                        "_id": "comp-1",
                        "cn": [{"p": "1", "data": {"ani": "1"}}],
                    }
                }
            ),
            "compositions_search_ibm": _FakeCollection({}),
        }
    )

    ctx = StrategyContext(
        environment_id="env-ibm-unresolved-path",
        config=_ibm_defaults(),
        adapters={"storage": _FakeStorage(db)},
        manifest=IBM_MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualIBMStrategy()
    raw_aql = """
    SELECT
        c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.health_thread.v0]/items[at0003]/value/id AS ProcesId
    FROM
        EHR e
            CONTAINS VERSION v
                CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.probs_base_composition.v0]
    WHERE
        c/archetype_details/template_id/value = 'IBM_TEMPLATE'
    """

    with pytest.raises(KehrnelError, match="Unable to resolve archetyped AQL path"):
        await strategy.compile_query(
            ctx,
            "openEHR",
            {
                "raw_aql": raw_aql,
                "debug": True,
            },
        )


@pytest.mark.asyncio
async def test_ibm_rebuild_codes_uses_ibm_strategy_bundle_dir(monkeypatch):
    captured = {}

    async def _fake_resolve_uri_async(ref, db, base_dir=None):
        captured["ref"] = ref
        captured["base_dir"] = Path(base_dir).resolve() if base_dir is not None else None
        return {"_id": "ar_code", "at": {}}

    monkeypatch.setattr(
        "kehrnel.engine.strategies.openehr.rps_dual.strategy.resolve_uri_async",
        _fake_resolve_uri_async,
    )

    db = _FakeDb(
        {
            "_codes": _FakeCollection({}),
        }
    )
    ctx = StrategyContext(
        environment_id="env-ibm-rebuild-codes",
        config=_ibm_defaults(),
        adapters={"storage": _FakeStorage(db)},
        manifest=IBM_MANIFEST.model_copy(deep=True),
        meta={},
    )
    strategy = RPSDualIBMStrategy()

    result = await strategy.run_op(ctx, "rebuild_codes", {})

    assert result["ok"] is True
    assert captured["ref"] == "file://bundles/dictionaries/_codes.json"
    assert captured["base_dir"] == DEFAULTS_PATH.parent.resolve()
    assert "ar_code" in db["_codes"].docs
