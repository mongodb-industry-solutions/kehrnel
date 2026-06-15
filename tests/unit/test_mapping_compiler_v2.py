import json
from pathlib import Path

import pytest

from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import CompositionFlattener


def make_flattener(mapping_obj, *, config_overrides=None):
    config = {"role": "primary"}
    if isinstance(config_overrides, dict):
        config.update(config_overrides)
    return CompositionFlattener(db=None, config=config, mappings_path="", mappings_content=mapping_obj)


def test_compile_field_rule_from_fields_compiles_rule():
    mapping = {
        "templates": [
            {
                "templateId": "openEHR-EHR-COMPOSITION.vaccination_list.v0",
                "fields": [
                    {
                        "path": "/context/other_context/items[openEHR-EHR-CLUSTER.admin_salut.v0]/items[at0007]/items[at0014]/value/defining_code/code_string",
                        "extract": "value.defining_code.code_string",
                    }
                ],
            }
        ],
    }
    flattener = make_flattener(mapping)
    tmpl_id = "openEHR-EHR-COMPOSITION.vaccination_list.v0"
    rules = flattener._compiled_rules_for_template(tmpl_id)
    assert rules
    rule = rules[0]
    assert rule["copy"][0] == "p"
    assert "data.value.defining_code.code_string" in rule["copy"][1]
    assert len(rule["_path"]) == 3


def test_parse_path_selectors_handles_archetypes_and_at_codes():
    flattener = make_flattener({"templates": []})
    selectors = flattener._parse_path_selectors(
        "/content[openEHR-EHR-SECTION.immunisation_list.v0]/items[openEHR-EHR-ACTION.medication.v1]/items[at0007]/value"
    )
    assert selectors[0]["selector"] == "openEHR-EHR-SECTION.immunisation_list.v0"
    assert selectors[0]["is_archetype"]
    assert selectors[-2]["selector"] == "openEHR-EHR-ACTION.medication.v1"
    assert selectors[-1]["selector"] == "at0007"


def test_mapping_unknown_shape_is_ignored():
    bad_mapping = {"templates": [{"templateId": "x", "searchNodes": "not-a-list"}]}
    flattener = make_flattener(bad_mapping)
    assert flattener.simple_fields == {}


def test_index_hints():
    mapping = {
        "templates": [
            {
                "templateId": "openEHR-EHR-COMPOSITION.vaccination_list.v0",
                "fields": [
                    {
                        "path": "/context/other_context/items[openEHR-EHR-CLUSTER.admin_salut.v0]/items[at0007]/items[at0014]/value/defining_code/code_string",
                        "extract": "value.defining_code.code_string",
                    }
                ],
            }
        ],
    }
    flattener = make_flattener(mapping)
    hints = flattener.get_index_hints()
    assert hints == {}


def test_path_encoding_profiles():
    flattener = make_flattener({"templates": []})
    # populate codebook to decode fullpath
    flattener.code_book["ar_code"] = {"openEHR-EHR-SECTION.immunisation_list.v0": 1}
    flattener.code_book["at"] = {"at0007": -7}
    flattener._refresh_codec()
    # path encoded numeric
    path_numeric = "1.-7"
    human = flattener._encode_path_for_profile(path_numeric, "profile.fullpath")
    assert human == "openEHR-EHR-SECTION.immunisation_list.v0.at0007"
    coded = flattener._encode_path_for_profile(path_numeric, "profile.codedpath")
    assert coded == path_numeric
    slash = flattener._encode_path_for_profile(path_numeric, "profile.codedpath")
    assert slash == path_numeric


def test_id_encoding_policy():
    flattener = make_flattener({"templates": []}, config_overrides={"ids": {"ehr_id": "string", "composition_id": "objectId"}})
    oid = flattener._encode_id("64b64c2e5f6270b5c2c2c2c2", "composition_id")
    from bson import ObjectId
    assert isinstance(oid, ObjectId)


def test_load_mappings_accepts_raw_analytics_template_shape():
    flattener = make_flattener(
        {
            "analyticsTemplate": {
                "templateId": "AddictionAlcoholTemplate",
                "fields": [
                    {
                        "path": "/content[openEHR-EHR-OBSERVATION.alcohol_use.v1]/data[at0001]/events[at0002]/data[at0003]/items[at0005]/items[at0015]/items[at0014]/value/magnitude",
                        "rmType": "DV_QUANTITY",
                    }
                ],
            }
        }
    )
    assert "AddictionAlcoholTemplate" in flattener.simple_fields


class _FakeAsyncCursor:
    def __init__(self, docs):
        self._docs = list(docs)
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._docs):
            raise StopAsyncIteration
        doc = self._docs[self._index]
        self._index += 1
        return doc


class _FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query):
        target_id = query.get("_id")
        for doc in self.docs:
            if doc.get("_id") == target_id:
                return doc
        return None

    def find(self, query, projection=None):
        filtered = []
        for doc in self.docs:
            analytics = doc.get("analyticsTemplate") or {}
            fields = analytics.get("fields") if isinstance(analytics, dict) else None
            if not fields:
                continue
            if query.get("domain") and doc.get("domain") != query.get("domain"):
                continue
            filtered.append(doc)
        return _FakeAsyncCursor(filtered)


class _FakeClient:
    def __init__(self, dbs=None):
        self._dbs = dict(dbs or {})

    def __getitem__(self, name):
        return self._dbs[name]


class _FakeDb:
    def __init__(self, collections=None, client=None):
        self._collections = dict(collections or {})
        self.client = client or _FakeClient()

    def __getitem__(self, name):
        return self._collections.setdefault(name, _FakeCollection())


@pytest.mark.asyncio
async def test_create_preloads_catalog_backed_analytics_mappings():
    catalog_db = _FakeDb(
        {
            "user-data-models": _FakeCollection(
                [
                    {
                        "name": "Mapped Template",
                        "domain": "openehr",
                        "analyticsTemplate": {
                            "templateId": "Mapped Template",
                            "fields": [
                                {
                                    "path": "/content[openEHR-EHR-OBSERVATION.alcohol_use.v1]/data[at0001]/events[at0002]/data[at0003]/items[at0005]/items[at0015]/items[at0014]/value/magnitude",
                                    "rmType": "DV_QUANTITY",
                                }
                            ],
                        },
                    },
                    {
                        "name": "Ignored Template",
                        "domain": "openehr",
                    },
                ]
            )
        }
    )
    client = _FakeClient({"catalog-db": catalog_db})
    runtime_db = _FakeDb(client=client)

    flattener = await CompositionFlattener.create(
        db=runtime_db,
        config={"role": "primary"},
        mappings_path="unused",
        mappings_content={
            "source": "catalog",
            "database_name": "catalog-db",
            "catalog_collection": "user-data-models",
            "domain": "openehr",
        },
    )

    assert "Mapped Template" in flattener.simple_fields
    assert "Ignored Template" not in flattener.simple_fields
