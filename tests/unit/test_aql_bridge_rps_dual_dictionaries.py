from types import SimpleNamespace

import pytest

import kehrnel.api.domains.openehr.aql.service as aql_service
from kehrnel.engine.domains.openehr.aql.parser import AQLParser
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.ast_validator import ASTValidator


class _FakeCollection:
    def __init__(self, docs):
        self.docs = docs

    async def count_documents(self, _filter):
        return len(self.docs)

    async def find_one(self, flt=None, projection=None):
        if flt and "_id" in flt:
            return self.docs.get(flt["_id"])
        return next(iter(self.docs.values())) if self.docs else None


class _FakeDB(dict):
    def __getitem__(self, name):
        return super().__getitem__(name)


def test_aql_parser_preserves_quoted_literal_and_nested_version_alias():
    query = (
        "SELECT c/uid/value AS uid "
        "FROM EHR e CONTAINS VERSION v CONTAINS COMPOSITION "
        "c[openEHR-EHR-COMPOSITION.probs_base_composition.v0] "
        "WHERE c/archetype_details/template_id/value="
        "'PO_Pregnancy_onset_and mother_follow_up_v.0.16_FORMULARIS'"
    )

    ast = AQLParser(query).parse()

    assert ast["where"]["path"] == "c/archetype_details/template_id/value"
    assert ast["where"]["value"] == "PO_Pregnancy_onset_and mother_follow_up_v.0.16_FORMULARIS"
    assert ASTValidator.detect_key_aliases(ast) == ("e", "c")
    assert ASTValidator.detect_version_alias(ast) == "v"


@pytest.mark.asyncio
async def test_build_aql_pipeline_uses_request_scoped_codes_and_shortcuts(monkeypatch):
    ast = {
        "select": {
            "distinct": False,
            "columns": {
                "0": {
                    "value": {"type": "dataMatchPath", "path": "c/context/start_time/value"},
                    "alias": "StartTime",
                },
                "1": {
                    "value": {"type": "dataMatchPath", "path": "c/uid/value"},
                    "alias": "compositionId",
                },
                "2": {
                    "value": {
                        "type": "dataMatchPath",
                        "path": "v/commit_audit/time_committed/value",
                    },
                    "alias": "DataRegistre",
                },
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
                    "value": "PO_Pregnancy_onset_and mother_follow_up_v.0.16_FORMULARIS",
                },
                "1": {
                    "path": "v/commit_audit/time_committed/value",
                    "operator": ">=",
                    "value": "1970-01-01T00:00:00+00:00",
                },
                "2": {
                    "path": "v/commit_audit/time_committed/value",
                    "operator": "<",
                    "value": "2026-04-14T14:46:03.217000+00:00",
                },
            },
        },
        "orderBy": {},
        "limit": None,
        "offset": None,
    }

    db = _FakeDB(
        {
            "compositions_rps": _FakeCollection({"sample": {"cn": [{"p": "1", "data": {"ani": 1}}]}}),
            "custom_codes": _FakeCollection(
                {
                    "codes_v2": {
                        "_id": "codes_v2",
                        "items": {
                            "openEHR-EHR-COMPOSITION.probs_base_composition.v0": 1,
                            "at0001": -1,
                        },
                    }
                }
            ),
            "custom_shortcuts": _FakeCollection(
                {
                    "short_v2": {
                        "_id": "short_v2",
                        "items": {
                            "context": "cx",
                            "start_time": "st",
                            "archetype_details": "ad",
                            "template_id": "ti",
                            "value": "v",
                        },
                    }
                }
            ),
        }
    )

    async def _fake_context(request, ensure_ingestion=False):
        return {
            "activation": SimpleNamespace(
                config={
                    "collections": {
                        "compositions": {"name": "compositions_rps"},
                        "search": {
                            "name": "compositions_search",
                            "enabled": True,
                            "atlasIndex": {"name": "search_nodes_index"},
                        },
                        "codes": {"name": "custom_codes", "doc_id": "codes_v2"},
                        "shortcuts": {"name": "custom_shortcuts", "doc_id": "short_v2"},
                    }
                }
            ),
            "database_name": "tenant_db",
        }

    monkeypatch.setattr(aql_service, "resolve_active_openehr_context", _fake_context)

    pipeline = await aql_service.build_aql_pipeline(ast, db, ehr_id="ehr-1", request=object())

    match_stage = pipeline[0]["$match"]
    assert match_stage["ehr_id"] == "ehr-1"
    assert match_stage["tid"] == "PO_Pregnancy_onset_and mother_follow_up_v.0.16_FORMULARIS"
    assert match_stage["time_c"]["$gte"].isoformat() == "1970-01-01T00:00:00+00:00"
    assert match_stage["time_c"]["$lt"].isoformat() == "2026-04-14T14:46:03.217000+00:00"
    assert match_stage["cn"]["$elemMatch"] == {"p": "1", "data.ani": 1}

    project_stage = pipeline[1]["$project"]
    assert project_stage["compositionId"] == "$comp_id"
    assert project_stage["DataRegistre"] == "$time_c"
    start_time = project_stage["StartTime"]
    assert start_time["$first"]["$map"]["in"] == "$$node.data.cx.st.v"
    assert start_time["$first"]["$map"]["input"]["$filter"]["cond"]["$regexMatch"]["input"] == "$$node.p"
