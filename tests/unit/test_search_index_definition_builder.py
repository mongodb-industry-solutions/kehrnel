import pytest
from bson import Binary, ObjectId

from kehrnel.engine.strategies.openehr.rps_dual.config import (
    build_flattener_config,
    normalize_config,
)
from kehrnel.engine.strategies.openehr.rps_dual.ingest.flattener import (
    CompositionFlattener,
)
from kehrnel.engine.strategies.openehr.rps_dual.index_definition_builder import (
    build_search_index_definition_from_mappings,
    flatten_search_index_field_types,
)


SHORTCUTS = {
    "keys": {
        "archetype_node_id": "ani",
        "name": "n",
        "value": "v",
        "defining_code": "df",
        "code_string": "cs",
        "terminology_id": "tid",
        "context": "cx",
        "start_time": "st",
    }
}


@pytest.mark.asyncio
async def test_build_search_index_definition_from_legacy_rules_uses_shortcuts_and_types():
    strategy_cfg = normalize_config({})
    mappings = {
        "rules": [
            {
                "description": "Copy context start time for any composition",
                "template_ids": "all",
                "conditions": {
                    "path_chain": [
                        "data/context/start_time/value",
                    ]
                },
                "copy": [
                    "data/context/start_time/value",
                    "p",
                ],
            },
            {
                "description": "Admin salut defining code",
                "template_ids": [
                    "openEHR-EHR-COMPOSITION.vaccination_list.v0",
                ],
                "conditions": {
                    "path_chain": [
                        "at0014",
                        "at0007",
                        "openEHR-EHR-CLUSTER.admin_salut.v0",
                    ]
                },
                "copy": [
                    "data/archetype_node_id",
                    "data/value/defining_code/code_string",
                    "p",
                ],
            },
        ]
    }

    result = await build_search_index_definition_from_mappings(
        strategy_cfg,
        mappings,
        shortcuts=SHORTCUTS,
    )

    fields = result["definition"]["mappings"]["fields"]
    data_fields = fields["sn"]["fields"]["data"]["fields"]

    assert fields["sn"]["fields"]["p"] == {
        "type": "string",
        "analyzer": "lucene.keyword",
    }
    assert fields["tid"] == {"type": "token"}
    assert fields["ehr_id"] == {"type": "token"}
    assert fields["comp_id"] == {"type": "token"}
    assert fields["sort_time"] == {"type": "date"}
    assert data_fields["ani"] == {"type": "number"}
    assert data_fields["cx"]["fields"]["st"]["fields"]["v"] == {"type": "date"}
    assert data_fields["v"]["fields"]["df"]["fields"]["cs"] == {"type": "token"}
    assert result["definition"]["storedSource"] == {"include": ["sn.p"]}
    assert result["metadata"]["data_field_count"] == 3
    assert result["metadata"]["sort_fields"] == ["comp_id", "sort_time"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("encoding", "identifier", "expected_python_type", "expected_search_type"),
    [
        (
            "string",
            "001b7ad3-98da-4d4b-b3c0-7557e97dce22::ehr.example::1",
            str,
            "token",
        ),
        ("objectid", "64b64c2e5f6270b5c2c2c2c2", ObjectId, "objectId"),
        ("uuid", "001b7ad3-98da-4d4b-b3c0-7557e97dce22", Binary, "uuid"),
        ("uuidbin", "001b7ad3-98da-4d4b-b3c0-7557e97dce22", Binary, "uuid"),
    ],
)
async def test_composition_sort_field_type_follows_persisted_id_encoding(
    encoding,
    identifier,
    expected_python_type,
    expected_search_type,
):
    strategy_cfg = normalize_config(
        {"ids": {"ehr_id": "string", "composition_id": encoding}}
    )

    result = await build_search_index_definition_from_mappings(
        strategy_cfg,
        {"templates": []},
    )
    flattener = CompositionFlattener(
        db=None,
        config=build_flattener_config(strategy_cfg),
        mappings_path="",
        mappings_content={"templates": []},
    )
    persisted_identifier = flattener._encode_id(identifier, "composition_id")

    fields = result["definition"]["mappings"]["fields"]
    assert isinstance(persisted_identifier, expected_python_type)
    assert fields["comp_id"] == {"type": expected_search_type}
    assert "comp_id" in result["metadata"]["sort_fields"]


@pytest.mark.asyncio
async def test_build_search_index_definition_from_analytics_template_fields():
    strategy_cfg = normalize_config({})
    mappings = {
        "analyticsTemplate": {
            "templateId": "AddictionAlcoholTemplate",
            "fields": [
                {
                    "path": "/content[openEHR-EHR-OBSERVATION.alcohol_use.v1]/data[at0001]/events[at0002]/data[at0003]/items[at0005]/items[at0015]/items[at0014]/value/magnitude",
                    "rmType": "DV_QUANTITY",
                },
                {
                    "path": "/content[openEHR-EHR-OBSERVATION.alcohol_use.v1]/name/value",
                    "rmType": "DV_TEXT",
                },
            ],
        }
    }

    result = await build_search_index_definition_from_mappings(
        strategy_cfg,
        mappings,
        shortcuts=SHORTCUTS,
    )

    data_fields = result["definition"]["mappings"]["fields"]["sn"]["fields"]["data"]["fields"]

    assert data_fields["v"]["fields"]["magnitude"] == {"type": "number"}
    assert data_fields["n"]["fields"]["v"] == {
        "type": "string",
        "analyzer": "lucene.standard",
    }
    assert result["metadata"]["template_ids"] == ["AddictionAlcoholTemplate"]
    assert result["metadata"]["sources"] == ["analytics_fields"]


@pytest.mark.asyncio
async def test_terminology_identifier_is_indexed_as_an_exact_token():
    strategy_cfg = normalize_config({})
    mappings = {
        "analyticsTemplate": {
            "templateId": "ClinicalProblems",
            "fields": [
                {
                    "path": (
                        "/content[openEHR-EHR-SECTION.problem_list.v0]"
                        "/items[openEHR-EHR-EVALUATION.problem_diagnosis.v1]"
                        "/data[at0001]/items[at0002]/value/defining_code"
                        "/terminology_id/value"
                    ),
                    "rmType": "DV_CODED_TEXT",
                }
            ],
        }
    }

    result = await build_search_index_definition_from_mappings(
        strategy_cfg,
        mappings,
        shortcuts=SHORTCUTS,
    )

    data_fields = result["definition"]["mappings"]["fields"]["sn"]["fields"][
        "data"
    ]["fields"]
    terminology_value = data_fields["v"]["fields"]["df"]["fields"]["tid"][
        "fields"
    ]["v"]

    assert terminology_value == {"type": "token"}
    assert flatten_search_index_field_types(result["definition"])[
        "sn.data.v.df.tid.v"
    ] == ["token"]
