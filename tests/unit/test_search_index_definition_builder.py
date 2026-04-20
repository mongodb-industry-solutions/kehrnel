import pytest

from kehrnel.engine.strategies.openehr.rps_dual.config import normalize_config
from kehrnel.engine.strategies.openehr.rps_dual.index_definition_builder import (
    build_search_index_definition_from_mappings,
)


SHORTCUTS = {
    "keys": {
        "archetype_node_id": "ani",
        "name": "n",
        "value": "v",
        "defining_code": "df",
        "code_string": "cs",
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
    assert fields["sort_time"] == {"type": "date"}
    assert data_fields["ani"] == {"type": "number"}
    assert data_fields["cx"]["fields"]["st"]["fields"]["v"] == {"type": "date"}
    assert data_fields["v"]["fields"]["df"]["fields"]["cs"] == {"type": "token"}
    assert result["definition"]["storedSource"] == {"include": ["sn.p"]}
    assert result["metadata"]["data_field_count"] == 3


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
