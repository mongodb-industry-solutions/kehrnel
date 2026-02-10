import json
from pathlib import Path

from kehrnel.strategies.openehr.rps_dual.ingest.flattener_g import CompositionFlattener


def make_flattener(mapping_obj):
    config = {"role": "primary"}
    return CompositionFlattener(db=None, config=config, mappings_path="", mappings_content=mapping_obj)


def test_compile_field_rule_from_v2_search_nodes():
    mapping = {
        "strategyId": "openehr.rpsDual",
        "version": "2.0",
        "templates": [
            {
                "templateId": "openEHR-EHR-COMPOSITION.vaccination_list.v0",
                "searchNodes": [
                    {
                        "name": "publishingCentre",
                        "path": "/context/other_context/items[openEHR-EHR-CLUSTER.admin_salut.v0]/items[at0007]/items[at0014]/value/defining_code/code_string",
                        "rmType": "DV_CODED_TEXT",
                        "extract": "value.defining_code.code_string",
                        "index": {"type": "token"},
                    }
                ],
            }
        ],
    }
    flattener = make_flattener(mapping)
    tmpl_id = "openEHR-EHR-COMPOSITION.vaccination_list.v0"
    flattener._ensure_simple_rules(tmpl_id, flattener.simple_fields[tmpl_id])
    rule = flattener.raw_rules[tmpl_id]["rules"][0]
    assert rule["when"]["pathChain"] == [
        "at0014",
        "at0007",
        "openEHR-EHR-CLUSTER.admin_salut.v0",
    ]
    assert "openEHR-EHR-COMPOSITION.vaccination_list.v0" in rule["when"]["contains"]
    assert "data.value.defining_code.code_string" in rule["copy"]
    assert rule["label"] == "publishingCentre"
    assert rule["rmType"] == "DV_CODED_TEXT"
    assert rule["index"] == {"type": "token"}


def test_parse_path_selectors_handles_archetypes_and_at_codes():
    flattener = make_flattener({"templates": []})
    selectors = flattener._parse_path_selectors(
        "/content[openEHR-EHR-SECTION.immunisation_list.v0]/items[openEHR-EHR-ACTION.medication.v1]/items[at0007]/value"
    )
    assert selectors[0]["selector"] == "openEHR-EHR-SECTION.immunisation_list.v0"
    assert selectors[0]["is_archetype"]
    assert selectors[-2]["selector"] == "openEHR-EHR-ACTION.medication.v1"
    assert selectors[-1]["selector"] == "at0007"


def test_mapping_validation_failure_raises():
    bad_mapping = {"templates": [{"templateId": "x", "searchNodes": "not-a-list"}]}
    try:
        make_flattener(bad_mapping)
        assert False, "Expected ValueError for invalid mapping schema"
    except ValueError as exc:
        assert "Mappings validation error" in str(exc)


def test_index_hints():
    mapping = {
        "strategyId": "openehr.rpsDual",
        "version": "2.0",
        "templates": [
            {
                "templateId": "openEHR-EHR-COMPOSITION.vaccination_list.v0",
                "searchNodes": [
                    {
                        "name": "publishingCentre",
                        "path": "/context/other_context/items[openEHR-EHR-CLUSTER.admin_salut.v0]/items[at0007]/items[at0014]/value/defining_code/code_string",
                        "index": {"type": "token"},
                    }
                ],
            }
        ],
    }
    flattener = make_flattener(mapping)
    tid = "openEHR-EHR-COMPOSITION.vaccination_list.v0"
    flattener._ensure_simple_rules(tid, flattener.simple_fields[tid])
    hints = flattener.get_index_hints()
    assert tid in hints
    assert hints[tid][0]["name"] == "publishingCentre"


def test_path_encoding_profiles():
    flattener = make_flattener({"templates": []})
    # populate codebook to decode fullpath
    flattener.code_book["ar_code"] = {"openEHR-EHR-SECTION.immunisation_list.v0": 1}
    flattener.code_book["at"] = {"at0007": -7}
    # path encoded numeric
    path_numeric = "1.-7"
    human = flattener._encode_path_for_profile(path_numeric, "profile.fullpath")
    assert human == "openEHR-EHR-SECTION.immunisation_list.v0.at0007"
    coded = flattener._encode_path_for_profile(path_numeric, "profile.codedpath")
    assert coded == path_numeric
    slash = flattener._encode_path_for_profile(path_numeric, "profile.codedpath")
    assert slash == path_numeric


def test_id_encoding_policy():
    flattener = make_flattener({"templates": [], "ids": {"ehr_id": "string", "composition_id": "objectId"}})
    oid = flattener._encode_id("64b64c2e5f6270b5c2c2c2c2", "composition_id")
    from bson import ObjectId
    assert isinstance(oid, ObjectId)
