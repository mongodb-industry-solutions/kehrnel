import json
from pathlib import Path

from kehrnel.strategies.openehr.rps_dual.ingest.flattener_g import CompositionFlattener


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
