from kehrnel.engine.strategies.openehr.rps_dual.query.strategy_selector import (
    should_prefer_match_for_cross_patient_ast,
)


def test_selector_prefers_match_for_template_anchored_exists_path():
    ast = {
        "from": {"rmType": "EHR", "alias": "e"},
        "contains": {
            "rmType": "COMPOSITION",
            "alias": "c",
            "predicate": {
                "path": "archetype_node_id",
                "operator": "=",
                "value": "openEHR-EHR-COMPOSITION.encounter.v1",
            },
        },
        "where": {
            "operator": "AND",
            "conditions": {
                "0": {
                    "path": "c/archetype_details/template_id/value",
                    "operator": "=",
                    "value": "air_adverse_reaction_record_v1",
                },
                "1": {
                    "path": (
                        "c/context/other_context[at0001]/items"
                        "[openEHR-EHR-CLUSTER.pharmacovigilance_notification_details.v0]"
                        "/items[at0002]/value/id"
                    ),
                    "operator": "EXISTS",
                    "value": None,
                },
            },
        },
        "orderBy": {
            "0": {"path": "c/context/start_time/value", "direction": "ASC"},
            "1": {"path": "c/uid/value", "direction": "ASC"},
        },
    }

    assert should_prefer_match_for_cross_patient_ast(ast, ehr_alias="e", composition_alias="c")


def test_selector_keeps_cross_alias_value_comparison_on_search_path():
    ast = {
        "from": {"rmType": "EHR", "alias": "e"},
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
                    "value": "openEHR-EHR-COMPOSITION.encounter.v1",
                },
                "contains": {
                    "operator": "AND",
                    "children": {
                        "0": {
                            "rmType": "EVALUATION",
                            "alias": "ar",
                            "predicate": {
                                "path": "archetype_node_id",
                                "operator": "=",
                                "value": "openEHR-EHR-EVALUATION.adverse_reaction_risk.v2",
                            },
                        },
                        "1": {
                            "rmType": "EVALUATION",
                            "alias": "ar2",
                            "predicate": {
                                "path": "archetype_node_id",
                                "operator": "=",
                                "value": "openEHR-EHR-EVALUATION.adverse_reaction_risk.v2",
                            },
                        },
                    },
                },
            },
        },
        "where": {
            "operator": "AND",
            "conditions": {
                "0": {
                    "path": "c/archetype_details/template_id/value",
                    "operator": "=",
                    "value": "air_adverse_reaction_record_v1",
                },
                "1": {
                    "path": "ar/data[at0001]/items[at0002]/value/defining_code/code_string",
                    "operator": "!=",
                    "value": "ar2/data[at0001]/items[at0002]/value/defining_code/code_string",
                },
            },
        },
    }

    assert not should_prefer_match_for_cross_patient_ast(ast, ehr_alias="e", composition_alias="c")
