from __future__ import annotations

import pytest

from kehrnel.api.domains.openehr.aql.service import (
    EXTENDED_AQL_FEATURE_MODE,
    PARITY_AQL_FEATURE_MODE,
    enforce_aql_feature_mode,
    normalize_aql_feature_mode,
)
from kehrnel.engine.domains.openehr.aql.parser import AQLParser


def test_normalize_aql_feature_mode_defaults_to_parity():
    assert normalize_aql_feature_mode(None) == PARITY_AQL_FEATURE_MODE
    assert normalize_aql_feature_mode("") == PARITY_AQL_FEATURE_MODE


def test_normalize_aql_feature_mode_rejects_unknown_value():
    with pytest.raises(ValueError, match="Unsupported AQL feature mode"):
        normalize_aql_feature_mode("future")


def test_enforce_aql_feature_mode_rejects_exists_in_parity_mode():
    ast = AQLParser(
        "SELECT c/context/start_time/value AS startTime FROM EHR e CONTAINS COMPOSITION c "
        "WHERE EXISTS c/context/start_time/value"
    ).parse_with_method("handwritten")

    with pytest.raises(NotImplementedError, match="X-AQL-Feature-Mode: extended"):
        enforce_aql_feature_mode(ast, PARITY_AQL_FEATURE_MODE)


def test_enforce_aql_feature_mode_allows_exists_in_extended_mode():
    ast = AQLParser(
        "SELECT c/context/start_time/value AS startTime FROM EHR e CONTAINS COMPOSITION c "
        "WHERE EXISTS c/context/start_time/value"
    ).parse_with_method("handwritten")

    assert enforce_aql_feature_mode(ast, EXTENDED_AQL_FEATURE_MODE) == EXTENDED_AQL_FEATURE_MODE


def test_enforce_aql_feature_mode_rejects_not_contains_in_parity_mode():
    ast = AQLParser(
        "SELECT c/uid/value AS compositionId FROM EHR e CONTAINS COMPOSITION c "
        "NOT CONTAINS ACTION a[openEHR-EHR-ACTION.procedure.v1]"
    ).parse_with_method("handwritten")

    with pytest.raises(NotImplementedError, match="NOT CONTAINS"):
        enforce_aql_feature_mode(ast, PARITY_AQL_FEATURE_MODE)


def test_enforce_aql_feature_mode_allows_not_contains_in_extended_mode():
    ast = AQLParser(
        "SELECT c/uid/value AS compositionId FROM EHR e CONTAINS COMPOSITION c "
        "NOT CONTAINS ACTION a[openEHR-EHR-ACTION.procedure.v1]"
    ).parse_with_method("handwritten")

    assert enforce_aql_feature_mode(ast, EXTENDED_AQL_FEATURE_MODE) == EXTENDED_AQL_FEATURE_MODE


def test_enforce_aql_feature_mode_rejects_path_to_path_comparison_in_parity_mode():
    ast = AQLParser(
        "SELECT c/uid/value AS compositionId FROM EHR e CONTAINS COMPOSITION c "
        "WHERE ar/data[at0001]/items[at0002]/value/defining_code/code_string "
        "!= ar2/data[at0001]/items[at0002]/value/defining_code/code_string"
    ).parse_with_method("handwritten")

    assert ast["where"]["value"] == {
        "type": "dataMatchPath",
        "path": "ar2/data[at0001]/items[at0002]/value/defining_code/code_string",
    }
    with pytest.raises(NotImplementedError, match="PATH-TO-PATH COMPARISON"):
        enforce_aql_feature_mode(ast, PARITY_AQL_FEATURE_MODE)


def test_enforce_aql_feature_mode_allows_path_to_path_comparison_in_extended_mode():
    ast = AQLParser(
        "SELECT c/uid/value AS compositionId FROM EHR e CONTAINS COMPOSITION c "
        "WHERE ar/data[at0001]/items[at0002]/value/defining_code/code_string "
        "!= ar2/data[at0001]/items[at0002]/value/defining_code/code_string"
    ).parse_with_method("handwritten")

    assert enforce_aql_feature_mode(ast, EXTENDED_AQL_FEATURE_MODE) == EXTENDED_AQL_FEATURE_MODE
