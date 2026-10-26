from __future__ import annotations

from datetime import datetime, timezone

import pytest

from kehrnel.engine.core.errors import KehrnelError
from kehrnel.engine.core.types import StrategyContext
from kehrnel.engine.strategies.openehr.rps_dual.config import build_schema_config, normalize_config
from kehrnel.engine.strategies.openehr.rps_dual.query.compiler import build_runtime_strategy
from kehrnel.engine.strategies.openehr.rps_dual.query.transformers.search_pipeline_builder import (
    SearchPipelineBuilder,
)
from kehrnel.engine.strategies.openehr.rps_dual.strategy import MANIFEST, RPSDualStrategy


class _SemanticPathResolver:
    archetype_resolver = None

    async def translate_aql_path(self, aql_path: str):
        paths = {
            "problem/code": ("^problem-code-path$", "data.v.df.cs"),
            "problem/terminology": ("^problem-code-path$", "data.v.df.tid.v"),
            "problem/description": ("^problem-description-path$", "data.v.text"),
            "problem/onset": ("^problem-onset-path$", "data.v"),
        }
        return paths[aql_path]


class _FailingSemanticPathResolver:
    archetype_resolver = None

    async def translate_aql_path(self, aql_path: str):
        raise KehrnelError(
            code="UNRESOLVED_AQL_PATH",
            status=400,
            message="dictionary entry missing",
        )


class _CustomShortcutPathResolver:
    archetype_resolver = None

    async def translate_aql_path(self, aql_path: str):
        assert aql_path == "problem/code"
        return "^problem-code-path$", "data.val.concept.custom_code"


def _builder(
    *,
    search_nodes: str = "sn",
    resolver=None,
    require_exact_path_correlation: bool = False,
    search_field_types=None,
) -> SearchPipelineBuilder:
    cfg = normalize_config({"fields": {"document": {"sn": search_nodes}}})
    schema = build_schema_config(cfg)
    return SearchPipelineBuilder(
        "e",
        "c",
        schema["composition"],
        resolver or _SemanticPathResolver(),
        {},
        strategy=build_runtime_strategy(cfg),
        search_index_name=schema["search"]["index_name"],
        require_exact_path_correlation=require_exact_path_correlation,
        search_field_types=search_field_types,
    )


def _embedded_must(clause: dict) -> list[dict]:
    embedded = clause["embeddedDocument"]
    assert embedded["path"] == "sn"
    return embedded["operator"]["compound"]["must"]


@pytest.mark.asyncio
async def test_direct_code_and_date_predicates_bind_path_and_value_in_each_search_node():
    stage = await _builder().build_search_stage(
        {
            "where": {
                "operator": "AND",
                "conditions": {
                    "0": {
                        "path": "problem/code",
                        "operator": "MATCHES",
                        "value": {"0": "84114007", "1": "42343007"},
                    },
                    "1": {
                        "path": "problem/onset",
                        "operator": ">=",
                        "value": "2025-01-01T00:00:00Z",
                    },
                },
            }
        }
    )

    clauses = stage["$search"]["compound"]["must"]
    assert len(clauses) == 2

    code_must = _embedded_must(clauses[0])
    assert code_must == [
        {"regex": {"path": "sn.p", "query": "problem-code-path"}},
        {
            "in": {
                "path": "sn.data.v.df.cs",
                "value": ["84114007", "42343007"],
            }
        },
    ]

    onset_must = _embedded_must(clauses[1])
    assert onset_must == [
        {"regex": {"path": "sn.p", "query": "problem-onset-path"}},
        {
            "range": {
                "path": "sn.data.v",
                "gte": datetime(2025, 1, 1, tzinfo=timezone.utc),
            }
        },
    ]


@pytest.mark.asyncio
async def test_direct_equals_does_not_allow_same_shaped_value_from_another_semantic_node():
    query = await _builder()._handle_direct_condition_search(
        {
            "path": "problem/code",
            "operator": "=",
            "value": "84114007",
        }
    )

    assert _embedded_must(query) == [
        {"regex": {"path": "sn.p", "query": "problem-code-path"}},
        {"equals": {"path": "sn.data.v.df.cs", "value": "84114007"}},
    ]


@pytest.mark.asyncio
async def test_direct_terminology_equals_uses_exact_operator_for_token_mapping():
    query = await _builder()._handle_direct_condition_search(
        {
            "path": "problem/terminology",
            "operator": "=",
            "value": "SNOMED-CT",
        }
    )

    assert _embedded_must(query) == [
        {"regex": {"path": "sn.p", "query": "problem-code-path"}},
        {"equals": {"path": "sn.data.v.df.tid.v", "value": "SNOMED-CT"}},
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operator", "value", "expected_pattern"),
    [
        (
            "MATCHES",
            {"0": "heart failure", "1": "cardiac failure"},
            "(heart\\ failure)|(cardiac\\ failure)",
        ),
        ("LIKE", "heart*", "^heart.*$"),
    ],
)
async def test_non_token_pattern_predicates_keep_regex_behavior(
    operator: str,
    value,
    expected_pattern: str,
):
    query = await _builder()._handle_direct_condition_search(
        {
            "path": "problem/description",
            "operator": operator,
            "value": value,
        }
    )

    assert _embedded_must(query) == [
        {"regex": {"path": "sn.p", "query": "problem-description-path"}},
        {"regex": {"path": "sn.data.v.text", "query": expected_pattern}},
    ]


@pytest.mark.asyncio
async def test_generated_field_types_drive_exact_operator_for_custom_shortcuts():
    query = await _builder(
        resolver=_CustomShortcutPathResolver(),
        search_field_types={"sn.data.val.concept.custom_code": ["token"]},
    )._handle_direct_condition_search(
        {
            "path": "problem/code",
            "operator": "MATCHES",
            "value": {"0": "84114007", "1": "42343007"},
        }
    )

    assert _embedded_must(query) == [
        {"regex": {"path": "sn.p", "query": "problem-code-path"}},
        {
            "in": {
                "path": "sn.data.val.concept.custom_code",
                "value": ["84114007", "42343007"],
            }
        },
    ]


def test_exact_token_matches_compiles_maximum_expansion_as_one_in_operator():
    values = {str(index): f"code-{index:04d}" for index in range(1024)}

    query = SearchPipelineBuilder._build_exact_token_matches_query(
        "sn.data.v.df.cs",
        values,
    )

    assert query == {
        "in": {
            "path": "sn.data.v.df.cs",
            "value": [f"code-{index:04d}" for index in range(1024)],
        }
    }


def test_exact_token_matches_rejects_expansion_above_governed_maximum():
    values = {str(index): f"code-{index:04d}" for index in range(1025)}

    with pytest.raises(KehrnelError) as caught:
        SearchPipelineBuilder._build_exact_token_matches_query(
            "sn.data.v.df.cs",
            values,
        )

    assert caught.value.code == "AQL_MATCHES_TOO_MANY_VALUES"
    assert caught.value.details["maximum"] == 1024


@pytest.mark.asyncio
async def test_path_value_correlation_uses_the_configured_search_nodes_field():
    query = await _builder(search_nodes="semantic_nodes")._handle_direct_condition_search(
        {
            "path": "problem/code",
            "operator": "=",
            "value": "84114007",
        }
    )

    embedded = query["embeddedDocument"]
    assert embedded["path"] == "semantic_nodes"
    assert embedded["operator"]["compound"]["must"] == [
        {
            "regex": {
                "path": "semantic_nodes.p",
                "query": "problem-code-path",
            }
        },
        {
            "equals": {
                "path": "semantic_nodes.data.v.df.cs",
                "value": "84114007",
            }
        },
    ]


def test_path_regex_is_normalized_for_atlas_search_lucene_dialect():
    assert (
        SearchPipelineBuilder._atlas_search_regex_pattern(
            r"^-2:-1:4(?::[^:]+)*$"
        )
        == r"-2:-1:4(:[^:]+)*"
    )


@pytest.mark.asyncio
async def test_strict_direct_predicate_normalizes_resolver_failure_to_fail_closed_error():
    builder = _builder(
        resolver=_FailingSemanticPathResolver(),
        require_exact_path_correlation=True,
    )

    with pytest.raises(KehrnelError) as caught:
        await builder._handle_direct_condition_search(
            {
                "path": "problem/code",
                "operator": "=",
                "value": "84114007",
            }
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_PATH_UNRESOLVED"
    assert caught.value.details == {
        "aqlPath": "problem/code",
        "requiredDictionary": "_codes",
        "resolverError": "UNRESOLVED_AQL_PATH",
    }


@pytest.mark.asyncio
async def test_strict_semantic_aql_fails_closed_without_coded_path_dictionary():
    raw_aql = """
    SELECT e/ehr_id/value AS ehr_id, c/uid/value AS composition_uid
    FROM EHR e
        CONTAINS COMPOSITION c[openEHR-EHR-COMPOSITION.report-result.v1]
        CONTAINS EVALUATION a0[openEHR-EHR-EVALUATION.problem_diagnosis.v1]
    WHERE c/archetype_details/template_id/value = $templateId
        AND a0/data[at0001]/items[at0002]/value/defining_code/code_string
            MATCHES {'195111005', '84114007'}
        AND a0/data[at0001]/items[at0003]/value/value >= $p1
    LIMIT 50
    """
    context = StrategyContext(
        environment_id="semantic-query-without-dictionaries",
        config=MANIFEST.default_config,
    )

    with pytest.raises(KehrnelError) as caught:
        await RPSDualStrategy().compile_query(
            context,
            "openEHR",
            {
                "raw_aql": raw_aql,
                "params": {
                    "templateId": "sample_laboratory_v0.4",
                    "p1": "2025-01-01",
                },
                "require_exact_path_correlation": True,
            },
        )

    assert caught.value.code == "OPENEHR_SEMANTIC_PATH_UNRESOLVED"
    assert caught.value.details["aqlPath"].startswith("a0/data[at0001]")
