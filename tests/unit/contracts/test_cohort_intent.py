from __future__ import annotations

from pydantic import ValidationError
import pytest

from kehrnel.contracts import (
    ClinicalCohortIntent,
    CohortGroup,
    CohortPredicate,
    ConceptMention,
    ResolutionRoute,
    StructuralResolverKind,
)


def _predicate(identifier: str = "condition-heart-failure") -> CohortPredicate:
    return CohortPredicate(
        predicate_id=identifier,
        semantic_role="condition",
        concept=ConceptMention(
            mention_id=f"mention-{identifier}",
            text="heart failure",
            semantic_category="clinical_condition",
            requested_relationship="descendant_or_self",
            terminology_system_hint="http://snomed.info/sct",
        ),
        temporal=[
            {
                "semanticRole": "clinical_onset",
                "operator": "gte",
                "value": "2025-01-01",
            }
        ],
        correlation_group="condition-occurrence",
    )


def test_cohort_intent_keeps_unresolved_meaning_separate_from_codes_and_paths():
    intent = ClinicalCohortIntent(
        intent_id="cohort-heart-failure",
        criteria=_predicate(),
    )

    wire = intent.to_wire()
    mention = wire["criteria"]["concept"]
    assert mention == {
        "mentionId": "mention-condition-heart-failure",
        "text": "heart failure",
        "semanticCategory": "clinical_condition",
        "requestedRelationship": "descendant_or_self",
        "terminologySystemHint": "http://snomed.info/sct",
    }
    assert "code" not in mention
    assert "path" not in str(wire).lower()


def test_cohort_boolean_tree_requires_one_child_for_not_and_unique_ids():
    with pytest.raises(ValidationError, match="NOT group must contain exactly one child"):
        CohortGroup(
            group_id="not-conditions",
            operator="not",
            children=[_predicate("one"), _predicate("two")],
        )

    with pytest.raises(ValidationError, match="node identifiers must be unique"):
        ClinicalCohortIntent(
            intent_id="duplicate-nodes",
            criteria=CohortGroup(
                group_id="all",
                operator="all",
                children=[_predicate("same"), _predicate("same")],
            ),
        )


def test_custom_structural_route_requires_a_compiled_semantic_pack():
    with pytest.raises(ValidationError, match="semanticPackRef"):
        ResolutionRoute(
            strategy_id="tenant.custom",
            structural_resolver_kind=StructuralResolverKind.COMPILED_SEMANTIC_PACK,
            structural_resolver_id="semantic-pack-resolver.v1",
        )
