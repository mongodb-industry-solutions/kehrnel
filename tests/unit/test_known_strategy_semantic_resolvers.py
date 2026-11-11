from __future__ import annotations

from pathlib import Path

import pytest

from kehrnel.contracts import (
    ClinicalCohortIntent,
    CohortGroup,
    CohortPredicate,
    ConceptMention,
    ResolutionRoute,
    ResolutionStatus,
    StructuralResolverKind,
)
from kehrnel.engine.domains.openehr.templates import compile_opt_semantic_catalog
from kehrnel.engine.semantics import (
    FhirConditionCohortStructuralResolver,
    OpenEhrOptCohortStructuralResolver,
    OpenEhrRoleBinding,
)


SAMPLE_LAB_OPT = Path(
    "src/kehrnel/engine/strategies/openehr/rps_dual/samples/reference/"
    "templates/sample_laboratory_v0_4.opt"
)


def _predicate() -> CohortPredicate:
    return CohortPredicate(
        predicate_id="p-condition",
        semantic_role="condition",
        concept=ConceptMention(
            mention_id="m-condition",
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


@pytest.mark.asyncio
async def test_fhir_resolver_proves_native_code_and_onset_bindings():
    route = ResolutionRoute(
        strategy_id="fhir.resource_store",
        structural_resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
        structural_resolver_id=FhirConditionCohortStructuralResolver.resolver_id,
    )
    receipt = await FhirConditionCohortStructuralResolver().bind(
        ClinicalCohortIntent(intent_id="hf", criteria=_predicate()),
        [],
        route=route,
        context={},
    )

    assert receipt.status is ResolutionStatus.RESOLVED
    assert [item.binding_id for item in receipt.bindings] == [
        "fhir-r4:Condition.code",
        "fhir-r4:Condition.onset-date",
    ]


@pytest.mark.asyncio
async def test_fhir_resolver_fails_closed_for_boolean_disjunction():
    route = ResolutionRoute(
        strategy_id="fhir.resource_store",
        structural_resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
        structural_resolver_id=FhirConditionCohortStructuralResolver.resolver_id,
    )
    second = _predicate().model_copy(
        update={
            "predicate_id": "p2",
            "concept": _predicate().concept.model_copy(
                update={"mention_id": "m-condition-2"}
            ),
        }
    )
    receipt = await FhirConditionCohortStructuralResolver().bind(
        ClinicalCohortIntent(
            intent_id="or",
            criteria=CohortGroup(
                group_id="any-condition",
                operator="any",
                children=[_predicate(), second],
            ),
        ),
        [],
        route=route,
        context={},
    )

    assert receipt.status is ResolutionStatus.UNSUPPORTED
    assert "Boolean operator" in receipt.reason


@pytest.mark.asyncio
async def test_openehr_resolver_uses_explicit_profile_bindings_not_label_matching():
    catalog = compile_opt_semantic_catalog(SAMPLE_LAB_OPT)
    diagnosis = next(field for field in catalog.fields if field.label == "Diagnosis")
    onset = next(
        field for field in catalog.fields if field.label == "Date/time clinically recognised"
    )
    resolver = OpenEhrOptCohortStructuralResolver(
        resolver_id="openehr-sample-diagnosis.v1",
        catalog=catalog,
        role_bindings=[
            OpenEhrRoleBinding("condition", diagnosis.field_id, "code"),
            OpenEhrRoleBinding("clinical_onset", onset.field_id, "value"),
        ],
    )
    route = ResolutionRoute(
        strategy_id="openehr.rps_dual",
        structural_resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
        structural_resolver_id=resolver.resolver_id,
    )
    receipt = await resolver.bind(
        ClinicalCohortIntent(intent_id="hf", criteria=_predicate()),
        [],
        route=route,
        context={},
    )

    assert receipt.status is ResolutionStatus.RESOLVED
    assert receipt.catalog_ref.digest == catalog.catalog_digest
    assert [item.details["fieldId"] for item in receipt.bindings] == [
        diagnosis.field_id,
        onset.field_id,
    ]
