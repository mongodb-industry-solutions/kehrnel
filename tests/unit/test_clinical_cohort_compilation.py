from __future__ import annotations

from pathlib import Path

import pytest

from kehrnel.api.domains.clinical_cohort_compilation import (
    compile_fhir_condition_cohort,
    compile_openehr_cohort,
)
from kehrnel.contracts import (
    ClinicalCohortIntent,
    ClinicalCohortResolution,
    CohortPredicate,
    ConceptMention,
    ResolutionRoute,
    StructuralResolverKind,
    TerminologyCandidate,
    TerminologyGroundingReceipt,
    create_terminology_expansion,
)
from kehrnel.engine.domains.openehr.semantic_query import OpenEhrSemanticQueryPlanner
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


def _intent() -> ClinicalCohortIntent:
    return ClinicalCohortIntent(
        intent_id="heart-failure",
        criteria=CohortPredicate(
            predicate_id="p-condition",
            semantic_role="condition",
            concept=ConceptMention(
                mention_id="m-heart-failure",
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
        ),
    )


def _terminology() -> TerminologyGroundingReceipt:
    candidate = TerminologyCandidate(
        system="http://snomed.info/sct",
        code="84114007",
        display="Heart failure",
        provider_id="snomed-production",
        version="http://snomed.info/sct/version/20260601",
        score_basis_points=10_000,
        authority="provider_verified",
        verified=True,
    )
    return TerminologyGroundingReceipt(
        mention_id="m-heart-failure",
        status="resolved",
        selected=candidate,
        candidates=[candidate],
        expansion=create_terminology_expansion(
            provider_id=candidate.provider_id,
            system=candidate.system,
            version=candidate.version,
            expression_language="ecl",
            expression="<< 84114007",
            codes=["84114007", "195111005"],
        ),
        reason="Provider verified.",
    )


@pytest.mark.asyncio
async def test_ready_resolution_compiles_to_existing_fhir_execution_request():
    route = ResolutionRoute(
        strategy_id="fhir.resource_store",
        structural_resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
        structural_resolver_id=FhirConditionCohortStructuralResolver.resolver_id,
        terminology_provider_ids=["snomed-production"],
    )
    structural = await FhirConditionCohortStructuralResolver().bind(
        _intent(), [_terminology()], route=route, context={}
    )
    resolution = ClinicalCohortResolution(
        decision="ready",
        intent=_intent(),
        route=route,
        terminology=[_terminology()],
        structural=structural,
    )

    request = compile_fhir_condition_cohort(resolution)

    assert request.criteria == {"onset-date": "ge2025-01-01"}
    assert request.expansion_request.expression == "<< 84114007"
    assert request.expansion_request.release_id == "20260601"
    assert request.expansion_request.max_codes == 2


@pytest.mark.asyncio
async def test_ready_resolution_compiles_through_existing_openehr_aql_planner():
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
        terminology_provider_ids=["snomed-production"],
    )
    structural = await resolver.bind(
        _intent(), [_terminology()], route=route, context={}
    )
    resolution = ClinicalCohortResolution(
        decision="ready",
        intent=_intent(),
        route=route,
        terminology=[_terminology()],
        structural=structural,
    )

    query = compile_openehr_cohort(resolution)
    plan = OpenEhrSemanticQueryPlanner(catalog).plan(query)

    assert query.predicates[0].semantic_unit_id == diagnosis.field_id
    assert query.predicates[1].semantic_unit_id == onset.field_id
    assert "MATCHES {'195111005', '84114007'}" in plan.aql
    assert ">= $p1" in plan.aql
    assert plan.verify_digest()
