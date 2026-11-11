"""Compile a ready protocol-neutral cohort resolution into native requests.

The compilation boundary consumes only approved terminology and structural
receipts. It never performs label matching, path guessing, terminology search,
or model inference.
"""

from __future__ import annotations

from typing import Mapping

from kehrnel.api.domains.fhir.terminology_cohort import (
    MAX_FHIR_COHORT_ROWS,
    FhirSnomedExpansionRequest,
    FhirTerminologyCohortQueryRequest,
)
from kehrnel.contracts import (
    ClinicalCohortResolution,
    CohortBooleanOperator,
    CohortGroup,
    CohortPredicate,
    ResolutionDecision,
    TerminologyGroundingReceipt,
)
from kehrnel.engine.domains.openehr.semantic_query import (
    OpenEhrSemanticQuery,
    SemanticPredicate,
    SemanticTemplateRef,
    create_terminology_expansion_receipt,
)


SNOMED_SYSTEM_URI = "http://snomed.info/sct"
DEFAULT_OPENEHR_TERMINOLOGY_IDS: Mapping[str, str] = {
    SNOMED_SYSTEM_URI: "SNOMED-CT"
}


class CohortCompilationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _ready(resolution: ClinicalCohortResolution) -> None:
    if resolution.decision is not ResolutionDecision.READY:
        raise CohortCompilationError(
            "COHORT_RESOLUTION_NOT_READY",
            "Only a ready cohort resolution can be compiled.",
        )
    if resolution.structural is None:
        raise CohortCompilationError(
            "COHORT_STRUCTURAL_RECEIPT_MISSING",
            "The ready cohort resolution has no structural receipt.",
        )


def _conjunctive_predicates(node: CohortPredicate | CohortGroup) -> list[CohortPredicate]:
    if isinstance(node, CohortPredicate):
        return [node]
    if node.operator is not CohortBooleanOperator.ALL:
        raise CohortCompilationError(
            "COHORT_BOOLEAN_NOT_SUPPORTED",
            f"The target compiler does not yet support {node.operator.value!r} groups.",
        )
    result: list[CohortPredicate] = []
    for child in node.children:
        result.extend(_conjunctive_predicates(child))
    return result


def _terminology_by_mention(
    resolution: ClinicalCohortResolution,
) -> dict[str, TerminologyGroundingReceipt]:
    return {item.mention_id: item for item in resolution.terminology}


def compile_fhir_condition_cohort(
    resolution: ClinicalCohortResolution,
) -> FhirTerminologyCohortQueryRequest:
    """Compile the supported Condition slice into its existing execution API."""

    _ready(resolution)
    if resolution.route.strategy_id != "fhir.resource_store":
        raise CohortCompilationError(
            "FHIR_STRATEGY_MISMATCH",
            "FHIR cohort compilation requires the fhir.resource_store route.",
        )
    predicates = _conjunctive_predicates(resolution.intent.criteria)
    if len(predicates) != 1 or predicates[0].concept is None:
        raise CohortCompilationError(
            "FHIR_CONDITION_PROFILE_MISMATCH",
            "The current FHIR compiler requires one coded Condition predicate.",
        )
    predicate = predicates[0]
    terminology = _terminology_by_mention(resolution)[predicate.concept.mention_id]
    expansion = terminology.expansion
    if expansion is None or expansion.system.rstrip("/") != SNOMED_SYSTEM_URI:
        raise CohortCompilationError(
            "FHIR_SNOMED_EXPANSION_REQUIRED",
            "The current FHIR cohort compiler requires a complete SNOMED CT expansion.",
        )
    if resolution.intent.result.limit > MAX_FHIR_COHORT_ROWS:
        raise CohortCompilationError(
            "FHIR_RESULT_LIMIT_EXCEEDED",
            f"FHIR cohort execution is bounded to {MAX_FHIR_COHORT_ROWS} rows.",
        )
    criteria: dict[str, str] = {}
    prefix = {"eq": "eq", "gt": "gt", "gte": "ge", "lt": "lt", "lte": "le"}
    for temporal in predicate.temporal:
        if temporal.semantic_role != "clinical_onset" or temporal.operator.value not in prefix:
            raise CohortCompilationError(
                "FHIR_TEMPORAL_CONSTRAINT_UNSUPPORTED",
                "The current FHIR Condition compiler only supports clinical-onset comparisons.",
            )
        if not isinstance(temporal.value, str) or not temporal.value.strip():
            raise CohortCompilationError(
                "FHIR_TEMPORAL_VALUE_INVALID",
                "FHIR onset-date requires a normalized date string.",
            )
        criteria["onset-date"] = prefix[temporal.operator.value] + temporal.value

    expression = (
        expansion.expression
        if expansion.expression_language.value != "value_set"
        else None
    )
    return FhirTerminologyCohortQueryRequest(
        criteria=criteria,
        limit=resolution.intent.result.limit,
        expansion_request=FhirSnomedExpansionRequest(
            expansion_id=predicate.concept.mention_id,
            provider_id=expansion.provider_id,
            version=expansion.version,
            release_id=_release_id(expansion.version),
            expression=expression,
            value_set_uri=expansion.value_set_uri,
            value_set_version=expansion.value_set_version,
            max_codes=len(expansion.codes),
        ),
    )


def compile_openehr_cohort(
    resolution: ClinicalCohortResolution,
    *,
    terminology_ids: Mapping[str, str] = DEFAULT_OPENEHR_TERMINOLOGY_IDS,
) -> OpenEhrSemanticQuery:
    """Compile approved bindings into the existing OPT-to-AQL semantic query."""

    _ready(resolution)
    if resolution.route.strategy_id != "openehr.rps_dual":
        raise CohortCompilationError(
            "OPENEHR_STRATEGY_MISMATCH",
            "openEHR cohort compilation requires the openehr.rps_dual route.",
        )
    structural = resolution.structural
    assert structural is not None
    if structural.catalog_ref is None:
        raise CohortCompilationError(
            "OPENEHR_CATALOG_REF_MISSING",
            "The openEHR structural receipt must identify its exact OPT catalog.",
        )
    bindings = {
        (item.predicate_id, item.semantic_role): item
        for item in structural.bindings
    }
    terminology = _terminology_by_mention(resolution)
    semantic_predicates: list[SemanticPredicate] = []
    expansion_receipts = []
    for predicate in _conjunctive_predicates(resolution.intent.criteria):
        if predicate.concept is not None:
            binding = _required_binding(bindings, predicate, predicate.semantic_role)
            grounded = terminology[predicate.concept.mention_id]
            expansion = grounded.expansion
            if expansion is None:
                raise CohortCompilationError(
                    "OPENEHR_TERMINOLOGY_EXPANSION_MISSING",
                    "The coded openEHR predicate has no complete terminology expansion.",
                )
            terminology_id = terminology_ids.get(expansion.system.rstrip("/"))
            if not terminology_id:
                raise CohortCompilationError(
                    "OPENEHR_TERMINOLOGY_ID_BINDING_REQUIRED",
                    f"No openEHR terminology identifier is registered for {expansion.system!r}.",
                )
            receipt = create_terminology_expansion_receipt(
                expansion_id=predicate.concept.mention_id,
                provider_id=expansion.provider_id,
                system_uri=expansion.system,
                terminology_id=terminology_id,
                value_set_uri=expansion.value_set_uri,
                version=expansion.version,
                expression=expansion.expression,
                codes=expansion.codes,
            )
            expansion_receipts.append(receipt)
            semantic_predicates.append(
                SemanticPredicate(
                    semantic_unit_id=str(binding.details["fieldId"]),
                    facet=str(binding.details["facet"]),
                    operator="matches",
                    terminology_expansion_ref=receipt.expansion_id,
                    correlation_group=predicate.correlation_group,
                )
            )
        if predicate.comparison is not None:
            binding = _required_binding(bindings, predicate, predicate.semantic_role)
            semantic_predicates.append(
                SemanticPredicate(
                    semantic_unit_id=str(binding.details["fieldId"]),
                    facet=str(binding.details["facet"]),
                    operator=predicate.comparison.operator.value,
                    value=predicate.comparison.value,
                    correlation_group=predicate.correlation_group,
                )
            )
        for temporal in predicate.temporal:
            binding = _required_binding(bindings, predicate, temporal.semantic_role)
            semantic_predicates.append(
                SemanticPredicate(
                    semantic_unit_id=str(binding.details["fieldId"]),
                    facet=str(binding.details["facet"]),
                    operator=temporal.operator.value,
                    value=temporal.value,
                    correlation_group=predicate.correlation_group,
                )
            )

    return OpenEhrSemanticQuery(
        scope="cross_patient",
        template_ref=SemanticTemplateRef(
            template_id=structural.catalog_ref.id,
            source_digest=_source_digest_from_structural(structural.bindings),
            catalog_digest=structural.catalog_ref.digest,
        ),
        predicates=semantic_predicates,
        terminology_expansions=expansion_receipts,
        limit=resolution.intent.result.limit,
    )


def _required_binding(bindings, predicate: CohortPredicate, semantic_role: str):
    binding = bindings.get((predicate.predicate_id, semantic_role))
    if binding is None or binding.model_kind != "openehr_opt_field":
        raise CohortCompilationError(
            "OPENEHR_STRUCTURAL_BINDING_MISSING",
            f"No approved OPT field binding exists for semantic role {semantic_role!r}.",
        )
    return binding


def _source_digest_from_structural(bindings) -> str:
    values = {
        str(item.details.get("sourceDigest"))
        for item in bindings
        if item.details.get("sourceDigest")
    }
    if len(values) != 1:
        raise CohortCompilationError(
            "OPENEHR_SOURCE_DIGEST_MISSING",
            "The structural receipt does not prove one exact OPT source digest.",
        )
    return values.pop()


def _release_id(version: str) -> str | None:
    normalized = version.rstrip("/")
    if "/version/" in normalized:
        return normalized.rsplit("/version/", 1)[-1]
    return normalized if "://" not in normalized else None
