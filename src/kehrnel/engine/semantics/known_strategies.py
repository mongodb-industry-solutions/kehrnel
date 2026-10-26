"""Native structural resolvers for the first governed cohort strategies.

These resolvers do not discover meaning.  They prove that an already typed
intent can be represented by one exact strategy/catalog version.  Unsupported
boolean or field semantics fail closed so the modeler can propose a different
architecture instead of generating an approximately correct query.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from kehrnel.contracts import (
    ArtifactRef,
    ClinicalCohortIntent,
    CohortBooleanOperator,
    CohortGroup,
    CohortPredicate,
    ResolutionRoute,
    ResolutionStatus,
    StructuralBinding,
    StructuralBindingReceipt,
    StructuralResolverKind,
    TerminologyGroundingReceipt,
)
from kehrnel.engine.domains.openehr.templates.semantic_catalog import (
    OpenEhrOptSemanticCatalog,
    QueryableField,
)


def _conjunctive_predicates(
    node: CohortPredicate | CohortGroup,
) -> tuple[list[CohortPredicate], str | None]:
    if isinstance(node, CohortPredicate):
        return [node], None
    if node.operator is not CohortBooleanOperator.ALL:
        return [], (
            f"Boolean operator {node.operator.value!r} is not yet supported by "
            "this native strategy profile."
        )
    predicates: list[CohortPredicate] = []
    for child in node.children:
        nested, reason = _conjunctive_predicates(child)
        if reason:
            return [], reason
        predicates.extend(nested)
    return predicates, None


class FhirConditionCohortStructuralResolver:
    """Prove compatibility with the current Condition cohort query slice."""

    resolver_id = "fhir-condition-cohort.v1"

    async def bind(
        self,
        intent: ClinicalCohortIntent,
        terminology: Sequence[TerminologyGroundingReceipt],
        *,
        route: ResolutionRoute,
        context: dict[str, Any],
    ) -> StructuralBindingReceipt:
        del terminology, context
        predicates, reason = _conjunctive_predicates(intent.criteria)
        if reason:
            return self._unsupported(route, reason)
        if len(predicates) != 1:
            return self._unsupported(
                route,
                "The current FHIR cohort profile accepts exactly one Condition predicate.",
            )
        predicate = predicates[0]
        if (
            predicate.semantic_role != "condition"
            or predicate.concept is None
            or predicate.comparison is not None
        ):
            return self._unsupported(
                route,
                "The current FHIR cohort profile requires one coded condition predicate.",
            )
        unsupported_temporal = [
            item
            for item in predicate.temporal
            if item.semantic_role != "clinical_onset"
            or item.operator.value not in {"eq", "gt", "gte", "lt", "lte"}
        ]
        if unsupported_temporal:
            return self._unsupported(
                route,
                "The FHIR Condition profile only supports bounded clinical-onset comparisons.",
            )
        if len(predicate.temporal) > 1:
            return self._unsupported(
                route,
                "The current FHIR cohort profile supports one clinical-onset bound per query.",
            )
        bindings = [
            StructuralBinding(
                predicate_id=predicate.predicate_id,
                semantic_role="condition",
                binding_id="fhir-r4:Condition.code",
                model_kind="fhir_search_parameter",
                details={
                    "resourceType": "Condition",
                    "searchParameter": "code",
                    "storageProjection": "_search.code_systemCode",
                },
            )
        ]
        for item in predicate.temporal:
            bindings.append(
                StructuralBinding(
                    predicate_id=predicate.predicate_id,
                    semantic_role=item.semantic_role,
                    binding_id="fhir-r4:Condition.onset-date",
                    model_kind="fhir_search_parameter",
                    details={
                        "resourceType": "Condition",
                        "searchParameter": "onset-date",
                        "operator": item.operator.value,
                    },
                )
            )
        return StructuralBindingReceipt(
            status=ResolutionStatus.RESOLVED,
            resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
            resolver_id=self.resolver_id,
            strategy_id=route.strategy_id,
            bindings=bindings,
            reason="The intent is representable by the governed FHIR Condition search profile.",
        )

    def _unsupported(self, route: ResolutionRoute, reason: str) -> StructuralBindingReceipt:
        return StructuralBindingReceipt(
            status=ResolutionStatus.UNSUPPORTED,
            resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
            resolver_id=self.resolver_id,
            strategy_id=route.strategy_id,
            reason=reason,
        )


@dataclass(frozen=True, slots=True)
class OpenEhrRoleBinding:
    semantic_role: str
    field_id: str
    facet: str


class OpenEhrOptCohortStructuralResolver:
    """Bind semantic roles to exact fields in one immutable OPT catalog."""

    def __init__(
        self,
        *,
        resolver_id: str,
        catalog: OpenEhrOptSemanticCatalog,
        role_bindings: Sequence[OpenEhrRoleBinding],
        strategy_version: str | None = None,
    ) -> None:
        if not resolver_id.strip():
            raise ValueError("resolver_id is required")
        if not catalog.verify_digest():
            raise ValueError("openEHR semantic catalog digest is invalid")
        self.resolver_id = resolver_id
        self.catalog = catalog
        self.strategy_version = strategy_version
        self._fields = {field.field_id: field for field in catalog.fields}
        self._roles: dict[str, OpenEhrRoleBinding] = {}
        for binding in role_bindings:
            if binding.semantic_role in self._roles:
                raise ValueError(f"duplicate openEHR semantic role {binding.semantic_role!r}")
            field = self._fields.get(binding.field_id)
            if field is None:
                raise ValueError(f"unknown OPT field id {binding.field_id!r}")
            if not _field_has_facet(field, binding.facet):
                raise ValueError(
                    f"facet {binding.facet!r} is not queryable for field {binding.field_id!r}"
                )
            self._roles[binding.semantic_role] = binding

    async def bind(
        self,
        intent: ClinicalCohortIntent,
        terminology: Sequence[TerminologyGroundingReceipt],
        *,
        route: ResolutionRoute,
        context: dict[str, Any],
    ) -> StructuralBindingReceipt:
        del terminology, context
        predicates, reason = _conjunctive_predicates(intent.criteria)
        if reason:
            return self._unsupported(route, reason)
        bindings: list[StructuralBinding] = []
        missing_roles: list[str] = []
        for predicate in predicates:
            if predicate.concept is not None:
                binding = self._roles.get(predicate.semantic_role)
                if binding is None:
                    missing_roles.append(predicate.semantic_role)
                else:
                    bindings.append(self._binding(predicate, binding))
            if predicate.comparison is not None:
                binding = self._roles.get(predicate.semantic_role)
                if binding is None:
                    missing_roles.append(predicate.semantic_role)
                elif not any(
                    item.predicate_id == predicate.predicate_id
                    and item.semantic_role == predicate.semantic_role
                    for item in bindings
                ):
                    bindings.append(self._binding(predicate, binding))
            for temporal in predicate.temporal:
                binding = self._roles.get(temporal.semantic_role)
                if binding is None:
                    missing_roles.append(temporal.semantic_role)
                else:
                    bindings.append(self._binding(predicate, binding))
        if missing_roles:
            return self._unsupported(
                route,
                "The pinned OPT query profile has no binding for: "
                + ", ".join(sorted(set(missing_roles))),
            )
        return StructuralBindingReceipt(
            status=ResolutionStatus.RESOLVED,
            resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
            resolver_id=self.resolver_id,
            strategy_id=route.strategy_id,
            strategy_version=self.strategy_version,
            catalog_ref=ArtifactRef(
                kind="openehr-opt-semantic-catalog",
                id=self.catalog.template.template_id,
                version=(
                    self.catalog.template.semantic_version
                    or self.catalog.template.source_digest
                ),
                digest=self.catalog.catalog_digest,
            ),
            bindings=bindings,
            reason="Every semantic role is bound to an exact field and facet in the pinned OPT catalog.",
        )

    def _binding(
        self,
        predicate: CohortPredicate,
        binding: OpenEhrRoleBinding,
    ) -> StructuralBinding:
        field = self._fields[binding.field_id]
        return StructuralBinding(
            predicate_id=predicate.predicate_id,
            semantic_role=binding.semantic_role,
            binding_id=f"{binding.field_id}:{binding.facet}",
            model_kind="openehr_opt_field",
            details={
                "fieldId": binding.field_id,
                "facet": binding.facet,
                "templateId": self.catalog.template.template_id,
                "sourceDigest": self.catalog.template.source_digest,
                "archetypeId": field.containing_archetype.archetype_id,
                "correlationAnchor": field.correlation_anchor.to_wire(),
            },
        )

    def _unsupported(self, route: ResolutionRoute, reason: str) -> StructuralBindingReceipt:
        return StructuralBindingReceipt(
            status=ResolutionStatus.UNSUPPORTED,
            resolver_kind=StructuralResolverKind.NATIVE_STRATEGY,
            resolver_id=self.resolver_id,
            strategy_id=route.strategy_id,
            strategy_version=self.strategy_version,
            catalog_ref=ArtifactRef(
                kind="openehr-opt-semantic-catalog",
                id=self.catalog.template.template_id,
                version=(
                    self.catalog.template.semantic_version
                    or self.catalog.template.source_digest
                ),
                digest=self.catalog.catalog_digest,
            ),
            reason=reason,
        )


def _field_has_facet(field: QueryableField, facet: str) -> bool:
    return any(
        query_path.facet == facet
        for variant in field.value_variants
        for query_path in variant.query_paths
    )
