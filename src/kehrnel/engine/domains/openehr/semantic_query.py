"""Governed semantic-intent to AQL planning for openEHR.

This module intentionally starts *after* natural-language interpretation.  An
LLM, rules engine, or user interface may propose a semantic intent, but it may
only reference fields that exist in a pinned OPT semantic catalog.  The planner
then deterministically validates those references and renders AQL for the
existing ``openehr.rps_dual`` compiler.

The first supported slice is deliberately conservative: predicates may target
the COMPOSITION and one contained archetype instance.  That is sufficient for
queries such as "patients with a SNOMED-expanded diagnosis since a date" while
preserving same-archetype correlation.  Queries spanning sibling archetypes are
rejected until their correlation semantics can be proved end to end.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime, time
from typing import Any, Literal, Mapping, Optional

from pydantic import Field, model_validator

from kehrnel.contracts.artifacts import ContractModel, Sha256Digest
from kehrnel.engine.domains.openehr.templates.semantic_catalog import (
    OpenEhrOptSemanticCatalog,
    QueryableField,
)


SEMANTIC_QUERY_CONTRACT = "openehr-semantic-query/v1"
SEMANTIC_QUERY_PLAN_CONTRACT = "openehr-semantic-query-plan/v1"
MAX_TERMINOLOGY_EXPANSION_CODES = 512
MAX_TERMINOLOGY_ID_LENGTH = 2048
MAX_QUERY_PREDICATES = 64
MAX_QUERY_PROJECTIONS = 32
MAX_QUERY_EXPANSIONS = 16
MAX_QUERY_TEXT_LENGTH = 4096
MAX_URI_LENGTH = 2048
MAX_ID_LENGTH = 256
MAX_QUERY_OFFSET = 1_000_000

_ZERO_DIGEST = "sha256:" + "0" * 64
_SAFE_ARCHETYPE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_SAFE_RM_TYPE = re.compile(r"^[A-Z][A-Z0-9_]*$")
_SAFE_CODE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_SAFE_AQL_PATH = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*"
    r"(?:/[A-Za-z_][A-Za-z0-9_]*(?:\[[A-Za-z0-9_.-]+\])?)+$"
)


def _validate_parameter_string(value: str, *, label: str) -> None:
    """Validate a bounded string that is passed as data, never AQL syntax."""

    if value != value.strip():
        raise ValueError(f"{label} must not have leading or trailing whitespace")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError(f"{label} must not contain control characters")


class SemanticQueryPlanningError(ValueError):
    """A fail-closed semantic planning error with a stable machine code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


class SemanticTemplateRef(ContractModel):
    template_id: str = Field(min_length=1, max_length=512)
    source_digest: Sha256Digest
    catalog_digest: Sha256Digest


class TerminologyExpansionReceipt(ContractModel):
    """Pinned, bounded output from a terminology expansion operation."""

    expansion_id: str = Field(
        min_length=1,
        max_length=MAX_ID_LENGTH,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    provider_id: str = Field(
        min_length=1,
        max_length=MAX_ID_LENGTH,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    system_uri: str = Field(min_length=1, max_length=MAX_URI_LENGTH)
    # openEHR permits terminology identifiers that are URIs.  This remains a
    # parameter value throughout planning/rendering, so URI punctuation is not
    # an AQL-injection risk and must not be restricted to identifier tokens.
    terminology_id: str = Field(min_length=1, max_length=MAX_TERMINOLOGY_ID_LENGTH)
    value_set_uri: Optional[str] = Field(default=None, max_length=MAX_URI_LENGTH)
    version: str = Field(min_length=1, max_length=MAX_ID_LENGTH)
    expression: str = Field(min_length=1, max_length=MAX_QUERY_TEXT_LENGTH)
    codes: list[str] = Field(
        min_length=1,
        max_length=MAX_TERMINOLOGY_EXPANSION_CODES,
    )
    truncated: bool = False
    expansion_digest: Sha256Digest

    @model_validator(mode="after")
    def is_canonical_and_verified(self) -> "TerminologyExpansionReceipt":
        for label, value in (
            ("systemUri", self.system_uri),
            ("terminologyId", self.terminology_id),
            ("valueSetUri", self.value_set_uri),
            ("version", self.version),
            ("expression", self.expression),
        ):
            if value is not None:
                _validate_parameter_string(value, label=label)
        if self.codes != sorted(set(self.codes)):
            raise ValueError("terminology expansion codes must be sorted and unique")
        if any(not _SAFE_CODE.fullmatch(code) for code in self.codes):
            raise ValueError(
                "terminology expansion contains a code that cannot be rendered safely"
            )
        expected = _terminology_expansion_digest(
            expansion_id=self.expansion_id,
            provider_id=self.provider_id,
            system_uri=self.system_uri,
            terminology_id=self.terminology_id,
            value_set_uri=self.value_set_uri,
            version=self.version,
            expression=self.expression,
            codes=self.codes,
            truncated=self.truncated,
        )
        if self.expansion_digest != expected:
            raise ValueError("terminology expansion digest does not match its content")
        return self


def create_terminology_expansion_receipt(
    *,
    expansion_id: str,
    provider_id: str,
    system_uri: str,
    terminology_id: str,
    value_set_uri: Optional[str] = None,
    version: str,
    expression: str,
    codes: list[str],
    truncated: bool = False,
) -> TerminologyExpansionReceipt:
    """Create a canonical receipt suitable for a semantic query."""

    canonical_codes = sorted(set(codes))
    digest = _terminology_expansion_digest(
        expansion_id=expansion_id,
        provider_id=provider_id,
        system_uri=system_uri,
        terminology_id=terminology_id,
        value_set_uri=value_set_uri,
        version=version,
        expression=expression,
        codes=canonical_codes,
        truncated=truncated,
    )
    return TerminologyExpansionReceipt(
        expansion_id=expansion_id,
        provider_id=provider_id,
        system_uri=system_uri,
        terminology_id=terminology_id,
        value_set_uri=value_set_uri,
        version=version,
        expression=expression,
        codes=canonical_codes,
        truncated=truncated,
        expansion_digest=digest,
    )


SemanticOperator = Literal[
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "matches",
    "like",
    "exists",
    "not_exists",
]


class SemanticPredicate(ContractModel):
    semantic_unit_id: str = Field(
        pattern=r"^openehr-field:sha256:[0-9a-f]{64}$"
    )
    facet: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_-]*$",
    )
    operator: SemanticOperator
    rm_type: Optional[str] = Field(
        default=None,
        max_length=64,
        pattern=r"^[A-Z][A-Z0-9_]*$",
    )
    terminology_id: Optional[str] = Field(
        default=None,
        min_length=1,
        max_length=MAX_TERMINOLOGY_ID_LENGTH,
    )
    value: Optional[Any] = None
    values: list[str] = Field(
        default_factory=list,
        max_length=MAX_TERMINOLOGY_EXPANSION_CODES,
    )
    terminology_expansion_ref: Optional[str] = Field(
        default=None,
        max_length=MAX_ID_LENGTH,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    correlation_group: Optional[str] = Field(
        default=None,
        max_length=MAX_ID_LENGTH,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )

    @model_validator(mode="after")
    def operand_matches_operator(self) -> "SemanticPredicate":
        if self.terminology_id is not None:
            _validate_parameter_string(
                self.terminology_id,
                label="terminologyId",
            )
        if self.facet != "code" and self.terminology_id is not None:
            raise ValueError("terminologyId is only valid for the code facet")
        if self.facet in {"code", "terminology"} and self.operator == "like":
            raise ValueError(
                "LIKE is not valid for coded facets; use an exact code or a "
                "governed terminology expansion"
            )
        if (
            self.facet == "code"
            and self.operator not in {"exists", "not_exists"}
            and self.terminology_expansion_ref is None
            and self.terminology_id is None
        ):
            raise ValueError(
                "a direct code predicate requires terminologyId to prevent cross-system matches"
            )
        if (
            self.terminology_expansion_ref is not None
            and self.terminology_id is not None
        ):
            raise ValueError(
                "terminologyId is supplied by the terminology expansion receipt"
            )
        if self.operator in {"exists", "not_exists"}:
            if (
                self.value is not None
                or self.values
                or self.terminology_expansion_ref is not None
                or self.terminology_id is not None
            ):
                raise ValueError(f"{self.operator} does not accept an operand")
            return self

        if self.operator == "matches":
            sources = int(bool(self.values)) + int(
                self.terminology_expansion_ref is not None
            )
            if sources != 1 or self.value is not None:
                raise ValueError(
                    "matches requires exactly one of values or terminologyExpansionRef"
                )
            if self.values:
                if len(self.values) > MAX_TERMINOLOGY_EXPANSION_CODES:
                    raise ValueError("matches contains too many values")
                if self.values != sorted(set(self.values)):
                    raise ValueError("matches values must be sorted and unique")
                if any(not _SAFE_CODE.fullmatch(value) for value in self.values):
                    raise ValueError("matches contains an unsafe value")
            return self

        if self.value is None or self.values or self.terminology_expansion_ref:
            raise ValueError(f"{self.operator} requires exactly one scalar value")
        if isinstance(self.value, (dict, list, tuple, set)):
            raise ValueError("predicate values must be scalar")
        if isinstance(self.value, str) and len(self.value) > MAX_QUERY_TEXT_LENGTH:
            raise ValueError("predicate string value exceeds the query policy limit")
        if self.operator == "like" and not isinstance(self.value, str):
            raise ValueError("like requires a string value")
        return self


class SemanticProjection(ContractModel):
    semantic_unit_id: str = Field(
        pattern=r"^openehr-field:sha256:[0-9a-f]{64}$"
    )
    facet: str = Field(
        min_length=1,
        max_length=64,
        pattern=r"^[a-z][a-z0-9_-]*$",
    )
    alias: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
    )
    rm_type: Optional[str] = Field(
        default=None,
        max_length=64,
        pattern=r"^[A-Z][A-Z0-9_]*$",
    )


class OpenEhrSemanticQuery(ContractModel):
    contract: Literal["openehr-semantic-query/v1"] = SEMANTIC_QUERY_CONTRACT
    scope: Literal["patient", "cross_patient"]
    template_ref: SemanticTemplateRef
    select: list[Literal["ehr_id", "composition_uid"]] = Field(
        default_factory=lambda: ["ehr_id", "composition_uid"]
    )
    projections: list[SemanticProjection] = Field(
        default_factory=list,
        max_length=MAX_QUERY_PROJECTIONS,
    )
    predicates: list[SemanticPredicate] = Field(
        min_length=1,
        max_length=MAX_QUERY_PREDICATES,
    )
    terminology_expansions: list[TerminologyExpansionReceipt] = Field(
        default_factory=list,
        max_length=MAX_QUERY_EXPANSIONS,
    )
    ehr_id: Optional[str] = Field(default=None, max_length=MAX_ID_LENGTH)
    limit: int = Field(default=50, ge=1, le=1000)
    offset: int = Field(default=0, ge=0, le=MAX_QUERY_OFFSET)

    @model_validator(mode="after")
    def query_shape_is_consistent(self) -> "OpenEhrSemanticQuery":
        if len(self.select) != len(set(self.select)):
            raise ValueError("select entries must be unique")
        projection_aliases = [projection.alias for projection in self.projections]
        if len(projection_aliases) != len(set(projection_aliases)):
            raise ValueError("projection aliases must be unique")
        reserved_aliases = {"_id", "ehr_id", "composition_uid"}
        collisions = sorted(set(projection_aliases).intersection(reserved_aliases))
        if collisions:
            raise ValueError(
                "projection aliases collide with reserved result fields: "
                + ", ".join(collisions)
            )
        if self.scope == "patient" and not self.ehr_id:
            raise ValueError("patient scope requires ehrId")
        if self.scope == "cross_patient" and self.ehr_id is not None:
            raise ValueError("cross_patient scope must not declare ehrId")
        expansion_ids = [item.expansion_id for item in self.terminology_expansions]
        if len(expansion_ids) != len(set(expansion_ids)):
            raise ValueError("terminology expansion ids must be unique")
        return self


class ResolvedSemanticField(ContractModel):
    semantic_unit_id: str
    label: str
    archetype_id: str
    archetype_rm_type: str
    facet: str
    aql_path: str
    correlation_anchor_path: str
    projection_paths: list[str] = Field(min_length=1)


class OpenEhrCompileRequest(ContractModel):
    """Public execution handoff with an explicit legacy-strategy adapter.

    The semantic API exposes normal camelCase contract fields.  RPS Dual still
    consumes the historical ``raw_aql`` key internally, so callers must make
    that translation deliberately instead of leaking an untyped dictionary
    through the public query-plan contract.
    """

    domain: Literal["openEHR"] = "openEHR"
    raw_aql: str = Field(min_length=1)
    params: dict[str, Any]
    debug: bool = True
    require_exact_path_correlation: bool = True
    requested_limit: int = Field(ge=1, le=1000)
    execution_limit: int = Field(ge=2, le=1001)
    offset: int = Field(ge=0, le=MAX_QUERY_OFFSET)

    def to_strategy_payload(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "raw_aql": self.raw_aql,
            "params": self.params,
            "debug": self.debug,
            "require_exact_path_correlation": self.require_exact_path_correlation,
        }


class OpenEhrSemanticQueryPlan(ContractModel):
    contract: Literal["openehr-semantic-query-plan/v1"] = (
        SEMANTIC_QUERY_PLAN_CONTRACT
    )
    semantic_query_digest: Sha256Digest
    catalog_ref: SemanticTemplateRef
    aql: str = Field(min_length=1)
    parameters: dict[str, Any]
    execution_request: OpenEhrCompileRequest
    aliases: dict[str, str]
    resolved_fields: list[ResolvedSemanticField]
    terminology_expansion_refs: list[str]
    terminology_expansion_receipts: list[TerminologyExpansionReceipt]
    requested_limit: int = Field(ge=1, le=1000)
    execution_limit: int = Field(ge=2, le=1001)
    offset: int = Field(ge=0, le=MAX_QUERY_OFFSET)
    warnings: list[str] = Field(default_factory=list)
    plan_digest: Sha256Digest

    @model_validator(mode="after")
    def execution_request_matches_plan(self) -> "OpenEhrSemanticQueryPlan":
        """Keep the executable request inseparable from its reviewed window."""

        if self.execution_limit != self.requested_limit + 1:
            raise ValueError(
                "executionLimit must reserve exactly one look-ahead row"
            )
        request = self.execution_request
        if request.raw_aql != self.aql:
            raise ValueError("executionRequest.rawAql must equal plan.aql")
        if request.params != self.parameters:
            raise ValueError("executionRequest.params must equal plan.parameters")
        if request.requested_limit != self.requested_limit:
            raise ValueError(
                "executionRequest.requestedLimit must equal plan.requestedLimit"
            )
        if request.execution_limit != self.execution_limit:
            raise ValueError(
                "executionRequest.executionLimit must equal plan.executionLimit"
            )
        if request.offset != self.offset:
            raise ValueError("executionRequest.offset must equal plan.offset")

        expected_window = f" LIMIT {self.execution_limit}"
        if self.offset:
            expected_window += f" OFFSET {self.offset}"
        if not self.aql.endswith(expected_window):
            raise ValueError(
                "plan AQL must end with its governed look-ahead LIMIT/OFFSET window"
            )
        return self

    def verify_digest(self) -> bool:
        return self.plan_digest == _query_plan_digest(self)


class OpenEhrSemanticQueryPlanner:
    """Validate a semantic query against one catalog and render safe AQL."""

    def __init__(self, catalog: OpenEhrOptSemanticCatalog):
        if not catalog.verify_digest():
            raise SemanticQueryPlanningError(
                "CATALOG_DIGEST_INVALID",
                "The OPT semantic catalog digest is invalid.",
            )
        self.catalog = catalog
        self._fields = {field.field_id: field for field in catalog.fields}

    def plan(
        self,
        query: OpenEhrSemanticQuery | Mapping[str, Any],
    ) -> OpenEhrSemanticQueryPlan:
        semantic_query = (
            query
            if isinstance(query, OpenEhrSemanticQuery)
            else OpenEhrSemanticQuery.model_validate(query)
        )
        if semantic_query.scope == "patient":
            raise SemanticQueryPlanningError(
                "PATIENT_SCOPE_NOT_YET_SUPPORTED",
                "The governed semantic planner currently supports population queries only; "
                "patient-scoped execution does not yet enforce the same exact path correlation.",
            )
        self._validate_template_ref(semantic_query.template_ref)

        expansion_by_id = {
            receipt.expansion_id: receipt
            for receipt in semantic_query.terminology_expansions
        }
        resolved_predicates = [
            (predicate, self._resolve_field(predicate.semantic_unit_id))
            for predicate in semantic_query.predicates
        ]
        resolved_projections = [
            (projection, self._resolve_field(projection.semantic_unit_id))
            for projection in semantic_query.projections
        ]
        all_fields = [field for _, field in resolved_predicates] + [
            field for _, field in resolved_projections
        ]

        aliases, contained_scope = self._resolve_aliases(all_fields)
        self._validate_correlation(resolved_predicates)
        self._validate_projection_correlation(resolved_projections)

        select_fragments = []
        for builtin in semantic_query.select:
            if builtin == "ehr_id":
                select_fragments.append("e/ehr_id/value AS ehr_id")
            elif builtin == "composition_uid":
                select_fragments.append("c/uid/value AS composition_uid")
        resolved_fields: list[ResolvedSemanticField] = []
        for projection, field in resolved_projections:
            path = self._field_path(
                field,
                projection.facet,
                projection.rm_type,
                aliases,
            )
            select_fragments.append(f"{path} AS {projection.alias}")
            resolved_fields.append(
                self._resolved_field(
                    field,
                    projection.facet,
                    path,
                    projection.rm_type,
                )
            )
        if not select_fragments:
            raise SemanticQueryPlanningError(
                "EMPTY_PROJECTION",
                "At least one built-in or semantic projection is required.",
            )

        root_archetype_id = self.catalog.template.root_archetype_id
        self._validate_archetype_token(root_archetype_id)
        from_fragment = (
            f"FROM EHR e CONTAINS COMPOSITION c[{root_archetype_id}]"
        )
        if contained_scope is not None:
            self._validate_archetype_token(contained_scope.archetype_id)
            if not _SAFE_RM_TYPE.fullmatch(contained_scope.rm_type):
                raise SemanticQueryPlanningError(
                    "UNSAFE_RM_TYPE",
                    "The OPT contains an RM type that cannot be rendered safely.",
                    details={"rmType": contained_scope.rm_type},
                )
            from_fragment += (
                f" CONTAINS {contained_scope.rm_type} "
                f"a0[{contained_scope.archetype_id}]"
            )

        parameters: dict[str, Any] = {
            "templateId": self.catalog.template.template_id,
        }
        where_fragments = [
            "c/archetype_details/template_id/value = $templateId"
        ]
        if semantic_query.scope == "patient":
            parameters["ehrId"] = semantic_query.ehr_id
            where_fragments.append("e/ehr_id/value = $ehrId")

        used_expansion_refs: set[str] = set()
        for index, (predicate, field) in enumerate(resolved_predicates):
            self._validate_predicate_compatibility(
                predicate,
                field,
                expansion_by_id,
            )
            path = self._field_path(
                field,
                predicate.facet,
                predicate.rm_type,
                aliases,
            )
            rendered, expansion_ref = self._render_predicate(
                predicate,
                field,
                path,
                index,
                parameters,
                expansion_by_id,
                aliases,
            )
            where_fragments.append(rendered)
            if expansion_ref:
                used_expansion_refs.add(expansion_ref)
            resolved_fields.append(
                self._resolved_field(
                    field,
                    predicate.facet,
                    path,
                    predicate.rm_type,
                    require_terminology=(
                        predicate.facet == "code"
                        and predicate.operator not in {"exists", "not_exists"}
                    ),
                )
            )

        unused_expansions = sorted(set(expansion_by_id) - used_expansion_refs)
        if unused_expansions:
            raise SemanticQueryPlanningError(
                "UNUSED_TERMINOLOGY_EXPANSION",
                "Every supplied terminology expansion must be referenced.",
                details={"expansionIds": unused_expansions},
            )

        # A semantic result window must be stable and must say whether another
        # row exists.  The composition UID is globally unique in openEHR and
        # is a supported RPS Dual sort key, so it provides deterministic
        # offset paging.  One additional row is fetched as a bounded look-ahead
        # and removed at the authenticated execution boundary.
        execution_limit = semantic_query.limit + 1
        aql = (
            "SELECT "
            + ", ".join(select_fragments)
            + " "
            + from_fragment
            + " WHERE "
            + " AND ".join(where_fragments)
            + " ORDER BY c/uid/value ASC"
            + f" LIMIT {execution_limit}"
            + (f" OFFSET {semantic_query.offset}" if semantic_query.offset else "")
        )
        semantic_query_digest = _digest_payload(
            semantic_query.to_wire(exclude_none=True)
        )
        execution_request = OpenEhrCompileRequest(
            raw_aql=aql,
            params=parameters,
            requested_limit=semantic_query.limit,
            execution_limit=execution_limit,
            offset=semantic_query.offset,
        )
        draft = OpenEhrSemanticQueryPlan(
            semantic_query_digest=semantic_query_digest,
            catalog_ref=semantic_query.template_ref,
            aql=aql,
            parameters=parameters,
            execution_request=execution_request,
            aliases=aliases,
            resolved_fields=sorted(
                resolved_fields,
                key=lambda item: (item.aql_path, item.semantic_unit_id, item.facet),
            ),
            terminology_expansion_refs=sorted(used_expansion_refs),
            terminology_expansion_receipts=[
                expansion_by_id[expansion_id]
                for expansion_id in sorted(used_expansion_refs)
            ],
            requested_limit=semantic_query.limit,
            execution_limit=execution_limit,
            offset=semantic_query.offset,
            plan_digest=_ZERO_DIGEST,
        )
        return draft.model_copy(update={"plan_digest": _query_plan_digest(draft)})

    def _validate_template_ref(self, reference: SemanticTemplateRef) -> None:
        expected = self.catalog.template
        mismatches: dict[str, dict[str, str]] = {}
        comparisons = {
            "templateId": (reference.template_id, expected.template_id),
            "sourceDigest": (reference.source_digest, expected.source_digest),
            "catalogDigest": (reference.catalog_digest, self.catalog.catalog_digest),
        }
        for name, (actual, wanted) in comparisons.items():
            if actual != wanted:
                mismatches[name] = {"actual": actual, "expected": wanted}
        if mismatches:
            raise SemanticQueryPlanningError(
                "TEMPLATE_REF_MISMATCH",
                "The semantic query does not reference this exact OPT catalog.",
                details={"mismatches": mismatches},
            )

    def _resolve_field(self, field_id: str) -> QueryableField:
        field = self._fields.get(field_id)
        if field is None:
            raise SemanticQueryPlanningError(
                "UNKNOWN_SEMANTIC_UNIT",
                "The semantic query references a field absent from the pinned OPT.",
                details={"semanticUnitId": field_id},
            )
        return field

    def _resolve_aliases(self, fields: list[QueryableField]):
        root_scope_id = next(
            (
                item.scope_id
                for item in self.catalog.archetypes
                if item.rm_type == "COMPOSITION"
                and item.archetype_id == self.catalog.template.root_archetype_id
            ),
            None,
        )
        aliases: dict[str, str] = {}
        if root_scope_id:
            aliases[root_scope_id] = "c"
        non_root = {
            field.containing_archetype.scope_id: field.containing_archetype
            for field in fields
            if field.containing_archetype.rm_type != "COMPOSITION"
        }
        if len(non_root) > 1:
            raise SemanticQueryPlanningError(
                "MULTI_ARCHETYPE_QUERY_NOT_YET_SUPPORTED",
                "This safe planner slice supports one contained archetype per query.",
                details={
                    "archetypeIds": sorted(
                        item.archetype_id for item in non_root.values()
                    )
                },
            )
        contained_scope = next(iter(non_root.values()), None)
        if contained_scope is not None:
            placements = [
                scope
                for scope in self.catalog.archetypes
                if scope.archetype_id == contained_scope.archetype_id
            ]
            if len(placements) > 1:
                raise SemanticQueryPlanningError(
                    "AMBIGUOUS_ARCHETYPE_PLACEMENT_NOT_SUPPORTED",
                    "This archetype occurs in more than one OPT location; the safe "
                    "planner cannot yet preserve its placement constraint in AQL.",
                    details={
                        "archetypeId": contained_scope.archetype_id,
                        "placements": sorted(
                            scope.absolute_aql_path for scope in placements
                        ),
                    },
                )
            aliases[contained_scope.scope_id] = "a0"
        return aliases, contained_scope

    def _validate_correlation(
        self,
        resolved: list[tuple[SemanticPredicate, QueryableField]],
    ) -> None:
        if len(resolved) < 2:
            return
        repeated = [
            (predicate, field)
            for predicate, field in resolved
            if field.occurrences.repeated
            or field.correlation_anchor.reason != "containing_archetype"
            or field.containing_archetype.occurrences.repeated
        ]
        if len(repeated) < 2:
            return
        groups = {predicate.correlation_group for predicate, _ in repeated}
        if None in groups or len(groups) != 1:
            raise SemanticQueryPlanningError(
                "CORRELATION_GROUP_REQUIRED",
                "Predicates below repeated OPT nodes must share one explicit correlation group.",
            )
        anchors = {
            field.correlation_anchor.absolute_aql_path for _, field in repeated
        }
        if len(anchors) != 1:
            raise SemanticQueryPlanningError(
                "CORRELATION_ANCHOR_MISMATCH",
                "The requested predicates do not share a provable OPT correlation anchor.",
                details={"anchors": sorted(anchors)},
            )
        nested_anchors = [
            field.correlation_anchor.absolute_aql_path
            for _, field in repeated
            if field.occurrences.repeated
            or field.correlation_anchor.absolute_aql_path
            != field.containing_archetype.absolute_aql_path
        ]
        if nested_anchors:
            raise SemanticQueryPlanningError(
                "NESTED_CORRELATION_NOT_YET_SUPPORTED",
                "The requested predicates share a repeated node below their bound "
                "archetype; that instance relationship cannot yet be preserved safely.",
                details={"anchors": sorted(set(nested_anchors))},
            )

    @staticmethod
    def _validate_projection_correlation(
        resolved: list[tuple[SemanticProjection, QueryableField]],
    ) -> None:
        unsafe = [
            field
            for _, field in resolved
            if field.occurrences.repeated
            or field.correlation_anchor.absolute_aql_path
            != field.containing_archetype.absolute_aql_path
        ]
        if unsafe:
            raise SemanticQueryPlanningError(
                "NESTED_PROJECTION_CORRELATION_NOT_YET_SUPPORTED",
                "A projected field below a repeated OPT node needs explicit row "
                "binding that the safe planner does not yet represent.",
                details={
                    "semanticUnitIds": sorted(field.field_id for field in unsafe),
                    "anchors": sorted(
                        {field.correlation_anchor.absolute_aql_path for field in unsafe}
                    ),
                },
            )

    def _field_path(
        self,
        field: QueryableField,
        facet: str,
        rm_type: Optional[str],
        aliases: Mapping[str, str],
    ) -> str:
        candidates = self._facet_candidates(field, facet, rm_type)
        if not candidates:
            raise SemanticQueryPlanningError(
                "UNSUPPORTED_FIELD_FACET",
                "The requested field facet is not available in the pinned OPT.",
                details={
                    "semanticUnitId": field.field_id,
                    "facet": facet,
                    "rmType": rm_type,
                },
            )
        suffixes = {suffix for _, suffix in candidates}
        rm_types = {candidate_rm_type for candidate_rm_type, _ in candidates}
        if len(suffixes) != 1 or (rm_type is None and len(rm_types) != 1):
            raise SemanticQueryPlanningError(
                "AMBIGUOUS_VALUE_VARIANT",
                "The requested facet maps to multiple OPT value paths; specify rmType.",
                details={
                    "semanticUnitId": field.field_id,
                    "facet": facet,
                    "rmTypes": sorted(rm for rm, _ in candidates),
                },
            )
        alias = aliases.get(field.containing_archetype.scope_id)
        if alias is None:
            raise SemanticQueryPlanningError(
                "UNRESOLVED_ARCHETYPE_ALIAS",
                "The field's containing archetype was not bound into the AQL query.",
                details={"semanticUnitId": field.field_id},
            )
        base = (
            field.composition_aql_path
            if alias == "c"
            else field.archetype_relative_aql_path
        )
        rendered = alias + base + next(iter(suffixes))
        if not _SAFE_AQL_PATH.fullmatch(rendered):
            raise SemanticQueryPlanningError(
                "UNSAFE_OPT_PATH",
                "The OPT-derived path cannot be rendered by the safe AQL planner.",
                details={
                    "semanticUnitId": field.field_id,
                    "path": rendered,
                },
            )
        return rendered

    @staticmethod
    def _facet_candidates(
        field: QueryableField,
        facet: str,
        rm_type: Optional[str],
    ) -> list[tuple[str, str]]:
        candidates: list[tuple[str, str]] = []
        for variant in field.value_variants:
            if rm_type is not None and variant.rm_type != rm_type:
                continue
            for path in variant.query_paths:
                if path.facet == facet:
                    candidates.append((variant.rm_type, path.aql_path_suffix))
        return candidates

    def _validate_predicate_compatibility(
        self,
        predicate: SemanticPredicate,
        field: QueryableField,
        expansion_by_id: Mapping[str, TerminologyExpansionReceipt],
    ) -> None:
        if (
            predicate.terminology_expansion_ref is not None
            and predicate.terminology_expansion_ref not in expansion_by_id
        ):
            raise SemanticQueryPlanningError(
                "TERMINOLOGY_EXPANSION_NOT_FOUND",
                "A predicate references an unavailable terminology expansion.",
                details={
                    "expansionId": predicate.terminology_expansion_ref,
                    "semanticUnitId": field.field_id,
                },
            )
        candidates = self._facet_candidates(
            field,
            predicate.facet,
            predicate.rm_type,
        )
        if not candidates:
            return  # _field_path returns the richer stable error.
        rm_types = {rm_type.upper() for rm_type, _ in candidates}

        if predicate.operator in {"neq", "not_exists"}:
            raise SemanticQueryPlanningError(
                "NEGATIVE_OPERATOR_NOT_YET_SUPPORTED",
                "The strict semantic execution path does not yet preserve the "
                "required negated-existential semantics for this operator.",
                details={
                    "semanticUnitId": field.field_id,
                    "operator": predicate.operator,
                },
            )

        if (
            predicate.terminology_expansion_ref is not None
            and predicate.facet != "code"
        ):
            raise SemanticQueryPlanningError(
                "TERMINOLOGY_EXPANSION_REQUIRES_CODE_FACET",
                "A terminology expansion may only bind to a coded field facet.",
                details={"semanticUnitId": field.field_id},
            )

        if predicate.operator in {"gt", "gte", "lt", "lte"}:
            comparable_types = {
                "DV_COUNT",
                "DV_DATE",
                "DV_DATE_TIME",
                "DV_ORDINAL",
                "DV_PROPORTION",
                "DV_QUANTITY",
                "DV_TIME",
            }
            if not rm_types or not rm_types.issubset(comparable_types):
                raise SemanticQueryPlanningError(
                    "OPERATOR_DATATYPE_MISMATCH",
                    "The comparison operator is not valid for the OPT value type.",
                    details={
                        "semanticUnitId": field.field_id,
                        "operator": predicate.operator,
                        "rmTypes": sorted(rm_types),
                    },
                )

        if predicate.facet in {"magnitude", "ordinal", "numerator", "denominator"}:
            if not isinstance(predicate.value, (int, float)) or isinstance(
                predicate.value, bool
            ) or not math.isfinite(predicate.value):
                raise SemanticQueryPlanningError(
                    "PREDICATE_VALUE_TYPE_MISMATCH",
                    "A numeric OPT facet requires a numeric predicate value.",
                    details={
                        "semanticUnitId": field.field_id,
                        "facet": predicate.facet,
                    },
                )
            if predicate.facet == "magnitude" and "DV_QUANTITY" in rm_types:
                raise SemanticQueryPlanningError(
                    "QUANTITY_UNIT_CONTEXT_REQUIRED",
                    "DV_QUANTITY comparisons require an explicit, correlated unit "
                    "contract that this planner slice does not yet represent.",
                    details={"semanticUnitId": field.field_id},
                )

        if predicate.facet in {"lower", "upper"} and predicate.operator not in {
            "exists",
            "not_exists",
        }:
            raise SemanticQueryPlanningError(
                "INTERVAL_BOUND_QUERY_NOT_YET_SUPPORTED",
                "Interval bounds are structured RM values and are not yet safe scalar predicates.",
                details={"semanticUnitId": field.field_id, "facet": predicate.facet},
            )

        if predicate.facet == "value" and predicate.operator not in {
            "exists",
            "not_exists",
        }:
            self._validate_value_lexical_form(predicate, field, rm_types)

        if predicate.facet in {"code", "display", "terminology", "units", "id"}:
            if predicate.operator not in {"matches", "exists", "not_exists"} and not isinstance(
                predicate.value, str
            ):
                raise SemanticQueryPlanningError(
                    "PREDICATE_VALUE_TYPE_MISMATCH",
                    "This OPT facet requires a string predicate value.",
                    details={
                        "semanticUnitId": field.field_id,
                        "facet": predicate.facet,
                    },
                )

        if (
            predicate.facet == "code"
            and predicate.operator not in {"exists", "not_exists"}
            and (
                field.occurrences.repeated
                or field.correlation_anchor.absolute_aql_path
                != field.containing_archetype.absolute_aql_path
            )
        ):
            raise SemanticQueryPlanningError(
                "CODE_FIELD_CORRELATION_NOT_YET_SUPPORTED",
                "A coded field below a repeated OPT node requires code and "
                "terminology values to be correlated to that exact repeated instance.",
                details={"semanticUnitId": field.field_id},
            )

        if predicate.facet == "code" and predicate.operator not in {
            "exists",
            "not_exists",
        }:
            self._validate_coding_constraints(
                predicate,
                field,
                expansion_by_id,
            )

    def _validate_coding_constraints(
        self,
        predicate: SemanticPredicate,
        field: QueryableField,
        expansion_by_id: Mapping[str, TerminologyExpansionReceipt],
    ) -> None:
        variants = [
            variant
            for variant in field.value_variants
            if (predicate.rm_type is None or variant.rm_type == predicate.rm_type)
            and any(path.facet == "code" for path in variant.query_paths)
        ]
        constraints = [
            constraint
            for variant in variants
            for constraint in variant.coding_constraints
        ]
        if not constraints:
            return

        receipt = (
            expansion_by_id.get(predicate.terminology_expansion_ref)
            if predicate.terminology_expansion_ref
            else None
        )
        terminology_id = (
            receipt.terminology_id if receipt is not None else predicate.terminology_id
        )
        values = (
            receipt.codes
            if receipt is not None
            else predicate.values
            if predicate.operator == "matches"
            else [str(predicate.value)]
        )

        def is_compatible(constraint: Any) -> bool:
            if (
                constraint.terminology_id is not None
                and terminology_id != constraint.terminology_id
            ):
                return False
            if constraint.allowed_codes and not set(values).issubset(
                constraint.allowed_codes
            ):
                return False
            if constraint.value_set_uri is not None:
                return bool(
                    receipt is not None
                    and receipt.value_set_uri == constraint.value_set_uri
                )
            return True

        if any(is_compatible(constraint) for constraint in constraints):
            return

        required_value_sets = sorted(
            {
                constraint.value_set_uri
                for constraint in constraints
                if constraint.value_set_uri
            }
        )
        code = (
            "VALUE_SET_VALIDATION_REQUIRED"
            if required_value_sets
            else "CODE_CONSTRAINT_MISMATCH"
        )
        raise SemanticQueryPlanningError(
            code,
            "The coded predicate is incompatible with the constraints declared by the OPT.",
            details={
                "semanticUnitId": field.field_id,
                "terminologyId": terminology_id,
                "values": values,
                "requiredValueSetUris": required_value_sets,
            },
        )

    @staticmethod
    def _validate_value_lexical_form(
        predicate: SemanticPredicate,
        field: QueryableField,
        rm_types: set[str],
    ) -> None:
        value = predicate.value
        if "DV_BOOLEAN" in rm_types and not isinstance(value, bool):
            raise SemanticQueryPlanningError(
                "PREDICATE_VALUE_TYPE_MISMATCH",
                "A DV_BOOLEAN value requires a Boolean predicate value.",
                details={"semanticUnitId": field.field_id},
            )

        string_value_types = {
            "DV_DATE",
            "DV_DATE_TIME",
            "DV_DURATION",
            "DV_TEXT",
            "DV_TIME",
            "DV_URI",
        }
        if rm_types.intersection(string_value_types) and not isinstance(value, str):
            raise SemanticQueryPlanningError(
                "PREDICATE_VALUE_TYPE_MISMATCH",
                "This openEHR value type requires a string predicate value.",
                details={
                    "semanticUnitId": field.field_id,
                    "rmTypes": sorted(rm_types),
                },
            )

        if not isinstance(value, str):
            return
        try:
            if rm_types == {"DV_DATE"}:
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    raise ValueError
                date.fromisoformat(value)
            elif rm_types == {"DV_DATE_TIME"}:
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                    date.fromisoformat(value)
                else:
                    datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif rm_types == {"DV_TIME"}:
                time.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SemanticQueryPlanningError(
                "PREDICATE_VALUE_LEXICAL_FORM_INVALID",
                "The predicate value is not a supported ISO lexical form for the OPT type.",
                details={
                    "semanticUnitId": field.field_id,
                    "rmTypes": sorted(rm_types),
                    "value": value,
                },
            ) from exc

    def _render_predicate(
        self,
        predicate: SemanticPredicate,
        field: QueryableField,
        path: str,
        index: int,
        parameters: dict[str, Any],
        expansion_by_id: Mapping[str, TerminologyExpansionReceipt],
        aliases: Mapping[str, str],
    ) -> tuple[str, Optional[str]]:
        if predicate.operator == "exists":
            return f"EXISTS {path}", None
        if predicate.operator == "not_exists":
            return f"NOT EXISTS {path}", None
        if predicate.operator == "matches":
            expansion_ref = predicate.terminology_expansion_ref
            receipt: Optional[TerminologyExpansionReceipt] = None
            if expansion_ref:
                receipt = expansion_by_id.get(expansion_ref)
                if receipt is None:
                    raise SemanticQueryPlanningError(
                        "TERMINOLOGY_EXPANSION_NOT_FOUND",
                        "A predicate references an unavailable terminology expansion.",
                        details={"expansionId": expansion_ref},
                    )
                if receipt.truncated:
                    raise SemanticQueryPlanningError(
                        "TERMINOLOGY_EXPANSION_TRUNCATED",
                        "A truncated expansion cannot be used for a clinical query.",
                        details={"expansionId": expansion_ref},
                    )
                values = receipt.codes
            else:
                values = predicate.values
            rendered_values = ", ".join(f"'{value}'" for value in values)
            code_match = f"{path} MATCHES {{{rendered_values}}}"
            terminology_id = (
                receipt.terminology_id if receipt is not None else predicate.terminology_id
            )
            if terminology_id is None:
                # The model validator normally prevents this; keep the planner
                # fail-closed if an object was constructed without validation.
                raise SemanticQueryPlanningError(
                    "CODE_SYSTEM_REQUIRED",
                    "A coded predicate requires an explicit terminology identifier.",
                    details={"semanticUnitId": field.field_id},
                )
            terminology_path = self._field_path(
                field,
                "terminology",
                predicate.rm_type,
                aliases,
            )
            terminology_parameter = f"terminology{index}"
            parameters[terminology_parameter] = terminology_id
            return (
                f"({terminology_path} = ${terminology_parameter} "
                f"AND {code_match})",
                expansion_ref,
            )

        parameter_name = f"p{index}"
        parameters[parameter_name] = predicate.value
        operators = {
            "eq": "=",
            "neq": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "like": "LIKE",
        }
        scalar_match = f"{path} {operators[predicate.operator]} ${parameter_name}"
        if predicate.facet != "code":
            return scalar_match, None

        terminology_path = self._field_path(
            field,
            "terminology",
            predicate.rm_type,
            aliases,
        )
        terminology_parameter = f"terminology{index}"
        parameters[terminology_parameter] = predicate.terminology_id
        return (
            f"({terminology_path} = ${terminology_parameter} "
            f"AND {scalar_match})",
            None,
        )

    def _resolved_field(
        self,
        field: QueryableField,
        facet: str,
        path: str,
        rm_type: Optional[str],
        *,
        require_terminology: bool = False,
    ) -> ResolvedSemanticField:
        projection_paths = {
            field.composition_aql_path + suffix
            for _, suffix in self._facet_candidates(field, facet, rm_type)
        }
        if require_terminology:
            projection_paths.update(
                field.composition_aql_path + suffix
                for _, suffix in self._facet_candidates(
                    field,
                    "terminology",
                    rm_type,
                )
            )
        if field.containing_archetype.rm_type != "COMPOSITION":
            projection_paths.add(
                field.containing_archetype.composition_aql_path
                + "/archetype_node_id"
            )
        return ResolvedSemanticField(
            semantic_unit_id=field.field_id,
            label=field.label,
            archetype_id=field.containing_archetype.archetype_id,
            archetype_rm_type=field.containing_archetype.rm_type,
            facet=facet,
            aql_path=path,
            correlation_anchor_path=field.correlation_anchor.absolute_aql_path,
            projection_paths=sorted(projection_paths),
        )

    @staticmethod
    def _validate_archetype_token(archetype_id: str) -> None:
        if not _SAFE_ARCHETYPE_ID.fullmatch(archetype_id):
            raise SemanticQueryPlanningError(
                "UNSAFE_ARCHETYPE_ID",
                "The OPT contains an archetype identifier that cannot be rendered safely.",
                details={"archetypeId": archetype_id},
            )


def plan_open_ehr_semantic_query(
    catalog: OpenEhrOptSemanticCatalog,
    query: OpenEhrSemanticQuery | Mapping[str, Any],
) -> OpenEhrSemanticQueryPlan:
    return OpenEhrSemanticQueryPlanner(catalog).plan(query)


def _terminology_expansion_digest(
    *,
    expansion_id: str,
    provider_id: str,
    system_uri: str,
    terminology_id: str,
    value_set_uri: Optional[str],
    version: str,
    expression: str,
    codes: list[str],
    truncated: bool,
) -> str:
    return _digest_payload(
        {
            "expansionId": expansion_id,
            "providerId": provider_id,
            "systemUri": system_uri,
            "terminologyId": terminology_id,
            "valueSetUri": value_set_uri,
            "version": version,
            "expression": expression,
            "codes": codes,
            "truncated": truncated,
        }
    )


def _query_plan_digest(plan: OpenEhrSemanticQueryPlan) -> str:
    return _digest_payload(
        plan.model_dump(
            mode="json",
            by_alias=True,
            exclude={"plan_digest"},
            exclude_none=True,
        )
    )


def _digest_payload(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


__all__ = [
    "MAX_TERMINOLOGY_EXPANSION_CODES",
    "SEMANTIC_QUERY_CONTRACT",
    "SEMANTIC_QUERY_PLAN_CONTRACT",
    "OpenEhrCompileRequest",
    "OpenEhrSemanticQuery",
    "OpenEhrSemanticQueryPlan",
    "OpenEhrSemanticQueryPlanner",
    "SemanticPredicate",
    "SemanticProjection",
    "SemanticQueryPlanningError",
    "SemanticTemplateRef",
    "TerminologyExpansionReceipt",
    "create_terminology_expansion_receipt",
    "plan_open_ehr_semantic_query",
]
