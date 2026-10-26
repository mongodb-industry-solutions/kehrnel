"""Governed, executable mapping contracts.

Mapping authors may continue to use YAML or graphical tooling, but runtimes
execute this typed intermediate representation.  The first version is
deliberately small: deterministic scalar mappings over a normalized joined
record, with no embedded code or network access.
"""

from __future__ import annotations

import math
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, model_validator

from .artifacts import (
    ArtifactRef,
    CapabilityMaturity,
    CatalogArtifactRole,
    CatalogMetadata,
    ContractModel,
    ExecutionState,
    Sha256Digest,
)


class SelectorDialect(str, Enum):
    DOTTED_PATH = "dotted_path"
    JSON_POINTER = "json_pointer"


class MappingTransformOperation(str, Enum):
    TO_STRING = "to_string"
    TO_INTEGER = "to_integer"
    TO_DECIMAL = "to_decimal"
    ISO_DATETIME = "iso_datetime"
    LOWERCASE = "lowercase"
    UPPERCASE = "uppercase"


class MappingExecutionMode(str, Enum):
    BATCH = "batch"
    CDC = "cdc"


class DeleteBehavior(str, Enum):
    REJECT = "reject"


class ErrorBehavior(str, Enum):
    REJECT_RECORD = "reject_record"
    FAIL_BATCH = "fail_batch"


class JoinCardinality(str, Enum):
    ONE_TO_ONE = "one_to_one"
    MANY_TO_ONE = "many_to_one"
    ONE_TO_MANY = "one_to_many"


class MappingSelector(ContractModel):
    dialect: SelectorDialect = SelectorDialect.DOTTED_PATH
    path: str = Field(min_length=1)


class SourceJoin(ContractModel):
    left_structure: str = Field(min_length=1)
    left_path: str = Field(min_length=1)
    right_structure: str = Field(min_length=1)
    right_path: str = Field(min_length=1)
    cardinality: JoinCardinality


class MappingSource(ContractModel):
    profile_ref: ArtifactRef
    interaction_ids: List[str] = Field(min_length=1)
    root_shape: Literal["normalized_joined_record"]
    structures: List[str] = Field(min_length=1)
    joins: List[SourceJoin] = Field(default_factory=list)

    @model_validator(mode="after")
    def source_is_coherent(self) -> "MappingSource":
        if self.profile_ref.kind != "source-profile":
            raise ValueError("mapping source profileRef.kind must be 'source-profile'")
        if len(self.interaction_ids) != len(set(self.interaction_ids)):
            raise ValueError("mapping source interactionIds must be unique")
        if len(self.structures) != len(set(self.structures)):
            raise ValueError("mapping source structures must be unique")
        known = set(self.structures)
        for join in self.joins:
            if join.left_structure not in known or join.right_structure not in known:
                raise ValueError("mapping joins must reference declared source structures")
        return self


class MappingTarget(ContractModel):
    model: Literal["openehr", "fhir", "cdisc", "custom_operational"]
    strategy_ids: List[str] = Field(min_length=1)
    schema_id: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    document_id_source: MappingSelector
    subject_key_source: Optional[MappingSelector] = None


class MappingTransformStep(ContractModel):
    operation: MappingTransformOperation


class MappingRule(ContractModel):
    rule_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    source: Optional[MappingSelector] = None
    constant: Any = None
    target: MappingSelector
    transforms: List[MappingTransformStep] = Field(default_factory=list)
    required: bool = True
    semantic_concept_id: Optional[str] = None
    terminology_ref: Optional[str] = None
    reversible: bool = True
    lossiness_note: Optional[str] = None

    @model_validator(mode="after")
    def rule_has_one_input_and_pointer_target(self) -> "MappingRule":
        has_constant = "constant" in self.model_fields_set
        if (self.source is None) == (not has_constant):
            raise ValueError("mapping rule requires exactly one of source or constant")
        if has_constant and not isinstance(
            self.constant, (str, int, float, bool, type(None))
        ):
            raise ValueError("mapping v1 constants must be scalar values")
        if has_constant and isinstance(self.constant, float) and not math.isfinite(
            self.constant
        ):
            raise ValueError("mapping v1 numeric constants must be finite")
        if self.target.dialect is not SelectorDialect.JSON_POINTER:
            raise ValueError("mapping rule targets must use json_pointer")
        if not self.target.path.startswith("/"):
            raise ValueError("mapping rule target JSON Pointer must start with '/'")
        if not self.reversible and not self.lossiness_note:
            raise ValueError("non-reversible mapping rules require lossinessNote")
        return self


class MappingExecutionPolicy(ContractModel):
    supported_modes: List[MappingExecutionMode] = Field(min_length=1)
    idempotency_selectors: List[MappingSelector] = Field(min_length=1)
    ordering_selector: Optional[MappingSelector] = None
    delete_behavior: DeleteBehavior = DeleteBehavior.REJECT
    error_behavior: ErrorBehavior = ErrorBehavior.REJECT_RECORD
    dead_letter_contract: str = "mapping-dead-letter/v1"

    @model_validator(mode="after")
    def execution_modes_are_unique(self) -> "MappingExecutionPolicy":
        if len(self.supported_modes) != len(set(self.supported_modes)):
            raise ValueError("mapping supportedModes must be unique")
        return self


class MappingDefinition(ContractModel):
    contract: Literal["mapping-definition/v1"]
    artifact_ref: ArtifactRef
    catalog: CatalogMetadata
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    capability_maturity: CapabilityMaturity
    source: MappingSource
    target: MappingTarget
    rules: List[MappingRule] = Field(min_length=1)
    execution: MappingExecutionPolicy
    evidence_refs: List[ArtifactRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def mapping_is_coherent(self) -> "MappingDefinition":
        if self.artifact_ref.kind != "mapping":
            raise ValueError("mapping artifactRef.kind must be 'mapping'")
        if self.catalog.role is not CatalogArtifactRole.MAPPING:
            raise ValueError("mapping catalog.role must be 'mapping'")
        rule_ids = [rule.rule_id for rule in self.rules]
        if len(rule_ids) != len(set(rule_ids)):
            raise ValueError("mapping definition contains duplicate ruleId values")
        target_paths = [rule.target.path for rule in self.rules]
        if len(target_paths) != len(set(target_paths)):
            raise ValueError("mapping definition contains duplicate target paths")
        return self


class MappingCatalog(ContractModel):
    contract: Literal["mapping-catalog/v1"]
    mappings: List[MappingDefinition] = Field(min_length=1)

    @model_validator(mode="after")
    def mappings_are_unique(self) -> "MappingCatalog":
        refs = [
            (mapping.artifact_ref.id, mapping.artifact_ref.version)
            for mapping in self.mappings
        ]
        if len(refs) != len(set(refs)):
            raise ValueError("mapping catalog contains duplicate id/version pairs")
        return self


class CdcOperation(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    REPLACE = "replace"
    DELETE = "delete"


class MappingCdcEvent(ContractModel):
    event_id: str = Field(min_length=1)
    operation: CdcOperation
    after: Optional[Dict[str, Any]] = None
    document_key: Dict[str, Any] = Field(default_factory=dict)
    sequence: Optional[str] = None

    @model_validator(mode="after")
    def non_delete_events_have_after_image(self) -> "MappingCdcEvent":
        if self.operation is not CdcOperation.DELETE and self.after is None:
            raise ValueError("non-delete CDC events require an after image")
        return self


class MappingSimulationRequest(ContractModel):
    contract: Literal["mapping-simulation-request/v1"]
    mapping_ref: ArtifactRef
    mode: MappingExecutionMode
    records: List[Dict[str, Any]] = Field(default_factory=list, max_length=100)
    events: List[MappingCdcEvent] = Field(default_factory=list, max_length=100)

    @model_validator(mode="after")
    def payload_matches_mode(self) -> "MappingSimulationRequest":
        if self.mapping_ref.kind != "mapping":
            raise ValueError("mappingRef.kind must be 'mapping'")
        if self.mode is MappingExecutionMode.BATCH:
            if not self.records or self.events:
                raise ValueError("batch simulation requires records and forbids events")
        if self.mode is MappingExecutionMode.CDC:
            if not self.events or self.records:
                raise ValueError("CDC simulation requires events and forbids records")
        return self


class FieldLineage(ContractModel):
    document_index: int = Field(ge=0)
    rule_id: str
    target_path: str
    source_path: Optional[str] = None
    source_kind: Literal["field", "constant"]
    transform_operations: List[MappingTransformOperation] = Field(default_factory=list)
    semantic_concept_id: Optional[str] = None
    terminology_ref: Optional[str] = None


class MappingRecordError(ContractModel):
    record_index: int = Field(ge=0)
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    rule_id: Optional[str] = None
    source_path: Optional[str] = None
    dead_letter_contract: str = Field(min_length=1)


class MappedDocumentRoute(ContractModel):
    document_index: int = Field(ge=0)
    document_id: str = Field(min_length=1)
    subject_key: Optional[str] = None


class MappingSimulationResult(ContractModel):
    contract: Literal["mapping-simulation-result/v1"]
    mapping_ref: ArtifactRef
    state: ExecutionState
    mode: MappingExecutionMode
    documents: List[Dict[str, Any]] = Field(default_factory=list)
    document_routes: List[MappedDocumentRoute] = Field(default_factory=list)
    document_idempotency_keys: List[Sha256Digest] = Field(default_factory=list)
    lineage: List[FieldLineage] = Field(default_factory=list)
    errors: List[MappingRecordError] = Field(default_factory=list)
    output_digest: Sha256Digest
