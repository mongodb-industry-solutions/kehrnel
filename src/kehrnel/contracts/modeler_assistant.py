"""Trust-boundary contracts for the external Data Modeler Assistant.

The assistant runs in MongoDB Agentic Engine, outside deterministic Kehrnel
Core.  It may suggest a restricted patch to one exact Solution Blueprint, but
it cannot mutate, compile, persist, or activate anything through these
contracts.  Validation and approval are separate, content-addressed records.

These models validate structure, internal references, limits, and canonical
digests.  They do not authenticate a server or model provider, classify data,
grant consent, verify membership in a tool registry, invoke Agentic Engine, or
apply a patch.  The hosting service must perform those runtime controls and
must create the invocation, execution, validation, and review records itself.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from enum import Enum
import hashlib
from typing import List, Literal, Optional

from pydantic import Field, JsonValue, field_validator, model_validator

from .artifacts import ArtifactRef, ContractModel, Sha256Digest
from .package_digest import canonical_json_bytes
from .source_architecture import (
    BlueprintOverride,
    BlueprintOverrideOperation,
    CompilationIssue,
    IssueSeverity,
)
from .architecture_foundation import SolutionBlueprint, solution_blueprint_content_digest


_NORMALIZED_ID_PATTERN = r"^[a-z0-9][a-z0-9._-]*$"
_OPAQUE_ID_PATTERN = r"^\S(?:.*\S)?$"
_EDITABLE_BLUEPRINT_POINTER_PATTERN = (
    r"^/(?:objective|participants|interactions|semanticAuthorities|"
    r"recordAuthorityPolicies|dataPostures|constraints|"
    r"targetCapabilityProfileRef|preferredFoundationRefs|assumptions|decisions)"
    r"(?:/(?:[^~/]|~[01])*)*$"
)
_MAX_CHANGE_VALUE_BYTES = 256 * 1024
_MAX_INVOCATION_PAYLOAD_BYTES = 4 * 1024 * 1024
_MAX_PROPOSAL_BYTES = 2 * 1024 * 1024


class ModelerEvidenceKind(str, Enum):
    """The bounded provenance classes an assistant may cite."""

    CATALOG_ARTIFACT = "catalog_artifact"
    SOURCE_OBSERVATION = "source_observation"
    USER_REQUIREMENT = "user_requirement"
    COMPILER_EVIDENCE = "compiler_evidence"
    PRIOR_DECISION = "prior_decision"


class ModelerAssumptionOrigin(str, Enum):
    DECLARED = "declared"
    INFERRED = "inferred"


class ModelerValidationStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    STALE_BASE = "stale_base"


class ModelerExposureKind(str, Enum):
    BLUEPRINT = "blueprint"
    CATALOG_ARTIFACT = "catalog_artifact"
    SOURCE_EVIDENCE = "source_evidence"
    RUNTIME_EVIDENCE = "runtime_evidence"
    USER_REQUIREMENT = "user_requirement"
    CONVERSATION_CONTEXT = "conversation_context"


class ModelerDataClassification(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class ModelerPolicyOutcome(str, Enum):
    ALLOW = "allow"
    DENY = "deny"


class ModelerConsentStatus(str, Enum):
    NOT_REQUIRED = "not_required"
    GRANTED = "granted"
    DENIED = "denied"


class ModelerChangeReviewDecision(str, Enum):
    APPROVE = "approve"
    REJECT = "reject"


class ModelerAssistantScope(ContractModel):
    """Identity and authorization context injected by the hosting server.

    Clients and the language model must not choose these values.  The
    ``authorizationDigest`` lets an integration fail closed if permissions
    change between proposal, validation, and approval.
    """

    binding: Literal["server_bound"]
    tenant_id: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    project_id: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    actor_id: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    session_id: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    role: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    purpose: str = Field(min_length=1, max_length=512, pattern=_OPAQUE_ID_PATTERN)
    authorization_digest: Sha256Digest


class BlueprintRevisionIdentity(ContractModel):
    """One exact immutable Blueprint revision used as proposal input."""

    blueprint_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    revision: int = Field(ge=1)
    digest: Sha256Digest


class ModelerAgenticDestination(ContractModel):
    """Exact Agentic Engine workspace selected by the hosting server."""

    engine: Literal["mongodb_agentic_engine"]
    workspace_ref: ArtifactRef
    deployment_id: str = Field(
        min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN
    )

    @model_validator(mode="after")
    def workspace_kind_is_explicit(self) -> "ModelerAgenticDestination":
        if self.workspace_ref.kind != "agentic-workspace":
            raise ValueError("workspaceRef.kind must be 'agentic-workspace'")
        return self


class ModelerExposureItem(ContractModel):
    """One content-addressed item authorized to cross the agent boundary."""

    exposure_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    kind: ModelerExposureKind
    classification: ModelerDataClassification
    purpose: str = Field(min_length=1, max_length=1_000)
    content_digest: Sha256Digest
    artifact_ref: Optional[ArtifactRef] = None
    included_paths: List[str] = Field(default_factory=list, max_length=256)

    @model_validator(mode="after")
    def exact_artifact_exposure(self) -> "ModelerExposureItem":
        if self.kind in {
            ModelerExposureKind.CATALOG_ARTIFACT,
            ModelerExposureKind.SOURCE_EVIDENCE,
            ModelerExposureKind.RUNTIME_EVIDENCE,
        } and self.artifact_ref is None:
            raise ValueError(f"{self.kind.value} exposure requires artifactRef")
        if (
            self.artifact_ref is not None
            and self.artifact_ref.digest != self.content_digest
        ):
            raise ValueError("exposure artifactRef.digest must match contentDigest")
        if len(self.included_paths) != len(set(self.included_paths)):
            raise ValueError("exposure includedPaths must be unique")
        return self


class ModelerPolicyDecision(ContractModel):
    """Explicit policy and consent decision made before an invocation."""

    outcome: ModelerPolicyOutcome
    policy_ref: ArtifactRef
    decision_ref: ArtifactRef
    consent_status: ModelerConsentStatus
    consent_ref: Optional[ArtifactRef] = None
    restrictions: List[str] = Field(default_factory=list, max_length=128)

    @model_validator(mode="after")
    def policy_and_consent_are_exact(self) -> "ModelerPolicyDecision":
        if self.policy_ref.kind != "agentic-policy":
            raise ValueError("policyRef.kind must be 'agentic-policy'")
        if self.decision_ref.kind != "agentic-policy-decision":
            raise ValueError(
                "decisionRef.kind must be 'agentic-policy-decision'"
            )
        if self.consent_status is ModelerConsentStatus.GRANTED:
            if self.consent_ref is None:
                raise ValueError("granted consent requires consentRef")
        elif self.consent_ref is not None:
            raise ValueError("consentRef is allowed only for granted consent")
        if (
            self.consent_ref is not None
            and self.consent_ref.kind != "agentic-consent-decision"
        ):
            raise ValueError(
                "consentRef.kind must be 'agentic-consent-decision'"
            )
        if (
            self.outcome is ModelerPolicyOutcome.ALLOW
            and self.consent_status is ModelerConsentStatus.DENIED
        ):
            raise ValueError("an allow decision cannot contain denied consent")
        if len(self.restrictions) != len(set(self.restrictions)):
            raise ValueError("policy restrictions must be unique")
        return self


class ModelerAgenticInvocation(ContractModel):
    """Server-created, content-addressed authorization to call Agentic Engine.

    The contract records an allowlist and policy decision but cannot prove who
    created them.  The host must authenticate this record and enforce tool
    membership and data exposure before dispatch.
    """

    contract: Literal["modeler-agentic-invocation/v1"]
    issued_by: Literal["data_lab_server"]
    invocation_id: str = Field(
        min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN
    )
    invocation_digest: Sha256Digest
    created_at: datetime
    scope: ModelerAssistantScope
    base_blueprint: BlueprintRevisionIdentity
    catalog_refs: List[ArtifactRef] = Field(min_length=1, max_length=256)
    authorized_evidence_refs: List[ArtifactRef] = Field(
        default_factory=list, max_length=512
    )
    destination: ModelerAgenticDestination
    exposure_manifest: List[ModelerExposureItem] = Field(
        min_length=1, max_length=512
    )
    exposure_manifest_digest: Sha256Digest
    policy_decision: ModelerPolicyDecision
    payload_digest: Sha256Digest = Field(
        description="Digest of the exact redacted payload dispatched to Agentic Engine."
    )
    payload_size_bytes: int = Field(ge=1, le=_MAX_INVOCATION_PAYLOAD_BYTES)
    tool_registry_ref: ArtifactRef

    @field_validator("created_at")
    @classmethod
    def created_at_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("createdAt must include a timezone")
        return value

    @model_validator(mode="after")
    def invocation_is_exact_and_content_addressed(
        self,
    ) -> "ModelerAgenticInvocation":
        if self.tool_registry_ref.kind != "agentic-tool-registry":
            raise ValueError(
                "toolRegistryRef.kind must be 'agentic-tool-registry'"
            )
        _assert_unique_artifact_refs(self.catalog_refs, label="catalogRefs")
        _assert_unique_artifact_refs(
            self.authorized_evidence_refs,
            label="authorizedEvidenceRefs",
        )
        catalog_keys = {_artifact_ref_key(ref) for ref in self.catalog_refs}
        evidence_keys = {
            _artifact_ref_key(ref) for ref in self.authorized_evidence_refs
        }
        overlap = sorted(catalog_keys & evidence_keys)
        if overlap:
            raise ValueError(
                "catalogRefs and authorizedEvidenceRefs must be separate: "
                f"{overlap}"
            )

        exposure_ids = [item.exposure_id for item in self.exposure_manifest]
        if len(exposure_ids) != len(set(exposure_ids)):
            raise ValueError("exposureId values must be unique")
        _assert_exposures_are_authorized(
            self.exposure_manifest,
            base_blueprint=self.base_blueprint,
            catalog_refs=catalog_keys,
            evidence_refs=evidence_keys,
        )
        if self.exposure_manifest_digest != modeler_exposure_manifest_digest(
            self.exposure_manifest
        ):
            raise ValueError(
                "exposureManifestDigest does not match exposureManifest"
            )
        if self.invocation_digest != modeler_invocation_content_digest(self):
            raise ValueError("invocationDigest does not match invocation content")
        return self


class ModelerProposalEvidence(ContractModel):
    """Content-addressed evidence; raw source data is intentionally excluded."""

    evidence_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    kind: ModelerEvidenceKind
    summary: str = Field(min_length=1, max_length=2_000)
    content_digest: Sha256Digest
    artifact_ref: Optional[ArtifactRef] = None

    @model_validator(mode="after")
    def artifact_evidence_is_exact(self) -> "ModelerProposalEvidence":
        if self.kind is ModelerEvidenceKind.USER_REQUIREMENT:
            if self.artifact_ref is not None:
                raise ValueError("user_requirement evidence cannot use artifactRef")
        elif self.artifact_ref is None:
            raise ValueError(f"{self.kind.value} evidence requires artifactRef")
        if (
            self.artifact_ref is not None
            and self.artifact_ref.digest != self.content_digest
        ):
            raise ValueError("artifactRef.digest must match contentDigest")
        return self


class ModelerProposalAssumption(ContractModel):
    """A visible premise used by the assistant rather than an asserted fact."""

    assumption_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    statement: str = Field(min_length=1, max_length=4_000)
    origin: ModelerAssumptionOrigin
    confidence_basis_points: int = Field(ge=0, le=10_000)
    evidence_ids: List[str] = Field(default_factory=list, max_length=128)

    @field_validator("evidence_ids")
    @classmethod
    def evidence_ids_are_unique(cls, values: List[str]) -> List[str]:
        if len(values) != len(set(values)):
            raise ValueError("assumption evidenceIds must be unique")
        return values


class ModelerToolReceipt(ContractModel):
    """Content-addressed receipt for one bounded Agentic Engine tool call."""

    receipt_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    receipt_digest: Sha256Digest
    call_id: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    tool_ref: ArtifactRef
    receipt_contract: str = Field(
        min_length=1,
        max_length=256,
        pattern=_OPAQUE_ID_PATTERN,
    )
    input_digest: Sha256Digest
    result_digest: Sha256Digest
    result_artifact_ref: Optional[ArtifactRef] = None

    @model_validator(mode="after")
    def receipt_is_exact(self) -> "ModelerToolReceipt":
        if self.tool_ref.kind != "agentic-tool":
            raise ValueError("toolRef.kind must be 'agentic-tool'")
        if (
            self.result_artifact_ref is not None
            and self.result_artifact_ref.digest != self.result_digest
        ):
            raise ValueError(
                "tool resultArtifactRef.digest must match resultDigest"
            )
        if self.receipt_digest != modeler_tool_receipt_content_digest(self):
            raise ValueError("tool receiptDigest does not match receipt content")
        return self


class ModelerAgenticExecutionReceipt(ContractModel):
    """Server-captured Agentic Engine execution identity and tool evidence.

    This is a structural receipt, not a cryptographic attestation from MongoDB.
    The host must obtain it from the configured Agentic Engine integration and
    verify every ``toolRef`` is a member of the pinned registry.
    """

    contract: Literal["modeler-agentic-execution-receipt/v1"]
    engine: Literal["mongodb_agentic_engine"]
    execution_digest: Sha256Digest
    invocation_id: str = Field(
        min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN
    )
    invocation_digest: Sha256Digest
    tool_registry_ref: ArtifactRef
    workspace_ref: ArtifactRef
    provider_id: str = Field(
        min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN
    )
    model_id: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    execution_id: str = Field(
        min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN
    )
    session_id: str = Field(
        min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN
    )
    started_at: datetime
    completed_at: datetime
    tool_receipts: List[ModelerToolReceipt] = Field(min_length=1, max_length=256)

    @field_validator("started_at", "completed_at")
    @classmethod
    def timestamps_are_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("execution timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def execution_receipt_is_exact(self) -> "ModelerAgenticExecutionReceipt":
        if self.workspace_ref.kind != "agentic-workspace":
            raise ValueError("workspaceRef.kind must be 'agentic-workspace'")
        if self.tool_registry_ref.kind != "agentic-tool-registry":
            raise ValueError(
                "toolRegistryRef.kind must be 'agentic-tool-registry'"
            )
        if self.completed_at < self.started_at:
            raise ValueError("completedAt cannot be before startedAt")
        receipt_ids = [receipt.receipt_id for receipt in self.tool_receipts]
        if len(receipt_ids) != len(set(receipt_ids)):
            raise ValueError("tool receiptId values must be unique")
        call_ids = [receipt.call_id for receipt in self.tool_receipts]
        if len(call_ids) != len(set(call_ids)):
            raise ValueError("tool callId values must be unique")
        if self.execution_digest != modeler_execution_content_digest(self):
            raise ValueError("executionDigest does not match execution content")
        return self


class ModelerBlueprintChange(ContractModel):
    """One explainable, non-executing Blueprint patch suggestion.

    Validation is delegated to :class:`BlueprintOverride`, preserving the same
    restricted ``add``/``remove``/``replace`` operations, editable roots, JSON
    Pointer rules, and canonical JSON value subset used by the Builder.
    """

    change_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    operation: BlueprintOverrideOperation
    path: str = Field(min_length=1, pattern=_EDITABLE_BLUEPRINT_POINTER_PATTERN)
    value: Optional[JsonValue] = None
    rationale: str = Field(min_length=1, max_length=4_000)
    assumption_ids: List[str] = Field(default_factory=list, max_length=128)
    evidence_ids: List[str] = Field(default_factory=list, max_length=128)
    depends_on_change_ids: List[str] = Field(default_factory=list, max_length=128)
    confidence_basis_points: int = Field(ge=0, le=10_000)

    @model_validator(mode="after")
    def change_is_safe_and_explainable(self) -> "ModelerBlueprintChange":
        BlueprintOverride(
            overrideId=self.change_id,
            operation=self.operation,
            path=self.path,
            value=self.value,
            reason=self.rationale,
            assumptionIds=self.assumption_ids,
        )
        for field_name, values in (
            ("assumptionIds", self.assumption_ids),
            ("evidenceIds", self.evidence_ids),
            ("dependsOnChangeIds", self.depends_on_change_ids),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"change {field_name} must be unique")
        if self.change_id in self.depends_on_change_ids:
            raise ValueError("a change cannot depend on itself")
        if not self.assumption_ids and not self.evidence_ids:
            raise ValueError("a change requires evidenceIds or assumptionIds")
        if self.value is not None:
            value_size = len(canonical_json_bytes({"value": self.value}))
            if value_size > _MAX_CHANGE_VALUE_BYTES:
                raise ValueError(
                    f"change value exceeds {_MAX_CHANGE_VALUE_BYTES} bytes"
                )
        return self


class ModelerBlueprintProposal(ContractModel):
    """Immutable Agentic Engine suggestion awaiting deterministic validation."""

    contract: Literal["modeler-blueprint-proposal/v1"]
    proposal_origin: Literal["mongodb_agentic_engine"]
    status: Literal["proposed"]
    proposal_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    proposal_digest: Sha256Digest
    scope: ModelerAssistantScope
    base_blueprint: BlueprintRevisionIdentity
    invocation: ModelerAgenticInvocation
    execution_receipt: ModelerAgenticExecutionReceipt
    summary: str = Field(min_length=1, max_length=2_000)
    rationale: str = Field(min_length=1, max_length=8_000)
    assumptions: List[ModelerProposalAssumption] = Field(
        default_factory=list, max_length=256
    )
    evidence: List[ModelerProposalEvidence] = Field(
        default_factory=list, max_length=512
    )
    changes: List[ModelerBlueprintChange] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def proposal_is_closed_and_content_addressed(self) -> "ModelerBlueprintProposal":
        assumption_ids = [item.assumption_id for item in self.assumptions]
        evidence_ids = [item.evidence_id for item in self.evidence]
        change_ids = [item.change_id for item in self.changes]
        for label, values in (
            ("assumptionId", assumption_ids),
            ("evidenceId", evidence_ids),
            ("changeId", change_ids),
        ):
            if len(values) != len(set(values)):
                raise ValueError(f"proposal {label} values must be unique")

        known_assumptions = set(assumption_ids)
        known_evidence = set(evidence_ids)
        known_changes = set(change_ids)
        unknown_assumption_evidence = sorted(
            {
                evidence_id
                for assumption in self.assumptions
                for evidence_id in assumption.evidence_ids
            }
            - known_evidence
        )
        unknown_change_assumptions = sorted(
            {
                assumption_id
                for change in self.changes
                for assumption_id in change.assumption_ids
            }
            - known_assumptions
        )
        unknown_change_evidence = sorted(
            {
                evidence_id
                for change in self.changes
                for evidence_id in change.evidence_ids
            }
            - known_evidence
        )
        unknown_dependencies = sorted(
            {
                dependency_id
                for change in self.changes
                for dependency_id in change.depends_on_change_ids
            }
            - known_changes
        )
        unknown = {
            "assumption evidence": unknown_assumption_evidence,
            "change assumptions": unknown_change_assumptions,
            "change evidence": unknown_change_evidence,
            "change dependencies": unknown_dependencies,
        }
        populated = {label: values for label, values in unknown.items() if values}
        if populated:
            raise ValueError(f"proposal contains unknown references: {populated}")

        if self.invocation.scope != self.scope:
            raise ValueError("invocation scope must match proposal scope")
        if self.invocation.base_blueprint != self.base_blueprint:
            raise ValueError(
                "invocation baseBlueprint must match proposal baseBlueprint"
            )
        if self.invocation.policy_decision.outcome is not ModelerPolicyOutcome.ALLOW:
            raise ValueError("a proposal requires an allowed policy decision")
        if self.invocation.policy_decision.consent_status is ModelerConsentStatus.DENIED:
            raise ValueError("a proposal cannot follow denied consent")
        if self.execution_receipt.session_id != self.scope.session_id:
            raise ValueError("execution receipt sessionId must match scope sessionId")
        if self.execution_receipt.invocation_id != self.invocation.invocation_id:
            raise ValueError(
                "execution receipt invocationId must match invocation"
            )
        if (
            self.execution_receipt.invocation_digest
            != self.invocation.invocation_digest
        ):
            raise ValueError(
                "execution receipt invocationDigest must match invocation"
            )
        if (
            self.execution_receipt.tool_registry_ref
            != self.invocation.tool_registry_ref
        ):
            raise ValueError(
                "execution receipt toolRegistryRef must match invocation"
            )
        if (
            self.execution_receipt.workspace_ref
            != self.invocation.destination.workspace_ref
        ):
            raise ValueError("execution receipt workspaceRef must match destination")
        if self.execution_receipt.started_at < self.invocation.created_at:
            raise ValueError("execution cannot start before invocation creation")
        bound_catalog_refs = {
            (reference.kind, reference.id, reference.version, reference.digest)
            for reference in self.invocation.catalog_refs
        }
        authorized_evidence_refs = {
            (reference.kind, reference.id, reference.version, reference.digest)
            for reference in self.invocation.authorized_evidence_refs
        }
        cited_catalog_refs = {
            (
                evidence.artifact_ref.kind,
                evidence.artifact_ref.id,
                evidence.artifact_ref.version,
                evidence.artifact_ref.digest,
            )
            for evidence in self.evidence
            if evidence.kind is ModelerEvidenceKind.CATALOG_ARTIFACT
            and evidence.artifact_ref is not None
        }
        unbound_catalog_refs = sorted(cited_catalog_refs - bound_catalog_refs)
        if unbound_catalog_refs:
            raise ValueError(
                "catalog evidence must be present in "
                "server-bound catalogRefs: "
                f"{unbound_catalog_refs}"
            )
        cited_runtime_refs = {
            (
                evidence.artifact_ref.kind,
                evidence.artifact_ref.id,
                evidence.artifact_ref.version,
                evidence.artifact_ref.digest,
            )
            for evidence in self.evidence
            if evidence.kind is not ModelerEvidenceKind.CATALOG_ARTIFACT
            and evidence.kind is not ModelerEvidenceKind.USER_REQUIREMENT
            and evidence.artifact_ref is not None
        }
        unauthorized_runtime_refs = sorted(
            cited_runtime_refs - authorized_evidence_refs
        )
        if unauthorized_runtime_refs:
            raise ValueError(
                "runtime/source evidence must be present in server-bound "
                f"authorizedEvidenceRefs: {unauthorized_runtime_refs}"
            )
        authorized_content_digests = {
            exposure.content_digest
            for exposure in self.invocation.exposure_manifest
        } | {
            receipt.result_digest
            for receipt in self.execution_receipt.tool_receipts
        }
        unexposed_evidence = sorted(
            evidence.evidence_id
            for evidence in self.evidence
            if evidence.content_digest not in authorized_content_digests
        )
        if unexposed_evidence:
            raise ValueError(
                "proposal evidence must come from the classified exposure "
                "manifest or an exact tool receipt: "
                f"{unexposed_evidence}"
            )

        _assert_acyclic_change_dependencies(self.changes)
        expected = modeler_proposal_content_digest(self)
        if self.proposal_digest != expected:
            raise ValueError("proposalDigest does not match the proposal content")
        if len(canonical_json_bytes(self.to_wire())) > _MAX_PROPOSAL_BYTES:
            raise ValueError(f"proposal exceeds {_MAX_PROPOSAL_BYTES} bytes")
        return self


class ModelerProposalValidationRequest(ContractModel):
    """Input to deterministic Core validation; it performs no mutation.

    ``selectedChangeIds`` may contain the complete proposal or a non-empty,
    dependency-closed subset selected in the Builder review experience.
    """

    contract: Literal["modeler-proposal-validation-request/v1"]
    proposal: ModelerBlueprintProposal
    base_blueprint: SolutionBlueprint
    selected_change_ids: List[str] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def base_blueprint_is_the_exact_proposal_input(
        self,
    ) -> "ModelerProposalValidationRequest":
        identity = self.proposal.base_blueprint
        if self.base_blueprint.blueprint_id != identity.blueprint_id:
            raise ValueError("base Blueprint does not match blueprintId")
        if self.base_blueprint.revision != identity.revision:
            raise ValueError("base Blueprint does not match revision")
        if solution_blueprint_content_digest(self.base_blueprint) != identity.digest:
            raise ValueError("base Blueprint does not match digest")
        _assert_dependency_closed_selection(
            self.proposal,
            self.selected_change_ids,
            label="selectedChangeIds",
        )
        return self


class ModelerProposalValidationResult(ContractModel):
    """Content-addressed receipt produced only by deterministic Core."""

    contract: Literal["modeler-proposal-validation-result/v1"]
    validator_origin: Literal["deterministic_core"]
    status: ModelerValidationStatus
    validation_digest: Sha256Digest
    scope: ModelerAssistantScope
    proposal_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    proposal_digest: Sha256Digest
    base_blueprint: BlueprintRevisionIdentity
    validated_change_ids: List[str] = Field(default_factory=list, max_length=256)
    candidate_blueprint_digest: Optional[Sha256Digest] = None
    issues: List[CompilationIssue] = Field(default_factory=list, max_length=256)

    @model_validator(mode="after")
    def validation_result_is_coherent(
        self,
    ) -> "ModelerProposalValidationResult":
        if len(self.validated_change_ids) != len(set(self.validated_change_ids)):
            raise ValueError("validatedChangeIds must be unique")
        blocking = any(
            issue.severity is IssueSeverity.BLOCKING for issue in self.issues
        )
        if self.status is ModelerValidationStatus.VALID:
            if not self.validated_change_ids:
                raise ValueError("valid results require validatedChangeIds")
            if self.candidate_blueprint_digest is None:
                raise ValueError("valid results require candidateBlueprintDigest")
            if blocking:
                raise ValueError("valid results cannot contain blocking issues")
        else:
            if self.candidate_blueprint_digest is not None:
                raise ValueError(
                    "invalid or stale results cannot contain candidateBlueprintDigest"
                )
            if not blocking:
                raise ValueError(
                    "invalid or stale results require at least one blocking issue"
                )
        expected = modeler_validation_content_digest(self)
        if self.validation_digest != expected:
            raise ValueError("validationDigest does not match the result content")
        return self


def modeler_exposure_manifest_digest(
    exposure_manifest: List[ModelerExposureItem] | List[dict],
) -> str:
    """Digest the classified list of data authorized for agent exposure."""

    payload = [
        item.to_wire() if isinstance(item, ModelerExposureItem) else deepcopy(item)
        for item in exposure_manifest
    ]
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def modeler_invocation_content_digest(
    invocation: ModelerAgenticInvocation | dict,
) -> str:
    """Digest an invocation while excluding its self-referential digest."""

    if isinstance(invocation, ModelerAgenticInvocation):
        payload = invocation.to_wire()
    else:
        payload = deepcopy(invocation)
    payload.pop("invocationDigest", None)
    payload.pop("invocation_digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def modeler_tool_receipt_content_digest(
    receipt: ModelerToolReceipt | dict,
) -> str:
    """Digest one tool receipt without its self-referential digest."""

    if isinstance(receipt, ModelerToolReceipt):
        payload = receipt.to_wire()
    else:
        payload = deepcopy(receipt)
    payload.pop("receiptDigest", None)
    payload.pop("receipt_digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def modeler_execution_content_digest(
    receipt: ModelerAgenticExecutionReceipt | dict,
) -> str:
    """Digest an execution receipt without its self-referential digest."""

    if isinstance(receipt, ModelerAgenticExecutionReceipt):
        payload = receipt.to_wire()
    else:
        payload = deepcopy(receipt)
    payload.pop("executionDigest", None)
    payload.pop("execution_digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class ModelerReviewerIdentity(ContractModel):
    """Reviewer identity and current authorization captured by the server."""

    binding: Literal["server_bound"]
    tenant_id: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    project_id: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    reviewer_id: str = Field(
        min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN
    )
    role: str = Field(min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN)
    review_session_id: str = Field(
        min_length=1, max_length=256, pattern=_OPAQUE_ID_PATTERN
    )
    current_authorization_digest: Sha256Digest
    reviewed_at: datetime

    @field_validator("reviewed_at")
    @classmethod
    def reviewed_at_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("reviewedAt must include a timezone")
        return value


class ModelerPerChangeReview(ContractModel):
    """One explicit human decision for one immutable proposed change."""

    change_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    decision: ModelerChangeReviewDecision
    rationale: str = Field(min_length=1, max_length=4_000)


class ModelerProposalApprovalRequest(ContractModel):
    """Human review decisions for an immutable proposal.

    This record still does not apply the patch, compile a strategy, persist a
    Blueprint, or activate a runtime.  A later authorized workflow must consume
    it and re-check all identities and digests.  Every proposed change is
    explicitly approved or rejected; partial acceptance is represented by a
    dependency-closed approved set and its exact rejected complement.  Rejecting
    every change does not require deterministic validation; approving any
    change requires a valid receipt for exactly that selected set.
    """

    contract: Literal["modeler-proposal-approval-request/v1"]
    intent: Literal["record_review_decisions"]
    review_id: str = Field(min_length=1, pattern=_NORMALIZED_ID_PATTERN)
    review_digest: Sha256Digest
    proposal: ModelerBlueprintProposal
    validation: Optional[ModelerProposalValidationResult] = None
    reviewer: ModelerReviewerIdentity
    decisions: List[ModelerPerChangeReview] = Field(min_length=1, max_length=256)
    rationale: str = Field(min_length=1, max_length=8_000)

    @model_validator(mode="after")
    def approval_is_exact_and_complete(self) -> "ModelerProposalApprovalRequest":
        if self.reviewer.tenant_id != self.proposal.scope.tenant_id:
            raise ValueError("reviewer tenantId does not match proposal scope")
        if self.reviewer.project_id != self.proposal.scope.project_id:
            raise ValueError("reviewer projectId does not match proposal scope")
        if self.reviewer.reviewed_at < self.proposal.execution_receipt.completed_at:
            raise ValueError("reviewedAt cannot be before execution completedAt")

        proposal_change_ids = {change.change_id for change in self.proposal.changes}
        decision_change_ids = [decision.change_id for decision in self.decisions]
        if len(decision_change_ids) != len(set(decision_change_ids)):
            raise ValueError("review decisions must contain unique changeIds")
        unknown = sorted(set(decision_change_ids) - proposal_change_ids)
        if unknown:
            raise ValueError(f"review references unknown change IDs: {unknown}")
        undecided = sorted(proposal_change_ids - set(decision_change_ids))
        if undecided:
            raise ValueError(
                f"every proposal change must be approved or rejected: {undecided}"
            )

        approved_change_ids = [
            decision.change_id
            for decision in self.decisions
            if decision.decision is ModelerChangeReviewDecision.APPROVE
        ]
        if not approved_change_ids:
            if self.validation is not None:
                _assert_validation_matches_proposal(self.validation, self.proposal)
            self._assert_review_digest()
            return self

        if self.validation is None:
            raise ValueError("approving changes requires a validation receipt")
        _assert_validation_matches_proposal(self.validation, self.proposal)
        if self.validation.status is not ModelerValidationStatus.VALID:
            raise ValueError("only deterministically valid changes can be approved")
        _assert_dependency_closed_selection(
            self.proposal, approved_change_ids, label="approved change decisions"
        )
        if set(self.validation.validated_change_ids) != set(approved_change_ids):
            raise ValueError(
                "approved change decisions must exactly match validatedChangeIds"
            )
        self._assert_review_digest()
        return self

    def _assert_review_digest(self) -> None:
        if self.review_digest != modeler_review_content_digest(self):
            raise ValueError("reviewDigest does not match review content")


def modeler_proposal_content_digest(
    proposal: ModelerBlueprintProposal | dict,
) -> str:
    """Digest a proposal while excluding its self-referential digest field."""

    if isinstance(proposal, ModelerBlueprintProposal):
        payload = proposal.to_wire()
    else:
        payload = deepcopy(proposal)
    payload.pop("proposalDigest", None)
    payload.pop("proposal_digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def modeler_validation_content_digest(
    result: ModelerProposalValidationResult | dict,
) -> str:
    """Digest a validation receipt without its self-referential digest."""

    if isinstance(result, ModelerProposalValidationResult):
        payload = result.to_wire()
    else:
        payload = deepcopy(result)
    payload.pop("validationDigest", None)
    payload.pop("validation_digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def modeler_review_content_digest(
    review: ModelerProposalApprovalRequest | dict,
) -> str:
    """Digest a review record without its self-referential digest."""

    if isinstance(review, ModelerProposalApprovalRequest):
        payload = review.to_wire()
    else:
        payload = deepcopy(review)
    payload.pop("reviewDigest", None)
    payload.pop("review_digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _assert_acyclic_change_dependencies(
    changes: List[ModelerBlueprintChange],
) -> None:
    dependencies = {
        change.change_id: tuple(change.depends_on_change_ids) for change in changes
    }
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(change_id: str) -> None:
        if change_id in visiting:
            raise ValueError("proposal change dependencies must be acyclic")
        if change_id in visited:
            return
        visiting.add(change_id)
        for dependency_id in dependencies[change_id]:
            visit(dependency_id)
        visiting.remove(change_id)
        visited.add(change_id)

    for change_id in dependencies:
        visit(change_id)


def _assert_dependency_closed_selection(
    proposal: ModelerBlueprintProposal,
    selected_change_ids: List[str],
    *,
    label: str,
) -> None:
    if not selected_change_ids:
        raise ValueError(f"{label} must be non-empty")
    selected = set(selected_change_ids)
    if len(selected_change_ids) != len(selected):
        raise ValueError(f"{label} must be unique")
    changes = {change.change_id: change for change in proposal.changes}
    unknown = sorted(selected - set(changes))
    if unknown:
        raise ValueError(f"{label} references unknown change IDs: {unknown}")
    missing_dependencies = sorted(
        {
            dependency_id
            for change_id in selected
            for dependency_id in changes[change_id].depends_on_change_ids
            if dependency_id not in selected
        }
    )
    if missing_dependencies:
        raise ValueError(
            f"{label} must be dependency-closed; missing: {missing_dependencies}"
        )


def _artifact_ref_key(reference: ArtifactRef) -> tuple[str, str, str, str]:
    return (
        reference.kind,
        reference.id,
        reference.version,
        reference.digest,
    )


def _assert_unique_artifact_refs(
    references: List[ArtifactRef],
    *,
    label: str,
) -> None:
    identities = [_artifact_ref_key(reference) for reference in references]
    if len(identities) != len(set(identities)):
        raise ValueError(f"{label} must contain unique exact artifact references")


def _assert_exposures_are_authorized(
    exposures: List[ModelerExposureItem],
    *,
    base_blueprint: BlueprintRevisionIdentity,
    catalog_refs: set[tuple[str, str, str, str]],
    evidence_refs: set[tuple[str, str, str, str]],
) -> None:
    blueprint_exposures = [
        item for item in exposures if item.kind is ModelerExposureKind.BLUEPRINT
    ]
    if len(blueprint_exposures) != 1:
        raise ValueError(
            "exposureManifest must contain exactly one Blueprint exposure"
        )
    blueprint_exposure = blueprint_exposures[0]
    if blueprint_exposure.artifact_ref is not None:
        raise ValueError("Blueprint exposure cannot use artifactRef")
    if blueprint_exposure.content_digest != base_blueprint.digest:
        raise ValueError("Blueprint exposure must match baseBlueprint.digest")

    for exposure in exposures:
        if exposure.kind is ModelerExposureKind.CATALOG_ARTIFACT:
            reference = exposure.artifact_ref
            if reference is None or _artifact_ref_key(reference) not in catalog_refs:
                raise ValueError(
                    "catalog exposure must reference an exact catalogRef"
                )
        elif exposure.kind in {
            ModelerExposureKind.SOURCE_EVIDENCE,
            ModelerExposureKind.RUNTIME_EVIDENCE,
        }:
            reference = exposure.artifact_ref
            if reference is None or _artifact_ref_key(reference) not in evidence_refs:
                raise ValueError(
                    "source/runtime exposure must reference an exact "
                    "authorizedEvidenceRef"
                )
        elif exposure.kind in {
            ModelerExposureKind.USER_REQUIREMENT,
            ModelerExposureKind.CONVERSATION_CONTEXT,
        } and exposure.artifact_ref is not None:
            raise ValueError(
                f"{exposure.kind.value} exposure cannot use artifactRef"
            )


def _assert_validation_matches_proposal(
    validation: ModelerProposalValidationResult,
    proposal: ModelerBlueprintProposal,
) -> None:
    if validation.scope != proposal.scope:
        raise ValueError("validation scope does not match the proposal")
    if validation.proposal_id != proposal.proposal_id:
        raise ValueError("validation does not match proposalId")
    if validation.proposal_digest != proposal.proposal_digest:
        raise ValueError("validation does not match proposalDigest")
    if validation.base_blueprint != proposal.base_blueprint:
        raise ValueError("validation does not match the base Blueprint")
