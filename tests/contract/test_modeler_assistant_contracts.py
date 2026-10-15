from __future__ import annotations

from copy import deepcopy

import jsonschema
import pytest
from pydantic import ValidationError

from kehrnel.contracts import (
    ModelerBlueprintProposal,
    ModelerProposalApprovalRequest,
    ModelerProposalValidationRequest,
    ModelerProposalValidationResult,
    SolutionBlueprint,
    contract_schemas,
    modeler_execution_content_digest,
    modeler_exposure_manifest_digest,
    modeler_invocation_content_digest,
    modeler_proposal_content_digest,
    modeler_review_content_digest,
    modeler_tool_receipt_content_digest,
    modeler_validation_content_digest,
    solution_blueprint_content_digest,
)


DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
DIGEST_C = "sha256:" + "c" * 64
DIGEST_D = "sha256:" + "d" * 64


def _source_ref() -> dict:
    return {
        "kind": "source-profile",
        "id": "exchange.openehr-json",
        "version": "1.0.0",
        "digest": DIGEST_A,
    }


def _blueprint_payload() -> dict:
    return {
        "contract": "solution-blueprint/v1",
        "blueprintId": "hospital-operational-record",
        "industry": "healthcare",
        "revision": 3,
        "objective": "Create a governed operational record with AQL access.",
        "participants": [
            {"participantId": "clinical-app", "label": "Clinical app", "kind": "application", "lifecycle": "existing", "sourceProfileRef": _source_ref()},
            {"participantId": "openehr-cdr", "label": "openEHR CDR", "kind": "repository", "lifecycle": "planned"},
        ],
        "interactions": [{
            "contract": "interaction-contract/v1",
            "artifactRef": {"kind": "interaction-contract", "id": "clinical-app.query-cdr", "version": "1.0.0", "digest": DIGEST_A},
            "interactionId": "query-cdr",
            "behavior": "query",
            "pattern": "request_response",
            "roles": {"requesterRef": "clinical-app", "providerRef": "openehr-cdr"},
            "interface": {"kind": "aql", "protocolVersion": "1.0.3"},
            "semanticScopeId": "clinical-record",
            "workloadRef": {"kind": "workload-profile", "id": "clinical-aql", "version": "1.0.0", "digest": DIGEST_B},
            "guarantees": {"acknowledgement": "accepted", "consistency": "strong", "idempotencyRequired": False},
        }],
        "semanticAuthorities": [{"scopeId": "clinical-record", "mode": "native", "authorityRef": {"kind": "openehr-template", "id": "clinical-record", "version": "1.0.0", "digest": DIGEST_C}}],
        "recordAuthorityPolicies": [],
        "dataPostures": [{"scopeId": "clinical-record", "posture": "mongodb_system_of_record", "servingRepositoryRef": "openehr-cdr"}],
        "constraints": {"tenancyMode": "multi_tenant", "regions": ["eu-west"]},
        "targetCapabilityProfileRef": {"kind": "target-capability-profile", "id": "atlas-production", "version": "1.0.0", "digest": DIGEST_D},
        "preferredFoundationRefs": [{"kind": "strategy", "id": "openehr.rps_dual", "version": "0.1.0", "digest": DIGEST_A}],
        "assumptions": [],
        "decisions": [],
    }


def _scope() -> dict:
    return {
        "binding": "server_bound",
        "tenantId": "tenant:health-system",
        "projectId": "project:operational-record",
        "actorId": "user:architect-1",
        "sessionId": "session:modeler-1",
        "role": "data-architect",
        "purpose": "Design the operational record for governed agent access",
        "authorizationDigest": DIGEST_B,
    }


def _workspace_ref() -> dict:
    return {
        "kind": "agentic-workspace",
        "id": "workspace.data-modeler",
        "version": "1.0.0",
        "digest": DIGEST_B,
    }


def _tool_registry_ref() -> dict:
    return {
        "kind": "agentic-tool-registry",
        "id": "tools.data-modeler",
        "version": "1.0.0",
        "digest": DIGEST_C,
    }


def _tool_receipt_payload() -> dict:
    payload = {
        "receiptId": "inspect-openehr-profile",
        "callId": "call:inspect-openehr-profile",
        "toolRef": {
            "kind": "agentic-tool",
            "id": "inspect-source-profile",
            "version": "1.0.0",
            "digest": DIGEST_D,
        },
        "receiptContract": "source-profile-inspection/v1",
        "inputDigest": DIGEST_C,
        "resultDigest": DIGEST_A,
        "resultArtifactRef": _source_ref(),
    }
    payload["receiptDigest"] = modeler_tool_receipt_content_digest(payload)
    return payload


def _invocation_payload(blueprint: SolutionBlueprint) -> dict:
    blueprint_digest = solution_blueprint_content_digest(blueprint)
    exposures = [
        {
            "exposureId": "base-blueprint",
            "kind": "blueprint",
            "classification": "confidential",
            "purpose": "Propose edits against the current project design.",
            "contentDigest": blueprint_digest,
            "includedPaths": ["/participants", "/interactions", "/semanticAuthorities"],
        },
        {
            "exposureId": "openehr-source-profile",
            "kind": "catalog_artifact",
            "classification": "internal",
            "purpose": "Ground recommendations in the supported source profile.",
            "contentDigest": DIGEST_A,
            "artifactRef": _source_ref(),
            "includedPaths": [],
        },
        {
            "exposureId": "agent-memory-requirement",
            "kind": "user_requirement",
            "classification": "internal",
            "purpose": "Ground the proposal in the architect's stated requirement.",
            "contentDigest": DIGEST_C,
            "includedPaths": [],
        },
    ]
    payload = {
        "contract": "modeler-agentic-invocation/v1",
        "issuedBy": "data_lab_server",
        "invocationId": "invocation:modeler-1",
        "createdAt": "2026-09-27T10:00:00Z",
        "scope": _scope(),
        "baseBlueprint": {
            "blueprintId": blueprint.blueprint_id,
            "revision": blueprint.revision,
            "digest": blueprint_digest,
        },
        "catalogRefs": [_source_ref()],
        "authorizedEvidenceRefs": [],
        "destination": {
            "engine": "mongodb_agentic_engine",
            "workspaceRef": _workspace_ref(),
            "deploymentId": "deployment:data-modeler-eu",
        },
        "exposureManifest": exposures,
        "exposureManifestDigest": modeler_exposure_manifest_digest(exposures),
        "policyDecision": {
            "outcome": "allow",
            "policyRef": {
                "kind": "agentic-policy",
                "id": "policy.data-modeler",
                "version": "1.0.0",
                "digest": DIGEST_B,
            },
            "decisionRef": {
                "kind": "agentic-policy-decision",
                "id": "decision.modeler-1",
                "version": "1.0.0",
                "digest": DIGEST_C,
            },
            "consentStatus": "not_required",
            "restrictions": ["no-direct-mutation"],
        },
        "payloadDigest": DIGEST_D,
        "payloadSizeBytes": 4_096,
        "toolRegistryRef": _tool_registry_ref(),
    }
    payload["invocationDigest"] = modeler_invocation_content_digest(payload)
    return payload


def _execution_receipt_payload(invocation: dict) -> dict:
    payload = {
        "contract": "modeler-agentic-execution-receipt/v1",
        "engine": "mongodb_agentic_engine",
        "invocationId": invocation["invocationId"],
        "invocationDigest": invocation["invocationDigest"],
        "toolRegistryRef": _tool_registry_ref(),
        "workspaceRef": _workspace_ref(),
        "providerId": "openai",
        "modelId": "gpt-5.5",
        "executionId": "execution:modeler-1",
        "sessionId": "session:modeler-1",
        "startedAt": "2026-09-27T10:00:01Z",
        "completedAt": "2026-09-27T10:00:02Z",
        "toolReceipts": [_tool_receipt_payload()],
    }
    payload["executionDigest"] = modeler_execution_content_digest(payload)
    return payload


def _proposal_payload() -> dict:
    blueprint = SolutionBlueprint.model_validate(_blueprint_payload())
    invocation = _invocation_payload(blueprint)
    payload = {
        "contract": "modeler-blueprint-proposal/v1",
        "proposalOrigin": "mongodb_agentic_engine",
        "status": "proposed",
        "proposalId": "proposal.add-decision-traces",
        "scope": _scope(),
        "baseBlueprint": {
            "blueprintId": blueprint.blueprint_id,
            "revision": blueprint.revision,
            "digest": solution_blueprint_content_digest(blueprint),
        },
        "invocation": invocation,
        "executionReceipt": _execution_receipt_payload(invocation),
        "summary": "Add governed decision traces to the operational record.",
        "rationale": "The agent workflow must retain reviewable decisions.",
        "assumptions": [
            {
                "assumptionId": "decision-traces-are-project-local",
                "statement": "Decision traces remain inside this project boundary.",
                "origin": "declared",
                "confidenceBasisPoints": 10_000,
                "evidenceIds": ["requirement.agent-memory"],
            }
        ],
        "evidence": [
            {
                "evidenceId": "requirement.agent-memory",
                "kind": "user_requirement",
                "summary": "The architect requested persisted decision traces.",
                "contentDigest": DIGEST_C,
            },
            {
                "evidenceId": "strategy.openehr-rps-dual",
                "kind": "catalog_artifact",
                "summary": "The current source is supported by the openEHR strategy.",
                "contentDigest": DIGEST_A,
                "artifactRef": _source_ref(),
            },
        ],
        "changes": [
            {
                "changeId": "add-decision-trace-context",
                "operation": "add",
                "path": "/decisions/-",
                "value": "Persist governed agent decision traces.",
                "rationale": "Make agent decisions durable and reviewable.",
                "assumptionIds": ["decision-traces-are-project-local"],
                "evidenceIds": ["requirement.agent-memory"],
                "dependsOnChangeIds": [],
                "confidenceBasisPoints": 9_000,
            }
        ],
    }
    payload["proposalDigest"] = modeler_proposal_content_digest(payload)
    return payload


def _validation_payload(
    proposal: ModelerBlueprintProposal | None = None,
    *,
    selected_change_ids: list[str] | None = None,
) -> dict:
    proposal = proposal or ModelerBlueprintProposal.model_validate(_proposal_payload())
    selected_change_ids = selected_change_ids or [
        change.change_id for change in proposal.changes
    ]
    payload = {
        "contract": "modeler-proposal-validation-result/v1",
        "validatorOrigin": "deterministic_core",
        "status": "valid",
        "scope": proposal.scope.to_wire(),
        "proposalId": proposal.proposal_id,
        "proposalDigest": proposal.proposal_digest,
        "baseBlueprint": proposal.base_blueprint.to_wire(),
        "validatedChangeIds": selected_change_ids,
        "candidateBlueprintDigest": DIGEST_C,
        "issues": [],
    }
    payload["validationDigest"] = modeler_validation_content_digest(payload)
    return payload


def _proposal_with_dependent_changes() -> ModelerBlueprintProposal:
    payload = _proposal_payload()
    payload["changes"].append(
        {
            "changeId": "add-memory-context",
            "operation": "add",
            "path": "/decisions/-",
            "value": "Retain approved episodic memory.",
            "rationale": "Retain approved episodic memory with its decision trace.",
            "assumptionIds": ["decision-traces-are-project-local"],
            "evidenceIds": [],
            "dependsOnChangeIds": ["add-decision-trace-context"],
            "confidenceBasisPoints": 8_000,
        }
    )
    payload["proposalDigest"] = modeler_proposal_content_digest(payload)
    return ModelerBlueprintProposal.model_validate(payload)


def _reviewer() -> dict:
    return {
        "binding": "server_bound",
        "tenantId": "tenant:health-system",
        "projectId": "project:operational-record",
        "reviewerId": "user:reviewer-1",
        "role": "data-architecture-approver",
        "reviewSessionId": "session:review-1",
        "currentAuthorizationDigest": DIGEST_D,
        "reviewedAt": "2026-09-27T10:01:00Z",
    }


def _review_decision(change_id: str, decision: str) -> dict:
    return {
        "changeId": change_id,
        "decision": decision,
        "rationale": f"Reviewer chose to {decision} this change.",
    }


def _review_payload(
    proposal: ModelerBlueprintProposal,
    *,
    validation: ModelerProposalValidationResult | None,
    decisions: list[dict],
    rationale: str = "Reviewed against the current project requirements.",
) -> dict:
    payload = {
        "contract": "modeler-proposal-approval-request/v1",
        "intent": "record_review_decisions",
        "reviewId": "review.modeler-1",
        "proposal": proposal.to_wire(),
        "reviewer": _reviewer(),
        "decisions": decisions,
        "rationale": rationale,
    }
    if validation is not None:
        payload["validation"] = validation.to_wire()
    payload["reviewDigest"] = modeler_review_content_digest(payload)
    return payload


def test_agentic_engine_proposal_is_typed_scoped_and_content_addressed():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())

    assert proposal.proposal_origin == "mongodb_agentic_engine"
    assert proposal.status == "proposed"
    assert proposal.scope.binding == "server_bound"
    assert proposal.scope.role == "data-architect"
    assert proposal.scope.purpose.startswith("Design the operational record")
    assert [reference.to_wire() for reference in proposal.invocation.catalog_refs] == [
        _source_ref()
    ]
    assert proposal.base_blueprint.revision == 3
    assert proposal.execution_receipt.engine == "mongodb_agentic_engine"
    assert proposal.execution_receipt.workspace_ref == (
        proposal.invocation.destination.workspace_ref
    )
    assert proposal.execution_receipt.provider_id == "openai"
    assert proposal.execution_receipt.model_id == "gpt-5.5"
    assert proposal.execution_receipt.tool_receipts[0].result_digest == DIGEST_A
    assert proposal.changes[0].path == "/decisions/-"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("path", "/blueprintId", "String should match pattern"),
        ("operation", "copy", "Input should be"),
        ("value", {"confidence": 0.75}, "canonical JSON subset"),
    ],
)
def test_agent_changes_reuse_the_restricted_builder_patch_contract(
    field: str, value, message: str
):
    payload = _proposal_payload()
    payload["changes"][0][field] = value
    if field != "value":
        payload["proposalDigest"] = modeler_proposal_content_digest(payload)

    with pytest.raises(ValidationError, match=message):
        ModelerBlueprintProposal.model_validate(payload)


def test_agent_proposals_enforce_text_change_value_and_total_size_limits():
    long_summary = _proposal_payload()
    long_summary["summary"] = "x" * 2_001
    long_summary["proposalDigest"] = modeler_proposal_content_digest(long_summary)
    with pytest.raises(ValidationError, match="at most 2000 characters"):
        ModelerBlueprintProposal.model_validate(long_summary)

    oversized_change = _proposal_payload()
    oversized_change["changes"][0]["value"] = "x" * (256 * 1024)
    oversized_change["proposalDigest"] = modeler_proposal_content_digest(
        oversized_change
    )
    with pytest.raises(ValidationError, match="change value exceeds"):
        ModelerBlueprintProposal.model_validate(oversized_change)

    oversized_proposal = _proposal_payload()
    oversized_proposal["changes"] = []
    for index in range(10):
        change = deepcopy(_proposal_payload()["changes"][0])
        change["changeId"] = f"large-change-{index}"
        change["value"] = "x" * 220_000
        oversized_proposal["changes"].append(change)
    oversized_proposal["proposalDigest"] = modeler_proposal_content_digest(
        oversized_proposal
    )
    with pytest.raises(ValidationError, match="proposal exceeds"):
        ModelerBlueprintProposal.model_validate(oversized_proposal)

    oversized_invocation = _proposal_payload()
    oversized_invocation["invocation"]["payloadSizeBytes"] = 4 * 1024 * 1024 + 1
    oversized_invocation["proposalDigest"] = modeler_proposal_content_digest(
        oversized_invocation
    )
    with pytest.raises(ValidationError, match="less than or equal to 4194304"):
        ModelerBlueprintProposal.model_validate(oversized_invocation)


def test_agent_cannot_attach_execution_or_activation_instructions():
    payload = _proposal_payload()
    payload["activate"] = True
    payload["proposalDigest"] = modeler_proposal_content_digest(payload)

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ModelerBlueprintProposal.model_validate(payload)


def test_server_scope_and_agentic_execution_receipt_cannot_drift():
    wrong_session = _proposal_payload()
    wrong_session["executionReceipt"]["sessionId"] = "session:other"
    wrong_session["executionReceipt"]["executionDigest"] = (
        modeler_execution_content_digest(wrong_session["executionReceipt"])
    )
    wrong_session["proposalDigest"] = modeler_proposal_content_digest(wrong_session)
    with pytest.raises(ValidationError, match="sessionId must match"):
        ModelerBlueprintProposal.model_validate(wrong_session)

    unbound_catalog = _proposal_payload()
    unbound_catalog["invocation"]["catalogRefs"] = [
        {
            "kind": "strategy",
            "id": "openehr.rps-dual",
            "version": "1.0.0",
            "digest": DIGEST_B,
        }
    ]
    unbound_catalog["proposalDigest"] = modeler_proposal_content_digest(
        unbound_catalog
    )
    with pytest.raises(ValidationError, match="exact catalogRef"):
        ModelerBlueprintProposal.model_validate(unbound_catalog)

    forged_tool_receipt = _proposal_payload()
    forged_tool_receipt["executionReceipt"]["toolReceipts"][0][
        "resultDigest"
    ] = DIGEST_B
    forged_tool_receipt["proposalDigest"] = modeler_proposal_content_digest(
        forged_tool_receipt
    )
    with pytest.raises(ValidationError, match="resultArtifactRef.digest"):
        ModelerBlueprintProposal.model_validate(forged_tool_receipt)


@pytest.mark.parametrize(
    "field",
    ["binding", "role", "purpose", "authorizationDigest"],
)
def test_proposal_requires_explicit_server_bound_scope_fields(field: str):
    payload = _proposal_payload()
    payload["scope"].pop(field)
    payload["proposalDigest"] = modeler_proposal_content_digest(payload)
    with pytest.raises(ValidationError, match="Field required"):
        ModelerBlueprintProposal.model_validate(payload)


def test_proposal_origin_and_agentic_workspace_identity_are_not_interchangeable():
    wrong_origin = _proposal_payload()
    wrong_origin["proposalOrigin"] = "deterministic_core"
    wrong_origin["proposalDigest"] = modeler_proposal_content_digest(wrong_origin)
    with pytest.raises(ValidationError, match="mongodb_agentic_engine"):
        ModelerBlueprintProposal.model_validate(wrong_origin)

    missing_workspace_version = _proposal_payload()
    missing_workspace_version["executionReceipt"]["workspaceRef"].pop("version")
    missing_workspace_version["proposalDigest"] = modeler_proposal_content_digest(
        missing_workspace_version
    )
    with pytest.raises(ValidationError, match="Field required"):
        ModelerBlueprintProposal.model_validate(missing_workspace_version)


def test_invocation_binds_policy_exposure_payload_destination_and_tool_registry():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    invocation = proposal.invocation

    assert invocation.issued_by == "data_lab_server"
    assert invocation.base_blueprint == proposal.base_blueprint
    assert invocation.destination.workspace_ref == proposal.execution_receipt.workspace_ref
    assert invocation.policy_decision.outcome.value == "allow"
    assert invocation.policy_decision.consent_status.value == "not_required"
    assert invocation.payload_digest == DIGEST_D
    assert invocation.exposure_manifest[0].classification.value == "confidential"
    assert invocation.tool_registry_ref == proposal.execution_receipt.tool_registry_ref
    assert proposal.execution_receipt.invocation_digest == invocation.invocation_digest


def test_invocation_and_execution_receipts_are_content_addressed_and_time_bound():
    tampered_invocation = _proposal_payload()
    tampered_invocation["invocation"]["payloadDigest"] = DIGEST_A
    with pytest.raises(ValidationError, match="invocationDigest"):
        ModelerBlueprintProposal.model_validate(tampered_invocation)

    tampered_execution = _proposal_payload()
    tampered_execution["executionReceipt"]["modelId"] = "other-model"
    with pytest.raises(ValidationError, match="executionDigest"):
        ModelerBlueprintProposal.model_validate(tampered_execution)

    backwards_time = _proposal_payload()
    backwards_time["executionReceipt"]["completedAt"] = "2026-09-27T09:59:00Z"
    backwards_time["executionReceipt"]["executionDigest"] = (
        modeler_execution_content_digest(backwards_time["executionReceipt"])
    )
    backwards_time["proposalDigest"] = modeler_proposal_content_digest(
        backwards_time
    )
    with pytest.raises(ValidationError, match="completedAt cannot be before"):
        ModelerBlueprintProposal.model_validate(backwards_time)

    no_tool_evidence = _proposal_payload()
    no_tool_evidence["executionReceipt"]["toolReceipts"] = []
    no_tool_evidence["proposalDigest"] = modeler_proposal_content_digest(
        no_tool_evidence
    )
    with pytest.raises(ValidationError, match="at least 1 item"):
        ModelerBlueprintProposal.model_validate(no_tool_evidence)

    registry_drift = _proposal_payload()
    registry_drift["executionReceipt"]["toolRegistryRef"]["digest"] = DIGEST_D
    registry_drift["executionReceipt"]["executionDigest"] = (
        modeler_execution_content_digest(registry_drift["executionReceipt"])
    )
    registry_drift["proposalDigest"] = modeler_proposal_content_digest(
        registry_drift
    )
    with pytest.raises(ValidationError, match="toolRegistryRef must match"):
        ModelerBlueprintProposal.model_validate(registry_drift)


def test_denied_policy_or_unclassified_evidence_cannot_produce_a_proposal():
    denied = _proposal_payload()
    denied["invocation"]["policyDecision"]["outcome"] = "deny"
    denied["invocation"]["invocationDigest"] = modeler_invocation_content_digest(
        denied["invocation"]
    )
    denied["executionReceipt"]["invocationDigest"] = denied["invocation"][
        "invocationDigest"
    ]
    denied["executionReceipt"]["executionDigest"] = (
        modeler_execution_content_digest(denied["executionReceipt"])
    )
    denied["proposalDigest"] = modeler_proposal_content_digest(denied)
    with pytest.raises(ValidationError, match="allowed policy decision"):
        ModelerBlueprintProposal.model_validate(denied)

    unclassified = _proposal_payload()
    unclassified["invocation"]["exposureManifest"] = [
        item
        for item in unclassified["invocation"]["exposureManifest"]
        if item["kind"] != "user_requirement"
    ]
    unclassified["invocation"]["exposureManifestDigest"] = (
        modeler_exposure_manifest_digest(
            unclassified["invocation"]["exposureManifest"]
        )
    )
    unclassified["invocation"]["invocationDigest"] = (
        modeler_invocation_content_digest(unclassified["invocation"])
    )
    unclassified["executionReceipt"]["invocationDigest"] = unclassified[
        "invocation"
    ]["invocationDigest"]
    unclassified["executionReceipt"]["executionDigest"] = (
        modeler_execution_content_digest(unclassified["executionReceipt"])
    )
    unclassified["proposalDigest"] = modeler_proposal_content_digest(unclassified)
    with pytest.raises(ValidationError, match="classified exposure manifest"):
        ModelerBlueprintProposal.model_validate(unclassified)


def test_runtime_evidence_is_separate_from_catalog_state_and_exactly_authorized():
    observation_ref = {
        "kind": "source-observation",
        "id": "observation.customer-source-1",
        "version": "1.0.0",
        "digest": DIGEST_D,
    }
    unauthorized = _proposal_payload()
    unauthorized["evidence"].append(
        {
            "evidenceId": "source.customer-observation",
            "kind": "source_observation",
            "summary": "Observed customer source shape.",
            "contentDigest": DIGEST_D,
            "artifactRef": observation_ref,
        }
    )
    unauthorized["changes"][0]["evidenceIds"].append(
        "source.customer-observation"
    )
    unauthorized["proposalDigest"] = modeler_proposal_content_digest(unauthorized)
    with pytest.raises(ValidationError, match="authorizedEvidenceRefs"):
        ModelerBlueprintProposal.model_validate(unauthorized)

    authorized = deepcopy(unauthorized)
    authorized["invocation"]["authorizedEvidenceRefs"] = [observation_ref]
    authorized["invocation"]["exposureManifest"].append(
        {
            "exposureId": "customer-source-observation",
            "kind": "source_evidence",
            "classification": "restricted",
            "purpose": "Ground the mapping proposal in the observed source shape.",
            "contentDigest": DIGEST_D,
            "artifactRef": observation_ref,
            "includedPaths": ["/sampleSchema"],
        }
    )
    authorized["invocation"]["exposureManifestDigest"] = (
        modeler_exposure_manifest_digest(
            authorized["invocation"]["exposureManifest"]
        )
    )
    authorized["invocation"]["invocationDigest"] = (
        modeler_invocation_content_digest(authorized["invocation"])
    )
    authorized["executionReceipt"]["invocationDigest"] = authorized[
        "invocation"
    ]["invocationDigest"]
    authorized["executionReceipt"]["executionDigest"] = (
        modeler_execution_content_digest(authorized["executionReceipt"])
    )
    authorized["proposalDigest"] = modeler_proposal_content_digest(authorized)
    proposal = ModelerBlueprintProposal.model_validate(authorized)
    assert proposal.invocation.authorized_evidence_refs[0].kind == (
        "source-observation"
    )


def test_proposal_rejects_tampering_unknown_evidence_and_dependency_cycles():
    tampered = _proposal_payload()
    tampered["summary"] = "Changed after signing"
    with pytest.raises(ValidationError, match="proposalDigest"):
        ModelerBlueprintProposal.model_validate(tampered)

    unknown = _proposal_payload()
    unknown["changes"][0]["evidenceIds"] = ["missing"]
    unknown["proposalDigest"] = modeler_proposal_content_digest(unknown)
    with pytest.raises(ValidationError, match="unknown references"):
        ModelerBlueprintProposal.model_validate(unknown)

    cyclic = _proposal_payload()
    cyclic["changes"].append(
        {
            "changeId": "add-memory-context",
            "operation": "add",
            "path": "/decisions/-",
            "value": "Retain approved episodic memory.",
            "rationale": "Retain approved episodic memory.",
            "assumptionIds": ["decision-traces-are-project-local"],
            "evidenceIds": [],
            "dependsOnChangeIds": ["add-decision-trace-context"],
            "confidenceBasisPoints": 8_000,
        }
    )
    cyclic["changes"][0]["dependsOnChangeIds"] = ["add-memory-context"]
    cyclic["proposalDigest"] = modeler_proposal_content_digest(cyclic)
    with pytest.raises(ValidationError, match="acyclic"):
        ModelerBlueprintProposal.model_validate(cyclic)


def test_deterministic_validation_request_requires_the_exact_base_blueprint():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    request = ModelerProposalValidationRequest.model_validate(
        {
            "contract": "modeler-proposal-validation-request/v1",
            "proposal": proposal.to_wire(),
            "baseBlueprint": _blueprint_payload(),
            "selectedChangeIds": ["add-decision-trace-context"],
        }
    )
    assert request.base_blueprint.revision == proposal.base_blueprint.revision

    stale = _blueprint_payload()
    stale["constraints"]["regions"] = ["us-east"]
    with pytest.raises(ValidationError, match="does not match digest"):
        ModelerProposalValidationRequest.model_validate(
            {
                "contract": "modeler-proposal-validation-request/v1",
                "proposal": proposal.to_wire(),
                "baseBlueprint": stale,
                "selectedChangeIds": ["add-decision-trace-context"],
            }
        )


def test_full_proposal_approval_remains_the_simple_case():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    validation = ModelerProposalValidationResult.model_validate(
        _validation_payload(proposal)
    )
    approval = ModelerProposalApprovalRequest.model_validate(
        _review_payload(
            proposal,
            validation=validation,
            decisions=[
                _review_decision("add-decision-trace-context", "approve")
            ],
            rationale="The proposed context is required and validated.",
        )
    )
    assert approval.validation.candidate_blueprint_digest == DIGEST_C

    tampered_review = approval.to_wire()
    tampered_review["rationale"] = "Changed after the review was recorded."
    with pytest.raises(ValidationError, match="reviewDigest"):
        ModelerProposalApprovalRequest.model_validate(tampered_review)

    partial = approval.to_wire()
    partial["decisions"][0]["changeId"] = "some-other-change"
    with pytest.raises(ValidationError, match="unknown change IDs"):
        ModelerProposalApprovalRequest.model_validate(partial)


def test_builder_can_validate_and_approve_a_dependency_closed_subset():
    proposal = _proposal_with_dependent_changes()
    selected = ["add-decision-trace-context"]
    request = ModelerProposalValidationRequest.model_validate(
        {
            "contract": "modeler-proposal-validation-request/v1",
            "proposal": proposal.to_wire(),
            "baseBlueprint": _blueprint_payload(),
            "selectedChangeIds": selected,
        }
    )
    assert request.selected_change_ids == selected

    validation = ModelerProposalValidationResult.model_validate(
        _validation_payload(proposal, selected_change_ids=selected)
    )
    approval = ModelerProposalApprovalRequest.model_validate(
        _review_payload(
            proposal,
            validation=validation,
            decisions=[
                _review_decision("add-decision-trace-context", "approve"),
                _review_decision("add-memory-context", "reject"),
            ],
            rationale="Keep decision traces but omit reusable memory for now.",
        )
    )
    assert [decision.decision.value for decision in approval.decisions] == [
        "approve",
        "reject",
    ]


@pytest.mark.parametrize(
    ("selected", "message"),
    [
        (
            ["add-memory-context"],
            "must be dependency-closed",
        ),
        (
            ["add-decision-trace-context", "add-decision-trace-context"],
            "must be unique",
        ),
        (["unknown-change"], "unknown change IDs"),
    ],
)
def test_validation_selection_must_be_known_unique_and_dependency_closed(
    selected: list[str], message: str
):
    proposal = _proposal_with_dependent_changes()
    with pytest.raises(ValidationError, match=message):
        ModelerProposalValidationRequest.model_validate(
            {
                "contract": "modeler-proposal-validation-request/v1",
                "proposal": proposal.to_wire(),
                "baseBlueprint": _blueprint_payload(),
                "selectedChangeIds": selected,
            }
        )


def test_subset_approval_decides_every_change_and_matches_validation_exactly():
    proposal = _proposal_with_dependent_changes()
    selected = ["add-decision-trace-context"]
    validation = ModelerProposalValidationResult.model_validate(
        _validation_payload(proposal, selected_change_ids=selected)
    )
    payload = _review_payload(
        proposal,
        validation=validation,
        decisions=[
            _review_decision("add-decision-trace-context", "approve")
        ],
        rationale="Review decision.",
    )
    with pytest.raises(ValidationError, match="must be approved or rejected"):
        ModelerProposalApprovalRequest.model_validate(payload)

    mismatched = deepcopy(payload)
    mismatched["decisions"] = [
        _review_decision("add-decision-trace-context", "approve"),
        _review_decision("add-memory-context", "approve"),
    ]
    with pytest.raises(ValidationError, match="exactly match validatedChangeIds"):
        ModelerProposalApprovalRequest.model_validate(mismatched)


def test_reviewer_can_reject_every_change_without_an_apply_or_validation_step():
    proposal = _proposal_with_dependent_changes()
    review = ModelerProposalApprovalRequest.model_validate(
        _review_payload(
            proposal,
            validation=None,
            decisions=[
                _review_decision("add-decision-trace-context", "reject"),
                _review_decision("add-memory-context", "reject"),
            ],
            rationale="The proposed memory posture is outside this project.",
        )
    )
    assert review.validation is None
    assert all(decision.decision.value == "reject" for decision in review.decisions)


def test_review_requires_current_server_bound_identity_in_the_same_project():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    validation = ModelerProposalValidationResult.model_validate(
        _validation_payload(proposal)
    )
    payload = _review_payload(
        proposal,
        validation=validation,
        decisions=[
            _review_decision("add-decision-trace-context", "approve")
        ],
        rationale="Review decision.",
    )
    payload["reviewer"]["projectId"] = "project:other"
    with pytest.raises(ValidationError, match="reviewer projectId"):
        ModelerProposalApprovalRequest.model_validate(payload)

    missing_current_auth = deepcopy(payload)
    missing_current_auth["reviewer"] = _reviewer()
    missing_current_auth["reviewer"].pop("currentAuthorizationDigest")
    with pytest.raises(ValidationError, match="Field required"):
        ModelerProposalApprovalRequest.model_validate(missing_current_auth)


def test_validation_receipts_fail_closed_and_are_content_addressed():
    invalid = _validation_payload()
    invalid["status"] = "invalid"
    invalid.pop("candidateBlueprintDigest")
    invalid["issues"] = [
        {
            "code": "PATCH_NOT_APPLICABLE",
            "severity": "blocking",
            "message": "The proposed path does not exist in the pinned Blueprint.",
            "refs": ["add-decision-trace-context"],
        }
    ]
    invalid["validationDigest"] = modeler_validation_content_digest(invalid)
    result = ModelerProposalValidationResult.model_validate(invalid)
    assert result.status.value == "invalid"

    tampered = _validation_payload()
    tampered["candidateBlueprintDigest"] = DIGEST_A
    with pytest.raises(ValidationError, match="validationDigest"):
        ModelerProposalValidationResult.model_validate(tampered)


def test_modeler_contracts_publish_valid_draft_2020_12_schemas():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    validation = ModelerProposalValidationResult.model_validate(
        _validation_payload(proposal)
    )
    validation_request = ModelerProposalValidationRequest.model_validate(
        {
            "contract": "modeler-proposal-validation-request/v1",
            "proposal": proposal.to_wire(),
            "baseBlueprint": _blueprint_payload(),
            "selectedChangeIds": ["add-decision-trace-context"],
        }
    )
    approval = ModelerProposalApprovalRequest.model_validate(
        _review_payload(
            proposal,
            validation=validation,
            decisions=[
                _review_decision("add-decision-trace-context", "approve")
            ],
            rationale="Reviewed and accepted.",
        )
    )
    schemas = contract_schemas()
    examples = {
        "modeler-agentic-invocation.schema.json": proposal.invocation.to_wire(),
        "modeler-agentic-execution-receipt.schema.json": (
            proposal.execution_receipt.to_wire()
        ),
        "modeler-blueprint-proposal.schema.json": proposal.to_wire(),
        "modeler-proposal-validation-request.schema.json": validation_request.to_wire(),
        "modeler-proposal-validation-result.schema.json": validation.to_wire(),
        "modeler-proposal-approval-request.schema.json": approval.to_wire(),
    }
    for name, example in examples.items():
        schema = schemas[name]
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.Draft202012Validator(schema).validate(example)


def test_artifact_evidence_requires_an_exact_content_digest():
    payload = _proposal_payload()
    payload["evidence"][1]["contentDigest"] = DIGEST_B
    payload["proposalDigest"] = modeler_proposal_content_digest(payload)
    with pytest.raises(ValidationError, match="artifactRef.digest"):
        ModelerBlueprintProposal.model_validate(payload)

    missing_ref = deepcopy(payload)
    missing_ref["evidence"][1]["contentDigest"] = DIGEST_A
    missing_ref["evidence"][1].pop("artifactRef")
    missing_ref["proposalDigest"] = modeler_proposal_content_digest(missing_ref)
    with pytest.raises(ValidationError, match="requires artifactRef"):
        ModelerBlueprintProposal.model_validate(missing_ref)
