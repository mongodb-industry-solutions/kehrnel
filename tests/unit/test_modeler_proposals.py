from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from kehrnel.contracts import (
    BlueprintOverride,
    BlueprintRevisionIdentity,
    ModelerBlueprintProposal,
    ModelerProposalValidationRequest,
    modeler_proposal_content_digest,
    solution_blueprint_content_digest,
)
from kehrnel.engine.core.blueprint_patching import apply_blueprint_override
from kehrnel.engine.core.modeler_proposals import validate_modeler_proposal
from tests.contract.test_modeler_assistant_contracts import (
    DIGEST_A,
    _blueprint_payload,
    _proposal_payload,
    _proposal_with_dependent_changes,
)


def _request(
    proposal: ModelerBlueprintProposal,
    selected_change_ids: list[str],
) -> ModelerProposalValidationRequest:
    return ModelerProposalValidationRequest.model_validate(
        {
            "contract": "modeler-proposal-validation-request/v1",
            "proposal": proposal.to_wire(),
            "baseBlueprint": _blueprint_payload(),
            "selectedChangeIds": selected_change_ids,
        }
    )


def _proposal_with_changes(changes: list[dict]) -> ModelerBlueprintProposal:
    payload = _proposal_payload()
    payload["changes"] = changes
    payload["proposalDigest"] = modeler_proposal_content_digest(payload)
    return ModelerBlueprintProposal.model_validate(payload)


def test_validates_into_exactly_one_new_revision_without_mutating_base():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    request = _request(proposal, ["add-decision-trace-context"])
    base_before = request.base_blueprint.to_wire()

    first = validate_modeler_proposal(
        request,
        current_blueprint_identity=proposal.base_blueprint,
    )
    second = validate_modeler_proposal(
        request,
        current_blueprint_identity=proposal.base_blueprint,
    )

    assert first.validation.status.value == "valid"
    assert first.candidate_blueprint is not None
    assert first.candidate_blueprint.revision == request.base_blueprint.revision + 1
    assert first.candidate_blueprint.decisions == ["Persist governed agent decision traces."]
    assert request.base_blueprint.to_wire() == base_before
    assert request.base_blueprint.decisions == []
    assert first.validation.candidate_blueprint_digest == (
        solution_blueprint_content_digest(first.candidate_blueprint)
    )
    assert first.validation.to_wire() == second.validation.to_wire()
    assert first.candidate_blueprint.to_wire() == second.candidate_blueprint.to_wire()


def test_selection_is_canonicalized_to_dependency_safe_proposal_order():
    payload = _proposal_with_dependent_changes().to_wire()
    # Put the dependent change first to prove validation does not blindly use
    # either proposal order or caller selection order.
    payload["changes"] = list(reversed(payload["changes"]))
    payload["proposalDigest"] = modeler_proposal_content_digest(payload)
    proposal = ModelerBlueprintProposal.model_validate(payload)
    request = _request(
        proposal,
        ["add-memory-context", "add-decision-trace-context"],
    )

    outcome = validate_modeler_proposal(
        request,
        current_blueprint_identity=proposal.base_blueprint,
    )

    assert outcome.validation.validated_change_ids == [
        "add-decision-trace-context",
        "add-memory-context",
    ]
    assert outcome.candidate_blueprint is not None
    assert outcome.candidate_blueprint.decisions == [
        "Persist governed agent decision traces.",
        "Retain approved episodic memory.",
    ]


def test_add_remove_and_replace_are_applied_in_one_immutable_candidate():
    add = deepcopy(_proposal_payload()["changes"][0])
    replace = deepcopy(add)
    replace.update(
        {
            "changeId": "replace-integration-posture",
            "operation": "replace",
            "path": "/dataPostures/0/posture",
            "value": "operational_replica",
            "dependsOnChangeIds": [],
        }
    )
    remove = deepcopy(add)
    remove.update(
        {
            "changeId": "remove-preferred-strategy",
            "operation": "remove",
            "path": "/preferredFoundationRefs/0",
            "dependsOnChangeIds": ["replace-integration-posture"],
        }
    )
    remove.pop("value")
    proposal = _proposal_with_changes([add, remove, replace])
    request = _request(
        proposal,
        [
            "remove-preferred-strategy",
            "add-decision-trace-context",
            "replace-integration-posture",
        ],
    )

    outcome = validate_modeler_proposal(
        request,
        current_blueprint_identity=proposal.base_blueprint,
    )

    assert outcome.validation.status.value == "valid"
    assert outcome.validation.validated_change_ids == [
        "add-decision-trace-context",
        "replace-integration-posture",
        "remove-preferred-strategy",
    ]
    assert outcome.candidate_blueprint is not None
    assert outcome.candidate_blueprint.data_postures[0].posture.value == (
        "operational_replica"
    )
    assert outcome.candidate_blueprint.preferred_foundation_refs == []


def test_escaped_pointer_tokens_and_list_indices_follow_rfc_subset():
    document = {
        "decisions": [
            {"a/b~c": "old"},
            {"name": "second"},
        ]
    }
    apply_blueprint_override(
        document,
        BlueprintOverride(
            overrideId="replace-escaped",
            operation="replace",
            path="/decisions/0/a~1b~0c",
            value="new",
            reason="Exercise escaped pointer tokens.",
        ),
    )
    apply_blueprint_override(
        document,
        BlueprintOverride(
            overrideId="insert-list-item",
            operation="add",
            path="/decisions/1",
            value={"name": "inserted"},
            reason="Exercise list insertion.",
        ),
    )
    apply_blueprint_override(
        document,
        BlueprintOverride(
            overrideId="remove-list-item",
            operation="remove",
            path="/decisions/0",
            reason="Exercise list removal.",
        ),
    )

    assert document == {
        "decisions": [
            {"name": "inserted"},
            {"name": "second"},
        ]
    }


@pytest.mark.parametrize("path", ["/decisions/9", "/decisions/01"])
def test_unresolvable_or_noncanonical_list_path_returns_invalid(path: str):
    payload = _proposal_payload()
    payload["changes"][0].update(
        {
            "operation": "add",
            "path": path,
        }
    )
    payload["proposalDigest"] = modeler_proposal_content_digest(payload)
    proposal = ModelerBlueprintProposal.model_validate(payload)

    outcome = validate_modeler_proposal(
        _request(proposal, ["add-decision-trace-context"]),
        current_blueprint_identity=proposal.base_blueprint,
    )

    assert outcome.candidate_blueprint is None
    assert outcome.validation.status.value == "invalid"
    assert outcome.validation.issues[0].code == "BLUEPRINT_PATCH_NOT_APPLICABLE"
    assert outcome.validation.candidate_blueprint_digest is None


def test_structurally_invalid_candidate_returns_blocking_result():
    change = deepcopy(_proposal_payload()["changes"][0])
    change.update(
        {
            "changeId": "remove-only-source",
            "operation": "remove",
            "path": "/participants/0",
            "dependsOnChangeIds": [],
        }
    )
    change.pop("value")
    proposal = _proposal_with_changes([change])

    outcome = validate_modeler_proposal(
        _request(proposal, ["remove-only-source"]),
        current_blueprint_identity=proposal.base_blueprint,
    )

    assert outcome.candidate_blueprint is None
    assert outcome.validation.status.value == "invalid"
    assert outcome.validation.issues[0].code == "BLUEPRINT_CANDIDATE_INVALID"
    assert outcome.validation.issues[0].severity.value == "blocking"


def test_current_identity_mismatch_is_stale_and_never_rebased():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    current = BlueprintRevisionIdentity(
        blueprintId=proposal.base_blueprint.blueprint_id,
        revision=proposal.base_blueprint.revision + 1,
        digest=DIGEST_A,
    )

    outcome = validate_modeler_proposal(
        _request(proposal, ["add-decision-trace-context"]),
        current_blueprint_identity=current,
    )

    assert outcome.candidate_blueprint is None
    assert outcome.validation.status.value == "stale_base"
    assert outcome.validation.issues[0].code == "BLUEPRINT_BASE_STALE"
    assert outcome.validation.base_blueprint == proposal.base_blueprint


def test_current_identity_is_mandatory_and_digest_mismatch_is_stale():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    request = _request(proposal, ["add-decision-trace-context"])

    with pytest.raises(TypeError, match="current_blueprint_identity"):
        validate_modeler_proposal(request)  # type: ignore[call-arg]

    current = BlueprintRevisionIdentity(
        blueprintId=proposal.base_blueprint.blueprint_id,
        revision=proposal.base_blueprint.revision,
        digest=DIGEST_A,
    )
    outcome = validate_modeler_proposal(
        request,
        current_blueprint_identity=current,
    )

    assert outcome.validation.status.value == "stale_base"
    assert outcome.candidate_blueprint is None


def test_revalidates_mutable_contract_snapshot_before_applying_changes():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    request = _request(proposal, ["add-decision-trace-context"])
    request.proposal.changes[0].value = "tampered-after-digest-validation"

    with pytest.raises(ValidationError, match="proposalDigest"):
        validate_modeler_proposal(
            request,
            current_blueprint_identity=proposal.base_blueprint,
        )


def test_revalidates_mutated_base_blueprint_before_applying_changes():
    proposal = ModelerBlueprintProposal.model_validate(_proposal_payload())
    request = _request(proposal, ["add-decision-trace-context"])
    request.base_blueprint.industry = "pharma"

    with pytest.raises(ValidationError, match="base Blueprint does not match digest"):
        validate_modeler_proposal(
            request,
            current_blueprint_identity=proposal.base_blueprint,
        )


def test_rejects_duplicate_preferred_foundation_refs_in_candidate():
    change = deepcopy(_proposal_payload()["changes"][0])
    change.update(
        {
            "changeId": "duplicate-preferred-foundation",
            "operation": "add",
            "path": "/preferredFoundationRefs/-",
            "value": _blueprint_payload()["preferredFoundationRefs"][0],
            "dependsOnChangeIds": [],
        }
    )
    proposal = _proposal_with_changes([change])

    outcome = validate_modeler_proposal(
        _request(proposal, ["duplicate-preferred-foundation"]),
        current_blueprint_identity=proposal.base_blueprint,
    )

    assert outcome.candidate_blueprint is None
    assert outcome.validation.status.value == "invalid"
    assert outcome.validation.issues[0].code == "BLUEPRINT_CANDIDATE_INVALID"


def test_rejects_overlapping_changes_without_explicit_dependency():
    first = deepcopy(_proposal_payload()["changes"][0])
    first.update(
        {
            "changeId": "first-posture",
            "operation": "replace",
            "path": "/dataPostures/0/posture",
            "value": "operational_replica",
            "dependsOnChangeIds": [],
        }
    )
    second = deepcopy(first)
    second.update(
        {
            "changeId": "second-posture",
            "value": "mongodb_system_of_record",
        }
    )
    proposal = _proposal_with_changes([first, second])

    outcome = validate_modeler_proposal(
        _request(proposal, ["first-posture", "second-posture"]),
        current_blueprint_identity=proposal.base_blueprint,
    )

    assert outcome.candidate_blueprint is None
    assert outcome.validation.status.value == "invalid"
    assert outcome.validation.issues[0].code == "BLUEPRINT_PATCH_CONFLICT"


def test_allows_overlapping_changes_when_dependency_makes_order_explicit():
    first = deepcopy(_proposal_payload()["changes"][0])
    first.update(
        {
            "changeId": "first-posture",
            "operation": "replace",
            "path": "/dataPostures/0/posture",
            "value": "operational_replica",
            "dependsOnChangeIds": [],
        }
    )
    second = deepcopy(first)
    second.update(
        {
            "changeId": "second-posture",
            "value": "mongodb_system_of_record",
            "dependsOnChangeIds": ["first-posture"],
        }
    )
    proposal = _proposal_with_changes([second, first])

    outcome = validate_modeler_proposal(
        _request(proposal, ["second-posture", "first-posture"]),
        current_blueprint_identity=proposal.base_blueprint,
    )

    assert outcome.validation.status.value == "valid"
    assert outcome.candidate_blueprint is not None
    assert outcome.candidate_blueprint.data_postures[0].posture.value == (
        "mongodb_system_of_record"
    )
