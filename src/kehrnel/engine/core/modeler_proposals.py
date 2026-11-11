"""Pure deterministic validation for Modeler Agent Blueprint proposals.

This module is deliberately below every transport and persistence boundary.
It accepts an already validated official proposal contract, derives at most
one candidate ``SolutionBlueprint`` revision, and emits a content-addressed
validation receipt.  It never calls a model, stores state, compiles or
activates a runtime, or silently rebases a stale proposal.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import ValidationError

from kehrnel.contracts.modeler_assistant import (
    BlueprintRevisionIdentity,
    ModelerBlueprintChange,
    ModelerProposalValidationRequest,
    ModelerProposalValidationResult,
    ModelerValidationStatus,
    modeler_validation_content_digest,
)
from kehrnel.contracts.source_architecture import (
    BlueprintOverride,
    CompilationIssue,
    IssueSeverity,
)
from kehrnel.contracts.architecture_foundation import (
    SolutionBlueprint,
    solution_blueprint_content_digest,
)
from kehrnel.engine.core.blueprint_patching import (
    apply_blueprint_override,
    pointers_overlap,
)


@dataclass(frozen=True, slots=True)
class ModelerProposalValidationOutcome:
    """A validation receipt and its exact, non-persisted candidate revision."""

    validation: ModelerProposalValidationResult
    candidate_blueprint: SolutionBlueprint | None

    def __post_init__(self) -> None:
        if self.validation.status is ModelerValidationStatus.VALID:
            if self.candidate_blueprint is None:
                raise ValueError("valid validation outcomes require a candidate Blueprint")
            if (
                solution_blueprint_content_digest(self.candidate_blueprint)
                != self.validation.candidate_blueprint_digest
            ):
                raise ValueError(
                    "candidate Blueprint digest does not match the validation receipt"
                )
        elif self.candidate_blueprint is not None:
            raise ValueError(
                "invalid or stale validation outcomes cannot expose a candidate Blueprint"
            )


def validate_modeler_proposal(
    request: ModelerProposalValidationRequest,
    *,
    current_blueprint_identity: BlueprintRevisionIdentity,
) -> ModelerProposalValidationOutcome:
    """Validate selected proposal changes against one exact Blueprint revision.

    ``current_blueprint_identity`` is supplied by the future authoritative
    revision store.  A mismatch produces a deterministic ``stale_base``
    result; the function never attempts to rebase.  Selection order from the
    caller is intentionally ignored: dependencies are applied first and ties
    retain their stable order in the immutable proposal.
    """

    # Contract models are intentionally convenient to construct throughout
    # the SDK and are therefore not globally frozen.  Re-parse wire snapshots
    # at this trust boundary so a caller cannot mutate a previously validated
    # proposal, Blueprint, selection, or current identity while retaining its
    # old content digest.
    request = ModelerProposalValidationRequest.model_validate(request.to_wire())
    current_blueprint_identity = BlueprintRevisionIdentity.model_validate(
        current_blueprint_identity.to_wire()
    )

    proposal = request.proposal
    selected_changes = _dependency_ordered_changes(
        proposal.changes,
        request.selected_change_ids,
    )
    selected_ids = [change.change_id for change in selected_changes]

    if current_blueprint_identity != proposal.base_blueprint:
        return _outcome(
            request,
            status=ModelerValidationStatus.STALE_BASE,
            validated_change_ids=selected_ids,
            issues=[
                CompilationIssue(
                    code="BLUEPRINT_BASE_STALE",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        "The proposal base is not the current Solution Blueprint "
                        "revision. Re-read the project and create a new proposal."
                    ),
                    refs=[
                        f"expected-blueprint:{proposal.base_blueprint.blueprint_id}",
                        f"expected-revision:{proposal.base_blueprint.revision}",
                        f"expected-digest:{proposal.base_blueprint.digest}",
                        f"current-blueprint:{current_blueprint_identity.blueprint_id}",
                        f"current-revision:{current_blueprint_identity.revision}",
                        f"current-digest:{current_blueprint_identity.digest}",
                    ],
                )
            ],
        )

    conflict = _first_unordered_overlap(selected_changes)
    if conflict is not None:
        left, right = conflict
        return _outcome(
            request,
            status=ModelerValidationStatus.INVALID,
            validated_change_ids=selected_ids,
            issues=[
                CompilationIssue(
                    code="BLUEPRINT_PATCH_CONFLICT",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        "Selected changes overlap without an explicit dependency. "
                        "Choose one change or make their application order explicit."
                    ),
                    refs=[
                        left.change_id,
                        left.path,
                        right.change_id,
                        right.path,
                    ],
                )
            ],
        )

    candidate_payload = request.base_blueprint.to_wire()
    candidate_payload["revision"] = request.base_blueprint.revision + 1

    for change in selected_changes:
        override = BlueprintOverride(
            overrideId=change.change_id,
            operation=change.operation,
            path=change.path,
            value=change.value,
            reason=change.rationale,
            assumptionIds=change.assumption_ids,
        )
        try:
            apply_blueprint_override(candidate_payload, override)
        except (IndexError, KeyError, TypeError, ValueError):
            return _outcome(
                request,
                status=ModelerValidationStatus.INVALID,
                validated_change_ids=selected_ids,
                issues=[
                    CompilationIssue(
                        code="BLUEPRINT_PATCH_NOT_APPLICABLE",
                        severity=IssueSeverity.BLOCKING,
                        message=(
                            "A selected change cannot be applied to the exact base "
                            "Blueprint. Review its operation and path."
                        ),
                        refs=[change.change_id, change.path],
                    )
                ],
            )

    try:
        candidate = SolutionBlueprint.model_validate(candidate_payload)
    except ValidationError:
        return _outcome(
            request,
            status=ModelerValidationStatus.INVALID,
            validated_change_ids=selected_ids,
            issues=[
                CompilationIssue(
                    code="BLUEPRINT_CANDIDATE_INVALID",
                    severity=IssueSeverity.BLOCKING,
                    message=(
                        "The selected changes do not produce a valid Solution "
                        "Blueprint. Review required fields and cross-references."
                    ),
                    refs=selected_ids,
                )
            ],
        )

    return _outcome(
        request,
        status=ModelerValidationStatus.VALID,
        validated_change_ids=selected_ids,
        candidate_blueprint=candidate,
        issues=[],
    )


def _dependency_ordered_changes(
    changes: list[ModelerBlueprintChange],
    selected_change_ids: list[str],
) -> list[ModelerBlueprintChange]:
    """Return a stable topological order for a validated selected subset."""

    selected = set(selected_change_ids)
    ordered: list[ModelerBlueprintChange] = []
    visited: set[str] = set()

    # The first currently eligible change in proposal order wins every tie.
    # This is a stable topological sort and is independent of request order.
    while len(visited) < len(selected):
        ready = next(
            (
                change
                for change in changes
                if change.change_id in selected
                and change.change_id not in visited
                and set(change.depends_on_change_ids).issubset(visited)
            ),
            None,
        )
        if ready is None:
            # The proposal/request contracts already reject unknown, missing,
            # and cyclic dependencies. Keep this helper fail-closed if a typed
            # model is modified after construction.
            raise ValueError(
                "selected proposal changes must be known, dependency-closed, and acyclic"
            )
        visited.add(ready.change_id)
        ordered.append(ready)
    return ordered


def _first_unordered_overlap(
    changes: list[ModelerBlueprintChange],
) -> tuple[ModelerBlueprintChange, ModelerBlueprintChange] | None:
    """Find ambiguous edits to the same Blueprint subtree.

    Overlapping edits are permitted only when either change declares the
    other as a direct or transitive dependency.  That makes sequential intent
    explicit instead of relying on a hidden last-write-wins order.
    """

    by_id = {change.change_id: change for change in changes}

    def depends_on(change: ModelerBlueprintChange, target_id: str) -> bool:
        pending = list(change.depends_on_change_ids)
        visited: set[str] = set()
        while pending:
            dependency_id = pending.pop()
            if dependency_id == target_id:
                return True
            if dependency_id in visited:
                continue
            visited.add(dependency_id)
            dependency = by_id.get(dependency_id)
            if dependency is not None:
                pending.extend(dependency.depends_on_change_ids)
        return False

    for index, left in enumerate(changes):
        for right in changes[index + 1 :]:
            if not pointers_overlap(left.path, right.path):
                continue
            if depends_on(left, right.change_id) or depends_on(
                right, left.change_id
            ):
                continue
            return left, right
    return None


def _outcome(
    request: ModelerProposalValidationRequest,
    *,
    status: ModelerValidationStatus,
    validated_change_ids: list[str],
    issues: list[CompilationIssue],
    candidate_blueprint: SolutionBlueprint | None = None,
) -> ModelerProposalValidationOutcome:
    candidate_digest = (
        solution_blueprint_content_digest(candidate_blueprint)
        if candidate_blueprint is not None
        else None
    )
    unsigned = {
        "contract": "modeler-proposal-validation-result/v1",
        "validatorOrigin": "deterministic_core",
        "status": status.value,
        "scope": request.proposal.scope.to_wire(),
        "proposalId": request.proposal.proposal_id,
        "proposalDigest": request.proposal.proposal_digest,
        "baseBlueprint": request.proposal.base_blueprint.to_wire(),
        "validatedChangeIds": validated_change_ids,
        "issues": [issue.to_wire() for issue in issues],
    }
    if candidate_digest is not None:
        unsigned["candidateBlueprintDigest"] = candidate_digest
    result = ModelerProposalValidationResult(
        **unsigned,
        validationDigest=modeler_validation_content_digest(unsigned),
    )
    return ModelerProposalValidationOutcome(
        validation=result,
        candidate_blueprint=candidate_blueprint,
    )


__all__ = [
    "ModelerProposalValidationOutcome",
    "validate_modeler_proposal",
]
