"""Deterministic, non-persisting Solution Blueprint canonicalization."""

from __future__ import annotations

from kehrnel.contracts.blueprint_canonicalization import (
    SolutionBlueprintCanonicalizationResult,
)
from kehrnel.contracts.architecture_foundation import (
    SolutionBlueprint,
    solution_blueprint_content_digest,
)


def canonicalize_solution_blueprint(
    blueprint: SolutionBlueprint,
) -> SolutionBlueprintCanonicalizationResult:
    """Return the complete canonical wire model after structural validation.

    Validation is intentionally limited to the Pydantic contract and the
    Blueprint's internal invariants.  No catalog, compiler, persistence, or
    runtime dependency is consulted.
    """

    payload = blueprint.to_wire()
    if "dataProductCompositions" in payload:
        for composition in payload["dataProductCompositions"]:
            composition["inputScopeIds"] = sorted(composition["inputScopeIds"])
        payload["dataProductCompositions"] = sorted(
            payload["dataProductCompositions"],
            key=lambda item: (item["targetScopeId"], item["compositionId"]),
        )
    canonical = SolutionBlueprint.model_validate(payload)
    return SolutionBlueprintCanonicalizationResult(
        contract="solution-blueprint-canonicalization-result/v1",
        canonical_blueprint=canonical,
        blueprint_digest=solution_blueprint_content_digest(canonical),
    )
