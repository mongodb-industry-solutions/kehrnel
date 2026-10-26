"""Trust-boundary contract for canonical Solution Blueprint validation."""

from __future__ import annotations

from typing import Literal

from pydantic import model_validator

from .artifacts import ContractModel, Sha256Digest
from .architecture_foundation import SolutionBlueprint, solution_blueprint_content_digest


class SolutionBlueprintCanonicalizationResult(ContractModel):
    """Canonical wire form produced by structural validation only.

    The constant readiness fields are intentionally explicit.  A successful
    response proves that the supplied document satisfies the
    canonical ``solution-blueprint/v1`` contract and its internal invariants; it
    does not prove that referenced catalog artifacts exist, that a strategy can
    compile it, or that it can be activated in a runtime.
    """

    contract: Literal["solution-blueprint-canonicalization-result/v1"]
    validation_scope: Literal["structural_only"] = "structural_only"
    canonical_blueprint: SolutionBlueprint
    blueprint_digest: Sha256Digest
    catalog_readiness: Literal["not_evaluated"] = "not_evaluated"
    compiler_readiness: Literal["not_evaluated"] = "not_evaluated"
    activation_readiness: Literal["not_evaluated"] = "not_evaluated"

    @model_validator(mode="after")
    def digest_matches_canonical_blueprint(
        self,
    ) -> "SolutionBlueprintCanonicalizationResult":
        if self.blueprint_digest != solution_blueprint_content_digest(
            self.canonical_blueprint
        ):
            raise ValueError("blueprintDigest does not match canonicalBlueprint")
        return self
