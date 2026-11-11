"""Compile the DICOM catalog starter and inspect its runtime recipe; no I/O."""
from __future__ import annotations

import json
from pathlib import Path

from kehrnel.contracts.source_architecture import (
    ReferenceArchitectureInstantiationRequest,
    StrategyCompilationRequest,
)
from kehrnel.engine.core.access_planner import compile_architecture_proposal
from kehrnel.engine.core.blueprint_templates import instantiate_reference_architecture
from kehrnel.engine.core.outcome_catalog import load_bundled_industry_outcome_catalog


ARCHITECTURE_ID = "architecture.healthcare.dicom-metadata"


def compile_starter(blueprint_id: str = "demo.dicom-metadata"):
    catalog = load_bundled_industry_outcome_catalog()
    architecture = next(
        item for item in catalog.reference_architectures
        if item.artifact_ref.id == ARCHITECTURE_ID
    )
    derivation = instantiate_reference_architecture(
        ReferenceArchitectureInstantiationRequest(
            contract="reference-architecture-instantiation-request/v1",
            referenceArchitectureRef=architecture.artifact_ref,
            blueprintId=blueprint_id,
            revision=1,
        ),
        catalog=catalog,
    )
    if derivation.blueprint is None:
        raise ValueError("DICOM starter could not be instantiated")
    proposal = compile_architecture_proposal(
        StrategyCompilationRequest(
            contract="strategy-compilation-request/v1",
            blueprint=derivation.blueprint,
        )
    )
    return architecture, derivation, proposal


if __name__ == "__main__":
    architecture, derivation, proposal = compile_starter()
    recipe = json.loads(
        (Path(__file__).resolve().parents[1] / "dicom" / "activation.json").read_text()
    )
    print(json.dumps({
        "referenceArchitectureRef": architecture.artifact_ref.to_wire(),
        "blueprintTemplateRef": derivation.blueprint_template_ref.to_wire(),
        "derivationDigest": derivation.derivation_digest,
        "proposalDigest": proposal.proposal_digest,
        "candidateStatus": proposal.strategy_candidate.status.value,
        "issues": [issue.to_wire() for issue in proposal.strategy_candidate.issues],
        "accessMechanisms": [item.mechanism.value for item in proposal.access_plan.structures],
        "runtimeRecipe": recipe,
        "runtimeActivated": False,
        "canonicalPackageActivationSupported": False,
        "next": "Review the preview, configure resolver-backed adapters, then explicitly activate the existing runtime. The canonical participant/interaction Builder integration is a later milestone.",
    }, indent=2))
