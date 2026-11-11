"""Compile the canonical DICOM metadata evaluation starter without activation.

POST the generated request to /v1/architecture-resolutions/compile for the same
result through HTTP. Reference target evidence is design-time, not tenant state.
"""
from __future__ import annotations

import hashlib
import json

from kehrnel.contracts import ArchitectureResolutionRequest, ArtifactRef
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.engine.core.architecture_plan_compiler import (
    compile_architecture_resolution, load_architecture_resolution_inputs,
)


def _ref(kind: str, name: str, content: dict) -> dict:
    return ArtifactRef(kind=kind, id=name, version="1.0.0", digest="sha256:" +
                       hashlib.sha256(canonical_json_bytes(content)).hexdigest()).to_wire()


def build_request() -> ArchitectureResolutionRequest:
    inputs = load_architecture_resolution_inputs()
    target = inputs.target_capability_profiles[0]
    binding = next(item for item in inputs.semantic_execution_bindings
                   if item.strategy_ref.id == "dicom.tiered_store")
    interactions = []
    for name, requester, shape in [
        ("select-studies-and-series", "metadata-viewer", "patient-date-modality-description"),
        ("browse-series-instances", "metadata-viewer", "single-series-numeric-page"),
        ("filter-promoted-metadata", "research-agent", "partial-same-sop-attributes"),
    ]:
        payload = {
            "interactionId": name, "behavior": "query", "pattern": "request_response",
            "roles": {"requesterRef": requester, "providerRef": "imaging-repository"},
            "interface": {"kind": "dicom", "protocolVersion": "dicom-query/v1"},
            "semanticScopeId": "imaging-metadata",
            "workloadRef": _ref("workload-profile", name, {"shape": shape, "maxRows": 500}),
            "guarantees": {"acknowledgement": "accepted", "consistency": "eventual"},
        }
        interactions.append({"contract": "interaction-contract/v1", **payload,
                             "artifactRef": _ref("interaction-contract", name, payload)})
    return ArchitectureResolutionRequest.model_validate({
        "contract": "architecture-resolution-request/v1",
        "deliveryLevel": "customer_evaluation",
        "blueprint": {
            "contract": "solution-blueprint/v1", "blueprintId": "dicom-metadata-evaluation",
            "industry": "healthcare", "revision": 1,
            "objective": "Evaluate catalog selection, ordered series pages and partial promoted metadata before production qualification.",
            "participants": [
                {"participantId": "imaging-repository", "label": "Imaging metadata repository", "kind": "repository", "lifecycle": "planned"},
                {"participantId": "metadata-viewer", "label": "Metadata viewer", "kind": "application", "lifecycle": "planned"},
                {"participantId": "research-agent", "label": "Research agent", "kind": "agent", "lifecycle": "planned"},
                {"participantId": "pixel-service", "label": "Separate image delivery integration", "kind": "application", "lifecycle": "existing"},
            ],
            "interactions": interactions,
            "semanticAuthorities": [{"scopeId": "imaging-metadata", "mode": "native",
                                     "authorityRef": binding.semantic_authority_ref.to_wire()}],
            "dataPostures": [{"scopeId": "imaging-metadata", "posture": "selective_materialization",
                              "servingRepositoryRef": "imaging-repository"}],
            "targetCapabilityProfileRef": target.artifact_ref.to_wire(),
            "preferredFoundationRefs": [binding.strategy_ref.to_wire()],
            "assumptions": [
                "The target profile is reference design evidence and must be replaced by qualified deployment evidence.",
                "Catalog ingestion and selected-header hydration use the separately activated native runtime.",
                "Patient identifiers remain center-scoped; acquisition metadata does not infer diagnoses.",
                "Research queries require an active field policy and explicit partial coverage.",
            ],
            "decisions": ["Pixel delivery is a separate integration.",
                          "Canonical package activation remains pending for this preview."],
        },
        "targetCapabilityProfile": target.to_wire(),
        "semanticExecutionBindings": [binding.to_wire()],
    })


def main():
    request = build_request()
    result = compile_architecture_resolution(request)
    print(json.dumps({"request": request.to_wire(), "resolution": result.to_wire()}, indent=2))


if __name__ == "__main__":
    main()
