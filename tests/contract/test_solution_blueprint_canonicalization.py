from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts import SolutionBlueprint, solution_blueprint_content_digest
from kehrnel.contracts.blueprint_canonicalization import SolutionBlueprintCanonicalizationResult
from kehrnel.engine.core.blueprint_canonicalization import canonicalize_solution_blueprint


FIXTURE = Path(__file__).parents[1] / "fixtures" / "architecture" / "v1" / "blueprint-greenfield-openehr.json"


def _blueprint_payload() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_canonicalizer_returns_the_single_canonical_blueprint_and_digest():
    result = canonicalize_solution_blueprint(SolutionBlueprint.model_validate(_blueprint_payload()))
    wire = result.to_wire()
    assert wire["canonicalBlueprint"]["contract"] == "solution-blueprint/v1"
    assert wire["canonicalBlueprint"]["participants"]
    assert wire["canonicalBlueprint"]["interactions"]
    assert wire["blueprintDigest"] == solution_blueprint_content_digest(wire["canonicalBlueprint"])


def test_endpoint_is_deterministic_and_disclaims_readiness(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    payload = _blueprint_payload()
    first = client.post("/v1/solution-blueprints/canonicalize", json=payload)
    second = client.post("/v1/solution-blueprints/canonicalize", json={key: payload[key] for key in reversed(payload)})
    assert first.status_code == 200
    assert first.json() == second.json()
    result = first.json()
    assert result["contract"] == "solution-blueprint-canonicalization-result/v1"
    assert result["validationScope"] == "structural_only"
    assert result["catalogReadiness"] == "not_evaluated"
    assert result["compilerReadiness"] == "not_evaluated"
    assert result["activationReadiness"] == "not_evaluated"


def test_endpoint_rejects_unknown_interaction_participant(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    payload = _blueprint_payload()
    payload["interactions"][0]["roles"]["requesterRef"] = "missing-participant"
    response = client.post("/v1/solution-blueprints/canonicalize", json=payload)
    assert response.status_code == 422
    assert "unknown participants" in str(response.json())


def test_omitted_defaults_and_explicit_defaults_have_the_same_digest():
    implicit_payload = _blueprint_payload()
    for field in ("constraints", "preferredFoundationRefs", "assumptions", "decisions"):
        implicit_payload.pop(field, None)
    implicit = SolutionBlueprint.model_validate(implicit_payload)
    explicit_payload = deepcopy(implicit_payload)
    explicit_payload.update({
        "constraints": {"tenancyMode": "single_tenant", "regions": []},
        "preferredFoundationRefs": [],
        "assumptions": [],
        "decisions": [],
    })
    explicit = SolutionBlueprint.model_validate(explicit_payload)
    assert canonicalize_solution_blueprint(implicit).blueprint_digest == canonicalize_solution_blueprint(explicit).blueprint_digest


def test_result_contract_rejects_a_digest_for_different_content():
    canonical = SolutionBlueprint.model_validate(_blueprint_payload())
    with pytest.raises(ValueError, match="blueprintDigest does not match"):
        SolutionBlueprintCanonicalizationResult(
            contract="solution-blueprint-canonicalization-result/v1",
            canonicalBlueprint=canonical,
            blueprintDigest="sha256:" + "f" * 64,
        )


def test_data_product_compositions_are_validated_and_canonicalized():
    payload = _blueprint_payload()
    scopes = [item["scopeId"] for item in payload["semanticAuthorities"]]
    if len(scopes) < 2:
        extra = deepcopy(payload["semanticAuthorities"][0])
        extra["scopeId"] = "derived-product"
        payload["semanticAuthorities"].append(extra)
        repository_id = next(
            item["participantId"]
            for item in payload["participants"]
            if item["kind"] == "repository"
        )
        payload["dataPostures"].append({
            "scopeId": "derived-product",
            "posture": "selective_materialization",
            "servingRepositoryRef": repository_id,
        })
        scopes.append("derived-product")
    payload["dataProductCompositions"] = [{
        "compositionId": "compose-derived",
        "targetScopeId": scopes[-1],
        "inputScopeIds": list(reversed(scopes[:-1])),
    }]
    canonical = canonicalize_solution_blueprint(SolutionBlueprint.model_validate(payload))
    composition = canonical.to_wire()["canonicalBlueprint"]["dataProductCompositions"][0]
    assert composition["inputScopeIds"] == sorted(scopes[:-1])


@pytest.mark.parametrize("mutation, message", [
    (lambda value, scopes: value.update(targetScopeId="missing"), "unknown semantic scopes"),
    (lambda value, scopes: value.update(inputScopeIds=[value["targetScopeId"]]), "cannot compose itself"),
])
def test_data_product_composition_rejects_invalid_scope_graph(mutation, message):
    payload = _blueprint_payload()
    scopes = [item["scopeId"] for item in payload["semanticAuthorities"]]
    value = {"compositionId": "compose-record", "targetScopeId": scopes[0], "inputScopeIds": [scopes[-1]]}
    mutation(value, scopes)
    payload["dataProductCompositions"] = [value]
    with pytest.raises(ValueError, match=message):
        SolutionBlueprint.model_validate(payload)


def test_data_product_composition_rejects_cycles():
    payload = _blueprint_payload()
    original_scope = payload["semanticAuthorities"][0]["scopeId"]
    extra = deepcopy(payload["semanticAuthorities"][0])
    extra["scopeId"] = "derived-product"
    payload["semanticAuthorities"].append(extra)
    repository_id = next(
        item["participantId"]
        for item in payload["participants"]
        if item["kind"] == "repository"
    )
    payload["dataPostures"].append({
        "scopeId": "derived-product",
        "posture": "selective_materialization",
        "servingRepositoryRef": repository_id,
    })
    payload["dataProductCompositions"] = [
        {"compositionId": "compose-a", "targetScopeId": original_scope, "inputScopeIds": ["derived-product"]},
        {"compositionId": "compose-b", "targetScopeId": "derived-product", "inputScopeIds": [original_scope]},
    ]
    with pytest.raises(ValueError, match="acyclic"):
        SolutionBlueprint.model_validate(payload)
