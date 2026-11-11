from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kehrnel.api.app import create_app
from kehrnel.contracts.architecture_foundation import SolutionBlueprint, solution_blueprint_content_digest
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


GOLDEN = FIXTURE.with_name("blueprint-hdl-golden.json")
SCHEMAS = Path(__file__).parents[2] / "src/kehrnel/contracts/schemas/v1"
ABSENT_DIGEST = "sha256:539a75d57c0eb94952f617720a03e891e7db135f0023dd0af5aac76c372bb697"
EMPTY_DIGEST = "sha256:e34a42f71be31de5a537c877a525de53baca8e0a92230ff32c8beebcd1cddf1b"


def _golden_payload():
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


@pytest.fixture
def structural_client(tmp_path, monkeypatch):
    from kehrnel.api import app as app_module
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "false")
    monkeypatch.setenv("KEHRNEL_INIT_INGESTION_RUNTIME", "false")
    monkeypatch.delenv("CORE_MONGODB_URL", raising=False)
    monkeypatch.setattr(app_module, "_load_manifests", lambda: ([], [], {}))
    with TestClient(create_app(str(tmp_path / "registry.json"), str(tmp_path / "bundles"))) as client:
        yield client


@pytest.mark.parametrize("explicit, expected_digest", [(False, ABSENT_DIGEST), (True, EMPTY_DIGEST)])
def test_optional_composition_presence_survives_models_nested_json_and_http(structural_client, explicit, expected_digest):
    import hashlib
    import jsonschema
    from kehrnel.contracts.package_digest import canonical_json_bytes

    payload = _golden_payload()
    if explicit:
        payload["dataProductCompositions"] = []
    model = SolutionBlueprint.model_validate(payload)
    for by_alias, key in [(True, "dataProductCompositions"), (False, "data_product_compositions")]:
        wire = model.model_dump(mode="json", by_alias=by_alias, exclude_none=True)
        assert (key in wire) is explicit
        assert (key in json.loads(model.model_dump_json(by_alias=by_alias, exclude_none=True))) is explicit
    for _ in range(2):
        result = canonicalize_solution_blueprint(model)
        wire = result.to_wire()
        assert wire["canonicalBlueprint"] == payload
        assert result.blueprint_digest == expected_digest
        assert json.loads(result.model_dump_json(by_alias=True, exclude_none=True)) == wire
        model = SolutionBlueprint.model_validate(wire["canonicalBlueprint"])
    response = structural_client.post("/v1/solution-blueprints/canonicalize", json=payload)
    assert response.status_code == 200, response.text
    wire = response.json()
    assert wire["canonicalBlueprint"] == payload
    assert wire["blueprintDigest"] == expected_digest
    assert "sha256:" + hashlib.sha256(canonical_json_bytes(wire["canonicalBlueprint"])).hexdigest() == expected_digest
    assert set(wire) == {"contract", "validationScope", "canonicalBlueprint", "blueprintDigest", "catalogReadiness", "compilerReadiness", "activationReadiness"}
    assert response.headers["cache-control"] == "private, no-store"
    assert "X-HDL-Principal-Assertion" in response.headers["vary"]
    for filename, value in [("solution-blueprint.schema.json", payload), ("solution-blueprint-canonicalization-result.schema.json", wire)]:
        jsonschema.Draft202012Validator(json.loads((SCHEMAS / filename).read_text())).validate(value)


def _composition_payload():
    payload = _golden_payload()
    for scope in ["source-b", "product-a", "product-b"]:
        payload["semanticAuthorities"].append({**deepcopy(payload["semanticAuthorities"][0]), "scopeId": scope})
    payload["dataProductCompositions"] = [
        {"compositionId": "compose-b", "targetScopeId": "product-b", "inputScopeIds": ["source-b", "clinical-record"]},
        {"compositionId": "compose-a", "targetScopeId": "product-a", "inputScopeIds": ["product-b", "clinical-record"]},
    ]
    return payload


def test_nonempty_compositions_survive_sorted_http_receipt_and_recanonicalization(structural_client):
    import hashlib
    from kehrnel.contracts.package_digest import canonical_json_bytes
    original = _composition_payload()
    permuted = deepcopy(original)
    permuted["dataProductCompositions"].reverse()
    for composition in permuted["dataProductCompositions"]:
        composition["inputScopeIds"].reverse()
    responses = [structural_client.post("/v1/solution-blueprints/canonicalize", json=value) for value in [original, permuted]]
    assert all(response.status_code == 200 for response in responses)
    first, second = [response.json() for response in responses]
    assert first == second
    compositions = first["canonicalBlueprint"]["dataProductCompositions"]
    assert [item["compositionId"] for item in compositions] == ["compose-a", "compose-b"]
    assert all(item["inputScopeIds"] == sorted(item["inputScopeIds"]) for item in compositions)
    assert len(compositions) == len(original["dataProductCompositions"])
    assert canonicalize_solution_blueprint(SolutionBlueprint.model_validate(first["canonicalBlueprint"])).to_wire() == first
    assert first["blueprintDigest"] == "sha256:" + hashlib.sha256(canonical_json_bytes(first["canonicalBlueprint"])).hexdigest()
    assert original == _composition_payload(), "canonicalization must not mutate its input"


@pytest.mark.parametrize("case", ["duplicate_id", "duplicate_target", "duplicate_input", "unknown_input", "unknown_target", "self", "cycle"])
def test_composition_graph_boundaries_fail_structurally(structural_client, case):
    payload = _composition_payload()
    a, b = payload["dataProductCompositions"]
    if case == "duplicate_id": b["compositionId"] = a["compositionId"]
    elif case == "duplicate_target": b["targetScopeId"] = a["targetScopeId"]
    elif case == "duplicate_input": a["inputScopeIds"] *= 2
    elif case == "unknown_input": a["inputScopeIds"] = ["missing"]
    elif case == "unknown_target": a["targetScopeId"] = "missing"
    elif case == "self": a["inputScopeIds"] = [a["targetScopeId"]]
    elif case == "cycle": a["inputScopeIds"] = [b["targetScopeId"]]; b["inputScopeIds"] = [a["targetScopeId"]]
    assert structural_client.post("/v1/solution-blueprints/canonicalize", json=payload).status_code == 422


@pytest.mark.parametrize("case", ["extra", "unknown_scope", "fhir_unpinned", "write_without_policy", "write_unpermitted", "derived_without_freshness"])
def test_existing_structural_boundaries_remain_enforced(structural_client, case):
    payload = _golden_payload()
    interaction = payload["interactions"][0]
    if case == "extra": payload["runtimeReady"] = True
    elif case == "unknown_scope": interaction["semanticScopeId"] = "missing"
    elif case == "fhir_unpinned": interaction["interface"] = {"kind": "fhir", "protocolVersion": "R4"}
    elif case.startswith("write_"):
        interaction["behavior"] = "create"
        if case == "write_unpermitted":
            payload["recordAuthorityPolicies"] = [{
                "contract": "record-authority-policy/v1",
                "artifactRef": {"kind": "record-authority-policy", "id": "clinical-policy", "version": "1.0.0", "digest": "sha256:" + "5" * 64},
                "scope": {"scopeId": "clinical-record", "domain": "clinical"},
                "authoritativeWriterRef": "openehr-cdr", "permittedWriterRefs": ["openehr-cdr"], "conflictPolicy": "reject",
                "versionPolicyRef": {"kind": "version-policy", "id": "clinical-versions", "version": "1.0.0", "digest": "sha256:" + "6" * 64},
                "correctionBehavior": "Append a corrected authoritative version.",
            }]
    elif case == "derived_without_freshness": interaction["guarantees"]["acknowledgement"] = "derived_visible"
    response = structural_client.post("/v1/solution-blueprints/canonicalize", json=payload)
    assert response.status_code == 422
    expected = {"extra": "Extra inputs are not permitted", "unknown_scope": "unknown semantic scope", "fhir_unpinned": "FHIR interfaces require capabilityRef", "write_without_policy": "requires a record authority policy", "write_unpermitted": "outside permittedWriterRefs", "derived_without_freshness": "requires freshnessTargetSeconds"}
    assert expected[case] in response.text


def test_interface_protocol_version_keeps_spaces_and_punctuation(structural_client):
    payload = _golden_payload()
    payload["interactions"][0]["interface"]["protocolVersion"] = "openEHR REST 1.0.3"
    response = structural_client.post("/v1/solution-blueprints/canonicalize", json=payload)
    assert response.status_code == 200
    assert response.json()["canonicalBlueprint"]["interactions"][0]["interface"]["protocolVersion"] == "openEHR REST 1.0.3"


@pytest.mark.parametrize("case", ["content", "readiness", "scope", "extra"])
def test_canonicalization_receipt_rejects_contract_or_digest_drift(case):
    wire = canonicalize_solution_blueprint(SolutionBlueprint.model_validate(_golden_payload())).to_wire()
    if case == "content": wire["canonicalBlueprint"]["objective"] += " Changed."
    elif case == "readiness": wire["compilerReadiness"] = "ready"
    elif case == "scope": wire["validationScope"] = "runtime"
    elif case == "extra": wire["runtimeReady"] = True
    with pytest.raises(ValueError):
        SolutionBlueprintCanonicalizationResult.model_validate(wire)


def test_validation_does_not_resolve_named_artifacts_or_prepare_runtime(structural_client, monkeypatch):
    registry = structural_client.app.state.strategy_runtime.registry
    def forbidden(*args, **kwargs):
        raise AssertionError("Structural validation must not retrieve named artifacts or activate a runtime")
    monkeypatch.setattr(registry, "get_manifest", forbidden)
    monkeypatch.setattr(structural_client.app.state.strategy_runtime, "activate", forbidden)
    payload = _golden_payload()
    payload["preferredFoundationRefs"] = [{"kind": "strategy", "id": "private.unknown", "version": "9.9.9", "digest": "sha256:" + "a" * 64}]
    response = structural_client.post("/v1/solution-blueprints/canonicalize", json=payload)
    assert response.status_code == 200
    assert response.json()["canonicalBlueprint"]["preferredFoundationRefs"] == payload["preferredFoundationRefs"]
    assert not registry.activations


def test_library_authored_nonempty_compositions_are_not_discarded():
    from kehrnel.contracts.architecture_foundation import DataProductComposition
    payload = _composition_payload()
    authored = payload.pop("dataProductCompositions")
    model = SolutionBlueprint.model_validate(payload)
    model.data_product_compositions.extend(DataProductComposition.model_validate(value) for value in authored)
    result = canonicalize_solution_blueprint(model)
    compositions = result.to_wire()["canonicalBlueprint"]["dataProductCompositions"]
    assert {value["compositionId"] for value in compositions} == {value["compositionId"] for value in authored}
    assert len(compositions) == len(authored)
