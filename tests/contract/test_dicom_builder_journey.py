from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from kehrnel.api.app import create_app
from kehrnel.contracts.source_architecture import (
    ReferenceArchitectureInstantiationRequest,
    StrategyCompilationRequest,
)
from kehrnel.engine.core.access_planner import compile_architecture_proposal
from kehrnel.engine.core.blueprint_templates import (
    blueprint_template_content_digest,
    instantiate_reference_architecture,
    load_solution_blueprint_templates,
)
from kehrnel.engine.core.outcome_catalog import load_industry_outcome_catalog
from kehrnel.engine.core.source_profiles import load_source_profile_catalog, source_profile_content_digest
from kehrnel.engine.core.source_categories import load_source_category_catalog
from kehrnel.engine.core.strategy_candidate_compiler import compile_strategy_candidate, StrategyCompilationError
from kehrnel.engine.core.strategy_portfolio import load_strategy_portfolio


ARCHITECTURE_ID = "architecture.healthcare.dicom-metadata"
ROOT = Path(__file__).resolve().parents[2]


def starter():
    catalog = load_industry_outcome_catalog()
    architecture = next(x for x in catalog.reference_architectures if x.artifact_ref.id == ARCHITECTURE_ID)
    request = ReferenceArchitectureInstantiationRequest(
        contract="reference-architecture-instantiation-request/v1",
        referenceArchitectureRef=architecture.artifact_ref,
        blueprintId="test.dicom-starter", revision=1,
    )
    derivation = instantiate_reference_architecture(request, catalog=catalog)
    assert derivation.blueprint is not None
    return architecture, request, derivation


def compilation(blueprint):
    return StrategyCompilationRequest(
        contract="strategy-compilation-request/v1", blueprint=blueprint,
    )


def test_imaging_discovery_pins_a_metadata_wrapper_and_leaves_provider_open():
    source = next(x for x in load_source_profile_catalog().profiles if x.artifact_ref.id == "exchange.dicom-metadata-json")
    assert source.artifact_ref.digest == source_profile_content_digest(source)
    assert source.technology.value == "application_api"
    assert {mode.value for mode in source.ingestion_modes} == {"batch", "api"}
    assert {x.name for x in source.structures} == {"studies", "instances"}
    assert source.mapping_routes[0].target_model == "dicom"
    category = next(x for x in load_source_category_catalog().categories if x.artifact_ref.id == "healthcare.imaging")
    assert source.artifact_ref in category.reference_profile_refs
    template = next(x for x in load_solution_blueprint_templates() if x.artifact_ref.id == "template.healthcare.dicom-metadata-starter")
    assert template.artifact_ref.digest == blueprint_template_content_digest(template)
    assert template.sources[0].profile_ref == source.artifact_ref
    assert template.constraints.regions == []
    assert all(x.latency_target_ms is None for x in template.consumptions)
    assert template.preferred_strategy_ids == ["dicom.tiered_store"]


def test_preview_compilation_is_deterministic_and_never_production_ready():
    architecture, request, derivation = starter()
    second = instantiate_reference_architecture(request, catalog=load_industry_outcome_catalog())
    assert second == derivation
    assert derivation.requires_review
    assert architecture.support_status.value == "evaluation_only"
    assert architecture.publication_status.value == "draft"
    portfolio = load_strategy_portfolio().entry_for("dicom.tiered_store")
    assert portfolio.track.value == "preview" and not portfolio.release_blocking
    first = compile_architecture_proposal(compilation(derivation.blueprint))
    assert first == compile_architecture_proposal(compilation(derivation.blueprint))
    candidate = first.strategy_candidate
    assert candidate.status.value == "requires_approval"
    assert {issue.code for issue in candidate.issues} == {"PREVIEW_STRATEGY_SELECTED"}
    assert candidate.source_plans[0].target_model == "dicom"
    assert candidate.source_plans[0].mapping_mode.value == "direct"
    assert {x.kind.value for x in candidate.interface_plans} == {"dicom", "agent_tools"}
    assert {x.mechanism.value for x in first.access_plan.structures} == {"direct_document", "targeted_index"}
    assert all(x.capability_maturity.value == "compilable" for x in first.access_plan.structures)
    assert first.access_plan.projection_budget.derived_copy_count == 0


def test_implicit_preview_selection_is_rejected_even_with_a_direct_source_route():
    _, _, derivation = starter()
    payload = derivation.blueprint.to_wire()
    payload["preferredStrategyIds"] = []
    with pytest.raises(StrategyCompilationError, match="explicit preferredStrategyIds"):
        compile_strategy_candidate(StrategyCompilationRequest.model_validate({
            "contract": "strategy-compilation-request/v1", "blueprint": payload,
        }))


def test_full_text_demand_does_not_publish_executable_search_evidence():
    _, _, derivation = starter()
    payload = derivation.blueprint.to_wire()
    payload["consumptions"][0]["workload"]["queryFeatures"].append("full_text")
    proposal = compile_architecture_proposal(StrategyCompilationRequest.model_validate({
        "contract": "strategy-compilation-request/v1", "blueprint": payload,
    }))
    search = next(x for x in proposal.access_plan.structures if x.mechanism.value == "atlas_search_index")
    assert search.capability_maturity.value == "specified"
    assert "ACCESS_IMPLEMENTATION_REQUIRED" in {x.code for x in proposal.access_plan.issues}


@pytest.mark.parametrize("field", ["digest", "version"])
def test_stale_source_references_fail_closed(field):
    _, _, derivation = starter()
    payload = derivation.blueprint.to_wire()
    payload["sources"][0]["profileRef"][field] = "sha256:" + "0" * 64 if field == "digest" else "99.0.0"
    with pytest.raises(StrategyCompilationError, match="source profile reference is not available"):
        compile_strategy_candidate(StrategyCompilationRequest.model_validate({
            "contract": "strategy-compilation-request/v1", "blueprint": payload,
        }))


def test_public_catalog_to_runtime_handoff_requires_an_explicit_activation(tmp_path, monkeypatch):
    monkeypatch.setenv("KEHRNEL_INIT_INGESTION_RUNTIME", "false")
    registry = tmp_path / "registry.json"
    app = create_app(str(registry))
    resolver_calls = []

    def resolver(*, bindings_ref, **context):
        assert bindings_ref == "test://dicom-builder"
        resolver_calls.append(context)
        # Mongo adapters are lazy here; discovery/compilation perform no DB I/O.
        return {"db": {"provider": "mongodb", "database": "dicom_demo",
                       "uri": "mongodb://127.0.0.1:28079"}}

    app.state.strategy_runtime.bindings_resolver = resolver
    with TestClient(app) as client:
        profiles = client.get("/v1/source-profiles")
        assert profiles.status_code == 200
        assert "exchange.dicom-metadata-json" in profiles.text
        catalog = client.get("/v1/outcome-catalog")
        assert catalog.status_code == 200, catalog.text
        architecture = next(x for x in catalog.json()["referenceArchitectures"] if x["artifactRef"]["id"] == ARCHITECTURE_ID)
        before = registry.read_bytes() if registry.exists() else None
        response = client.post(f"/v1/reference-architectures/{ARCHITECTURE_ID}/instantiate", json={
            "contract": "reference-architecture-instantiation-request/v1",
            "referenceArchitectureRef": architecture["artifactRef"],
            "blueprintId": "demo.dicom", "revision": 1,
        })
        assert response.status_code == 200, response.text
        assert response.json()["requiresReview"]
        # This is the existing source-aware bridge, not the canonical package model.
        blueprint = response.json()["blueprint"]
        assert blueprint["contract"] == "legacy-source-blueprint/internal"
        compiled = client.post("/v1/architecture-proposals/compile", json={
            "contract": "strategy-compilation-request/v1", "blueprint": blueprint,
        })
        assert compiled.status_code == 200, compiled.text
        assert compiled.json()["strategyCandidate"]["status"] == "requires_approval"
        assert (registry.read_bytes() if registry.exists() else None) == before

        # Separate explicit preview activation; the database adapter remains lazy.
        recipe = json.loads((ROOT / "examples/dicom/activation.json").read_text())
        selection = compiled.json()["strategyCandidate"]["strategySelections"][0]
        assert recipe["strategy_id"] == selection["strategyId"]
        assert recipe["version"] == architecture["strategyRefs"][0]["version"]
        missing_binding = client.post("/v1/environments/dicom-builder-preview/activate", json=recipe)
        assert missing_binding.status_code == 400
        assert missing_binding.json()["error"]["code"] == "BINDINGS_REF_REQUIRED"
        activated = client.post("/v1/environments/dicom-builder-preview/activate", json={
            **recipe, "bindings_ref": "test://dicom-builder",
        })
        assert activated.status_code == 200, activated.text
        discovery = client.post("/v1/environments/dicom-builder-preview/run", json={
            "domain": "dicom", "operation": "dicom_query_catalog", "payload": {},
        })
        assert discovery.status_code == 200, discovery.text
        assert discovery.json()["result"]["queryContract"] == "dicom-query/v1"
        for request_file in sorted((ROOT / "examples/dicom/queries").glob("*.json")):
            query = json.loads(request_file.read_text())
            compiled_query = client.post("/v1/environments/dicom-builder-preview/compile_query", json={
                "domain": "dicom", "query": query,
            })
            assert compiled_query.status_code == 200, (request_file.name, compiled_query.text)

        assert resolver_calls
