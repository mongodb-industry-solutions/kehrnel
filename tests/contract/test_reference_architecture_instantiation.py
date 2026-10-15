from __future__ import annotations

import hashlib
from copy import deepcopy

import pytest
from fastapi.testclient import TestClient

import kehrnel.api.core.admin.routes as admin_routes
import kehrnel.engine.core.blueprint_templates as blueprint_template_module
from kehrnel.api.app import _json_safe_validation_value, create_app
from kehrnel.contracts.artifacts import ArtifactRef
from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.source_architecture import (
    BlueprintInstantiationStatus,
    ReferenceArchitectureInstantiationRequest,
)
from kehrnel.engine.core.access_planner import compile_architecture_proposal
from kehrnel.engine.core.blueprint_templates import (
    BlueprintTemplateCatalogError,
    ReferenceArchitectureInstantiationError,
    assert_blueprint_template_refs,
    blueprint_template_content_digest,
    instantiate_reference_architecture,
    load_bundled_solution_blueprint_templates,
    load_solution_blueprint_templates,
    reference_architecture_instantiation_request_digest,
)
from kehrnel.engine.core.outcome_catalog import (
    load_bundled_industry_outcome_catalog,
    load_industry_outcome_catalog,
)
from kehrnel.engine.core.source_profiles import SourceProfileCatalogError
from kehrnel.contracts.source_architecture import StrategyCompilationRequest


def _catalog_and_architecture(architecture_id: str):
    catalog = load_industry_outcome_catalog()
    architecture = next(
        item
        for item in catalog.reference_architectures
        if item.artifact_ref.id == architecture_id
    )
    return catalog, architecture


def _request(architecture, **changes):
    payload = {
        "contract": "reference-architecture-instantiation-request/v1",
        "referenceArchitectureRef": architecture.artifact_ref.to_wire(),
        "blueprintId": "customer.openehr-cdr",
        "revision": 1,
        "assumptionDecisions": [],
        "overrides": [],
        **changes,
    }
    return ReferenceArchitectureInstantiationRequest.model_validate(payload)


def _digest(result) -> str:
    payload = result.to_wire()
    payload.pop("derivationDigest")
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def test_bundled_templates_are_content_addressed_and_pin_exact_sources():
    templates = load_solution_blueprint_templates()

    assert len(templates) == 3
    by_id = {template.artifact_ref.id: template for template in templates}
    assert set(by_id) == {
        "template.healthcare.openehr-cdr-starter",
        "template.healthcare.fhir-repository-starter",
        "template.pharmaceutical.cdisc-sdr-starter",
    }
    for template in templates:
        assert template.artifact_ref.digest == blueprint_template_content_digest(
            template
        )
    assert [
        source.profile_ref.id
        for source in by_id["template.healthcare.openehr-cdr-starter"].sources
    ] == ["exchange.openehr-json"]
    assert [
        source.profile_ref.id
        for source in by_id["template.healthcare.fhir-repository-starter"].sources
    ] == ["exchange.fhir-r4-json"]
    assert [
        source.profile_ref.id
        for source in by_id["template.pharmaceutical.cdisc-sdr-starter"].sources
    ] == ["exchange.cdisc-dataset-json"]


def test_openehr_reference_architecture_instantiates_deterministically():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    request = _request(architecture)

    first = instantiate_reference_architecture(request, catalog=catalog)
    second = instantiate_reference_architecture(request, catalog=catalog)

    assert first == second
    assert first.status is BlueprintInstantiationStatus.INSTANTIATED
    assert first.request_digest == reference_architecture_instantiation_request_digest(
        request
    )
    assert first.requested_blueprint_id == request.blueprint_id
    assert first.requested_revision == request.revision
    assert first.derivation_digest == _digest(first)
    assert first.requires_review is True
    assert first.blueprint is not None
    assert first.blueprint.blueprint_id == "customer.openehr-cdr"
    assert first.blueprint.preferred_strategy_ids == ["openehr.rps_dual"]
    assert {item.consumption_id for item in first.blueprint.consumptions} == {
        "canonical-aql",
        "population-cohort",
    }
    assert {issue.code for issue in first.issues} == {
        "BLUEPRINT_ASSUMPTIONS_REQUIRE_REVIEW"
    }

    proposal = compile_architecture_proposal(
        StrategyCompilationRequest(
            contract="strategy-compilation-request/v1",
            blueprint=first.blueprint,
            discoveredProfiles=[],
        )
    )
    assert proposal.strategy_candidate.status.value == "ready"
    assert proposal.strategy_candidate.strategy_selections[0].strategy_id == (
        "openehr.rps_dual"
    )


@pytest.mark.parametrize(
    (
        "architecture_id",
        "blueprint_id",
        "template_id",
        "source_profile_id",
        "strategy_id",
        "consumption_ids",
    ),
    [
        (
            "architecture.healthcare.fhir-repository",
            "customer.fhir-repository",
            "template.healthcare.fhir-repository-starter",
            "exchange.fhir-r4-json",
            "fhir.resource_store",
            {"canonical-fhir", "population-search"},
        ),
        (
            "architecture.pharmaceutical.cdisc-sdr",
            "customer.cdisc-sdr",
            "template.pharmaceutical.cdisc-sdr-starter",
            "exchange.cdisc-dataset-json",
            "cdisc.sdr",
            {"governed-study-query", "governed-study-export"},
        ),
    ],
)
def test_supported_follow_up_architectures_instantiate_compiler_ready_starters(
    architecture_id,
    blueprint_id,
    template_id,
    source_profile_id,
    strategy_id,
    consumption_ids,
):
    catalog, architecture = _catalog_and_architecture(architecture_id)

    result = instantiate_reference_architecture(
        _request(architecture, blueprintId=blueprint_id),
        catalog=catalog,
    )

    assert result.status is BlueprintInstantiationStatus.INSTANTIATED
    assert result.blueprint_template_ref is not None
    assert result.blueprint_template_ref.id == template_id
    assert result.blueprint is not None
    assert [source.profile_ref.id for source in result.blueprint.sources] == [
        source_profile_id
    ]
    assert {
        consumption.consumption_id for consumption in result.blueprint.consumptions
    } == consumption_ids
    assert {issue.code for issue in result.issues} == {
        "BLUEPRINT_ASSUMPTIONS_REQUIRE_REVIEW"
    }

    proposal = compile_architecture_proposal(
        StrategyCompilationRequest(
            contract="strategy-compilation-request/v1",
            blueprint=result.blueprint,
            discoveredProfiles=[],
        )
    )
    assert proposal.strategy_candidate.status.value == "ready"
    assert proposal.access_plan.status.value == "ready"
    assert {
        selection.strategy_id
        for selection in proposal.strategy_candidate.strategy_selections
    } == {strategy_id}


def test_accepting_every_template_assumption_removes_review_requirement():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    template = load_solution_blueprint_templates()[0]
    decisions = [
        {
            "assumptionId": assumption.assumption_id,
            "status": "accepted",
            "rationale": "Confirmed for this preview.",
        }
        for assumption in template.assumptions
    ]

    result = instantiate_reference_architecture(
        _request(architecture, assumptionDecisions=decisions),
        catalog=catalog,
    )

    assert result.status is BlueprintInstantiationStatus.INSTANTIATED
    assert result.requires_review is False
    assert result.issues == []


def test_rejected_assumption_requires_a_cited_override():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    result = instantiate_reference_architecture(
        _request(
            architecture,
            assumptionDecisions=[
                {
                    "assumptionId": "deployment.single-tenant",
                    "status": "rejected",
                    "rationale": "The customer needs tenant isolation.",
                }
            ],
        ),
        catalog=catalog,
    )

    assert result.status is BlueprintInstantiationStatus.NOT_INSTANTIABLE
    assert result.blueprint is None
    assert result.issues[0].code == "REJECTED_ASSUMPTION_REQUIRES_OVERRIDE"
    assert result.derivation_digest == _digest(result)


def test_cited_override_produces_reviewable_tenant_blueprint():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    result = instantiate_reference_architecture(
        _request(
            architecture,
            assumptionDecisions=[
                {
                    "assumptionId": "deployment.single-tenant",
                    "status": "rejected",
                    "rationale": "The customer needs tenant isolation.",
                }
            ],
            overrides=[
                {
                    "overrideId": "deployment.multi-tenant",
                    "operation": "replace",
                    "path": "/constraints/tenancyMode",
                    "value": "multi_tenant",
                    "reason": "Model the customer's shared service.",
                    "assumptionIds": ["deployment.single-tenant"],
                }
            ],
        ),
        catalog=catalog,
    )

    assert result.status is BlueprintInstantiationStatus.INSTANTIATED
    assert result.blueprint is not None
    assert result.blueprint.constraints.tenancy_mode == "multi_tenant"
    assert result.requires_review is True
    assert "BLUEPRINT_OVERRIDES_REQUIRE_REVIEW" in {
        issue.code for issue in result.issues
    }


def test_override_that_removes_required_source_is_not_instantiable():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    result = instantiate_reference_architecture(
        _request(
            architecture,
            overrides=[
                {
                    "overrideId": "remove.only-source",
                    "operation": "remove",
                    "path": "/sources/0",
                    "reason": "Exercise a missing-input outcome.",
                    "assumptionIds": ["source.api-ingestion"],
                }
            ],
        ),
        catalog=catalog,
    )

    assert result.status is BlueprintInstantiationStatus.NOT_INSTANTIABLE
    assert result.blueprint is None
    assert result.issues[0].code == "BLUEPRINT_OVERRIDE_NOT_APPLICABLE"


def test_category_only_architecture_returns_truthful_missing_template_issue():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.emergency-network"
    )

    result = instantiate_reference_architecture(
        _request(architecture, blueprintId="customer.emergency-network"),
        catalog=catalog,
    )

    assert result.status is BlueprintInstantiationStatus.NOT_INSTANTIABLE
    assert result.blueprint_template_ref is None
    assert result.blueprint is None
    assert result.issues[0].code == "BLUEPRINT_TEMPLATE_REQUIRED"


def test_no_template_derivation_is_bound_to_requested_identity_and_revision():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.emergency-network"
    )
    first = instantiate_reference_architecture(
        _request(
            architecture,
            blueprintId="customer.emergency-one",
            revision=1,
        ),
        catalog=catalog,
    )
    second = instantiate_reference_architecture(
        _request(
            architecture,
            blueprintId="customer.emergency-two",
            revision=2,
        ),
        catalog=catalog,
    )

    assert first.request_digest != second.request_digest
    assert first.derivation_digest != second.derivation_digest
    assert second.requested_blueprint_id == "customer.emergency-two"
    assert second.requested_revision == 2


def test_irrelevant_assumption_citation_is_rejected():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    request = _request(
        architecture,
        overrides=[
            {
                "overrideId": "irrelevant-citation",
                "operation": "replace",
                "path": "/constraints/tenancyMode",
                "value": "multi_tenant",
                "reason": "This does not concern the source ingestion assumption.",
                "assumptionIds": ["source.api-ingestion"],
            }
        ],
    )

    with pytest.raises(ReferenceArchitectureInstantiationError) as raised:
        instantiate_reference_architecture(request, catalog=catalog)

    assert raised.value.status == 422
    assert raised.value.code == "IRRELEVANT_ASSUMPTION_CITATION"


def test_accepted_assumption_cannot_be_overridden():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    request = _request(
        architecture,
        assumptionDecisions=[
            {
                "assumptionId": "deployment.single-tenant",
                "status": "accepted",
                "rationale": "Confirmed.",
            }
        ],
        overrides=[
            {
                "overrideId": "contradict-accepted",
                "operation": "replace",
                "path": "/constraints/tenancyMode",
                "value": "multi_tenant",
                "reason": "Contradicts the accepted premise.",
                "assumptionIds": [],
            }
        ],
    )

    with pytest.raises(ReferenceArchitectureInstantiationError) as raised:
        instantiate_reference_architecture(request, catalog=catalog)

    assert raised.value.status == 422
    assert raised.value.code == "ACCEPTED_ASSUMPTION_OVERRIDDEN"


def test_override_cannot_select_strategy_outside_reference_architecture():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    request = _request(
        architecture,
        overrides=[
            {
                "overrideId": "unsupported-strategy",
                "operation": "replace",
                "path": "/preferredStrategyIds/0",
                "value": "fhir.resource_store",
                "reason": "Exercise the Reference Architecture boundary.",
                "assumptionIds": [],
            }
        ],
    )

    with pytest.raises(ReferenceArchitectureInstantiationError) as raised:
        instantiate_reference_architecture(request, catalog=catalog)

    assert raised.value.status == 422
    assert raised.value.code == (
        "PREFERRED_STRATEGY_OUTSIDE_REFERENCE_ARCHITECTURE"
    )


def test_public_cached_catalog_loaders_return_isolated_copies():
    first_templates = load_bundled_solution_blueprint_templates()
    first_templates[0].industry = "poisoned"
    second_templates = load_bundled_solution_blueprint_templates()
    assert second_templates[0].industry == "healthcare"

    first_catalog = load_bundled_industry_outcome_catalog()
    first_catalog.catalog_id = "poisoned"
    first_catalog.reference_architectures[0].name = "poisoned"
    second_catalog = load_bundled_industry_outcome_catalog()
    assert second_catalog.catalog_id == "industry-outcomes"
    assert second_catalog.reference_architectures[0].name != "poisoned"


def test_instantiation_rechecks_tampered_injected_template():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    template = deepcopy(load_solution_blueprint_templates()[0])
    template.industry = "pharmaceutical"

    with pytest.raises(ReferenceArchitectureInstantiationError) as raised:
        instantiate_reference_architecture(
            _request(architecture),
            catalog=catalog,
            templates=(template,),
        )

    assert raised.value.status == 503
    assert raised.value.code == "BLUEPRINT_TEMPLATE_INCOMPATIBLE"


def test_catalog_rejects_template_assumption_path_that_does_not_exist():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    templates = load_solution_blueprint_templates()
    template = deepcopy(
        next(
            item
            for item in templates
            if item.artifact_ref.id == "template.healthcare.openehr-cdr-starter"
        )
    )
    template.assumptions[0].applies_to_paths = ["/sources/0/notAField"]
    template_payload = template.to_wire()
    template_payload["artifactRef"]["digest"] = blueprint_template_content_digest(
        template
    )
    updated_template = type(template).model_validate(template_payload)
    updated_ref = updated_template.artifact_ref
    updated_architecture = architecture.model_copy(
        update={"blueprint_template_ref": updated_ref},
        deep=True,
    )
    updated_architectures = [
        updated_architecture
        if item.artifact_ref.id == architecture.artifact_ref.id
        else item
        for item in catalog.reference_architectures
    ]
    updated_catalog = catalog.model_copy(
        update={"reference_architectures": updated_architectures},
        deep=True,
    )

    with pytest.raises(BlueprintTemplateCatalogError, match="missing paths"):
        assert_blueprint_template_refs(
            updated_catalog,
            templates=tuple(
                updated_template
                if item.artifact_ref.id == updated_template.artifact_ref.id
                else item
                for item in templates
            ),
        )


def test_stale_reference_is_rejected_before_template_selection():
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    payload = architecture.artifact_ref.to_wire()
    payload["digest"] = "sha256:" + "0" * 64
    request = _request(architecture, referenceArchitectureRef=payload)

    with pytest.raises(ReferenceArchitectureInstantiationError) as raised:
        instantiate_reference_architecture(request, catalog=catalog)

    assert raised.value.code == "REFERENCE_ARCHITECTURE_REF_STALE"
    assert raised.value.status == 409


def test_instantiation_api_has_stable_success_and_error_shapes(tmp_path):
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    body = _request(architecture).to_wire()

    response = client.post(
        f"/v1/internal/legacy/reference-architectures/{architecture.artifact_ref.id}/instantiate",
        json=body,
    )
    assert response.status_code == 200
    assert response.json()["status"] == "instantiated"
    assert response.json()["blueprint"]["blueprintId"] == "customer.openehr-cdr"

    mismatch = client.post(
        "/v1/internal/legacy/reference-architectures/architecture.healthcare.fhir-repository/instantiate",
        json=body,
    )
    assert mismatch.status_code == 400
    assert mismatch.json()["error"]["code"] == (
        "REFERENCE_ARCHITECTURE_REFERENCE_MISMATCH"
    )

    stale_body = dict(body)
    stale_body["referenceArchitectureRef"] = dict(body["referenceArchitectureRef"])
    stale_body["referenceArchitectureRef"]["digest"] = "sha256:" + "0" * 64
    stale = client.post(
        f"/v1/internal/legacy/reference-architectures/{architecture.artifact_ref.id}/instantiate",
        json=stale_body,
    )
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "REFERENCE_ARCHITECTURE_REF_STALE"

    no_template = next(
        item
        for item in catalog.reference_architectures
        if item.artifact_ref.id == "architecture.healthcare.emergency-network"
    )
    no_template_body = _request(
        no_template,
        blueprintId="customer.emergency-network",
    ).to_wire()
    unavailable = client.post(
        f"/v1/internal/legacy/reference-architectures/{no_template.artifact_ref.id}/instantiate",
        json=no_template_body,
    )
    assert unavailable.status_code == 200
    assert unavailable.json()["status"] == "not_instantiable"
    assert unavailable.json()["issues"][0]["code"] == "BLUEPRINT_TEMPLATE_REQUIRED"


@pytest.mark.parametrize(
    ("architecture_id", "blueprint_id", "template_id"),
    [
        (
            "architecture.healthcare.fhir-repository",
            "customer.fhir-repository",
            "template.healthcare.fhir-repository-starter",
        ),
        (
            "architecture.pharmaceutical.cdisc-sdr",
            "customer.cdisc-sdr",
            "template.pharmaceutical.cdisc-sdr-starter",
        ),
    ],
)
def test_instantiation_api_opens_supported_follow_up_architectures(
    tmp_path,
    architecture_id,
    blueprint_id,
    template_id,
):
    catalog, architecture = _catalog_and_architecture(architecture_id)
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    response = client.post(
        f"/v1/internal/legacy/reference-architectures/{architecture_id}/instantiate",
        json=_request(architecture, blueprintId=blueprint_id).to_wire(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "instantiated"
    assert payload["blueprintTemplateRef"]["id"] == template_id
    assert payload["blueprint"]["blueprintId"] == blueprint_id
    assert {issue["code"] for issue in payload["issues"]} == {
        "BLUEPRINT_ASSUMPTIONS_REQUIRE_REVIEW"
    }


def test_instantiation_api_rejects_unknown_architecture_and_float_override(tmp_path):
    _, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    body = _request(architecture).to_wire()

    unknown_body = deepcopy(body)
    unknown_body["referenceArchitectureRef"]["id"] = "architecture.unknown"
    unknown = client.post(
        "/v1/internal/legacy/reference-architectures/architecture.unknown/instantiate",
        json=unknown_body,
    )
    assert unknown.status_code == 404
    assert unknown.json()["error"]["code"] == "REFERENCE_ARCHITECTURE_NOT_FOUND"

    float_body = deepcopy(body)
    float_body["overrides"] = [
        {
            "overrideId": "float-not-canonical",
            "operation": "replace",
            "path": "/consumptions/0/latencyTargetMs",
            "value": 12.5,
            "reason": "A float cannot enter a canonical digest.",
            "assumptionIds": ["workload.interactive-aql"],
        }
    ]
    rejected = client.post(
        f"/v1/internal/legacy/reference-architectures/{architecture.artifact_ref.id}/instantiate",
        json=float_body,
    )
    assert rejected.status_code == 422


def test_validation_error_context_cannot_emit_non_finite_json_numbers():
    normalized = _json_safe_validation_value(
        {"error": ValueError("invalid"), "input": float("nan")}
    )

    assert normalized == {"error": "invalid", "input": "nan"}


def test_instantiation_api_reports_missing_template_as_stable_503(
    tmp_path,
    monkeypatch,
):
    catalog, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )
    missing_ref = ArtifactRef(
        kind="solution-blueprint-template",
        id="template.missing",
        version="1.0.0",
        digest="sha256:" + "0" * 64,
    )
    changed_architecture = architecture.model_copy(
        update={"blueprint_template_ref": missing_ref},
        deep=True,
    )
    changed_catalog = catalog.model_copy(
        update={
            "reference_architectures": [
                changed_architecture
                if item.artifact_ref.id == architecture.artifact_ref.id
                else item
                for item in catalog.reference_architectures
            ]
        },
        deep=True,
    )
    monkeypatch.setattr(
        admin_routes,
        "load_bundled_industry_outcome_catalog",
        lambda: changed_catalog,
    )
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    response = client.post(
        f"/v1/internal/legacy/reference-architectures/{architecture.artifact_ref.id}/instantiate",
        json=_request(architecture).to_wire(),
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "BLUEPRINT_TEMPLATE_UNAVAILABLE"


def test_instantiation_api_classifies_compiler_dependency_failure_as_503(
    tmp_path,
    monkeypatch,
):
    _, architecture = _catalog_and_architecture(
        "architecture.healthcare.openehr-cdr"
    )

    def unavailable(_request):
        raise SourceProfileCatalogError("unavailable test dependency")

    monkeypatch.setattr(
        blueprint_template_module,
        "compile_strategy_candidate",
        unavailable,
    )
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    response = client.post(
        f"/v1/internal/legacy/reference-architectures/{architecture.artifact_ref.id}/instantiate",
        json=_request(architecture).to_wire(),
    )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == (
        "BLUEPRINT_COMPILER_DEPENDENCY_UNAVAILABLE"
    )
