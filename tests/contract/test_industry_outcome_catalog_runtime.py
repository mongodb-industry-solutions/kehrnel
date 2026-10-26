from __future__ import annotations

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import kehrnel.api.core.admin.routes as admin_routes
import kehrnel.engine.core.outcome_catalog as outcome_catalog_module
from kehrnel.api.app import create_app
from kehrnel.contracts import CapabilityMaturity, PublicationStatus, SupportStatus
from kehrnel.engine.core.outcome_catalog import (
    DEFAULT_OUTCOME_CATALOG_PATH,
    IndustryOutcomeCatalogError,
    assert_outcome_catalog_links,
    load_industry_outcome_catalog,
    outcome_catalog_artifact_digest,
)


ZERO_DIGEST = "sha256:" + "0" * 64


def _minimal_catalog() -> dict:
    outcome = {
        "contract": "outcome-pattern/v1",
        "artifactRef": {
            "kind": "outcome-pattern",
            "id": "healthcare.patient-context",
            "version": "1.0.0",
            "digest": ZERO_DIGEST,
        },
        "catalog": {
            "role": "outcome_pattern",
            "provenance": "mongodb_curated",
            "paths": [
                {
                    "industry": "healthcare",
                    "segments": ["outcomes", "patient-context"],
                }
            ],
            "tags": ["patient-context"],
        },
        "name": "Patient context",
        "summary": "Retrieve governed patient context with source provenance.",
        "industryExperienceIds": ["healthcare"],
        "domainIds": ["D01"],
        "scenarios": [
            {
                "scenarioId": "review-context",
                "name": "Review patient context",
                "actorRoles": ["clinician"],
                "trigger": "A care episode is opened.",
                "goal": "Retrieve the current longitudinal context.",
                "observableResult": "The context and its provenance are visible.",
                "freshnessExpectation": "Interactive current",
                "authorityInvariant": "Source systems retain declared authority.",
                "agentExample": "Explain the current context with citations.",
                "successMeasures": ["Every fact has a source reference."],
                "interactionIds": ["retrieve-context"],
            }
        ],
        "sourceCategoryIds": ["healthcare.ehr"],
        "dataProductIds": ["P01"],
        "interactions": [
            {
                "interactionId": "retrieve-context",
                "name": "Retrieve context",
                "description": "Read governed patient context.",
                "effect": "retrieve",
                "autonomyModes": ["direct_application", "agent_assisted"],
                "readProductIds": ["P01"],
                "writeTargets": [],
                "approvalRequired": False,
                "auditRequired": True,
            }
        ],
        "capabilityMaturity": "not_assessed",
        "supportStatus": "evaluation_only",
        "publicationStatus": "draft",
        "evidenceRefs": [],
    }
    outcome["artifactRef"]["digest"] = outcome_catalog_artifact_digest(outcome)
    outcome_ref = dict(outcome["artifactRef"])
    definitions = {
        "domains": [
            {
                "contract": "portfolio-domain/v1",
                "artifactRef": {
                    "kind": "portfolio-domain",
                    "id": "D01",
                    "version": "1.0.0",
                    "digest": ZERO_DIGEST,
                },
                "catalog": {
                    "role": "portfolio_domain",
                    "provenance": "mongodb_curated",
                    "paths": [
                        {"industry": "healthcare", "segments": ["domains", "care"]}
                    ],
                    "tags": [],
                },
                "name": "Care delivery",
                "description": "Care delivery outcomes.",
                "primarySegments": ["Care delivery"],
                "industryExperienceIds": ["healthcare"],
            }
        ],
        "dataProducts": [
            {
                "contract": "data-product/v1",
                "artifactRef": {
                    "kind": "data-product",
                    "id": "P01",
                    "version": "1.0.0",
                    "digest": ZERO_DIGEST,
                },
                "catalog": {
                    "role": "data_product",
                    "provenance": "mongodb_curated",
                    "paths": [
                        {
                            "industry": "healthcare",
                            "segments": ["data-products", "patient-context"],
                        }
                    ],
                    "tags": [],
                },
                "name": "Patient context",
                "description": "Governed longitudinal patient facts.",
                "representationOptions": ["Bounded patient context documents"],
                "inputOutcomeRefs": [outcome_ref],
                "outputOutcomeRefs": [],
            }
        ],
        "architectureCapabilities": [
            {
                "contract": "architecture-capability/v1",
                "artifactRef": {
                    "kind": "architecture-capability",
                    "id": "B01",
                    "version": "1.0.0",
                    "digest": ZERO_DIGEST,
                },
                "catalog": {
                    "role": "architecture_capability",
                    "provenance": "mongodb_curated",
                    "paths": [
                        {
                            "industry": "healthcare",
                            "segments": ["architecture-capabilities", "documents"],
                        }
                    ],
                    "tags": [],
                },
                "name": "Canonical documents",
                "description": "Preserve bounded operational documents.",
                "generatedArtifacts": ["Document schema"],
                "reuseExamples": ["Patient context"],
                "coreOutcomeRefs": [outcome_ref],
                "optionalOutcomeRefs": [],
                "scope": "Test definition",
            }
        ],
        "visualPatterns": [
            {
                "contract": "visual-pattern/v1",
                "artifactRef": {
                    "kind": "visual-pattern",
                    "id": "v01",
                    "version": "1.0.0",
                    "digest": ZERO_DIGEST,
                },
                "catalog": {
                    "role": "visual_pattern",
                    "provenance": "mongodb_curated",
                    "paths": [
                        {
                            "industry": "healthcare",
                            "segments": ["visual-patterns", "timeline"],
                        }
                    ],
                    "tags": [],
                },
                "name": "Record timeline",
                "description": "Show context over time.",
                "screenComponents": ["Timeline"],
                "audienceInteraction": "Select one event.",
                "principleDemonstrated": "Facts retain provenance.",
                "architectInspection": "Inspect object and source identity.",
                "operationExamples": ["retrieveContext"],
            }
        ],
    }
    for group in definitions.values():
        for definition in group:
            definition["artifactRef"]["digest"] = outcome_catalog_artifact_digest(
                definition
            )

    catalog = {
        "contract": "industry-outcome-catalog/v1",
        "artifactRef": {
            "kind": "industry-outcome-catalog",
            "id": "test-industry-outcomes",
            "version": "0.1.0",
            "digest": ZERO_DIGEST,
        },
        "catalogId": "test-industry-outcomes",
        "version": "0.1.0",
        **definitions,
        "outcomes": [outcome],
        "referenceArchitectures": [],
        "demoExperiences": [],
    }
    catalog["artifactRef"]["digest"] = outcome_catalog_artifact_digest(catalog)
    return catalog


def test_loader_accepts_content_addressed_catalog(tmp_path):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(_minimal_catalog()), encoding="utf-8")

    catalog = load_industry_outcome_catalog(path)

    assert catalog.catalog_id == "test-industry-outcomes"
    assert catalog.outcomes[0].artifact_ref.id == "healthcare.patient-context"


def test_loader_rejects_catalog_content_changed_without_new_digest(tmp_path):
    payload = _minimal_catalog()
    payload["outcomes"][0]["summary"] = "Changed after the artifact was signed."
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(IndustryOutcomeCatalogError, match="digest mismatch"):
        load_industry_outcome_catalog(path)


def test_loader_pins_top_level_catalog_version_and_membership(tmp_path):
    payload = _minimal_catalog()
    payload["version"] = "0.1.1"
    payload["artifactRef"]["version"] = "0.1.1"
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(IndustryOutcomeCatalogError, match="catalog digest mismatch"):
        load_industry_outcome_catalog(path)


def test_loader_rejects_mapping_connected_to_wrong_source_profile(tmp_path):
    payload = json.loads(DEFAULT_OUTCOME_CATALOG_PATH.read_text(encoding="utf-8"))
    architectures = {
        item["artifactRef"]["id"]: item
        for item in payload["referenceArchitectures"]
    }
    openmrs = architectures["architecture.healthcare.openmrs-openehr-laboratory"]
    fhir = architectures["architecture.healthcare.fhir-repository"]
    openmrs["sourceCategoryRefs"] = deepcopy(fhir["sourceCategoryRefs"])
    openmrs["sourceProfileRefs"] = deepcopy(fhir["sourceProfileRefs"])
    openmrs["artifactRef"]["digest"] = outcome_catalog_artifact_digest(openmrs)
    payload["demoExperiences"] = []
    payload["artifactRef"]["digest"] = outcome_catalog_artifact_digest(payload)
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(
        IndustryOutcomeCatalogError,
        match="requires undeclared source profile 'app.openmrs.mysql'",
    ):
        load_industry_outcome_catalog(path)


def test_strategy_resolution_ignores_an_unrelated_broken_pack(monkeypatch):
    catalog = load_industry_outcome_catalog()
    strategy_refs = {
        reference.id: reference
        for architecture in catalog.reference_architectures
        for reference in architecture.strategy_refs
    }
    paths = ["broken", *sorted(strategy_refs)]

    monkeypatch.setattr(
        outcome_catalog_module,
        "iter_strategy_manifest_paths",
        lambda _root: iter(paths),
    )

    def load_manifest(path):
        if path == "broken":
            raise ValueError("unrelated preview pack is invalid")
        reference = strategy_refs[path]
        domain = "openEHR" if path.startswith("openehr.") else path.split(".", 1)[0]
        return SimpleNamespace(
            id=reference.id,
            version=reference.version,
            package_digest=reference.digest,
            domain=domain,
        )

    monkeypatch.setattr(
        outcome_catalog_module,
        "load_strategy_manifest",
        load_manifest,
    )

    assert_outcome_catalog_links(catalog, strategy_root="unused")


def test_dependency_failures_are_normalized_as_catalog_errors(tmp_path, monkeypatch):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(_minimal_catalog()), encoding="utf-8")

    def unavailable_categories():
        raise ValueError("source categories unavailable")

    monkeypatch.setattr(
        outcome_catalog_module,
        "load_source_category_catalog",
        unavailable_categories,
    )

    with pytest.raises(
        IndustryOutcomeCatalogError,
        match="catalog dependencies are unavailable",
    ):
        load_industry_outcome_catalog(path)


def test_bundled_catalog_preserves_breadth_without_overstating_delivery():
    catalog = load_industry_outcome_catalog()

    assert len(catalog.outcomes) == 121
    assert len(catalog.reference_architectures) == 9
    assert len(catalog.demo_experiences) == 9
    assert all(
        outcome.capability_maturity is CapabilityMaturity.NOT_ASSESSED
        and outcome.support_status is SupportStatus.EVALUATION_ONLY
        and outcome.publication_status is PublicationStatus.DRAFT
        and not outcome.evidence_refs
        for outcome in catalog.outcomes
    )

    architectures = {
        item.artifact_ref.id: item for item in catalog.reference_architectures
    }
    primary = architectures["architecture.healthcare.openehr-cdr"]
    assert primary.capability_maturity is CapabilityMaturity.COMPILABLE
    assert {reference.id for reference in primary.strategy_refs} == {
        "openehr.rps_dual",
        "openehr.rps_dual_ibm",
    }

    guided = architectures["architecture.healthcare.openmrs-openehr-laboratory"]
    assert guided.capability_maturity is CapabilityMaturity.SIMULATED
    assert [reference.id for reference in guided.mapping_refs] == [
        "mapping.openmrs-to-openehr.lab-observation"
    ]

    planned = architectures["architecture.healthcare.emergency-network"]
    assert planned.capability_maturity is CapabilityMaturity.SPECIFIED
    assert not planned.strategy_refs
    assert not planned.evidence_refs


def test_bundled_catalog_is_exposed_through_versioned_api(tmp_path):
    client = TestClient(create_app(str(tmp_path / "registry.json")))
    response = client.get("/v1/outcome-catalog")

    assert response.status_code == 200
    assert response.headers["etag"].startswith('"sha256:')
    assert response.headers["cache-control"] == "private, max-age=60"
    payload = response.json()
    assert payload["contract"] == "industry-outcome-catalog/v1"
    assert len(payload["outcomes"]) == 121
    assert len(payload["referenceArchitectures"]) == 9
    assert len(payload["demoExperiences"]) == 9

    unchanged = client.get(
        "/v1/outcome-catalog",
        headers={"If-None-Match": response.headers["etag"]},
    )
    assert unchanged.status_code == 304
    assert unchanged.content == b""

    weak_list_match = client.get(
        "/v1/outcome-catalog",
        headers={
            "If-None-Match": f'"sha256:{"f" * 64}", W/{response.headers["etag"]}'
        },
    )
    assert weak_list_match.status_code == 304

    wildcard_match = client.get(
        "/v1/outcome-catalog",
        headers={"If-None-Match": "*"},
    )
    assert wildcard_match.status_code == 304


def test_outcome_catalog_obeys_global_api_key_auth(tmp_path, monkeypatch):
    monkeypatch.setenv("KEHRNEL_AUTH_ENABLED", "true")
    monkeypatch.setenv("KEHRNEL_API_KEYS", "outcome-catalog-test-key")
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    missing = client.get("/v1/outcome-catalog")
    invalid = client.get(
        "/v1/outcome-catalog", headers={"X-API-Key": "incorrect"}
    )
    authorized = client.get(
        "/v1/outcome-catalog",
        headers={"X-API-Key": "outcome-catalog-test-key"},
    )

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert authorized.status_code == 200


def test_outcome_catalog_dependency_failure_is_a_stable_503(tmp_path, monkeypatch):
    def unavailable_catalog():
        raise IndustryOutcomeCatalogError("internal catalog path must not leak")

    monkeypatch.setattr(
        admin_routes,
        "load_bundled_industry_outcome_catalog",
        unavailable_catalog,
    )
    client = TestClient(create_app(str(tmp_path / "registry.json")))

    response = client.get("/v1/outcome-catalog")

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "OUTCOME_CATALOG_UNAVAILABLE"
    assert "internal catalog path" not in response.text
