import json
from pathlib import Path

import jsonschema
import pytest

from kehrnel.contracts.schema_registry import (
    PUBLISHED_SCHEMA_DIR,
    contract_schemas,
    published_contract_digest,
    write_contract_schemas,
)


def test_generated_contract_schemas_match_committed_bytes(tmp_path):
    write_contract_schemas(tmp_path)
    expected = sorted(path.name for path in PUBLISHED_SCHEMA_DIR.iterdir() if path.is_file())
    actual = sorted(path.name for path in tmp_path.iterdir() if path.is_file())
    assert actual == expected
    for name in expected:
        assert (tmp_path / name).read_bytes() == (PUBLISHED_SCHEMA_DIR / name).read_bytes()


def test_contract_schemas_are_draft_2020_12_and_have_stable_ids():
    for name, schema in contract_schemas().items():
        jsonschema.Draft202012Validator.check_schema(schema)
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["$id"] == f"urn:mongodb:data-lab:contracts:v1:{name.removesuffix('.schema.json')}"


def test_published_contract_manifest_verifies_exact_schema_bytes():
    manifest = json.loads((PUBLISHED_SCHEMA_DIR / "manifest.json").read_text(encoding="utf-8"))
    assert published_contract_digest() == manifest["contractDigest"]
    assert manifest["contractDigest"].startswith("sha256:")


def test_foundation_examples_validate_against_published_schemas():
    digest = "sha256:" + "a" * 64
    catalog = {
        "role": "target_strategy",
        "provenance": "mongodb_curated",
        "paths": [
            {
                "industry": "healthcare",
                "segments": ["data-standards", "openehr"],
            }
        ],
        "tags": [],
    }
    source_ref = {
        "kind": "source-profile",
        "id": "exchange.openehr-json",
        "version": "1.0.0",
        "digest": digest,
    }
    source_profile = {
        "contract": "source-profile/v1",
        "artifactRef": source_ref,
        "catalog": {**catalog, "role": "source_profile"},
        "origin": "known",
        "technology": "standard_exchange",
        "categoryIds": ["healthcare.interoperability"],
        "capabilityMaturity": "production_validated",
        "ingestionModes": ["api"],
        "structures": [{"name": "COMPOSITION", "kind": "resource"}],
        "interactions": [],
        "semanticBindings": [],
        "mappingRoutes": [],
    }
    source_category = {
        "contract": "source-category/v1",
        "artifactRef": {
            "kind": "source-category",
            "id": "healthcare.interoperability",
            "version": "1.0.0",
            "digest": digest,
        },
        "catalog": {
            **catalog,
            "role": "source_category",
            "paths": [
                {
                    "industry": "healthcare",
                    "segments": ["source-categories", "interoperability"],
                }
            ],
        },
        "name": "Interoperability",
        "description": "Standards-native health information exchange.",
        "capabilityMaturity": "specified",
        "semanticDomains": ["exchange"],
        "functionalInteractions": [
            {
                "interactionId": "exchange-record",
                "label": "Exchange record",
                "description": "Receive and publish a standards-native record.",
                "semanticConceptIds": ["healthcare.exchange.record"],
            }
        ],
        "referenceProfileRefs": [source_ref],
    }
    legacy_blueprint = {
      "contract": "legacy-source-blueprint/internal",
        "blueprintId": "hospital-operational-space",
        "industry": "healthcare",
        "revision": 1,
        "sources": [
            {
                "instanceId": "ehr",
                "profileRef": source_ref,
                "ingestionMode": "api",
            }
        ],
        "context": [],
        "consumptions": [
            {"consumptionId": "aql", "kind": "openehr", "operations": []}
        ],
        "constraints": {"tenancyMode": "single_tenant", "regions": []},
        "preferredStrategyIds": [],
    }
    strategy_candidate = {
        "contract": "strategy-candidate/v1",
        "blueprintId": "hospital-operational-space",
        "blueprintRevision": 1,
        "status": "ready",
        "candidateDigest": digest,
        "requiresApproval": False,
        "sourcePlans": [],
        "strategySelections": [],
        "interfacePlans": [],
        "issues": [],
    }
    access_plan = {
        "contract": "access-plan/v1",
        "blueprintId": "hospital-operational-space",
        "blueprintRevision": 1,
        "candidateDigest": digest,
        "status": "ready",
        "requiresApproval": False,
        "accessPlanDigest": digest,
        "structures": [
            {
                "accessStructureId": "access.openehr-rps-dual.direct-document",
                "mechanism": "direct_document",
                "strategyId": "openehr.rps_dual",
                "servesConsumptionIds": ["aql"],
                "sourceInstanceIds": ["ehr"],
                "rationale": "Preserve the authoritative composition.",
                "consistency": "authoritative",
                "requiresDerivedCopy": False,
                "capabilityMaturity": "compilable",
                "observationKind": "estimated",
            }
        ],
        "rejectedAlternatives": [],
        "projectionBudget": {
            "observationKind": "estimated",
            "accessStructureCount": 1,
            "indexLikeStructureCount": 0,
            "computedFieldCount": 0,
            "derivedCopyCount": 0,
            "externalExportCount": 0,
            "servingConsumptionCount": 1,
        },
        "issues": [],
    }
    examples = {
        "artifact-ref.schema.json": {
            "kind": "strategy",
            "id": "fhir.resource_store",
            "version": "0.13.1",
            "digest": digest,
        },
        "capability-claim.schema.json": {
            "capabilityId": "fhir.search",
            "capabilityMaturity": "pilot_validated",
            "supportStatus": "supported",
            "runtimeAvailability": "available",
            "evidenceRefs": [],
        },
        "action-availability.schema.json": {
            "actionId": "ask-clinical-question",
            "state": "enabled",
            "requiredCapabilityIds": ["fhir.search"],
        },
        "problem-details.schema.json": {
            "type": "about:blank",
            "title": "Invalid request",
            "status": 422,
            "code": "REQUEST_INVALID",
            "correlationId": "corr-1",
            "fieldErrors": [],
            "retryability": "never",
        },
        "strategy-portfolio.schema.json": {
            "contract": "strategy-portfolio/v1",
            "entries": [
                {
                    "strategyId": "openehr.rps_dual",
                    "track": "primary",
                    "priority": 10,
                    "releaseBlocking": True,
                    "statement": "Primary openEHR release path.",
                    "catalog": catalog,
                }
            ],
        },
        "source-profile.schema.json": source_profile,
        "source-profile-catalog.schema.json": {
            "contract": "source-profile-catalog/v1",
            "profiles": [source_profile],
        },
        "source-category.schema.json": source_category,
        "source-category-catalog.schema.json": {
            "contract": "source-category-catalog/v1",
            "categories": [source_category],
        },
        "solution-blueprint.schema.json": json.loads(
            (
                Path(__file__).parents[1]
                / "fixtures"
                / "architecture"
                / "v1"
                / "blueprint-greenfield-openehr.json"
            ).read_text(encoding="utf-8")
        ),
    }
    for name, value in examples.items():
        schema = json.loads((PUBLISHED_SCHEMA_DIR / name).read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator(schema).validate(value)


def test_package_file_schema_rejects_noncanonical_policy_and_paths():
    schema = json.loads(
        (PUBLISHED_SCHEMA_DIR / "strategy-package-file-manifest.schema.json").read_text(
            encoding="utf-8"
        )
    )
    valid = {
        "contract": "strategy-package-file-manifest/v1",
        "scope": "strategy-directory/v1",
        "hashAlgorithm": "sha256",
        "pathEncoding": "utf-8-nfc-posix",
        "bytePolicy": "exact",
        "presentationMetadata": "included",
        "files": [{"path": "manifest.json", "size": 2, "sha256": "a" * 64}],
    }
    jsonschema.Draft202012Validator(schema).validate(valid)

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(schema).validate(
            {**valid, "hashAlgorithm": "md5"}
        )
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(schema).validate(
            {**valid, "exclusions": []}
        )
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(schema).validate(
            {**valid, "files": [{"path": "../manifest.json", "size": 2, "sha256": "a" * 64}]}
        )
