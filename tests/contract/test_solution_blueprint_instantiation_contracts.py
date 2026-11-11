from __future__ import annotations

from copy import deepcopy

import jsonschema
import pytest
from pydantic import ValidationError

from kehrnel.contracts.source_architecture import LegacyBlueprintOverride as BlueprintOverride

from kehrnel.contracts import (
    ReferenceArchitecture,
    ReferenceArchitectureInstantiationRequest,
    ReferenceArchitectureInstantiationResult,
    LegacySourceBlueprint,
    SolutionBlueprintTemplate,
    SolutionBlueprintTemplateCatalog,
    contract_schemas,
)


DIGEST = "sha256:" + "a" * 64


def _ref(kind: str, artifact_id: str) -> dict:
    return {
        "kind": kind,
        "id": artifact_id,
        "version": "1.0.0",
        "digest": DIGEST,
    }


def _source_ref() -> dict:
    return _ref("source-profile", "exchange.openehr-json")


def _design() -> dict:
    return {
        "industry": "healthcare",
        "sources": [
            {
                "instanceId": "clinical-feed",
                "profileRef": _source_ref(),
                "ingestionMode": "api",
                "integrationPosture": "selective_materialization",
            }
        ],
        "context": [],
        "consumptions": [
            {
                "consumptionId": "canonical-aql",
                "kind": "openehr",
                "protocol": "openEHR REST + AQL",
                "operations": ["execute-aql"],
            }
        ],
        "constraints": {"tenancyMode": "single_tenant", "regions": []},
        "preferredStrategyIds": ["openehr.rps_dual"],
    }


def _template_payload() -> dict:
    return {
        "contract": "solution-blueprint-template/v1",
        "artifactRef": _ref(
            "solution-blueprint-template", "template.healthcare.openehr-cdr"
        ),
        "catalog": {
            "role": "solution_blueprint_template",
            "provenance": "mongodb_curated",
            "paths": [
                {
                    "industry": "healthcare",
                    "segments": ["blueprint-templates", "clinical-record"],
                }
            ],
            "tags": ["openehr"],
        },
        **_design(),
        "assumptions": [
            {
                "assumptionId": "source-is-canonical-openehr",
                "statement": (
                    "The source supplies canonical openEHR JSON compositions."
                ),
                "appliesToPaths": ["/sources/0/profileRef"],
                "evidenceRefs": [_source_ref()],
            }
        ],
    }


def _blueprint_payload() -> dict:
    return {
      "contract": "legacy-source-blueprint/internal",
        "blueprintId": "hospital-operational-record",
        "revision": 1,
        **_design(),
    }


def _request_payload() -> dict:
    return {
        "contract": "reference-architecture-instantiation-request/v1",
        "referenceArchitectureRef": _ref(
            "reference-architecture", "architecture.healthcare.openehr-cdr"
        ),
        "blueprintId": "hospital-operational-record",
        "revision": 1,
        "assumptionDecisions": [
            {
                "assumptionId": "source-is-canonical-openehr",
                "status": "accepted",
                "rationale": "The integration contract confirms canonical JSON.",
            }
        ],
        "overrides": [
            {
                "overrideId": "use-operational-replica",
                "operation": "replace",
                "path": "/sources/0/integrationPosture",
                "value": "operational_replica",
                "reason": "The workload requires bounded local reads.",
                "assumptionIds": ["source-is-canonical-openehr"],
            }
        ],
    }


def test_solution_blueprint_v2_wire_shape_is_unchanged():
    blueprint = LegacySourceBlueprint.model_validate(_blueprint_payload())
    wire = blueprint.to_wire()

    assert set(wire) == {
        "contract",
        "blueprintId",
        "industry",
        "revision",
        "sources",
        "context",
        "consumptions",
        "constraints",
        "preferredStrategyIds",
    }
    assert "originReferenceArchitectureRef" not in wire
    assert "overrides" not in wire


def test_solution_blueprint_template_is_an_exact_immutable_catalog_artifact():
    template = SolutionBlueprintTemplate.model_validate(_template_payload())

    assert template.artifact_ref.kind == "solution-blueprint-template"
    assert template.catalog.role.value == "solution_blueprint_template"
    assert template.assumptions[0].applies_to_paths == [
        "/sources/0/profileRef"
    ]

    invalid = _template_payload()
    invalid["artifactRef"] = _ref("reference-architecture", "wrong-kind")
    with pytest.raises(ValidationError, match="artifactRef.kind"):
        SolutionBlueprintTemplate.model_validate(invalid)


def test_solution_blueprint_template_catalog_rejects_duplicate_versions():
    template = _template_payload()
    catalog = SolutionBlueprintTemplateCatalog.model_validate(
        {
            "contract": "solution-blueprint-template-catalog/v1",
            "templates": [template],
        }
    )
    assert catalog.templates[0].artifact_ref.id == (
        "template.healthcare.openehr-cdr"
    )

    with pytest.raises(ValidationError, match="duplicate id/version"):
        SolutionBlueprintTemplateCatalog.model_validate(
            {
                "contract": "solution-blueprint-template-catalog/v1",
                "templates": [template, deepcopy(template)],
            }
        )


def test_reference_architecture_accepts_only_blueprint_template_refs():
    payload = {
        "contract": "reference-architecture/v1",
        "artifactRef": _ref(
            "reference-architecture", "architecture.healthcare.openehr-cdr"
        ),
        "catalog": {
            "role": "reference_architecture",
            "provenance": "mongodb_curated",
            "paths": [
                {
                    "industry": "healthcare",
                    "segments": ["reference-architectures", "clinical-record"],
                }
            ],
        },
        "name": "openEHR CDR",
        "summary": "A canonical operational repository.",
        "architectureKind": "canonical_repository",
        "outcomePatternRefs": [_ref("outcome-pattern", "outcome.app065")],
        "sourceCategoryRefs": [_ref("source-category", "healthcare.interoperability")],
        "sourceProfileRefs": [_source_ref()],
        "dataProductIds": ["P01"],
        "capabilityIds": ["B01"],
        "consumptionKinds": ["openehr"],
        "interactionIds": ["app065.retrieve"],
        "blueprintTemplateRef": _template_payload()["artifactRef"],
    }
    assert ReferenceArchitecture.model_validate(payload).blueprint_template_ref

    payload["blueprintTemplateRef"] = _ref(
        "source-profile", "exchange.openehr-json"
    )
    with pytest.raises(ValidationError, match="blueprintTemplateRef"):
        ReferenceArchitecture.model_validate(payload)


@pytest.mark.parametrize(
    "path",
    [
        "sources/0/ingestionMode",
        "/sources/~2invalid",
        "/blueprintId",
        "",
    ],
)
def test_blueprint_overrides_require_safe_editable_json_pointers(path: str):
    payload = _request_payload()["overrides"][0]
    payload["path"] = path

    with pytest.raises(ValidationError):
        BlueprintOverride.model_validate(payload)


def test_blueprint_override_operation_and_value_semantics_are_unambiguous():
    replace = _request_payload()["overrides"][0]
    assert BlueprintOverride.model_validate(replace).value == "operational_replica"

    missing_value = deepcopy(replace)
    missing_value.pop("value")
    with pytest.raises(ValidationError, match="require a non-null value"):
        BlueprintOverride.model_validate(missing_value)

    remove_with_value = deepcopy(replace)
    remove_with_value["operation"] = "remove"
    with pytest.raises(ValidationError, match="cannot include a value"):
        BlueprintOverride.model_validate(remove_with_value)


@pytest.mark.parametrize(
    "value",
    [
        1.5,
        {"nested": [{"ratio": 2.0}]},
        {"médico": "allowed only as a value"},
        {"nested": [{"válue": 7}]},
    ],
)
def test_blueprint_override_rejects_noncanonical_values_recursively(value):
    payload = deepcopy(_request_payload()["overrides"][0])
    payload["value"] = value

    with pytest.raises(ValidationError, match="canonical JSON subset"):
        BlueprintOverride.model_validate(payload)


@pytest.mark.parametrize(
    "value",
    [
        42,
        "Málaga",
        {"asciiKey": ["患者", 7, True]},
    ],
)
def test_blueprint_override_accepts_canonical_unicode_values(value):
    payload = deepcopy(_request_payload()["overrides"][0])
    payload["value"] = value

    assert BlueprintOverride.model_validate(payload).value == value


def test_instantiation_request_pins_origin_and_records_explicit_changes():
    request = ReferenceArchitectureInstantiationRequest.model_validate(
        _request_payload()
    )

    assert request.reference_architecture_ref.kind == "reference-architecture"
    assert request.overrides[0].path == "/sources/0/integrationPosture"
    assert request.assumption_decisions[0].status.value == "accepted"


def test_instantiated_result_preserves_template_origin_and_reviewable_diff():
    template = SolutionBlueprintTemplate.model_validate(_template_payload())
    request = ReferenceArchitectureInstantiationRequest.model_validate(
        _request_payload()
    )
    result = ReferenceArchitectureInstantiationResult.model_validate(
        {
            "contract": "reference-architecture-instantiation-result/v1",
            "status": "instantiated",
            "requestDigest": DIGEST,
            "derivationDigest": DIGEST,
            "requestedBlueprintId": "hospital-operational-record",
            "requestedRevision": 1,
            "referenceArchitectureRef": request.reference_architecture_ref.to_wire(),
            "blueprintTemplateRef": template.artifact_ref.to_wire(),
            "blueprint": _blueprint_payload(),
            "assumptions": [item.to_wire() for item in template.assumptions],
            "assumptionDecisions": [
                item.to_wire() for item in request.assumption_decisions
            ],
            "overrides": [item.to_wire() for item in request.overrides],
            "requiresReview": True,
            "issues": [],
        }
    )

    assert result.blueprint is not None
    assert result.blueprint_template_ref == template.artifact_ref
    assert result.overrides == request.overrides


def test_result_rejects_unknown_assumption_links():
    payload = {
        "contract": "reference-architecture-instantiation-result/v1",
        "status": "instantiated",
        "requestDigest": DIGEST,
        "derivationDigest": DIGEST,
        "requestedBlueprintId": "hospital-operational-record",
        "requestedRevision": 1,
        "referenceArchitectureRef": _request_payload()[
            "referenceArchitectureRef"
        ],
        "blueprintTemplateRef": _template_payload()["artifactRef"],
        "blueprint": _blueprint_payload(),
        "assumptions": _template_payload()["assumptions"],
        "assumptionDecisions": [
            {
                "assumptionId": "unknown",
                "status": "accepted",
                "rationale": "This must fail closed.",
            }
        ],
        "overrides": [],
        "requiresReview": True,
        "issues": [],
    }

    with pytest.raises(ValidationError, match="unknown assumptions"):
        ReferenceArchitectureInstantiationResult.model_validate(payload)


def test_missing_template_is_a_structured_non_instantiable_result():
    result = ReferenceArchitectureInstantiationResult.model_validate(
        {
            "contract": "reference-architecture-instantiation-result/v1",
            "status": "not_instantiable",
            "requestDigest": DIGEST,
            "derivationDigest": DIGEST,
            "requestedBlueprintId": "hospital-operational-record",
            "requestedRevision": 1,
            "referenceArchitectureRef": _request_payload()[
                "referenceArchitectureRef"
            ],
            "requiresReview": False,
            "issues": [
                {
                    "code": "BLUEPRINT_TEMPLATE_UNAVAILABLE",
                    "severity": "blocking",
                    "message": (
                        "The Reference Architecture does not publish a blueprint "
                        "template."
                    ),
                    "refs": ["architecture.healthcare.openehr-cdr"],
                }
            ],
        }
    )

    assert result.blueprint is None
    assert result.blueprint_template_ref is None
    assert result.status.value == "not_instantiable"


def test_instantiated_result_cannot_hide_a_blocking_issue():
    payload = {
        "contract": "reference-architecture-instantiation-result/v1",
        "status": "instantiated",
        "requestDigest": DIGEST,
        "derivationDigest": DIGEST,
        "requestedBlueprintId": "hospital-operational-record",
        "requestedRevision": 1,
        "referenceArchitectureRef": _request_payload()[
            "referenceArchitectureRef"
        ],
        "blueprintTemplateRef": _template_payload()["artifactRef"],
        "blueprint": _blueprint_payload(),
        "requiresReview": True,
        "issues": [
            {
                "code": "TEMPLATE_INVALID",
                "severity": "blocking",
                "message": "The exact template could not be validated.",
                "refs": [],
            }
        ],
    }

    with pytest.raises(ValidationError, match="cannot contain blocking"):
        ReferenceArchitectureInstantiationResult.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("requestedBlueprintId", "different-blueprint", "requestedBlueprintId"),
        ("requestedRevision", 2, "requestedRevision"),
    ],
)
def test_instantiated_result_identity_matches_the_request(
    field: str, value, message: str
):
    payload = {
        "contract": "reference-architecture-instantiation-result/v1",
        "status": "instantiated",
        "requestDigest": DIGEST,
        "derivationDigest": DIGEST,
        "requestedBlueprintId": "hospital-operational-record",
        "requestedRevision": 1,
        "referenceArchitectureRef": _request_payload()[
            "referenceArchitectureRef"
        ],
        "blueprintTemplateRef": _template_payload()["artifactRef"],
        "blueprint": _blueprint_payload(),
        "requiresReview": False,
        "issues": [],
    }
    payload[field] = value

    with pytest.raises(ValidationError, match=message):
        ReferenceArchitectureInstantiationResult.model_validate(payload)


def test_internal_instantiation_contracts_validate_against_model_schema_shapes():
    template = SolutionBlueprintTemplate.model_validate(_template_payload())
    request = ReferenceArchitectureInstantiationRequest.model_validate(
        _request_payload()
    )
    result = ReferenceArchitectureInstantiationResult.model_validate(
        {
            "contract": "reference-architecture-instantiation-result/v1",
            "status": "instantiated",
            "requestDigest": DIGEST,
            "derivationDigest": DIGEST,
            "requestedBlueprintId": "hospital-operational-record",
            "requestedRevision": 1,
            "referenceArchitectureRef": request.reference_architecture_ref.to_wire(),
            "blueprintTemplateRef": template.artifact_ref.to_wire(),
            "blueprint": _blueprint_payload(),
            "assumptions": [item.to_wire() for item in template.assumptions],
            "assumptionDecisions": [
                item.to_wire() for item in request.assumption_decisions
            ],
            "overrides": [item.to_wire() for item in request.overrides],
            "requiresReview": True,
            "issues": [],
        }
    )
    schemas = {
        "solution-blueprint-template.schema.json": SolutionBlueprintTemplate.model_json_schema(by_alias=True),
        "solution-blueprint-template-catalog.schema.json": SolutionBlueprintTemplateCatalog.model_json_schema(by_alias=True),
        "reference-architecture-instantiation-request.schema.json": ReferenceArchitectureInstantiationRequest.model_json_schema(by_alias=True),
        "reference-architecture-instantiation-result.schema.json": ReferenceArchitectureInstantiationResult.model_json_schema(by_alias=True),
    }
    assert not set(schemas).intersection(contract_schemas())

    jsonschema.Draft202012Validator(
        schemas["solution-blueprint-template.schema.json"]
    ).validate(template.to_wire())
    jsonschema.Draft202012Validator(
        schemas["solution-blueprint-template-catalog.schema.json"]
    ).validate(
        SolutionBlueprintTemplateCatalog(
            contract="solution-blueprint-template-catalog/v1",
            templates=[template],
        ).to_wire()
    )
    jsonschema.Draft202012Validator(
        schemas["reference-architecture-instantiation-request.schema.json"]
    ).validate(request.to_wire())
    jsonschema.Draft202012Validator(
        schemas["reference-architecture-instantiation-result.schema.json"]
    ).validate(result.to_wire())
