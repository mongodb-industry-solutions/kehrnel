from copy import deepcopy
import json
from pathlib import Path

import jsonschema
import pytest
from pydantic import ValidationError

from kehrnel.contracts import (
    BenchmarkEvidence,
    ImplementationPackage,
    PerformanceForecast,
    PerformanceModel,
    ResolvedArchitecturePlan,
    RuntimeQueryPlanTrace,
    SemanticExecutionBindingV1,
    SolutionBlueprint,
    TargetCapabilityProfile,
    WorkloadProfile,
    performance_forecast_content_digest,
    resolved_architecture_plan_content_digest,
    solution_blueprint_content_digest,
)
from kehrnel.contracts.schema_registry import contract_schemas


FIXTURES = Path(__file__).parents[1] / "fixtures" / "architecture" / "v1"

FIXTURE_SCHEMAS = {
    "blueprint-greenfield-openehr.json": "solution-blueprint.schema.json",
    "blueprint-greenfield-fhir.json": "solution-blueprint.schema.json",
    "blueprint-modernization.json": "solution-blueprint.schema.json",
    "blueprint-authority-conflict.json": "solution-blueprint.schema.json",
    "blueprint-derived-freshness.json": "solution-blueprint.schema.json",
    "target-capability-profile.json": "target-capability-profile.schema.json",
    "semantic-execution-binding-openehr.json": "semantic-execution-binding.schema.json",
    "resolved-architecture-plan-openehr.json": "resolved-architecture-plan.schema.json",
    "resolved-architecture-plan-unsupported-target.json": "resolved-architecture-plan.schema.json",
    "workload-cohort-selective.json": "workload-profile.schema.json",
    "benchmark-evidence-openehr.json": "benchmark-evidence.schema.json",
    "performance-model-openehr.json": "performance-model.schema.json",
    "performance-forecast-openehr.json": "performance-forecast.schema.json",
    "implementation-package-openehr.json": "implementation-package.schema.json",
    "runtime-query-plan-trace-openehr.json": "runtime-query-plan-trace.schema.json",
}


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("fixture_name,schema_name", FIXTURE_SCHEMAS.items())
def test_phase0_golden_artifacts_match_the_published_json_schemas(
    fixture_name, schema_name
):
    schema = contract_schemas()[schema_name]
    jsonschema.Draft202012Validator(schema).validate(_load(fixture_name))


@pytest.mark.parametrize(
    "name",
    [
        "blueprint-greenfield-openehr.json",
        "blueprint-greenfield-fhir.json",
        "blueprint-modernization.json",
        "blueprint-authority-conflict.json",
        "blueprint-derived-freshness.json",
    ],
)
def test_phase0_golden_blueprints_validate(name):
    blueprint = SolutionBlueprint.model_validate(_load(name))
    assert blueprint.contract == "solution-blueprint/v1"
    assert blueprint.interactions


@pytest.mark.parametrize(
    "name",
    ["blueprint-greenfield-openehr.json", "blueprint-greenfield-fhir.json"],
)
def test_greenfield_blueprints_do_not_invent_sources(name):
    blueprint = SolutionBlueprint.model_validate(_load(name))
    assert all(item.source_profile_ref is None for item in blueprint.participants)
    assert any(item.lifecycle.value == "planned" for item in blueprint.participants)


def test_write_interaction_requires_record_authority():
    value = _load("blueprint-greenfield-openehr.json")
    value["recordAuthorityPolicies"] = []
    with pytest.raises(ValidationError, match="requires a record authority policy"):
        SolutionBlueprint.model_validate(value)


def test_writer_must_be_permitted_by_record_authority():
    value = _load("blueprint-authority-conflict.json")
    value["recordAuthorityPolicies"][0]["permittedWriterRefs"] = [
        "registration-system"
    ]
    with pytest.raises(ValidationError, match="outside permittedWriterRefs"):
        SolutionBlueprint.model_validate(value)


def test_derived_visibility_requires_an_explicit_freshness_target():
    value = _load("blueprint-derived-freshness.json")
    del value["interactions"][0]["guarantees"]["freshnessTargetSeconds"]
    with pytest.raises(
        ValidationError,
        match="derived_visible acknowledgement requires freshnessTargetSeconds",
    ):
        SolutionBlueprint.model_validate(value)


def test_fhir_interfaces_pin_a_capability_statement():
    value = _load("blueprint-greenfield-fhir.json")
    del value["interactions"][0]["interface"]["capabilityRef"]
    with pytest.raises(ValidationError, match="FHIR interfaces require capabilityRef"):
        SolutionBlueprint.model_validate(value)


def test_compiled_and_runtime_artifact_chain_validates():
    target = TargetCapabilityProfile.model_validate(
        _load("target-capability-profile.json")
    )
    binding = SemanticExecutionBindingV1.model_validate(
        _load("semantic-execution-binding-openehr.json")
    )
    plan = ResolvedArchitecturePlan.model_validate(
        _load("resolved-architecture-plan-openehr.json")
    )
    package = ImplementationPackage.model_validate(
        _load("implementation-package-openehr.json")
    )
    trace = RuntimeQueryPlanTrace.model_validate(
        _load("runtime-query-plan-trace-openehr.json")
    )

    assert plan.target_capability_profile_ref == target.artifact_ref
    assert binding.artifact_ref in plan.semantic_binding_refs
    assert package.resolved_plan_ref == plan.artifact_ref
    assert trace.resolved_plan_ref == plan.artifact_ref
    assert trace.semantic_binding_ref == binding.artifact_ref
    assert trace.plan_family_id == plan.query_plan_families[0].plan_family_id


def test_performance_evidence_chain_is_a_forecast_not_a_guarantee():
    workload = WorkloadProfile.model_validate(_load("workload-cohort-selective.json"))
    evidence = BenchmarkEvidence.model_validate(
        _load("benchmark-evidence-openehr.json")
    )
    model = PerformanceModel.model_validate(_load("performance-model-openehr.json"))
    forecast = PerformanceForecast.model_validate(
        _load("performance-forecast-openehr.json")
    )

    assert evidence.workload_ref == workload.artifact_ref
    assert evidence.artifact_ref in model.benchmark_evidence_refs
    assert model.artifact_ref == forecast.performance_model_ref
    assert evidence.artifact_ref in forecast.benchmark_evidence_refs
    assert forecast.is_guarantee is False
    assert forecast.p50_latency_microseconds.upper <= (
        forecast.p95_latency_microseconds.upper
    )


def test_strong_forecast_grade_requires_benchmark_evidence():
    value = _load("performance-forecast-openehr.json")
    value["benchmarkEvidenceRefs"] = []
    with pytest.raises(ValidationError, match="requires benchmark evidence"):
        PerformanceForecast.model_validate(value)


def test_analytical_forecast_names_required_validation_tests():
    value = _load("performance-forecast-openehr.json")
    value["evidenceGrade"] = "d_analytical"
    value["benchmarkEvidenceRefs"] = []
    value["requiredTests"] = []
    with pytest.raises(ValidationError, match="requires requiredTests"):
        PerformanceForecast.model_validate(value)


def test_ready_plan_requires_a_strategy_instance():
    value = _load("resolved-architecture-plan-openehr.json")
    value["strategyInstances"] = []
    value["resourceOwnership"] = []
    value["materializations"] = []
    value["queryPlanFamilies"] = []
    with pytest.raises(ValidationError, match="require strategyInstances"):
        ResolvedArchitecturePlan.model_validate(value)


def test_blocked_plan_requires_a_blocking_issue():
    value = _load("resolved-architecture-plan-openehr.json")
    value["status"] = "blocked"
    with pytest.raises(ValidationError, match="require a blocking issue"):
        ResolvedArchitecturePlan.model_validate(value)


def test_unsupported_required_target_capability_fails_closed():
    blocked = ResolvedArchitecturePlan.model_validate(
        _load("resolved-architecture-plan-unsupported-target.json")
    )
    assert blocked.status.value == "blocked"
    assert blocked.target_compatibility[0].compatible is False

    value = _load("resolved-architecture-plan-unsupported-target.json")
    value["status"] = "requires_approval"
    with pytest.raises(
        ValidationError,
        match="required target capabilities are incompatible",
    ):
        ResolvedArchitecturePlan.model_validate(value)


def test_phase0_content_digests_are_deterministic_and_content_sensitive():
    blueprint = SolutionBlueprint.model_validate(
        _load("blueprint-greenfield-openehr.json")
    )
    plan = ResolvedArchitecturePlan.model_validate(
        _load("resolved-architecture-plan-openehr.json")
    )
    forecast = PerformanceForecast.model_validate(
        _load("performance-forecast-openehr.json")
    )

    assert solution_blueprint_content_digest(blueprint) == (
        solution_blueprint_content_digest(blueprint)
    )
    assert resolved_architecture_plan_content_digest(plan) == (
        resolved_architecture_plan_content_digest(plan)
    )
    assert performance_forecast_content_digest(forecast) == (
        performance_forecast_content_digest(forecast)
    )

    changed = deepcopy(blueprint.to_wire())
    changed["objective"] += " Changed."
    assert solution_blueprint_content_digest(changed) != (
        solution_blueprint_content_digest(blueprint)
    )


def test_phase0_contracts_are_in_the_schema_registry():
    names = set(contract_schemas())
    assert {
        "benchmark-evidence.schema.json",
        "implementation-package.schema.json",
        "interaction-contract.schema.json",
        "performance-forecast.schema.json",
        "performance-model.schema.json",
        "record-authority-policy.schema.json",
        "resolved-architecture-plan.schema.json",
        "runtime-query-plan-trace.schema.json",
        "semantic-execution-binding.schema.json",
        "solution-blueprint.schema.json",
        "target-capability-profile.schema.json",
        "workload-profile.schema.json",
    } <= names
