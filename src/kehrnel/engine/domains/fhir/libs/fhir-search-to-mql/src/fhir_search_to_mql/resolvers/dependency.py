"""
FHIR resource dependency graph for bulk MQL operations.

Kept in sync with ``fhir_gen.resolvers.dependency`` in fhir-data-generation
(MQL_SHIPPED_RESOURCES, CORE_DEPENDENCIES, _GENERATION_PRIORITY). When you
add or change a resource config, update both repos.
"""

from __future__ import annotations

import heapq
from collections import defaultdict
from typing import Sequence

# Resources with shipped YAML configs (84). Keep in sync with configs/*.yaml
# and fhir-data-generation/fhir_gen/resolvers/dependency.py
MQL_SHIPPED_RESOURCES: tuple[str, ...] = (
    "Account",
    "AdverseEvent",
    "AllergyIntolerance",
    "Appointment",
    "AuditEvent",
    "Basic",
    "BiologicallyDerivedProduct",
    "BodyStructure",
    "CarePlan",
    "CareTeam",
    "ChargeItem",
    "ChargeItemDefinition",
    "Claim",
    "ClaimResponse",
    "ClinicalImpression",
    "Communication",
    "Composition",
    "Condition",
    "Consent",
    "Contract",
    "Coverage",
    "CoverageEligibilityRequest",
    "CoverageEligibilityResponse",
    "DetectedIssue",
    "Device",
    "DeviceDispense",
    "DeviceRequest",
    "DeviceUsage",
    "DiagnosticReport",
    "DocumentReference",
    "Encounter",
    "Endpoint",
    "EnrollmentRequest",
    "EnrollmentResponse",
    "EpisodeOfCare",
    "ExplanationOfBenefit",
    "FamilyMemberHistory",
    "Flag",
    "GenomicStudy",
    "Goal",
    "Group",
    "HealthcareService",
    "Immunization",
    "ImmunizationRecommendation",
    "ImagingStudy",
    "InsurancePlan",
    "Invoice",
    "Location",
    "Measure",
    "MeasureReport",
    "Medication",
    "MedicationAdministration",
    "MedicationDispense",
    "MedicationRequest",
    "MedicationStatement",
    "NutritionIntake",
    "NutritionOrder",
    "Observation",
    "Organization",
    "OrganizationAffiliation",
    "Patient",
    "PaymentNotice",
    "PaymentReconciliation",
    "Person",
    "Practitioner",
    "PractitionerRole",
    "Procedure",
    "Provenance",
    "Questionnaire",
    "QuestionnaireResponse",
    "RelatedPerson",
    "RequestOrchestration",
    "ResearchStudy",
    "ResearchSubject",
    "RiskAssessment",
    "Schedule",
    "ServiceRequest",
    "Slot",
    "Specimen",
    "Substance",
    "SupplyDelivery",
    "SupplyRequest",
    "Task",
    "VisionPrescription",
)

# Process anchor types before dependents (denormalize / index order).
_GENERATION_PRIORITY: tuple[str, ...] = (
    "Organization",
    "Location",
    "Medication",
    "Substance",
    "Device",
    "Measure",
    "Questionnaire",
    "InsurancePlan",
    "ChargeItemDefinition",
    "Endpoint",
    "BiologicallyDerivedProduct",
    "Patient",
    "Practitioner",
    "PractitionerRole",
    "HealthcareService",
    "Schedule",
    "Goal",
    "CareTeam",
    "Coverage",
    "Group",
    "ResearchStudy",
    "EnrollmentRequest",
    "CoverageEligibilityRequest",
    "Encounter",
    "Condition",
    "Observation",
    "MedicationRequest",
    "MedicationAdministration",
    "MedicationDispense",
    "ServiceRequest",
    "Procedure",
    "DiagnosticReport",
    "AllergyIntolerance",
    "Immunization",
    "CarePlan",
    "Claim",
    "ClaimResponse",
    "Appointment",
    "Task",
)

# Direct dependencies for each MQL-shipped resource (anchors use []).
CORE_DEPENDENCIES: dict[str, list[str]] = {
    "Patient": [],
    "Person": ["Patient"],
    "Practitioner": [],
    "PractitionerRole": ["Practitioner", "Organization"],
    "RelatedPerson": ["Patient"],
    "Organization": [],
    "OrganizationAffiliation": ["Organization", "Practitioner", "Location"],
    "Location": ["Organization"],
    "Endpoint": ["Organization"],
    "HealthcareService": ["Organization", "Location"],
    "Group": ["Patient"],
    "Schedule": ["Practitioner", "Location"],
    "Slot": ["Schedule"],
    "Appointment": ["Patient", "Practitioner", "Location"],
    "Encounter": ["Patient", "Practitioner", "Organization", "Location"],
    "EpisodeOfCare": ["Patient", "Organization", "Practitioner"],
    "Account": ["Patient", "Organization"],
    "Condition": ["Patient", "Practitioner", "Encounter"],
    "AllergyIntolerance": ["Patient", "Practitioner", "Encounter"],
    "Observation": ["Patient", "Practitioner", "Encounter"],
    "DiagnosticReport": ["Patient", "Practitioner", "Encounter", "Observation"],
    "ImagingStudy": ["Patient", "Practitioner", "Encounter"],
    "Specimen": ["Patient", "Practitioner"],
    "ClinicalImpression": ["Patient", "Practitioner", "Encounter"],
    "FamilyMemberHistory": ["Patient"],
    "BodyStructure": ["Patient"],
    "Composition": ["Patient", "Practitioner", "Encounter"],
    "DocumentReference": ["Patient", "Practitioner", "Organization"],
    "DetectedIssue": ["Patient", "Practitioner"],
    "Flag": ["Patient", "Practitioner"],
    "RiskAssessment": ["Patient", "Practitioner", "Encounter"],
    "Medication": [],
    "Substance": [],
    "MedicationRequest": ["Patient", "Practitioner", "Encounter", "Medication"],
    "MedicationAdministration": ["Patient", "Practitioner", "Encounter", "Medication"],
    "MedicationDispense": ["Patient", "Practitioner", "MedicationRequest", "Medication"],
    "MedicationStatement": ["Patient", "Practitioner", "Encounter", "Medication"],
    "ServiceRequest": ["Patient", "Practitioner", "Encounter"],
    "Procedure": ["Patient", "Practitioner", "Encounter", "Location"],
    "DeviceRequest": ["Patient", "Practitioner", "Encounter", "Device"],
    "RequestOrchestration": ["Patient", "Practitioner", "ServiceRequest"],
    "CarePlan": ["Patient", "Practitioner", "CareTeam", "Condition", "Goal"],
    "CareTeam": ["Patient", "Practitioner", "Organization"],
    "Goal": ["Patient"],
    "Task": ["Patient", "Practitioner"],
    "Communication": ["Patient", "Practitioner", "Encounter"],
    "NutritionOrder": ["Patient", "Practitioner", "Encounter"],
    "NutritionIntake": ["Patient", "Practitioner", "Encounter"],
    "VisionPrescription": ["Patient", "Practitioner", "Encounter"],
    "Device": ["Organization"],
    "DeviceUsage": ["Patient", "Practitioner", "Device"],
    "DeviceDispense": ["Patient", "Device"],
    "SupplyRequest": ["Patient", "Practitioner", "Organization"],
    "SupplyDelivery": ["Patient", "Practitioner", "Organization"],
    "BiologicallyDerivedProduct": ["Organization", "Practitioner"],
    "Immunization": ["Patient", "Practitioner", "Location"],
    "ImmunizationRecommendation": ["Patient", "Immunization"],
    "AdverseEvent": ["Patient", "Practitioner", "Encounter"],
    "Measure": [],
    "MeasureReport": ["Measure", "Patient", "Practitioner", "Organization"],
    "Coverage": ["Patient", "Organization"],
    "CoverageEligibilityRequest": ["Patient", "Organization", "Coverage"],
    "CoverageEligibilityResponse": [
        "Patient",
        "Organization",
        "Coverage",
        "CoverageEligibilityRequest",
    ],
    "Claim": ["Patient", "Practitioner", "Organization", "Coverage", "Encounter"],
    "ClaimResponse": ["Patient", "Practitioner", "Organization", "Claim"],
    "ExplanationOfBenefit": [
        "Patient",
        "Practitioner",
        "Organization",
        "Coverage",
        "Claim",
    ],
    "ChargeItem": ["Patient", "Practitioner", "Encounter"],
    "ChargeItemDefinition": ["Organization"],
    "Invoice": ["Patient", "Practitioner", "Organization", "Account"],
    "PaymentNotice": ["Organization", "Practitioner", "Claim"],
    "PaymentReconciliation": ["Organization", "PaymentNotice"],
    "InsurancePlan": ["Organization"],
    "EnrollmentRequest": ["Patient", "Organization", "Coverage"],
    "EnrollmentResponse": ["Organization", "EnrollmentRequest"],
    "ResearchStudy": ["Organization", "Practitioner"],
    "ResearchSubject": ["Patient", "ResearchStudy"],
    "GenomicStudy": ["Patient", "Practitioner"],
    "Questionnaire": [],
    "QuestionnaireResponse": ["Patient", "Practitioner", "Questionnaire"],
    "Consent": ["Patient", "Organization"],
    "Contract": ["Patient", "Organization", "Practitioner"],
    "AuditEvent": ["Patient", "Practitioner"],
    "Provenance": ["Patient", "Practitioner", "Organization", "Encounter"],
    "Basic": ["Patient", "Practitioner", "Encounter"],
}


def _direct_dependencies(resource_name: str) -> list[str]:
    deps = list(CORE_DEPENDENCIES.get(resource_name, []))
    return [d for d in deps if d != resource_name]


def _collect_transitive(resource_name: str, collected: set[str]) -> None:
    if resource_name in collected:
        return
    collected.add(resource_name)
    for dep in _direct_dependencies(resource_name):
        _collect_transitive(dep, collected)


def _priority_key(name: str) -> tuple[int, str]:
    try:
        return (_GENERATION_PRIORITY.index(name), name)
    except ValueError:
        return (len(_GENERATION_PRIORITY), name)


def resolve_order(resource_names: list[str]) -> list[str]:
    """
    Topological sort — dependencies before dependents.

    Includes the transitive closure of CORE_DEPENDENCIES for the given types.
    """
    all_nodes: set[str] = set()
    for name in resource_names:
        _collect_transitive(name, all_nodes)

    in_degree: dict[str, int] = {n: 0 for n in all_nodes}
    graph: dict[str, list[str]] = defaultdict(list)

    for node in all_nodes:
        for dep in _direct_dependencies(node):
            if dep not in all_nodes:
                continue
            graph[dep].append(node)
            in_degree[node] += 1

    heap: list[tuple[tuple[int, str], str]] = []
    for n in all_nodes:
        if in_degree[n] == 0:
            heapq.heappush(heap, (_priority_key(n), n))

    result: list[str] = []
    while heap:
        _, node = heapq.heappop(heap)
        result.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                heapq.heappush(heap, (_priority_key(neighbor), neighbor))

    if len(result) != len(all_nodes):
        raise ValueError("Circular dependency detected in resource graph")

    return result


def resolve_configured_order(
    resource_names: Sequence[str],
    configured: set[str],
    *,
    include_dependencies: bool = True,
) -> list[str]:
    """
    Expand *resource_names* with transitive dependencies, restricted to *configured*.

    Types without a YAML config are omitted (partial deployments). Order matches
    ``resolve_order`` so anchors are processed before dependents.
    """
    requested = list(resource_names)
    if not include_dependencies:
        return [r for r in requested if r in configured]
    order = resolve_order(requested)
    return [r for r in order if r in configured]


def assert_mql_dependencies_complete() -> None:
    """Raise if any MQL-shipped resource lacks CORE_DEPENDENCIES."""
    missing = [r for r in MQL_SHIPPED_RESOURCES if r not in CORE_DEPENDENCIES]
    if missing:
        raise RuntimeError(
            f"MQL_SHIPPED_RESOURCES missing from CORE_DEPENDENCIES: {missing}"
        )
