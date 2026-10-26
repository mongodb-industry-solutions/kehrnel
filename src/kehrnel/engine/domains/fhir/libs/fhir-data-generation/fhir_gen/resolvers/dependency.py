"""FHIR resource dependency graph and topological ordering."""

from __future__ import annotations

import heapq
from collections import defaultdict

from ..schema.registry import SchemaRegistry, registry

# Resources with fhir-search-to-mql configs (84). Keep in sync with
# fhir-search-to-mql/src/fhir_search_to_mql/configs/*.yaml and
# fhir-search-to-mql/src/fhir_search_to_mql/resolvers/dependency.py
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

# Generate anchor resources before dependents that reference them.
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

# Pre-generate these types before generating a dependent resource.
# Every MQL-shipped resource is listed; anchors use [].
CORE_DEPENDENCIES: dict[str, list[str]] = {
    # ── Identity & directory ──
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
    # ── Scheduling & episodes ──
    "Schedule": ["Practitioner", "Location"],
    "Slot": ["Schedule"],
    "Appointment": ["Patient", "Practitioner", "Location"],
    "Encounter": ["Patient", "Practitioner", "Organization", "Location"],
    "EpisodeOfCare": ["Patient", "Organization", "Practitioner"],
    "Account": ["Patient", "Organization"],
    # ── Clinical ──
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
    # ── Medications ──
    "Medication": [],
    "Substance": [],
    "MedicationRequest": ["Patient", "Practitioner", "Encounter", "Medication"],
    "MedicationAdministration": ["Patient", "Practitioner", "Encounter", "Medication"],
    "MedicationDispense": ["Patient", "Practitioner", "MedicationRequest", "Medication"],
    "MedicationStatement": ["Patient", "Practitioner", "Encounter", "Medication"],
    # ── Orders & care coordination ──
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
    # ── Devices & supplies ──
    "Device": ["Organization"],
    "DeviceUsage": ["Patient", "Practitioner", "Device"],
    "DeviceDispense": ["Patient", "Device"],
    "SupplyRequest": ["Patient", "Practitioner", "Organization"],
    "SupplyDelivery": ["Patient", "Practitioner", "Organization"],
    "BiologicallyDerivedProduct": ["Organization", "Practitioner"],
    # ── Immunizations ──
    "Immunization": ["Patient", "Practitioner", "Location"],
    "ImmunizationRecommendation": ["Patient", "Immunization"],
    # ── Safety & quality ──
    "AdverseEvent": ["Patient", "Practitioner", "Encounter"],
    "Measure": [],
    "MeasureReport": ["Measure", "Patient", "Practitioner", "Organization"],
    # ── Financial / RCM ──
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
    # ── Payer & enrollment ──
    "InsurancePlan": ["Organization"],
    "EnrollmentRequest": ["Patient", "Organization", "Coverage"],
    "EnrollmentResponse": ["Organization", "EnrollmentRequest"],
    # ── Research & genomics ──
    "ResearchStudy": ["Organization", "Practitioner"],
    "ResearchSubject": ["Patient", "ResearchStudy"],
    "GenomicStudy": ["Patient", "Practitioner"],
    # ── Forms ──
    "Questionnaire": [],
    "QuestionnaireResponse": ["Patient", "Practitioner", "Questionnaire"],
    # ── Privacy & legal ──
    "Consent": ["Patient", "Organization"],
    "Contract": ["Patient", "Organization", "Practitioner"],
    # ── Audit & extensions ──
    "AuditEvent": ["Patient", "Practitioner"],
    "Provenance": ["Patient", "Practitioner", "Organization", "Encounter"],
    "Basic": ["Patient", "Practitioner", "Encounter"],
}

# Non-MQL resources still supported by fhir-gen (optional deps).
_CORE_DEPENDENCIES_EXTENDED: dict[str, list[str]] = {
    "AppointmentResponse": ["Appointment", "Patient", "Practitioner", "Location"],
    "SubscriptionStatus": ["Subscription", "Patient"],
    "ImmunizationEvaluation": ["Immunization", "Patient", "Practitioner"],
    "Transport": ["Patient", "Location", "Encounter"],
    "BiologicallyDerivedProductDispense": [
        "Patient",
        "Practitioner",
        "Location",
        "BiologicallyDerivedProduct",
    ],
    "DeviceAlert": ["Patient", "Device"],
    "MedicationKnowledge": ["Medication", "Organization"],
}

CORE_DEPENDENCIES.update(_CORE_DEPENDENCIES_EXTENDED)


def _active_registry(schema_registry: SchemaRegistry | None) -> SchemaRegistry:
    return schema_registry or registry


def _known_resources(schema_registry: SchemaRegistry | None = None) -> set[str]:
    return set(_active_registry(schema_registry).all_resources())


def _direct_dependencies(
    resource_name: str, schema_registry: SchemaRegistry | None = None
) -> list[str]:
    """CORE_DEPENDENCIES plus schema-derived Reference targets when unknown."""
    active_registry = _active_registry(schema_registry)
    known = _known_resources(active_registry)
    deps = list(CORE_DEPENDENCIES.get(resource_name, []))
    if resource_name not in CORE_DEPENDENCIES:
        try:
            for dep in active_registry.references_for(resource_name):
                if dep not in deps and dep != resource_name:
                    deps.append(dep)
        except KeyError:
            pass
    return [d for d in deps if d != resource_name and d in known]


def _collect_transitive(
    resource_name: str,
    collected: set[str],
    schema_registry: SchemaRegistry | None = None,
) -> None:
    """Add resource and all transitive dependencies to collected."""
    if resource_name in collected:
        return
    collected.add(resource_name)
    for dep in _direct_dependencies(resource_name, schema_registry):
        _collect_transitive(dep, collected, schema_registry)


def _priority_key(name: str) -> tuple[int, str]:
    try:
        return (_GENERATION_PRIORITY.index(name), name)
    except ValueError:
        return (len(_GENERATION_PRIORITY), name)


def resolve_order(
    resource_names: list[str], schema_registry: SchemaRegistry | None = None
) -> list[str]:
    """
    Topological sort — dependencies before dependents.
    Includes transitive closure of CORE_DEPENDENCIES and schema references.
    Among ready nodes, prefers foundation types (Organization before Patient, etc.).
    """
    all_nodes: set[str] = set()
    for name in resource_names:
        _collect_transitive(name, all_nodes, schema_registry)

    in_degree: dict[str, int] = {n: 0 for n in all_nodes}
    graph: dict[str, list[str]] = defaultdict(list)

    for node in all_nodes:
        for dep in _direct_dependencies(node, schema_registry):
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


def assert_mql_dependencies_complete() -> None:
    """Raise if any MQL-shipped resource lacks CORE_DEPENDENCIES."""
    missing = [r for r in MQL_SHIPPED_RESOURCES if r not in CORE_DEPENDENCIES]
    if missing:
        raise RuntimeError(
            f"MQL_SHIPPED_RESOURCES missing from CORE_DEPENDENCIES: {missing}"
        )
