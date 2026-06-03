"""FHIR resource dependency graph and topological ordering."""

from __future__ import annotations

import heapq
from collections import defaultdict

from ..schema.registry import registry

# Generate anchor resources before dependents that reference them.
_GENERATION_PRIORITY: tuple[str, ...] = (
    "Organization",
    "Location",
    "Medication",
    "Substance",
    "Device",
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

# Commonly referenced resources — pre-generate before dependents
CORE_DEPENDENCIES: dict[str, list[str]] = {
    "Patient": [],
    "Practitioner": [],
    "Organization": [],
    "Location": [],
    "PractitionerRole": ["Practitioner", "Organization"],
    "Encounter": ["Patient", "Practitioner", "Organization", "Location"],
    "Observation": ["Patient", "Practitioner", "Encounter"],
    "Condition": ["Patient", "Practitioner", "Encounter"],
    "MedicationRequest": ["Patient", "Practitioner", "Encounter", "Medication"],
    "MedicationAdministration": ["Patient", "Practitioner", "Encounter", "Medication"],
    "MedicationDispense": ["Patient", "Practitioner", "MedicationRequest"],
    "AllergyIntolerance": ["Patient", "Practitioner", "Encounter"],
    "Procedure": ["Patient", "Practitioner", "Encounter", "Location"],
    "DiagnosticReport": ["Patient", "Practitioner", "Encounter", "Observation"],
    "ServiceRequest": ["Patient", "Practitioner", "Encounter"],
    "CarePlan": ["Patient", "Practitioner", "CareTeam", "Condition", "Goal"],
    "CareTeam": ["Patient", "Practitioner", "Organization"],
    "Goal": ["Patient"],
    "Immunization": ["Patient", "Practitioner", "Location"],
    "Coverage": ["Patient", "Organization"],
    "Claim": ["Patient", "Practitioner", "Organization", "Coverage", "Encounter"],
    "ClaimResponse": ["Patient", "Practitioner", "Organization", "Claim"],
    "Appointment": ["Patient", "Practitioner", "Location"],
    "AppointmentResponse": ["Appointment", "Patient", "Practitioner", "Location"],
    "SubscriptionStatus": ["Subscription", "Patient"],
    "CoverageEligibilityRequest": ["Patient", "Organization", "Coverage"],
    "CoverageEligibilityResponse": [
        "Patient", "Organization", "Coverage", "CoverageEligibilityRequest",
    ],
    "ImmunizationEvaluation": ["Immunization", "Patient", "Practitioner"],
    "Transport": ["Patient", "Location", "Encounter"],
    "VisionPrescription": ["Patient", "Practitioner", "Encounter"],
    "BiologicallyDerivedProductDispense": ["Patient", "Practitioner", "Location"],
    "DocumentReference": ["Patient", "Practitioner", "Organization"],
    "Medication": [],
    "Substance": [],
    "Device": ["Organization"],
    "Group": [],
    "RelatedPerson": ["Patient"],
    "Schedule": ["Practitioner", "Location"],
    "Slot": ["Schedule"],
    "EpisodeOfCare": ["Patient", "Organization", "Practitioner"],
    "HealthcareService": ["Organization", "Location"],
    "RiskAssessment": ["Patient", "Practitioner", "Encounter"],
    "Task": ["Patient", "Practitioner"],
    "Communication": ["Patient", "Practitioner", "Encounter"],
    "Flag": ["Patient", "Practitioner"],
    "AuditEvent": ["Patient", "Practitioner"],
    "Consent": ["Patient", "Organization"],
    "Contract": ["Patient", "Organization"],
    "NutritionOrder": ["Patient", "Practitioner", "Encounter"],
    "Specimen": ["Patient", "Practitioner"],
    "ImagingStudy": ["Patient", "Practitioner", "Encounter"],
    "FamilyMemberHistory": ["Patient"],
    "ClinicalImpression": ["Patient", "Practitioner", "Encounter"],
    "DetectedIssue": ["Patient", "Practitioner"],
    "QuestionnaireResponse": ["Patient", "Practitioner"],
    "PaymentNotice": ["Organization", "Practitioner"],
    "PaymentReconciliation": ["Organization"],
    "Account": ["Patient", "Organization"],
    "ChargeItem": ["Patient", "Practitioner", "Encounter"],
    "Invoice": ["Patient", "Practitioner", "Organization"],
    "ResearchStudy": ["Organization", "Practitioner"],
    "ResearchSubject": ["Patient", "ResearchStudy"],
}


def _known_resources() -> set[str]:
    return set(registry.all_resources())


def _direct_dependencies(resource_name: str) -> list[str]:
    """CORE_DEPENDENCIES plus schema-derived Reference targets when unknown."""
    known = _known_resources()
    deps = list(CORE_DEPENDENCIES.get(resource_name, []))
    if resource_name not in CORE_DEPENDENCIES:
        try:
            for dep in registry.references_for(resource_name):
                if dep not in deps and dep != resource_name:
                    deps.append(dep)
        except KeyError:
            pass
    return [d for d in deps if d != resource_name and d in known]


def _collect_transitive(resource_name: str, collected: set[str]) -> None:
    """Add resource and all transitive dependencies to collected."""
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
    Includes transitive closure of CORE_DEPENDENCIES and schema references.
    Among ready nodes, prefers foundation types (Organization before Patient, etc.).
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
