"""
Valid default FHIR search query strings per resource for E2E fallbacks and compartments.

Derived from ``src/fhir_search_to_mql/configs/<Resource>.yaml`` — avoid ``status=active``
when a resource has no ``status`` search parameter (e.g. RiskAssessment, AuditEvent).
"""

from __future__ import annotations

# One convertible query per MQL-shipped resource (used for fallbacks + compartment filters).
RESOURCE_SEARCH_QUERIES: dict[str, str] = {
    "Account": "status=active",
    "AdverseEvent": "actuality=actual",
    "AllergyIntolerance": "clinical-status=active",
    "Appointment": "status=booked",
    "AuditEvent": "action=C",
    "Basic": "code=referral",
    "BiologicallyDerivedProduct": "product-status=available",
    "BodyStructure": "patient=p1",
    "CarePlan": "status=active",
    "CareTeam": "status=active",
    "ChargeItem": "status=billable",
    "ChargeItemDefinition": "status=active",
    "Claim": "status=active",
    "ClaimResponse": "outcome=complete",
    "ClinicalImpression": "status=completed",
    "Communication": "status=completed",
    "Composition": "status=final",
    "Condition": "clinical-status=active",
    "Consent": "status=active",
    "Contract": "status=executed",
    "Coverage": "status=active",
    "CoverageEligibilityRequest": "status=active",
    "CoverageEligibilityResponse": "outcome=complete",
    "DetectedIssue": "status=final",
    "Device": "status=active",
    "DeviceDispense": "status=completed",
    "DeviceRequest": "status=active",
    "DeviceUsage": "status=active",
    "DiagnosticReport": "status=final",
    "DocumentReference": "status=current",
    "Encounter": "status=finished",
    "Endpoint": "status=active",
    "EnrollmentRequest": "status=active",
    "EnrollmentResponse": "status=active",
    "EpisodeOfCare": "status=active",
    "ExplanationOfBenefit": "status=active",
    "FamilyMemberHistory": "status=completed",
    "Flag": "status=active",
    "GenomicStudy": "status=registered",
    "Goal": "lifecycle-status=active",
    "Group": "type=person",
    "HealthcareService": "active=true",
    "ImagingStudy": "status=available",
    "Immunization": "status=completed",
    "ImmunizationRecommendation": "status=completed",
    "InsurancePlan": "status=active",
    "Invoice": "status=issued",
    "Location": "status=active",
    "Measure": "status=active",
    "MeasureReport": "status=complete",
    "Medication": "status=active",
    "MedicationAdministration": "status=completed",
    "MedicationDispense": "status=completed",
    "MedicationRequest": "status=active",
    "MedicationStatement": "status=active",
    "NutritionIntake": "status=completed",
    "NutritionOrder": "status=active",
    "Observation": "status=final",
    "Organization": "active=true",
    "OrganizationAffiliation": "active=true",
    "Patient": "active=true",
    "PaymentNotice": "status=active",
    "PaymentReconciliation": "status=active",
    "Person": "name=Smith",
    "Practitioner": "active=true",
    "PractitionerRole": "active=true",
    "Procedure": "status=completed",
    "Provenance": "activity=UPDATE",
    "Questionnaire": "status=active",
    "QuestionnaireResponse": "status=completed",
    "RelatedPerson": "patient=p1",
    "RequestOrchestration": "status=active",
    "ResearchStudy": "status=active",
    "ResearchSubject": "status=active",
    "RiskAssessment": "method=clinical",
    "Schedule": "active=true",
    "ServiceRequest": "status=active",
    "Slot": "status=free",
    "Specimen": "status=available",
    "Substance": "status=active",
    "SupplyDelivery": "status=completed",
    "SupplyRequest": "status=active",
    "Task": "status=in-progress",
    "VisionPrescription": "status=active",
}

# Safe default when a new resource is added without an explicit entry.
DEFAULT_SEARCH_QUERY = "_lastUpdated=ge2020-01-01"


def fallback_query(resource_type: str) -> str:
    """Default query for bundled resources missing explicit scenario searches."""
    return RESOURCE_SEARCH_QUERIES.get(resource_type, DEFAULT_SEARCH_QUERY)


def compartment_query(resource_type: str) -> str:
    """Additional filter for Patient/Encounter/Device compartment searches."""
    return RESOURCE_SEARCH_QUERIES.get(resource_type, DEFAULT_SEARCH_QUERY)
