"""Shared assertions for MQL-shipped (84) resource generation tests."""

from __future__ import annotations

from typing import Any, Iterator

from fhir_gen.resolvers.dependency import MQL_SHIPPED_RESOURCES
from fhir_gen.resolvers.reference import ReferenceStore

# Top-level fields enrichers set on each MQL-shipped resource (when deps exist).
MQL_ENRICHED_FIELDS: dict[str, tuple[str, ...]] = {
    "Account": ("status", "name"),
    "AdverseEvent": ("status", "actuality", "code"),
    "AllergyIntolerance": ("clinicalStatus", "code"),
    "Appointment": ("status", "participant"),
    "AuditEvent": ("action", "recorded", "outcome"),
    "Basic": ("code", "created"),
    "BiologicallyDerivedProduct": ("productCategory", "productStatus"),
    "BodyStructure": ("active", "morphology"),
    "CarePlan": ("status", "subject"),
    "CareTeam": ("status", "participant"),
    "ChargeItem": ("status", "code"),
    "ChargeItemDefinition": ("status", "url"),
    "Claim": ("status", "patient"),
    "ClaimResponse": ("status", "outcome"),
    "ClinicalImpression": ("status", "subject"),
    "Communication": ("status", "payload"),
    "Composition": ("status", "type", "date"),
    "Condition": ("clinicalStatus", "code"),
    "Consent": ("status", "category"),
    "Contract": ("status", "issued"),
    "Coverage": ("status", "beneficiary"),
    "CoverageEligibilityRequest": ("status", "purpose", "patient"),
    "CoverageEligibilityResponse": ("status", "purpose"),
    "DetectedIssue": ("status", "category"),
    "Device": ("name",),
    "DeviceDispense": ("status", "device"),
    "DeviceRequest": ("status", "intent", "code"),
    "DeviceUsage": ("status",),
    "DiagnosticReport": ("status", "code"),
    "DocumentReference": ("status", "content"),
    "Encounter": ("status", "subject"),
    "Endpoint": ("status", "address"),
    "EnrollmentRequest": ("status", "created"),
    "EnrollmentResponse": ("status", "created"),
    "EpisodeOfCare": ("status", "type"),
    "ExplanationOfBenefit": ("status", "type", "use"),
    "FamilyMemberHistory": ("status", "relationship"),
    "Flag": ("status", "code"),
    "GenomicStudy": ("status",),
    "Goal": ("lifecycleStatus", "description"),
    "Group": ("type", "name"),
    "HealthcareService": ("category",),
    "Immunization": ("status", "vaccineCode"),
    "ImmunizationRecommendation": ("date", "recommendation"),
    "ImagingStudy": ("status", "modality"),
    "InsurancePlan": ("status", "name"),
    "Invoice": ("status", "lineItem"),
    "Location": ("status", "name"),
    "Measure": ("status", "title"),
    "MeasureReport": ("status", "type", "measure"),
    "Medication": ("code",),
    "MedicationAdministration": ("status", "medication"),
    "MedicationDispense": ("status", "medication"),
    "MedicationRequest": ("status", "medication"),
    "MedicationStatement": ("status", "medication"),
    "NutritionIntake": ("status", "code"),
    "NutritionOrder": ("status", "intent"),
    "Observation": ("status", "code"),
    "Organization": ("name",),
    "OrganizationAffiliation": ("active", "period"),
    "Patient": ("gender", "name"),
    "PaymentNotice": ("status", "paymentStatus"),
    "PaymentReconciliation": ("status", "outcome"),
    "Person": ("name", "gender"),
    "Practitioner": ("name", "active"),
    "PractitionerRole": ("active", "period"),
    "Procedure": ("status", "code"),
    "Provenance": ("recorded", "activity"),
    "Questionnaire": ("status", "url"),
    "QuestionnaireResponse": ("status", "item", "questionnaire"),
    "RelatedPerson": ("patient", "relationship"),
    "RequestOrchestration": ("status", "intent"),
    "ResearchStudy": ("status", "title"),
    "ResearchSubject": ("status", "study"),
    "RiskAssessment": ("status", "prediction"),
    "Schedule": ("active", "serviceType"),
    "ServiceRequest": ("status", "code"),
    "Slot": ("status", "schedule"),
    "Specimen": ("type", "collection"),
    "Substance": ("status", "code"),
    "SupplyDelivery": ("status",),
    "SupplyRequest": ("status", "category"),
    "Task": ("status", "intent"),
    "VisionPrescription": ("status", "lensSpecification"),
}

_missing = set(MQL_SHIPPED_RESOURCES) - set(MQL_ENRICHED_FIELDS)
if _missing:
    raise RuntimeError(f"MQL_ENRICHED_FIELDS missing: {sorted(_missing)}")


def iter_reference_strings(value: Any) -> Iterator[str]:
    """Yield FHIR reference strings from nested resource JSON."""
    if isinstance(value, dict):
        ref = value.get("reference")
        if isinstance(ref, str):
            yield ref
        for child in value.values():
            yield from iter_reference_strings(child)
    elif isinstance(value, list):
        for item in value:
            yield from iter_reference_strings(item)


def assert_enriched_fields(resource_type: str, resource: dict[str, Any]) -> None:
    """Assert enricher-populated top-level fields are present."""
    assert resource.get("resourceType") == resource_type
    assert resource.get("id")
    for field in MQL_ENRICHED_FIELDS[resource_type]:
        assert field in resource, f"{resource_type} missing enriched field {field!r}"


def assert_references_valid(store: ReferenceStore, resource: dict[str, Any]) -> None:
    """Every Reference.reference in the resource must resolve in the store."""
    for ref in iter_reference_strings(resource):
        if ref.startswith("#"):
            continue
        assert store.reference_is_valid(ref), f"Broken reference {ref!r}"
