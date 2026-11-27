"""
Cross-resource denormalization: R4 flat CodeableConcept vs R5 CodeableReference.

Configs use R5 paths; path_resolver expands legacy branches automatically.
"""

from __future__ import annotations

import pytest

from fhir_search_to_mql import ResourceDenormalizer


@pytest.fixture(scope="module")
def denormalizer() -> ResourceDenormalizer:
    return ResourceDenormalizer()


@pytest.mark.parametrize(
    "resource_type,legacy_resource,expected_search_key,expected_value",
    [
        (
            "Slot",
            {
                "resourceType": "Slot",
                "id": "s1",
                "status": "free",
                "start": "2024-07-15T09:00:00Z",
                "end": "2024-07-15T09:30:00Z",
                "schedule": {"reference": "Schedule/sched-1"},
                "serviceType": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/service-type",
                                "code": "533",
                            }
                        ]
                    }
                ],
            },
            "serviceType_codes",
            "533",
        ),
        (
            "Schedule",
            {
                "resourceType": "Schedule",
                "id": "sch1",
                "active": True,
                "actor": [{"reference": "Practitioner/p1"}],
                "serviceType": [{"coding": [{"system": "http://x", "code": "124"}]}],
            },
            "serviceType_codes",
            "124",
        ),
        (
            "ServiceRequest",
            {
                "resourceType": "ServiceRequest",
                "id": "sr1",
                "status": "active",
                "intent": "order",
                "subject": {"reference": "Patient/p1"},
                "code": {"coding": [{"system": "http://snomed.info/sct", "code": "71388002"}]},
            },
            "codeConcept_codes",
            "71388002",
        ),
        (
            "Device",
            {
                "resourceType": "Device",
                "id": "d1",
                "status": "active",
                "definition": {"coding": [{"system": "http://snomed.info/sct", "code": "706767009"}]},
            },
            "code_codes",
            "706767009",
        ),
        (
            "Condition",
            {
                "resourceType": "Condition",
                "id": "c1",
                "subject": {"reference": "Patient/p1"},
                "code": {"coding": [{"code": "44054006"}]},
                "evidence": [{"coding": [{"code": "ev-1"}]}],
            },
            "evidence_codes",
            "ev-1",
        ),
        (
            "Procedure",
            {
                "resourceType": "Procedure",
                "id": "proc1",
                "status": "completed",
                "subject": {"reference": "Patient/p1"},
                "reason": [{"coding": [{"code": "109006"}]}],
            },
            "reasonCode_codes",
            "109006",
        ),
        (
            "Medication",
            {
                "resourceType": "Medication",
                "id": "med1",
                "status": "active",
                "ingredient": [{"item": {"coding": [{"code": "387517004"}]}}],
            },
            "ingredientCode_codes",
            "387517004",
        ),
        (
            "MedicationRequest",
            {
                "resourceType": "MedicationRequest",
                "id": "mr1",
                "status": "active",
                "intent": "order",
                "subject": {"reference": "Patient/p1"},
                "medication": {"coding": [{"code": "319785009"}]},
            },
            "medicationConcept_codes",
            "319785009",
        ),
        (
            "MedicationAdministration",
            {
                "resourceType": "MedicationAdministration",
                "id": "ma1",
                "status": "completed",
                "subject": {"reference": "Patient/p1"},
                "medication": {"coding": [{"code": "319785009"}]},
            },
            "medicationConcept_codes",
            "319785009",
        ),
        (
            "MedicationDispense",
            {
                "resourceType": "MedicationDispense",
                "id": "md1",
                "status": "completed",
                "subject": {"reference": "Patient/p1"},
                "medication": {"coding": [{"code": "319785009"}]},
            },
            "medicationConcept_codes",
            "319785009",
        ),
        (
            "AllergyIntolerance",
            {
                "resourceType": "AllergyIntolerance",
                "id": "ai1",
                "patient": {"reference": "Patient/p1"},
                "code": {"coding": [{"code": "91935009"}]},
            },
            "code_codes",
            "91935009",
        ),
        (
            "DiagnosticReport",
            {
                "resourceType": "DiagnosticReport",
                "id": "dr1",
                "status": "final",
                "subject": {"reference": "Patient/p1"},
                "code": {"coding": [{"code": "11502-2"}]},
            },
            "code_codes",
            "11502-2",
        ),
        (
            "CareTeam",
            {
                "resourceType": "CareTeam",
                "id": "ct1",
                "status": "active",
                "subject": {"reference": "Patient/p1"},
                "category": [{"coding": [{"code": "LA27976-2"}]}],
            },
            "category_codes",
            "LA27976-2",
        ),
        (
            "Goal",
            {
                "resourceType": "Goal",
                "id": "g1",
                "lifecycleStatus": "active",
                "subject": {"reference": "Patient/p1"},
                "description": {"coding": [{"code": "406156006"}]},
            },
            "description_codes",
            "406156006",
        ),
        (
            "CarePlan",
            {
                "resourceType": "CarePlan",
                "id": "cp1",
                "status": "active",
                "intent": "plan",
                "subject": {"reference": "Patient/p1"},
                "category": [{"coding": [{"code": "assess-plan"}]}],
            },
            "category_codes",
            "assess-plan",
        ),
        (
            "Immunization",
            {
                "resourceType": "Immunization",
                "id": "imm1",
                "status": "completed",
                "patient": {"reference": "Patient/p1"},
                "vaccineCode": {"coding": [{"code": "140"}]},
            },
            "vaccineCode_codes",
            "140",
        ),
        (
            "Coverage",
            {
                "resourceType": "Coverage",
                "id": "cov1",
                "status": "active",
                "beneficiary": {"reference": "Patient/p1"},
                "type": {"coding": [{"code": "EHCPOL"}]},
            },
            "type_codes",
            "EHCPOL",
        ),
        (
            "Claim",
            {
                "resourceType": "Claim",
                "id": "claim1",
                "status": "active",
                "use": "claim",
                "type": {"coding": [{"code": "professional"}]},
                "patient": {"reference": "Patient/p1"},
                "priority": {"coding": [{"code": "normal"}]},
            },
            "priority_codes",
            "normal",
        ),
        (
            "ClaimResponse",
            {
                "resourceType": "ClaimResponse",
                "id": "cr1",
                "status": "active",
                "use": "claim",
                "outcome": "complete",
                "type": {"coding": [{"code": "professional"}]},
                "patient": {"reference": "Patient/p1"},
            },
            "patientId",
            "p1",
        ),
        (
            "DocumentReference",
            {
                "resourceType": "DocumentReference",
                "id": "doc1",
                "status": "current",
                "content": [{"attachment": {"contentType": "text/plain"}}],
                "subject": {"reference": "Patient/p1"},
                "type": {"coding": [{"code": "34117-2"}]},
            },
            "type_codes",
            "34117-2",
        ),
        (
            "Substance",
            {
                "resourceType": "Substance",
                "id": "sub1",
                "status": "active",
                "code": {"concept": {"coding": [{"code": "387517004"}]}},
            },
            "code_codes",
            "387517004",
        ),
        (
            "RelatedPerson",
            {
                "resourceType": "RelatedPerson",
                "id": "rp1",
                "patient": {"reference": "Patient/p1"},
                "relationship": [{"coding": [{"code": "WIFE"}]}],
            },
            "relationship_codes",
            "WIFE",
        ),
        (
            "EpisodeOfCare",
            {
                "resourceType": "EpisodeOfCare",
                "id": "eoc1",
                "patient": {"reference": "Patient/p1"},
                "type": [{"coding": [{"code": "hacc"}]}],
            },
            "type_codes",
            "hacc",
        ),
        (
            "HealthcareService",
            {
                "resourceType": "HealthcareService",
                "id": "hs1",
                "name": "Clinic",
                "category": [{"coding": [{"code": "17"}]}],
            },
            "category_codes",
            "17",
        ),
        (
            "RiskAssessment",
            {
                "resourceType": "RiskAssessment",
                "id": "ra1",
                "subject": {"reference": "Patient/p1"},
                "method": {"coding": [{"code": "clinical"}]},
            },
            "method_codes",
            "clinical",
        ),
        (
            "Task",
            {
                "resourceType": "Task",
                "id": "task1",
                "for": {"reference": "Patient/p1"},
                "code": {"coding": [{"code": "103693007"}]},
            },
            "code_codes",
            "103693007",
        ),
        (
            "Communication",
            {
                "resourceType": "Communication",
                "id": "comm1",
                "subject": {"reference": "Patient/p1"},
                "category": [{"coding": [{"code": "notification"}]}],
            },
            "category_codes",
            "notification",
        ),
        (
            "Flag",
            {
                "resourceType": "Flag",
                "id": "flag1",
                "status": "active",
                "code": {"coding": [{"code": "304379003"}]},
                "subject": {"reference": "Patient/p1"},
                "category": [{"coding": [{"code": "safety"}]}],
            },
            "category_codes",
            "safety",
        ),
        (
            "AuditEvent",
            {
                "resourceType": "AuditEvent",
                "id": "ae1",
                "action": "R",
                "code": {"coding": [{"code": "110100"}]},
                "agent": [{"who": {"reference": "Practitioner/p1"}}],
                "source": {"observer": {"reference": "Device/d1"}},
                "patient": {"reference": "Patient/p1"},
            },
            "code_codes",
            "110100",
        ),
        (
            "Consent",
            {
                "resourceType": "Consent",
                "id": "consent1",
                "status": "active",
                "subject": {"reference": "Patient/p1"},
                "provision": [{"purpose": [{"code": "PATRQT"}]}],
            },
            "provisionPurpose_codes",
            "PATRQT",
        ),
        (
            "Contract",
            {
                "resourceType": "Contract",
                "id": "ctr1",
                "status": "executed",
                "subject": [{"reference": "Patient/p1"}],
                "identifier": [{"value": "CTR-001"}],
            },
            "identifier_values",
            "CTR-001",
        ),
        (
            "NutritionOrder",
            {
                "resourceType": "NutritionOrder",
                "id": "no1",
                "status": "active",
                "subject": {"reference": "Patient/p1"},
                "oralDiet": {"type": [{"coding": [{"code": "226211001"}]}]},
            },
            "oralDietType_codes",
            "226211001",
        ),
        (
            "Specimen",
            {
                "resourceType": "Specimen",
                "id": "sp1",
                "status": "available",
                "subject": {"reference": "Patient/p1"},
                "type": {"coding": [{"code": "119297000"}]},
            },
            "type_codes",
            "119297000",
        ),
        (
            "ImagingStudy",
            {
                "resourceType": "ImagingStudy",
                "id": "img1",
                "status": "available",
                "subject": {"reference": "Patient/p1"},
                "series": [{"uid": "1.2.3.4.5", "modality": {"coding": [{"code": "CT"}]}}],
            },
            "seriesModality_codes",
            "CT",
        ),
        (
            "FamilyMemberHistory",
            {
                "resourceType": "FamilyMemberHistory",
                "id": "fmh1",
                "status": "completed",
                "patient": {"reference": "Patient/p1"},
                "relationship": {"coding": [{"code": "FTH"}]},
                "condition": [{"code": {"coding": [{"code": "44054006"}]}}],
            },
            "conditionCode_codes",
            "44054006",
        ),
        (
            "ClinicalImpression",
            {
                "resourceType": "ClinicalImpression",
                "id": "ci1",
                "status": "completed",
                "subject": {"reference": "Patient/p1"},
                "finding": [
                    {"item": {"concept": {"coding": [{"code": "386661006"}]}}}
                ],
            },
            "findingConcept_codes",
            "386661006",
        ),
        (
            "DetectedIssue",
            {
                "resourceType": "DetectedIssue",
                "id": "di1",
                "status": "final",
                "subject": {"reference": "Patient/p1"},
                "code": {"coding": [{"code": "DRG"}]},
            },
            "code_codes",
            "DRG",
        ),
        (
            "QuestionnaireResponse",
            {
                "resourceType": "QuestionnaireResponse",
                "id": "qr1",
                "status": "completed",
                "questionnaire": "Questionnaire/q1",
                "subject": {"reference": "Patient/p1"},
            },
            "questionnaire_values",
            "Questionnaire/q1",
        ),
        (
            "PaymentNotice",
            {
                "resourceType": "PaymentNotice",
                "id": "pn1",
                "status": "active",
                "amount": {"value": 50, "currency": "USD"},
                "recipient": {"reference": "Organization/org1"},
                "paymentStatus": {"coding": [{"code": "paid"}]},
            },
            "paymentStatus_codes",
            "paid",
        ),
        (
            "PaymentReconciliation",
            {
                "resourceType": "PaymentReconciliation",
                "id": "pr1",
                "status": "active",
                "outcome": "complete",
                "type": {"coding": [{"code": "payment"}]},
                "amount": {"value": 100, "currency": "USD"},
                "requestor": {"reference": "Practitioner/p1"},
            },
            "requestorId",
            "p1",
        ),
        (
            "Account",
            {
                "resourceType": "Account",
                "id": "acct1",
                "status": "active",
                "subject": [{"reference": "Patient/p1"}],
                "type": {"coding": [{"code": "PBILLACCT"}]},
            },
            "type_codes",
            "PBILLACCT",
        ),
        (
            "ChargeItem",
            {
                "resourceType": "ChargeItem",
                "id": "ci1",
                "status": "billable",
                "code": {"coding": [{"code": "99213"}]},
                "subject": {"reference": "Patient/p1"},
            },
            "code_codes",
            "99213",
        ),
        (
            "Invoice",
            {
                "resourceType": "Invoice",
                "id": "inv1",
                "status": "issued",
                "type": {"coding": [{"code": "invoice"}]},
                "subject": {"reference": "Patient/p1"},
            },
            "type_codes",
            "invoice",
        ),
        (
            "ResearchStudy",
            {
                "resourceType": "ResearchStudy",
                "id": "rs1",
                "status": "active",
                "title": "Legacy Study",
                "condition": [{"coding": [{"code": "38341003"}]}],
            },
            "condition_codes",
            "38341003",
        ),
        (
            "ResearchSubject",
            {
                "resourceType": "ResearchSubject",
                "id": "rsub1",
                "status": "active",
                "study": {"reference": "ResearchStudy/rs1"},
                "subject": {"reference": "Patient/p1"},
                "progress": [
                    {"subjectState": {"coding": [{"code": "screening"}]}}
                ],
            },
            "progressSubjectState_codes",
            "screening",
        ),
        (
            "Composition",
            {
                "resourceType": "Composition",
                "id": "comp1",
                "status": "final",
                "type": {"coding": [{"code": "18842-5"}]},
                "author": [{"reference": "Practitioner/p1"}],
            },
            "type_codes",
            "18842-5",
        ),
        (
            "Questionnaire",
            {
                "resourceType": "Questionnaire",
                "id": "quest1",
                "status": "active",
                "jurisdiction": [{"coding": [{"code": "US"}]}],
            },
            "jurisdiction_codes",
            "US",
        ),
    ],
)
def test_legacy_codeable_concept_denormalizes(
    denormalizer,
    resource_type: str,
    legacy_resource: dict,
    expected_search_key: str,
    expected_value: str,
):
    search = denormalizer.denormalize(legacy_resource).get("_search", {})
    assert expected_value in search.get(expected_search_key, [])
