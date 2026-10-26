"""Context-aware clinical narrative strings for synthetic FHIR data."""

from __future__ import annotations

import random
from typing import Sequence

# ── Generic fallbacks ───────────────────────────────────────────────────────

_GENERIC: tuple[str, ...] = (
    "Clinical documentation reviewed and updated per care team.",
    "Patient assessment completed; plan of care discussed with patient.",
    "Vital signs within expected range for current clinical status.",
    "Medication reconciliation performed at point of care.",
    "Care coordination note documented for continuity of care.",
    "History and physical findings documented in the medical record.",
    "Patient goals of care reviewed with multidisciplinary team.",
    "Orders placed per established clinical pathway.",
    "Results communicated to ordering provider per policy.",
    "Transition-of-care summary sent to primary care physician.",
)

# ── Field-name templates (cross-resource) ───────────────────────────────────

_FIELD_TEMPLATES: dict[str, tuple[str, ...]] = {
    "description": (
        # Outpatient & primary care
        "Follow-up visit for ongoing condition management.",
        "Routine assessment and care coordination documented.",
        "Outpatient evaluation with review of treatment plan.",
        "Annual wellness examination with preventive screening.",
        "Telehealth visit for medication refill and symptom review.",
        # Acute & emergency
        "Emergency department evaluation for acute symptom onset.",
        "Urgent care visit for minor injury and wound care.",
        "Observation stay for rule-out of acute coronary syndrome.",
        # Inpatient
        "Inpatient admission for medical management and monitoring.",
        "Postoperative day 1 assessment; incision clean and dry.",
        "ICU transfer for hemodynamic monitoring and ventilation support.",
        # Diagnostics
        "Diagnostic workup ordered for abnormal screening result.",
        "Imaging study performed with IV contrast per protocol.",
        "Laboratory panel collected for chronic disease monitoring.",
        # Medication & pharmacy
        "Medication therapy management visit documented.",
        "Anticoagulation clinic visit for INR review and dosing.",
        "Discharge medication list reconciled with outpatient regimen.",
        # Behavioral & maternal
        "Behavioral health intake for anxiety and mood assessment.",
        "Prenatal visit at 28 weeks; fetal heart tones documented.",
        # Rehab & home health
        "Physical therapy session for gait training after hip replacement.",
        "Home health skilled nursing visit for wound dressing change.",
        # Billing & admin
        "Clinical service documented for billing and quality reporting.",
        "Professional fee claim for established patient office visit.",
        # Industrial / enterprise
        "Bed management note: patient placed on medical-surgical unit.",
        "Operating room schedule updated for elective procedure block.",
        "Revenue cycle hold released after coding query resolved.",
        "Payer eligibility confirmed for scheduled elective admission.",
        "Supply chain replenishment triggered for critical par level.",
        "Clinical trial screening visit completed per protocol.",
        "Population health outreach for overdue colorectal screening.",
    ),
    "comment": (
        "Patient tolerated procedure without immediate complications.",
        "Specimen collected per laboratory protocol.",
        "Nursing note: patient ambulating with assistance.",
        "Pharmacy verified dose and route prior to administration.",
        "Discharge instructions reviewed with patient and caregiver.",
        "Fall precautions in place; bed alarm activated.",
        "Isolation precautions maintained per infection control policy.",
        "Blood product transfusion completed without reaction.",
        "Device alarm silenced after clinical assessment.",
        "Prior authorization reference number attached to order.",
        "Charge capture corrected after CDI review.",
        "Adverse event reported to pharmacovigilance mailbox.",
        "Genomic consent on file for germline sequencing.",
        "Audit log entry created for record access.",
    ),
    "conclusion": (
        "Imaging findings consistent with prior study; no acute abnormality.",
        "Laboratory results reviewed; values within reference range.",
        "Diagnostic impression supports continued outpatient management.",
        "Report finalized after attending physician review.",
        "Mild degenerative changes; correlate clinically.",
        "No evidence of acute intracranial process.",
        "Stable chronic findings; recommend routine follow-up imaging.",
        "Critical result called to ordering provider and documented.",
        "Pathology consistent with benign process.",
        "EKG shows normal sinus rhythm without acute ischemic changes.",
    ),
    "title": (
        "Care Plan Summary",
        "Visit Documentation",
        "Clinical Note",
        "Results Review",
        "Treatment Update",
        "Discharge Summary",
        "Operative Report",
        "Consultation Note",
        "Progress Note",
        "Nursing Assessment",
        "Quality Measure Report",
        "Prior Authorization Request",
        "Device Implant Record",
        "Research Visit Note",
    ),
    "detail": (
        "Monitor blood pressure twice daily and log readings.",
        "Continue prescribed therapy and return if symptoms worsen.",
        "Follow up with primary care within two weeks.",
        "Patient education provided on medication adherence.",
        "Complete pulmonary rehabilitation sessions three times weekly.",
        "Check wound daily for redness, swelling, or drainage.",
        "Maintain carbohydrate-controlled diet per diabetes educator.",
        "Avoid NSAIDs while on anticoagulation therapy.",
        "Submit prior auth documentation within 48 hours.",
        "Calibrate infusion pump before next chemotherapy cycle.",
    ),
    "note": (
        "Patient reports improved symptoms since last visit.",
        "Allergies reviewed; no new adverse reactions reported.",
        "Social history updated during intake.",
        "Advance directive on file and reviewed with patient.",
        "Smoking cessation counseling provided today.",
        "Caregiver present and involved in plan of care.",
        "Interpreter services used for informed consent.",
        "Patient declined recommended screening at this time.",
        "Insurance plan changed; verify benefits before scheduling.",
        "Cohort risk score updated for population health registry.",
    ),
    "reason": (
        "Chief complaint: worsening shortness of breath.",
        "Referral for specialist evaluation of persistent symptoms.",
        "Preventive care visit per annual wellness schedule.",
        "Preoperative clearance for elective joint replacement.",
        "Workers compensation evaluation for occupational injury.",
        "Second opinion requested for treatment plan.",
        "Readmission risk mitigation within 7 days of discharge.",
        "Prior authorization required for advanced imaging.",
    ),
    "summary": (
        "Stable chronic conditions with no acute concerns today.",
        "Acute complaint evaluated; supportive care initiated.",
        "Hospital course complicated by healthcare-associated infection.",
        "Patient improved and ready for discharge to home.",
        "Measure numerator met for diabetes HbA1c control.",
    ),
    "purpose": (
        "Eligibility verification for covered health plan benefits.",
        "Prior authorization supporting documentation.",
        "Utilization review for inpatient level of care.",
        "Claims adjudication supporting medical necessity.",
        "Regulatory audit sample for HIPAA access controls.",
        "Device tracking for recall notification.",
    ),
    "display": (
        "General medical examination",
        "Laboratory panel",
        "Therapeutic procedure",
        "Chronic disease management",
        "Behavioral health assessment",
        "Surgical procedure",
        "Radiology study",
        "Immunization administration",
        "Revenue cycle service",
        "Clinical trial procedure",
    ),
    "text": (
        "Do you experience chest pain with exertion?",
        "Have you taken all prescribed medications this week?",
        "Rate your pain on a scale of 0 to 10 today.",
        "Have you had a fever in the last 48 hours?",
        "Are you currently taking blood thinners?",
        "Do you have a history of diabetes or high blood pressure?",
        "Have you fallen in the past 12 months?",
        "Are you pregnant or could you be pregnant?",
        "Do you use tobacco products?",
        "Have you completed your recommended cancer screenings?",
        "Did you receive your flu vaccine this season?",
        "Are you able to perform activities of daily living independently?",
    ),
    "name": (
        "Outpatient Clinic",
        "Medical Center",
        "Community Hospital",
        "Ambulatory Surgery Center",
        "Cancer Center Infusion Suite",
        "Behavioral Health Pavilion",
        "Women's Health Center",
        "Rehabilitation Institute",
        "Blood Bank Processing Lab",
        "Population Health Command Center",
    ),
    "contentstring": (
        "Appointment reminder: please arrive 15 minutes early for registration.",
        "Your lab results are available in the patient portal.",
        "Please contact the clinic if you develop fever or worsening symptoms.",
        "Prior authorization approved; scheduling may proceed.",
        "Claim processed; explanation of benefits mailed.",
        "Device maintenance due; contact biomedical engineering.",
        "Research coordinator: please complete week 4 diary.",
        "Cohort alert: preventive care gap identified for your panel.",
    ),
}

# ── Resource + field overrides (most specific) ──────────────────────────────

_RESOURCE_FIELD: dict[str, dict[str, tuple[str, ...]]] = {
    "Patient": {
        "note": (
            "Patient portal activated; identity verified with photo ID.",
            "Preferred language documented as Spanish.",
            "Emergency contact updated at registration.",
        ),
    },
    "Encounter": {
        "description": (
            "Outpatient encounter for chronic disease follow-up.",
            "Emergency department visit for acute symptom evaluation.",
            "Inpatient admission for observation and treatment.",
            "Surgical encounter for laparoscopic cholecystectomy.",
            "Behavioral health crisis intervention encounter.",
            "Maternity triage encounter at 36 weeks gestation.",
            "Rehabilitation day program encounter for stroke recovery.",
            "Hospital operations: bed assigned from ED overflow queue.",
        ),
    },
    "Appointment": {
        "description": (
            "Annual wellness visit scheduled with primary care.",
            "Orthopedic follow-up for postoperative assessment.",
            "Cardiology referral appointment for stress test review.",
            "MRI appointment with contrast; NPO instructions provided.",
            "Oncology infusion chair reserved for cycle 3 chemotherapy.",
            "Capacity planning: OR block released to outpatient clinic.",
        ),
    },
    "Observation": {
        "comment": (
            "Fasting sample; patient confirmed NPO since midnight.",
            "Point-of-care test performed at bedside.",
            "Critical high glucose; provider notified per protocol.",
            "Specimen hemolyzed; recollection ordered.",
            "Population health: HbA1c above target for diabetes cohort.",
        ),
    },
    "Condition": {
        "note": (
            "Problem list entry reconciled with patient-reported history.",
            "Clinical status updated after specialist consultation.",
            "Chronic kidney disease stage documented from recent labs.",
            "Hypertension listed as active problem on care plan.",
        ),
    },
    "Procedure": {
        "description": (
            "Colonoscopy with polypectomy performed without complication.",
            "Central venous catheter placement under ultrasound guidance.",
            "Joint injection for osteoarthritis pain management.",
        ),
    },
    "ServiceRequest": {
        "description": (
            "Referral to cardiology for chest pain evaluation.",
            "Home health nursing for post-discharge wound care.",
            "Prior authorization requested for PET-CT staging study.",
        ),
    },
    "MedicationRequest": {
        "note": (
            "Generic substitution permitted per formulary policy.",
            "Counseling provided on administration and common side effects.",
            "Opioid agreement signed; PDMP reviewed.",
            "Pharmacy safety: duplicate therapy alert overridden with rationale.",
        ),
    },
    "MedicationDispense": {
        "description": (
            "Dispensed 30-day supply; patient counseled on once-daily dosing.",
            "Partial fill due to insurance days-supply limit.",
            "Specialty pharmacy shipment for biologic therapy.",
        ),
    },
    "DetectedIssue": {
        "detail": (
            "Drug-drug interaction: ACE inhibitor with potassium supplement.",
            "Allergy conflict: penicillin ordered; allergy on file.",
            "Dose exceeds usual maximum for renal function.",
        ),
    },
    "AdverseEvent": {
        "description": (
            "Mild rash after antibiotic administration; antihistamine given.",
            "Post-vaccination syncope; recovered after supine positioning.",
            "Device-related pressure injury reported during hospital stay.",
        ),
    },
    "Immunization": {
        "description": (
            "Influenza vaccine administered in left deltoid.",
            "COVID-19 booster per CDC schedule.",
            "Tdap given during prenatal visit.",
        ),
    },
    "DiagnosticReport": {
        "conclusion": (
            "No acute cardiopulmonary abnormality identified.",
            "Mild degenerative changes; correlate clinically.",
            "BI-RADS Category 2: benign finding; routine screening.",
            "Critical lab: potassium 6.2 mmol/L; repeat sent.",
        ),
    },
    "Specimen": {
        "description": (
            "Blood draw for type and screen prior to surgery.",
            "Tissue biopsy sent to pathology in formalin.",
            "Throat swab for respiratory panel.",
        ),
    },
    "CarePlan": {
        "description": (
            "Goals include glycemic control, blood pressure management, and annual screening.",
            "Post-discharge plan emphasizes wound care and physical therapy adherence.",
            "Heart failure plan: daily weights and low-sodium diet education.",
            "COPD action plan with inhaler technique reinforcement.",
        ),
    },
    "CareTeam": {
        "description": (
            "Multidisciplinary team for complex chronic care management.",
            "Transplant team includes hepatology, surgery, and social work.",
        ),
    },
    "Goal": {
        "description": (
            "Patient goal: walk 30 minutes daily without shortness of breath.",
            "Target HbA1c below 7% within six months.",
        ),
    },
    "Task": {
        "description": (
            "Complete medication reconciliation before discharge.",
            "Schedule follow-up imaging within 30 days.",
            "Bed turnover cleaning requested for incoming admission.",
            "Coding query: clarify principal diagnosis for DRG.",
            "Recall notification for implanted device firmware update.",
        ),
    },
    "Communication": {
        "payload": (
            "Care team message: please review updated treatment plan.",
            "Patient portal message regarding upcoming appointment.",
            "Payer request: submit clinical notes for medical necessity review.",
            "Interop alert: outside records available from regional HIE.",
        ),
    },
    "QuestionnaireResponse": {
        "text": (
            "Do you have chest pain at rest?",
            "Are you currently taking blood thinners?",
            "Have you had fever in the last 48 hours?",
            "PHQ-9: little interest or pleasure in doing things?",
            "Audit-C: how often do you have a drink containing alcohol?",
        ),
    },
    "Location": {
        "name": (
            "East Wing Outpatient Clinic",
            "Radiology Suite",
            "Ambulatory Surgery Center",
            "ED Fast Track Bay 4",
            "NICU Level III Unit",
            "Central Sterile Supply",
            "Blood Bank Refrigerator A2",
        ),
    },
    "Organization": {
        "name": (
            "Regional Medical Center",
            "Community Health Network",
            "National Payer Services",
            "Academic Medical Partners",
        ),
    },
    "HealthcareService": {
        "description": (
            "Cardiology consultation service accepting internal referrals.",
            "Population health chronic care management program.",
            "FHIR bulk data export endpoint for payer analytics.",
        ),
    },
    "Endpoint": {
        "description": (
            "Production FHIR R4 base URL for clinical data exchange.",
            "Sandbox endpoint for partner certification testing.",
        ),
    },
    "OrganizationAffiliation": {
        "description": (
            "Hospital system affiliation with community clinic network.",
            "Payer-provider contracted network participation.",
        ),
    },
    "ChargeItem": {
        "description": (
            "Office visit, established patient, moderate complexity.",
            "Laboratory test, comprehensive metabolic panel.",
            "Revenue cycle: facility fee for observation hours.",
            "Surgical supply implant charge with device identifier.",
        ),
    },
    "Invoice": {
        "note": (
            "Patient responsibility after insurance adjudication.",
            "Self-pay discount applied per financial assistance policy.",
        ),
    },
    "Account": {
        "description": (
            "Inpatient account opened for admission; guarantor verified.",
            "Outpatient account for same-day surgery bundle.",
        ),
    },
    "Claim": {
        "description": (
            "Professional services for office visit and evaluation.",
            "Facility charges for outpatient diagnostic testing.",
            "Institutional claim for inpatient stay with procedure codes.",
            "Pharmacy claim for specialty medication fill.",
        ),
    },
    "ClaimResponse": {
        "description": (
            "Claim accepted with contracted fee schedule adjustment.",
            "Partial denial: medical necessity documentation requested.",
        ),
    },
    "Coverage": {
        "description": (
            "Commercial PPO primary coverage effective this plan year.",
            "Medicare Part B coverage for outpatient services.",
            "Medicaid managed care plan for pediatric member.",
        ),
    },
    "CoverageEligibilityRequest": {
        "description": (
            "Eligibility check for scheduled inpatient admission.",
            "Benefits verification for durable medical equipment.",
        ),
    },
    "CoverageEligibilityResponse": {
        "description": (
            "Active coverage confirmed; copay and deductible returned.",
            "Member not eligible on date of service; coverage termed.",
        ),
    },
    "ExplanationOfBenefit": {
        "description": (
            "Patient responsibility includes deductible and coinsurance.",
            "Allowed amount based on in-network contracted rate.",
        ),
    },
    "PaymentNotice": {
        "description": (
            "Remittance advice posted for professional claim batch.",
        ),
    },
    "PaymentReconciliation": {
        "description": (
            "ERA reconciliation completed for payer remittance file.",
        ),
    },
    "Device": {
        "description": (
            "Implantable cardioverter-defibrillator with remote monitoring.",
            "Infusion pump asset tag assigned to oncology unit.",
            "Industrial: device UDI tracked in asset management system.",
        ),
    },
    "DeviceRequest": {
        "description": (
            "Order for home continuous positive airway pressure device.",
            "Request for wheelchair evaluation and fitting.",
        ),
    },
    "DeviceUsage": {
        "description": (
            "Patient using home glucose monitor twice daily.",
            "Ventilator support documented in ICU flowsheet.",
        ),
    },
    "DeviceDispense": {
        "description": (
            "Dispensed insulin pen needles per prescription.",
        ),
    },
    "SupplyRequest": {
        "description": (
            "Restock ward supplies per par level review.",
            "Blood bank request for packed red cells, type-specific.",
            "PPE replenishment for emergency department.",
        ),
    },
    "SupplyDelivery": {
        "description": (
            "Supply delivery received and logged to central stores.",
            "Blood product delivery to OR fridge with temperature log.",
        ),
    },
    "BiologicallyDerivedProduct": {
        "description": (
            "Leukoreduced packed red blood cells, Rh compatible.",
            "Platelet unit from apheresis collection.",
        ),
    },
    "ResearchStudy": {
        "description": (
            "Phase II trial evaluating safety and efficacy of investigational therapy.",
            "Observational RWE study of anticoagulation in atrial fibrillation.",
            "Registry protocol for rare disease natural history.",
        ),
    },
    "ResearchSubject": {
        "description": (
            "Screening visit completed; inclusion criteria met.",
            "Subject withdrawn at patient request after randomization.",
        ),
    },
    "GenomicStudy": {
        "description": (
            "Whole-exome sequencing for hereditary cancer panel.",
            "Pharmacogenomic testing for CYP2C19 metabolizer status.",
        ),
    },
    "Measure": {
        "description": (
            "HEDIS measure: colorectal cancer screening in adults.",
            "CMS quality measure for hospital readmission reduction.",
        ),
    },
    "MeasureReport": {
        "description": (
            "Quality measure performance for reporting period.",
            "Provider panel scorecard for diabetes blood pressure control.",
            "Regulatory submission package for MIPS reporting.",
        ),
    },
    "AuditEvent": {
        "description": (
            "User accessed patient chart from authorized workstation.",
            "Bulk export job completed for interoperability compliance.",
            "Failed login attempt flagged by security monitoring.",
        ),
    },
    "Provenance": {
        "description": (
            "Record imported from regional health information exchange.",
            "Data lineage: transformed from legacy HL7 v2 feed.",
        ),
    },
    "Group": {
        "description": (
            "Diabetes registry cohort for outreach campaign.",
            "Risk stratification group: high readmission probability.",
            "Employer population panel for annual wellness incentives.",
        ),
    },
    "RiskAssessment": {
        "description": (
            "Ten-year ASCVD risk calculated using pooled cohort equations.",
            "Fall risk assessment: high risk; PT consult ordered.",
            "Population analytics: elevated likelihood of unplanned admission.",
        ),
    },
    "Composition": {
        "title": (
            "Discharge Summary",
            "History and Physical",
            "Operative Note",
            "Emergency Department Provider Note",
        ),
    },
    "ClinicalImpression": {
        "description": (
            "Assessment: likely viral upper respiratory infection.",
            "Impression: stable heart failure on current regimen.",
        ),
    },
    "FamilyMemberHistory": {
        "note": (
            "Mother with type 2 diabetes diagnosed at age 52.",
            "Father with early myocardial infarction at age 48.",
        ),
    },
    "Subscription": {
        "description": (
            "Webhook subscription for Appointment create and update events.",
        ),
    },
    "Basic": {
        "text": (
            "Custom registry flag: complex care management enrollment.",
        ),
    },
}

_QUESTIONNAIRE_ANSWERS: tuple[str, ...] = (
    "No",
    "Yes",
    "Sometimes",
    "Not applicable",
    "Never",
    "Rarely",
    "Often",
    "Daily",
    "Mild",
    "Moderate",
    "Severe",
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "Within the past 2 weeks",
    "More than half the days",
    "Patient declined to answer",
)


def _normalize_field(field_name: str | None) -> str | None:
    if not field_name:
        return None
    key = field_name.lower()
    if key == "contentstring":
        return "contentstring"
    return key


def _pick(rng: random.Random, options: Sequence[str]) -> str:
    return rng.choice(tuple(options))


def clinical_text(
    rng: random.Random,
    *,
    resource_type: str | None = None,
    field_name: str | None = None,
    max_length: int = 200,
) -> str:
    """Return a clinically plausible string for a FHIR resource field."""
    fn = _normalize_field(field_name)
    candidates: Sequence[str] = ()

    if resource_type and fn:
        candidates = _RESOURCE_FIELD.get(resource_type, {}).get(fn, ())
        if not candidates and fn == "contentstring":
            candidates = _RESOURCE_FIELD.get(resource_type, {}).get("payload", ())

    if not candidates and fn == "answer":
        candidates = _QUESTIONNAIRE_ANSWERS

    if not candidates and fn and fn in _FIELD_TEMPLATES:
        candidates = _FIELD_TEMPLATES[fn]

    if not candidates:
        candidates = _GENERIC

    text = _pick(rng, candidates)
    if len(text) > max_length:
        text = text[: max(1, max_length - 3)].rstrip() + "..."
    return text


def clinical_paragraph(
    rng: random.Random,
    *,
    resource_type: str | None = None,
    field_name: str | None = "description",
    sentences: int = 2,
    max_length: int = 500,
) -> str:
    parts = [
        clinical_text(rng, resource_type=resource_type, field_name=field_name)
        for _ in range(max(1, sentences))
    ]
    text = " ".join(parts)
    return text[:max_length] if len(text) > max_length else text


def clinical_short_label(
    rng: random.Random,
    *,
    resource_type: str | None = None,
    field_name: str | None = "display",
) -> str:
    return clinical_text(
        rng, resource_type=resource_type, field_name=field_name, max_length=60
    )


def clinical_question_text(rng: random.Random) -> str:
    return _pick(rng, _FIELD_TEMPLATES["text"])


def clinical_question_answer(rng: random.Random) -> str:
    return _pick(rng, _QUESTIONNAIRE_ANSWERS)


def clinical_markdown(
    rng: random.Random,
    *,
    resource_type: str | None = None,
    field_name: str | None = "note",
) -> str:
    heading = clinical_short_label(rng, resource_type=resource_type, field_name="title")
    body = clinical_paragraph(
        rng, resource_type=resource_type, field_name=field_name, sentences=2
    )
    return f"## {heading}\n\n{body}"


def clinical_xhtml(
    rng: random.Random,
    *,
    resource_type: str | None = None,
    field_name: str | None = "note",
) -> str:
    text = clinical_text(rng, resource_type=resource_type, field_name=field_name)
    return f'<div xmlns="http://www.w3.org/1999/xhtml"><p>{text}</p></div>'
