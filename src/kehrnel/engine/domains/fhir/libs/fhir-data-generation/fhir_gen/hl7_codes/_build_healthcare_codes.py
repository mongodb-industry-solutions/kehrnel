"""Build healthcare_codes.yaml from FHIR R5 / HL7 terminology standard code systems."""
from __future__ import annotations

import copy
from pathlib import Path

import yaml

OUT = Path(__file__).with_name("healthcare_codes.yaml")

# --- helpers -----------------------------------------------------------------

def cs(system: str, codes: list[tuple[str, str]], binding: str = "REQUIRED") -> dict:
    return {
        "system": system,
        "binding": binding,
        "codes": [{"code": c, "display": d} for c, d in codes],
    }


def alias_section(data: dict, name: str, target: str) -> None:
    if target in data:
        data[name] = copy.deepcopy(data[target])


# --- new / extended sections (FHIR R5 + terminology.hl7.org) ---------------

NEW_SECTIONS: dict[str, dict] = {}

# Demographics & primitives
NEW_SECTIONS["mime_types"] = cs(
    "http://terminology.hl7.org/CodeSystem/mimetypes",
    [
        ("application/pdf", "PDF"),
        ("application/fhir+json", "FHIR JSON"),
        ("application/json", "JSON"),
        ("application/xml", "XML"),
        ("text/plain", "Plain Text"),
        ("text/html", "HTML"),
        ("image/jpeg", "JPEG"),
        ("image/png", "PNG"),
        ("image/dicom", "DICOM"),
        ("audio/mpeg", "MPEG Audio"),
        ("video/mp4", "MP4 Video"),
    ],
    "EXAMPLE",
)

NEW_SECTIONS["participation_type"] = cs(
    "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
    [
        ("ATND", "attender"),
        ("ADM", "admitter"),
        ("SBJ", "subject"),
        ("PART", "Participation"),
        ("REF", "referrer"),
        ("IRCP", "information recipient"),
    ],
    "EXTENSIBLE",
)

NEW_SECTIONS["contact_relationship"] = cs(
    "http://terminology.hl7.org/CodeSystem/v2-0131",
    [
        ("C", "Emergency Contact"),
        ("E", "Employer"),
        ("F", "Federal Agency"),
        ("I", "Insurance Company"),
        ("N", "Next-of-Kin"),
        ("S", "State Agency"),
        ("U", "Unknown"),
        ("O", "Other"),
        ("P", "Parent"),
        ("G", "Guardian"),
    ],
    "EXTENSIBLE",
)

# Encounter
NEW_SECTIONS["encounter_status"] = cs(
    "http://hl7.org/fhir/encounter-status",
    [
        ("planned", "Planned"),
        ("in-progress", "In Progress"),
        ("on-hold", "On Hold"),
        ("discharged", "Discharged"),
        ("completed", "Completed"),
        ("cancelled", "Cancelled"),
        ("discontinued", "Discontinued"),
        ("entered-in-error", "Entered in Error"),
        ("unknown", "Unknown"),
    ],
)

NEW_SECTIONS["encounter_class"] = cs(
    "http://terminology.hl7.org/CodeSystem/v3-ActCode",
    [
        ("AMB", "ambulatory"),
        ("EMER", "emergency"),
        ("FLD", "field"),
        ("HH", "home health"),
        ("IMP", "inpatient encounter"),
        ("ACUTE", "inpatient acute"),
        ("NONAC", "inpatient non-acute"),
        ("OBSENC", "observation encounter"),
        ("PRENC", "pre-admission"),
        ("SS", "short stay"),
        ("VR", "virtual"),
    ],
    "EXTENSIBLE",
)

# Observation
NEW_SECTIONS["observation_status"] = cs(
    "http://hl7.org/fhir/observation-status",
    [
        ("registered", "Registered"),
        ("preliminary", "Preliminary"),
        ("final", "Final"),
        ("amended", "Amended"),
        ("corrected", "Corrected"),
        ("cancelled", "Cancelled"),
        ("entered-in-error", "Entered in Error"),
        ("unknown", "Unknown"),
    ],
)

NEW_SECTIONS["observation_categories"] = cs(
    "http://terminology.hl7.org/CodeSystem/observation-category",
    [
        ("vital-signs", "Vital Signs"),
        ("imaging", "Imaging"),
        ("laboratory", "Laboratory"),
        ("procedure", "Procedure"),
        ("survey", "Survey"),
        ("exam", "Exam"),
        ("therapy", "Therapy"),
        ("activity", "Activity"),
        ("social-history", "Social History"),
    ],
    "EXTENSIBLE",
)

NEW_SECTIONS["loinc_observations"] = {
    "system": "http://loinc.org",
    "binding": "PREFERRED",
    "codes": [
        {"code": "8867-4", "display": "Heart rate", "value": 72, "unit": "beats/min", "low": 60, "high": 100},
        {"code": "9279-1", "display": "Respiratory rate", "value": 16, "unit": "breaths/min", "low": 12, "high": 20},
        {"code": "8310-5", "display": "Body temperature", "value": 37.0, "unit": "Cel", "low": 36.1, "high": 37.8},
        {"code": "85354-9", "display": "Blood pressure panel", "value": 120, "unit": "mm[Hg]", "low": 90, "high": 140},
        {"code": "8480-6", "display": "Systolic blood pressure", "value": 120, "unit": "mm[Hg]", "low": 90, "high": 140},
        {"code": "8462-4", "display": "Diastolic blood pressure", "value": 80, "unit": "mm[Hg]", "low": 60, "high": 90},
        {"code": "29463-7", "display": "Body weight", "value": 70, "unit": "kg", "low": 40, "high": 150},
        {"code": "8302-2", "display": "Body height", "value": 170, "unit": "cm", "low": 140, "high": 210},
        {"code": "39156-5", "display": "BMI", "value": 24.2, "unit": "kg/m2", "low": 18.5, "high": 24.9},
        {"code": "2708-6", "display": "Oxygen saturation", "value": 98, "unit": "%", "low": 95, "high": 100},
        {"code": "2339-0", "display": "Glucose", "value": 95, "unit": "mg/dL", "low": 70, "high": 140},
        {"code": "2093-3", "display": "Total cholesterol", "value": 180, "unit": "mg/dL", "low": 125, "high": 200},
        {"code": "718-7", "display": "Hemoglobin", "value": 14, "unit": "g/dL", "low": 12, "high": 17},
        {"code": "6690-2", "display": "WBC count", "value": 7.5, "unit": "10*3/uL", "low": 4.5, "high": 11.0},
        {"code": "4548-4", "display": "HbA1c", "value": 5.6, "unit": "%", "low": 4.0, "high": 5.7},
    ],
}

# Condition
NEW_SECTIONS["condition_clinical_status"] = cs(
    "http://terminology.hl7.org/CodeSystem/condition-clinical",
    [
        ("active", "Active"),
        ("recurrence", "Recurrence"),
        ("relapse", "Relapse"),
        ("inactive", "Inactive"),
        ("remission", "Remission"),
        ("resolved", "Resolved"),
    ],
)

NEW_SECTIONS["condition_verification_status"] = cs(
    "http://terminology.hl7.org/CodeSystem/condition-ver-status",
    [
        ("unconfirmed", "Unconfirmed"),
        ("provisional", "Provisional"),
        ("differential", "Differential"),
        ("confirmed", "Confirmed"),
        ("refuted", "Refuted"),
        ("entered-in-error", "Entered in Error"),
    ],
)

NEW_SECTIONS["condition_category"] = cs(
    "http://terminology.hl7.org/CodeSystem/condition-category",
    [
        ("problem-list-item", "Problem List Item"),
        ("encounter-diagnosis", "Encounter Diagnosis"),
        ("health-concern", "Health Concern"),
    ],
)

NEW_SECTIONS["flag_codes"] = cs(
    "http://snomed.info/sct",
    [
        ("160245001", "Airborne precautions"),
        ("409524006", "Contact precautions"),
        ("409525007", "Droplet precautions"),
        ("386472006", "Fall risk"),
        ("225728007", "Latex allergy"),
        ("182836005", "Medication review due"),
    ],
    "EXAMPLE",
)

NEW_SECTIONS["condition_severity"] = cs(
    "http://snomed.info/sct",
    [
        ("255604002", "Mild"),
        ("6736007", "Moderate"),
        ("24484000", "Severe"),
    ],
    "PREFERRED",
)

SNOMED_CONDITIONS = [
    ("38341003", "Hypertensive disorder"),
    ("73211009", "Diabetes mellitus"),
    ("13645005", "Chronic obstructive lung disease"),
    ("49436004", "Atrial fibrillation"),
    ("195967001", "Asthma"),
    ("84114007", "Heart failure"),
    ("22298006", "Myocardial infarction"),
    ("44054006", "Type 2 diabetes mellitus"),
    ("396275006", "Osteoarthritis"),
    ("35489007", "Depressive disorder"),
    ("40930008", "Hypothyroidism"),
    ("235856003", "Chronic kidney disease"),
    ("87433001", "Pulmonary embolism"),
    ("91302008", "Sepsis"),
    ("233604007", "Pneumonia"),
]

NEW_SECTIONS["snomed_conditions"] = cs("http://snomed.info/sct", SNOMED_CONDITIONS, "PREFERRED")

# Allergy
NEW_SECTIONS["allergy_clinical_status"] = cs(
    "http://terminology.hl7.org/CodeSystem/allergyintolerance-clinical",
    [("active", "Active"), ("inactive", "Inactive"), ("resolved", "Resolved")],
)

NEW_SECTIONS["allergy_verification_status"] = cs(
    "http://terminology.hl7.org/CodeSystem/allergyintolerance-verification",
    [
        ("unconfirmed", "Unconfirmed"),
        ("confirmed", "Confirmed"),
        ("refuted", "Refuted"),
        ("entered-in-error", "Entered in Error"),
    ],
)

ALLERGY_SUBSTANCES = [
    ("91930004", "Allergy to eggs"),
    ("91934008", "Allergy to nuts"),
    ("91935009", "Allergy to peanuts"),
    ("91936005", "Allergy to seafood"),
    ("91937002", "Allergy to fish"),
    ("91938007", "Allergy to dairy product"),
    ("91939004", "Allergy to wheat"),
    ("91940001", "Allergy to soy"),
    ("293586001", "Allergy to penicillin"),
    ("294505008", "Allergy to sulfonamide"),
    ("294627000", "Allergy to aspirin"),
    ("418038007", "Propensity to adverse reaction to substance"),
]

NEW_SECTIONS["allergy_substances"] = cs("http://snomed.info/sct", ALLERGY_SUBSTANCES, "PREFERRED")
NEW_SECTIONS["snomed_allergies"] = copy.deepcopy(NEW_SECTIONS["allergy_substances"])

NEW_SECTIONS["reaction_severity"] = cs(
    "http://hl7.org/fhir/reaction-event-severity",
    [("mild", "Mild"), ("moderate", "Moderate"), ("severe", "Severe")],
)

# Procedure / immunization (event-status)
EVENT_STATUS = [
    ("preparation", "Preparation"),
    ("in-progress", "In Progress"),
    ("not-done", "Not Done"),
    ("on-hold", "On Hold"),
    ("stopped", "Stopped"),
    ("completed", "Completed"),
    ("entered-in-error", "Entered in Error"),
    ("unknown", "Unknown"),
]
NEW_SECTIONS["procedure_status"] = cs("http://hl7.org/fhir/event-status", EVENT_STATUS)
NEW_SECTIONS["immunization_status"] = cs("http://hl7.org/fhir/event-status", EVENT_STATUS)

SNOMED_PROCEDURES = [
    ("80146002", "Appendectomy"),
    ("387713003", "Surgical procedure"),
    ("103693007", "Diagnostic procedure"),
    ("169396008", "Antenatal care"),
    ("252160004", "Standard chest X-ray"),
    ("73761001", "Colonoscopy"),
    ("386053000", "Evaluation procedure"),
    ("11429006", "Consultation"),
    ("33879002", "Administration of vaccine"),
    ("71388002", "Procedure on heart"),
]
NEW_SECTIONS["snomed_procedures"] = cs("http://snomed.info/sct", SNOMED_PROCEDURES, "PREFERRED")

# Medication
NEW_SECTIONS["medication_status"] = cs(
    "http://hl7.org/fhir/medication-status",
    [("active", "Active"), ("inactive", "Inactive"), ("entered-in-error", "Entered in Error")],
)

NEW_SECTIONS["medication_request_status"] = cs(
    "http://hl7.org/fhir/medicationrequest-status",
    [
        ("active", "Active"),
        ("on-hold", "On Hold"),
        ("cancelled", "Cancelled"),
        ("completed", "Completed"),
        ("entered-in-error", "Entered in Error"),
        ("stopped", "Stopped"),
        ("draft", "Draft"),
        ("unknown", "Unknown"),
    ],
)

NEW_SECTIONS["medication_request_intent"] = cs(
    "http://hl7.org/fhir/medicationrequest-intent",
    [
        ("proposal", "Proposal"),
        ("plan", "Plan"),
        ("order", "Order"),
        ("original-order", "Original Order"),
        ("reflex-order", "Reflex Order"),
        ("filler-order", "Filler Order"),
        ("instance-order", "Instance Order"),
        ("option", "Option"),
    ],
)

NEW_SECTIONS["medication_admin_status"] = cs(
    "http://hl7.org/fhir/medication-admin-status",
    [
        ("in-progress", "In Progress"),
        ("not-done", "Not Done"),
        ("on-hold", "On Hold"),
        ("completed", "Completed"),
        ("entered-in-error", "Entered in Error"),
        ("stopped", "Stopped"),
        ("unknown", "Unknown"),
    ],
)

NEW_SECTIONS["medication_dispense_status"] = cs(
    "http://hl7.org/fhir/medicationdispense-status",
    [
        ("preparation", "Preparation"),
        ("in-progress", "In Progress"),
        ("cancelled", "Cancelled"),
        ("on-hold", "On Hold"),
        ("completed", "Completed"),
        ("entered-in-error", "Entered in Error"),
        ("stopped", "Stopped"),
        ("declined", "Declined"),
        ("unknown", "Unknown"),
    ],
)

RXNORM_MEDS = [
    ("197361", "Lisinopril 10 MG Oral Tablet"),
    ("860975", "Metformin 500 MG Oral Tablet"),
    ("197380", "Atenolol 50 MG Oral Tablet"),
    ("197319", "Aspirin 81 MG Oral Tablet"),
    ("198211", "Simvastatin 20 MG Oral Tablet"),
    ("197454", "Hydrochlorothiazide 25 MG Oral Tablet"),
    ("197361", "Lisinopril 20 MG Oral Tablet"),
    ("1049502", "Amoxicillin 500 MG Oral Capsule"),
    ("198440", "Omeprazole 20 MG Oral Capsule"),
    ("197381", "Warfarin 5 MG Oral Tablet"),
    ("197313", "Levothyroxine 100 MCG Oral Tablet"),
    ("198051", "Prednisone 10 MG Oral Tablet"),
    ("197319", "Ibuprofen 200 MG Oral Tablet"),
    ("197380", "Albuterol 90 MCG Inhalation"),
    ("198211", "Atorvastatin 40 MG Oral Tablet"),
]
NEW_SECTIONS["rxnorm_medications"] = cs("http://www.nlm.nih.gov/research/umls/rxnorm", RXNORM_MEDS, "PREFERRED")
NEW_SECTIONS["snomed_medications"] = cs(
    "http://snomed.info/sct",
    [
        ("387517004", "Paracetamol"),
        ("387458008", "Aspirin"),
        ("387467008", "Ibuprofen"),
        ("372756006", "Warfarin"),
        ("387467008", "Amoxicillin"),
    ],
    "PREFERRED",
)

NEW_SECTIONS["dosage_routes"] = cs(
    "http://snomed.info/sct",
    [
        ("26643006", "Oral route"),
        ("78421000", "Intramuscular route"),
        ("47625008", "Intravenous route"),
        ("34206005", "Subcutaneous route"),
        ("37161004", "Rectal route"),
        ("46713006", "Nasal route"),
        ("37839007", "Sublingual route"),
        ("54485002", "Ophthalmic route"),
        ("6064005", "Topical route"),
        ("16857009", "Vaginal route"),
    ],
    "PREFERRED",
)

NEW_SECTIONS["dosage_timing"] = {
    "system": "http://terminology.hl7.org/CodeSystem/timing-abbreviation",
    "binding": "EXAMPLE",
    "codes": [
        {"code": "QD", "display": "Once daily", "frequency": 1, "period": 1, "period_unit": "d"},
        {"code": "BID", "display": "Twice daily", "frequency": 2, "period": 1, "period_unit": "d"},
        {"code": "TID", "display": "Three times daily", "frequency": 3, "period": 1, "period_unit": "d"},
        {"code": "QID", "display": "Four times daily", "frequency": 4, "period": 1, "period_unit": "d"},
        {"code": "Q4H", "display": "Every 4 hours", "frequency": 1, "period": 4, "period_unit": "h"},
        {"code": "Q6H", "display": "Every 6 hours", "frequency": 1, "period": 6, "period_unit": "h"},
        {"code": "Q8H", "display": "Every 8 hours", "frequency": 1, "period": 8, "period_unit": "h"},
        {"code": "Q12H", "display": "Every 12 hours", "frequency": 1, "period": 12, "period_unit": "h"},
        {"code": "QHS", "display": "At bedtime", "frequency": 1, "period": 1, "period_unit": "d"},
        {"code": "PRN", "display": "As needed", "frequency": 1, "period": 1, "period_unit": "d"},
    ],
}

# Financial
NEW_SECTIONS["claim_status"] = cs(
    "http://hl7.org/fhir/claim-status",
    [
        ("active", "Active"),
        ("cancelled", "Cancelled"),
        ("draft", "Draft"),
        ("entered-in-error", "Entered in Error"),
    ],
)

NEW_SECTIONS["coverage_status"] = cs(
    "http://hl7.org/fhir/fm-status",
    [
        ("active", "Active"),
        ("cancelled", "Cancelled"),
        ("draft", "Draft"),
        ("entered-in-error", "Entered in Error"),
    ],
)

NEW_SECTIONS["coverage_type"] = cs(
    "http://terminology.hl7.org/CodeSystem/v3-ActCode",
    [
        ("EHCPOL", "extended healthcare"),
        ("HSAPOL", "health spending account"),
        ("AUTOPOL", "automobile"),
        ("COL", "collision coverage policy"),
        ("LIFE", "life insurance policy"),
        ("DIS", "disability insurance policy"),
        ("PUBLICPOL", "public healthcare"),
        ("DENTPRG", "dental program"),
        ("DISEASEPRG", "public health program"),
    ],
    "EXTENSIBLE",
)

# Workflow
NEW_SECTIONS["care_plan_status"] = cs("http://hl7.org/fhir/request-status", [
    ("draft", "Draft"), ("active", "Active"), ("on-hold", "On Hold"),
    ("revoked", "Revoked"), ("completed", "Completed"),
    ("entered-in-error", "Entered in Error"), ("unknown", "Unknown"),
])

NEW_SECTIONS["care_plan_intent"] = cs("http://hl7.org/fhir/request-intent", [
    ("proposal", "Proposal"), ("plan", "Plan"), ("order", "Order"),
    ("option", "Option"), ("directive", "Directive"),
])

NEW_SECTIONS["goal_status"] = cs("http://hl7.org/fhir/goal-status", [
    ("proposed", "Proposed"), ("planned", "Planned"), ("accepted", "Accepted"),
    ("active", "Active"), ("on-hold", "On Hold"), ("completed", "Completed"),
    ("cancelled", "Cancelled"), ("entered-in-error", "Entered in Error"),
    ("rejected", "Rejected"),
])

NEW_SECTIONS["task_status"] = cs("http://hl7.org/fhir/task-status", [
    ("draft", "Draft"), ("requested", "Requested"), ("received", "Received"),
    ("accepted", "Accepted"), ("rejected", "Rejected"), ("ready", "Ready"),
    ("cancelled", "Cancelled"), ("in-progress", "In Progress"), ("on-hold", "On Hold"),
    ("failed", "Failed"), ("completed", "Completed"), ("entered-in-error", "Entered in Error"),
])

NEW_SECTIONS["document_reference_status"] = cs("http://hl7.org/fhir/document-reference-status", [
    ("current", "Current"), ("superseded", "Superseded"), ("entered-in-error", "Entered in Error"),
])

NEW_SECTIONS["composition_status"] = cs("http://hl7.org/fhir/composition-status", [
    ("registered", "Registered"), ("partial", "Partial"), ("preliminary", "Preliminary"),
    ("final", "Final"), ("amended", "Amended"), ("corrected", "Corrected"),
    ("appended", "Appended"), ("cancelled", "Cancelled"),
    ("entered-in-error", "Entered in Error"), ("deprecated", "Deprecated"), ("unknown", "Unknown"),
])

# Diagnostic
NEW_SECTIONS["diagnostic_report_status"] = cs("http://hl7.org/fhir/diagnostic-report-status", [
    ("registered", "Registered"), ("partial", "Partial"), ("preliminary", "Preliminary"),
    ("final", "Final"), ("amended", "Amended"), ("corrected", "Corrected"),
    ("appended", "Appended"), ("cancelled", "Cancelled"),
    ("entered-in-error", "Entered in Error"), ("unknown", "Unknown"),
])

NEW_SECTIONS["diagnostic_report_categories"] = cs(
    "http://terminology.hl7.org/CodeSystem/v2-0074",
    [
        ("LAB", "Laboratory"),
        ("RAD", "Radiology"),
        ("PATH", "Pathology"),
        ("CUS", "Cardiac Ultrasound"),
        ("CT", "CAT Scan"),
        ("MRI", "Magnetic Resonance"),
        ("NUC", "Nuclear Medicine Scan"),
        ("OTH", "Other"),
    ],
    "EXTENSIBLE",
)

# Immunization (CVX on terminology.hl7.org)
NEW_SECTIONS["vaccines"] = cs(
    "http://hl7.org/fhir/sid/cvx",
    [
        ("08", "Hep B, adolescent or pediatric"),
        ("10", "IPV"),
        ("20", "DTaP"),
        ("21", "Varicella"),
        ("03", "MMR"),
        ("33", "Pneumococcal conjugate"),
        ("115", "Tdap"),
        ("140", "Influenza, seasonal, injectable"),
        ("141", "Influenza, seasonal, intranasal"),
        ("52", "Hep A, adult"),
        ("83", "Hep A, ped/adol, 2 dose"),
        ("114", "Meningococcal MCV4P"),
        ("62", "HPV, quadrivalent"),
        ("165", "HPV9"),
        ("187", "COVID-19, mRNA, LNP-S"),
    ],
    "PREFERRED",
)

NEW_SECTIONS["body_sites"] = cs(
    "http://snomed.info/sct",
    [
        ("368209003", "Right arm"),
        ("368208006", "Left arm"),
        ("368210008", "Right thigh"),
        ("368211007", "Left thigh"),
        ("368207001", "Right buttock"),
        ("368206005", "Left buttock"),
        ("91775009", "Structure of right deltoid region"),
        ("49521007", "Left lower forearm"),
        ("89666000", "Right hand"),
        ("7771000", "Left hand"),
        ("69536005", "Head structure"),
        ("45048000", "Neck structure"),
        ("818983003", "Structure of left thigh"),
    ],
    "PREFERRED",
)

NEW_SECTIONS["countries"] = {
    "system": "urn:iso:std:iso:3166",
    "binding": "REQUIRED",
    "codes": [
        {"code": "US", "display": "United States"},
        {"code": "GB", "display": "United Kingdom"},
        {"code": "CA", "display": "Canada"},
        {"code": "AU", "display": "Australia"},
        {"code": "DE", "display": "Germany"},
        {"code": "FR", "display": "France"},
        {"code": "IN", "display": "India"},
        {"code": "JP", "display": "Japan"},
        {"code": "BR", "display": "Brazil"},
        {"code": "MX", "display": "Mexico"},
        {"code": "ZA", "display": "South Africa"},
        {"code": "IE", "display": "Ireland"},
        {"code": "NZ", "display": "New Zealand"},
        {"code": "NL", "display": "Netherlands"},
        {"code": "SE", "display": "Sweden"},
    ],
}

# Additional resource status / workflow (FHIR R5 CodeSystems)
NEW_SECTIONS["flag_status"] = cs("http://hl7.org/fhir/flag-status", [
    ("active", "Active"), ("inactive", "Inactive"), ("entered-in-error", "Entered in Error"),
])

NEW_SECTIONS["consent_status"] = cs("http://hl7.org/fhir/consent-state-codes", [
    ("draft", "Draft"), ("proposed", "Proposed"), ("active", "Active"),
    ("rejected", "Rejected"), ("inactive", "Inactive"), ("entered-in-error", "Entered in Error"),
])

NEW_SECTIONS["communication_status"] = cs("http://hl7.org/fhir/event-status", EVENT_STATUS)

NEW_SECTIONS["supply_delivery_status"] = cs("http://hl7.org/fhir/supplydelivery-status", [
    ("in-progress", "In Progress"), ("completed", "Completed"),
    ("abandoned", "Abandoned"), ("entered-in-error", "Entered in Error"),
])

NEW_SECTIONS["loinc_composition_types"] = cs(
    "http://loinc.org",
    [
        ("18842-5", "Discharge summary"),
        ("11488-4", "Consultation note"),
        ("34117-2", "History and physical note"),
        ("88569-4", "Outpatient Note"),
    ],
    "EXAMPLE",
)

NEW_SECTIONS["loinc_questionnaire_panels"] = cs(
    "http://loinc.org",
    [
        ("44249-1", "Health and social assessment panel"),
        ("69737-5", "PHQ-9 quick depression assessment"),
    ],
    "EXAMPLE",
)

NEW_SECTIONS["publication_status"] = cs(
    "http://hl7.org/fhir/publication-status",
    [
        ("draft", "Draft"),
        ("active", "Active"),
        ("retired", "Retired"),
        ("unknown", "Unknown"),
    ],
)

NEW_SECTIONS["claim_type"] = cs(
    "http://terminology.hl7.org/CodeSystem/claim-type",
    [
        ("institutional", "Institutional"),
        ("professional", "Professional"),
        ("pharmacy", "Pharmacy"),
        ("oral", "Oral"),
        ("vision", "Vision"),
    ],
)

NEW_SECTIONS["claim_use"] = cs(
    "http://hl7.org/fhir/claim-use",
    [
        ("claim", "Claim"),
        ("preauthorization", "Preauthorization"),
        ("predetermination", "Predetermination"),
    ],
)

NEW_SECTIONS["adverse_event_actuality"] = cs(
    "http://hl7.org/fhir/adverse-event-actuality",
    [("actual", "Actual"), ("potential", "Potential")],
)

NEW_SECTIONS["adverse_event_seriousness"] = cs(
    "http://terminology.hl7.org/CodeSystem/adverse-event-seriousness",
    [("serious", "Serious"), ("non-serious", "Non-serious")],
    "EXTENSIBLE",
)

NEW_SECTIONS["immunization_forecast_status"] = cs(
    "http://terminology.hl7.org/CodeSystem/immunization-recommendation-status",
    [
        ("due", "Due"),
        ("overdue", "Overdue"),
        ("eligible", "Eligible"),
        ("not-due", "Not Due"),
    ],
    "EXTENSIBLE",
)

NEW_SECTIONS["device_dispense_status"] = cs("http://hl7.org/fhir/devicedispense-status", EVENT_STATUS)

NEW_SECTIONS["device_usage_status"] = cs(
    "http://hl7.org/fhir/deviceusage-status",
    [
        ("active", "Active"),
        ("completed", "Completed"),
        ("entered-in-error", "Entered in Error"),
    ],
)

NEW_SECTIONS["biologically_derived_product_category"] = cs(
    "http://hl7.org/fhir/biologicallyderivedproductcategory",
    [
        ("organ", "Organ"),
        ("tissue", "Tissue"),
        ("fluid", "Fluid"),
        ("cells", "Cells"),
    ],
)

NEW_SECTIONS["biologically_derived_product_status"] = cs(
    "http://hl7.org/fhir/biologicallyderivedproductstatus",
    [("available", "Available"), ("unavailable", "Unavailable")],
)

NEW_SECTIONS["measure_report_status"] = cs(
    "http://hl7.org/fhir/measure-report-status",
    [
        ("complete", "Complete"),
        ("pending", "Pending"),
        ("error", "Error"),
    ],
)

NEW_SECTIONS["measure_report_type"] = cs(
    "http://hl7.org/fhir/measure-report-type",
    [
        ("individual", "Individual"),
        ("subject-list", "Subject List"),
        ("summary", "Summary"),
        ("data-collection", "Data Collection"),
    ],
)

NEW_SECTIONS["genomic_study_status"] = cs(
    "http://hl7.org/fhir/genomicstudy-status",
    [
        ("registered", "Registered"),
        ("available", "Available"),
        ("cancelled", "Cancelled"),
        ("entered-in-error", "Entered in Error"),
    ],
)

NEW_SECTIONS["endpoint_connection_type"] = cs(
    "http://terminology.hl7.org/CodeSystem/endpoint-connection-type",
    [
        ("hl7-fhir-rest", "HL7 FHIR"),
        ("hl7-fhir-msg", "HL7 FHIR Messaging"),
    ],
)

NEW_SECTIONS["provenance_activity"] = cs(
    "http://terminology.hl7.org/CodeSystem/v3-DataOperation",
    [
        ("CREATE", "create"),
        ("UPDATE", "revise"),
        ("DELETE", "delete"),
    ],
)

NEW_SECTIONS["basic_resource_codes"] = cs(
    "http://terminology.hl7.org/CodeSystem/basic-resource-type",
    [
        ("referral", "Referral"),
        ("disease", "Disease outbreak"),
    ],
    "EXAMPLE",
)

NEW_SECTIONS["nutrition_foods"] = cs(
    "http://snomed.info/sct",
    [
        ("226211001", "Apple"),
        ("78275001", "Orange fruit"),
        ("228360005", "Whole milk"),
    ],
    "PREFERRED",
)

NEW_SECTIONS["supply_categories"] = cs(
    "http://terminology.hl7.org/CodeSystem/supply-category",
    [
        ("central", "Central Supply"),
        ("non-stock", "Non-Stock"),
    ],
)

NEW_SECTIONS["eligibility_purpose"] = cs(
    "http://terminology.hl7.org/CodeSystem/coverageeligibilityrequest-purpose",
    [
        ("auth-requirements", "Coverage auth requirements"),
        ("benefits", "Benefits"),
        ("discovery", "Discovery"),
        ("validation", "Validation"),
    ],
)

NEW_SECTIONS["payment_status"] = cs(
    "http://terminology.hl7.org/CodeSystem/paymentstatus",
    [("paid", "Paid"), ("cleared", "Cleared")],
)

NEW_SECTIONS["process_priority"] = cs(
    "http://terminology.hl7.org/CodeSystem/processpriority",
    [
        ("stat", "Immediate"),
        ("normal", "Normal"),
        ("deferred", "Deferred"),
    ],
)

NEW_SECTIONS["contract_signing_types"] = cs(
    "http://terminology.hl7.org/CodeSystem/contract-signer-type-codes",
    [("VERIFIED", "Verified")],
    "EXTENSIBLE",
)

NEW_SECTIONS["contract_status"] = cs(
    "http://hl7.org/fhir/contract-status",
    [
        ("executed", "Executed"),
        ("amended", "Amended"),
        ("terminated", "Terminated"),
        ("entered-in-error", "Entered in Error"),
    ],
)

NEW_SECTIONS["insurance_plan_types"] = cs(
    "http://terminology.hl7.org/CodeSystem/insurance-plan-type",
    [("medical", "Medical"), ("dental", "Dental"), ("vision", "Vision")],
    "EXTENSIBLE",
)

NEW_SECTIONS["snomed_devices"] = cs(
    "http://snomed.info/sct",
    [
        ("706172005", "Wheelchair"),
        ("449345005", "Insulin pump"),
        ("86184003", "Electrocardiograph"),
    ],
    "PREFERRED",
)

NEW_SECTIONS["biologically_derived_product_codes"] = cs(
    "http://snomed.info/sct",
    [
        ("119297000", "Blood product"),
        ("180270004", "Whole blood"),
        ("33389009", "Platelet concentrate"),
    ],
    "PREFERRED",
)

NEW_SECTIONS["medication_statement_status"] = cs(
    "http://hl7.org/fhir/medication-statement-status",
    [
        ("recorded", "Recorded"),
        ("entered-in-error", "Entered in Error"),
        ("draft", "Draft"),
    ],
)

NEW_SECTIONS["payment_reconciliation_outcome"] = cs(
    "http://hl7.org/fhir/payment-outcome",
    [
        ("queued", "Queued"),
        ("complete", "Complete"),
        ("error", "Error"),
        ("partial", "Partial"),
    ],
)

NEW_SECTIONS["provenance_participant_type"] = cs(
    "http://terminology.hl7.org/CodeSystem/provenance-participant-type",
    [
        ("author", "Author"),
        ("verifier", "Verifier"),
        ("transmitter", "Transmitter"),
    ],
    "EXTENSIBLE",
)

NEW_SECTIONS["eye_laterality"] = cs(
    "http://hl7.org/fhir/vision-eye-codes",
    [("right", "Right Eye"), ("left", "Left Eye")],
)

NEW_SECTIONS["adverse_event_status"] = cs("http://hl7.org/fhir/event-status", EVENT_STATUS)

NEW_SECTIONS["specimen_status"] = cs("http://hl7.org/fhir/specimen-status", [
    ("available", "Available"), ("unavailable", "Unavailable"),
    ("unsatisfactory", "Unsatisfactory"), ("entered-in-error", "Entered in Error"),
])

NEW_SECTIONS["observation_interpretation"] = cs(
    "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
    [
        ("N", "Normal"), ("A", "Abnormal"), ("H", "High"), ("L", "Low"),
        ("HH", "Critical high"), ("LL", "Critical low"), ("U", "Significant change up"),
        ("D", "Significant change down"),
    ],
    "EXTENSIBLE",
)

NEW_SECTIONS["us_states"] = {
    "system": "urn:iso:std:iso:3166-2",
    "binding": "EXAMPLE",
    "codes": [{"code": c, "display": c} for c in [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
        "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
        "VA", "WA", "WV", "WI", "WY", "DC",
    ]],
}


def _dedupe_section_codes(section: dict) -> None:
    """Remove duplicate code entries within a YAML section (last display wins)."""
    codes = section.get("codes")
    if not isinstance(codes, list):
        return
    seen: dict[str, dict] = {}
    for entry in codes:
        if not isinstance(entry, dict):
            continue
        code = entry.get("code")
        if code is None:
            continue
        seen[str(code)] = entry
    section["codes"] = list(seen.values())


def merge_existing(existing: dict) -> dict:
    """Merge: keep existing sections, overlay NEW_SECTIONS, extend conditions."""
    out = copy.deepcopy(existing)
    for key, section in NEW_SECTIONS.items():
        if key not in out:
            out[key] = section
        elif isinstance(section, dict) and isinstance(out.get(key), dict):
            if "system" in section and "system" in out[key]:
                out[key]["system"] = section["system"]
            if "codes" in section:
                existing_codes = {c["code"] for c in out[key].get("codes", []) if isinstance(c, dict)}
                for entry in section.get("codes", []):
                    if entry.get("code") not in existing_codes:
                        out[key].setdefault("codes", []).append(copy.deepcopy(entry))
                        existing_codes.add(entry["code"])
        elif key == "conditions" and "codes" in out.get(key, {}):
            # extend conditions list
            existing_codes = {c["code"] for c in out["conditions"]["codes"]}
            for entry in section.get("codes", []):
                if entry["code"] not in existing_codes:
                    out["conditions"]["codes"].append(entry)
    # Expand service_category / service_type if sparse
    for ext_key, extra in [
        ("service_category", [("2", "Aged Care"), ("5", "Counselling"), ("8", "Cancer Services")]),
        ("service_type", [("118", "Gastroenterology"), ("135", "Physiotherapy"), ("159", "Urology")]),
    ]:
        if ext_key in out:
            codes = {c["code"] for c in out[ext_key].get("codes", [])}
            for code, display in extra:
                if code not in codes:
                    out[ext_key]["codes"].append({"code": code, "display": display})
    for section in out.values():
        if isinstance(section, dict):
            _dedupe_section_codes(section)
    return out


def add_aliases(data: dict) -> None:
    pairs = [
        ("gender", "administrative_gender"),
        ("languages", "language"),
        ("identifier_types", "identifier_type"),
        ("telecom_system", "contact_point_system"),
        ("telecom_use", "contact_point_use"),
        ("practitioner_roles", "practitioner_role"),
        ("organization_types", "organization_type"),
        ("location_types", "location_type"),
        ("relatedperson_relationship", "contact_relationship"),
        ("supply_request_status", "request_status"),
        ("enrollment_status", "coverage_status"),
        ("explanation_of_benefit_status", "claim_status"),
        ("charge_item_definition_status", "publication_status"),
        ("questionnaire_status", "publication_status"),
        ("insurance_plan_status", "publication_status"),
        ("measure_status", "publication_status"),
        ("vision_prescription_status", "medication_request_status"),
        ("nutrition_intake_status", "procedure_status"),
        ("device_request_status", "request_status"),
        ("request_orchestration_status", "request_status"),
        ("coverage_eligibility_request_status", "coverage_status"),
        ("coverage_eligibility_response_status", "coverage_status"),
        ("payment_notice_status", "claim_status"),
        ("payment_reconciliation_status", "claim_status"),
    ]
    for alias, target in pairs:
        if alias != target:
            alias_section(data, alias, target)
    # snomed_conditions mirrors conditions if not set
    if "snomed_conditions" not in data and "conditions" in data:
        data["snomed_conditions"] = copy.deepcopy(data["conditions"])
    elif "conditions" in data and "snomed_conditions" in data:
        # merge conditions into snomed_conditions
        codes = {c["code"] for c in data["snomed_conditions"]["codes"]}
        for entry in data["conditions"]["codes"]:
            if entry["code"] not in codes:
                data["snomed_conditions"]["codes"].append(copy.deepcopy(entry))


def main() -> None:
    existing: dict = {}
    if OUT.exists():
        with open(OUT, encoding="utf-8") as f:
            existing = yaml.safe_load(f) or {}

    data = merge_existing(existing)
    add_aliases(data)

    header = (
        "# Healthcare Code Systems — FHIR R5 synthetic data generation\n"
        "# Sources: https://terminology.hl7.org (HL7 Terminology, FHIR R5)\n"
        "#          https://www.hl7.org/fhir/terminologies.html\n"
        "#          http://loinc.org | http://snomed.info/sct | RxNorm | CVX\n"
        "# Regenerate: python -m fhir_gen.hl7_codes._build_healthcare_codes\n"
        "#   or: python fhir_gen/hl7_codes/_build_healthcare_codes.py\n"
        "# Section keys match fhir_gen.codes.loader.get_codes(section)\n"
    )

    with open(OUT, "w", encoding="utf-8") as f:
        f.write(header)
        yaml.dump(
            data,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
            width=120,
        )

    required_prompt_sections = [
        "gender", "languages", "marital_status", "contact_relationship", "identifier_types",
        "name_use", "address_use", "address_type", "telecom_system", "telecom_use",
        "appointment_status", "encounter_status", "encounter_class", "observation_status",
        "condition_clinical_status", "condition_verification_status", "allergy_clinical_status",
        "allergy_verification_status", "medication_request_status", "loinc_observations",
        "snomed_conditions", "snomed_procedures", "dosage_routes", "dosage_timing",
        "body_sites", "rxnorm_medications", "vaccines",
    ]
    missing = [s for s in required_prompt_sections if s not in data]
    print(f"Wrote {OUT} ({len(data)} sections)")
    if missing:
        print("WARNING missing:", missing)
    else:
        print("All generator-required sections present.")


if __name__ == "__main__":
    main()
