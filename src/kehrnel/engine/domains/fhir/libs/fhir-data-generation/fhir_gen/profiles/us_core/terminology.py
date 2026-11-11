"""US Core v9.0.0 canonical URLs and terminology constants."""

# Base canonical URL for US Core profiles
US_CORE_BASE = "http://hl7.org/fhir/us/core/StructureDefinition"

# US Core profile canonical URLs (R4 only)
US_CORE_PROFILES = {
    "Patient": f"{US_CORE_BASE}/us-core-patient",
    "Practitioner": f"{US_CORE_BASE}/us-core-practitioner",
    "PractitionerRole": f"{US_CORE_BASE}/us-core-practitionerrole",
    "Organization": f"{US_CORE_BASE}/us-core-organization",
    "Location": f"{US_CORE_BASE}/us-core-location",
    "Encounter": f"{US_CORE_BASE}/us-core-encounter",
    "Condition": f"{US_CORE_BASE}/us-core-condition-problems-health-concerns",
    "AllergyIntolerance": f"{US_CORE_BASE}/us-core-allergyintolerance",
    "Observation": f"{US_CORE_BASE}/us-core-observation-lab",
    "DiagnosticReport": f"{US_CORE_BASE}/us-core-diagnosticreport-lab",
    "Immunization": f"{US_CORE_BASE}/us-core-immunization",
    "MedicationRequest": f"{US_CORE_BASE}/us-core-medicationrequest",
    "MedicationDispense": f"{US_CORE_BASE}/us-core-medicationdispense",
    "Device": f"{US_CORE_BASE}/us-core-implantable-device",
    "Medication": f"{US_CORE_BASE}/us-core-medication",
    "Procedure": f"{US_CORE_BASE}/us-core-procedure",
    "DocumentReference": f"{US_CORE_BASE}/us-core-documentreference",
    "CarePlan": f"{US_CORE_BASE}/us-core-careplan",
    "CareTeam": f"{US_CORE_BASE}/us-core-careteam",
    "Goal": f"{US_CORE_BASE}/us-core-goal",
    "ServiceRequest": f"{US_CORE_BASE}/us-core-servicerequest",
    "Coverage": f"{US_CORE_BASE}/us-core-coverage",
    "RelatedPerson": f"{US_CORE_BASE}/us-core-relatedperson",
    "Specimen": f"{US_CORE_BASE}/us-core-specimen",
    "FamilyMemberHistory": f"{US_CORE_BASE}/us-core-familymemberhistory",
    "Provenance": f"{US_CORE_BASE}/us-core-provenance",
    "QuestionnaireResponse": f"{US_CORE_BASE}/us-core-questionnaireresponse",
}

# US Core extension URLs
EXT_RACE = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
EXT_ETHNICITY = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"
EXT_BIRTHSEX = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex"
EXT_GENDER_IDENTITY = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-genderIdentity"
EXT_TRIBAL_AFFILIATION = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-tribal-affiliation"
EXT_SEX = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-sex"
EXT_INTERPRETER_REQUIRED = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-interpreter-required"

# US Core race ombCategory codes (OMB 1997)
RACE_OMB_CODES = [
    {"code": "1002-5", "display": "American Indian or Alaska Native", "system": "urn:oid:2.16.840.1.113883.6.238"},
    {"code": "2028-9", "display": "Asian", "system": "urn:oid:2.16.840.1.113883.6.238"},
    {"code": "2054-5", "display": "Black or African American", "system": "urn:oid:2.16.840.1.113883.6.238"},
    {"code": "2076-8", "display": "Native Hawaiian or Other Pacific Islander", "system": "urn:oid:2.16.840.1.113883.6.238"},
    {"code": "2106-3", "display": "White", "system": "urn:oid:2.16.840.1.113883.6.238"},
    {"code": "2131-1", "display": "Other Race", "system": "urn:oid:2.16.840.1.113883.6.238"},
    {"code": "ASKU", "display": "Asked but no answer", "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor"},
    {"code": "UNK", "display": "Unknown", "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor"},
]

# US Core ethnicity ombCategory codes
ETHNICITY_OMB_CODES = [
    {"code": "2135-2", "display": "Hispanic or Latino", "system": "urn:oid:2.16.840.1.113883.6.238"},
    {"code": "2186-5", "display": "Not Hispanic or Latino", "system": "urn:oid:2.16.840.1.113883.6.238"},
    {"code": "ASKU", "display": "Asked but no answer", "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor"},
    {"code": "UNK", "display": "Unknown", "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor"},
]

# US Core birthsex codes (AdministrativeGender + UNK)
BIRTHSEX_CODES = ["M", "F", "X", "UNK"]
