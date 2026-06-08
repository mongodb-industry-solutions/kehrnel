"""Specialized domain resource enrichers (Prompt 13)."""

from __future__ import annotations

import random
from typing import Any

from ...codes import (
    codeable_from_section,
    codeable_reference_from_section,
    concept_from_section,
    pick_code,
)
from ...codes.loader import get_system, random_code
from ...resolvers.reference import ReferenceStore
from ..special_types import SpecialTypeGenerator


def enrich_Specimen(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["available", "unavailable", "unsatisfactory", "entered-in-error"])
    r["type"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["119297000", "119342007", "122555007", "258527002", "119323008"]),
        display=rng.choice([
            "Blood specimen", "Saliva specimen", "Venous blood specimen",
            "Nail specimen", "Urine specimen",
        ]),
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    r["receivedTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    body = random_code("body_sites", rng)
    collection: dict[str, Any] = {
        "collectedDateTime": t.p.gen_dateTime(min_year=2023, max_year=2024),
        "quantity": t.gen_Quantity(value=round(rng.uniform(1.0, 50.0), 1), unit="mL"),
    }
    if store.has("Practitioner"):
        collection["collector"] = store.get_reference("Practitioner", rng)
    if body:
        collection["bodySite"] = t.gen_CodeableConcept(
            system=get_system("body_sites"),
            code=body["code"],
            display=body["display"],
        )
    r["collection"] = collection
    r["identifier"] = [t.gen_Identifier(system="http://lab.example.org/specimens")]
    return r


def enrich_ImagingStudy(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["registered", "available", "cancelled", "entered-in-error", "unknown"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Procedure"):
        r["procedure"] = store.get_reference("Procedure", rng)
    r["started"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    if store.has("Practitioner"):
        r["referrer"] = store.get_reference("Practitioner", rng)
    r["numberOfSeries"] = rng.randint(1, 5)
    r["numberOfInstances"] = rng.randint(10, 500)
    r["modality"] = [t.gen_Coding(
        system="http://dicom.nema.org/resources/ontology/DCM",
        code=rng.choice(["CT", "MR", "US", "XR", "PET", "NM", "PT", "CR", "DX"]),
        display=rng.choice([
            "Computed Tomography", "Magnetic Resonance", "Ultrasound",
            "X-Ray", "Positron Emission Tomography", "Nuclear Medicine",
        ]),
    )]
    r["description"] = rng.choice([
        "CT Chest with contrast", "MRI Brain without contrast",
        "Ultrasound abdomen", "Chest X-Ray PA and Lateral",
        "PET Scan whole body", "MRI Spine lumbar",
    ])
    r["series"] = [{
        "uid": t.p.gen_oid(),
        "number": i + 1,
        "modality": t.gen_Coding(
            system="http://dicom.nema.org/resources/ontology/DCM",
            code=rng.choice(["CT", "MR", "US"]),
        ),
        "description": rng.choice(["Axial", "Coronal", "Sagittal"]),
        "numberOfInstances": rng.randint(10, 200),
        "started": t.p.gen_dateTime(min_year=2023, max_year=2024),
        "instance": [{
            "uid": t.p.gen_oid(),
            "sopClass": "urn:oid:1.2.840.10008.5.1.4.1.1.2",
            "number": j + 1,
        } for j in range(rng.randint(1, 5))],
    } for i in range(rng.randint(1, 3))]
    return r


def enrich_Device(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["active", "inactive", "entered-in-error"])
    r["name"] = [{
        "value": rng.choice([
            "Pulse Oximeter", "Blood Pressure Monitor", "Insulin Pump", "Cardiac Monitor",
            "Ventilator", "Defibrillator", "Infusion Pump", "ECG Machine",
        ]),
        "type": "user-friendly-name",
    }]
    r["type"] = [t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["706767009", "19257004", "360008003"]),
        display=rng.choice(["Patient monitor", "Defibrillator", "Immunization agent"]),
    )]
    r["manufacturer"] = rng.choice([
        "Medtronic", "Philips", "GE Healthcare", "Siemens", "Abbott", "Boston Scientific",
    ])
    r["manufactureDate"] = t.p.gen_dateTime(min_year=2020, max_year=2023)
    r["expirationDate"] = t.p.gen_dateTime(min_year=2025, max_year=2030)
    r["serialNumber"] = f"SN{rng.randint(100000000, 999999999)}"
    if store.has("Organization"):
        r["owner"] = store.get_reference("Organization", rng)
    if store.has("Location"):
        r["location"] = store.get_reference("Location", rng)
    return r


def enrich_ResearchStudy(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "active", "administratively-completed", "approved",
        "closed-to-accrual", "closed-to-accrual-and-intervention",
        "completed", "disapproved", "in-review",
        "temporarily-closed-to-accrual",
        "temporarily-closed-to-accrual-and-intervention", "withdrawn",
    ])
    r["title"] = rng.choice([
        "Effect of Exercise on Type 2 Diabetes Outcomes",
        "Novel Therapy for Hypertension Management",
        "Genetic Factors in Cardiovascular Disease",
        "Immunotherapy for Non-Small Cell Lung Cancer",
        "Cognitive Behavioral Therapy for Depression",
    ])
    r["phase"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/research-study-phase",
        code=rng.choice([
            "n-a", "early-phase-1", "phase-1", "phase-1-phase-2",
            "phase-2", "phase-2-phase-3", "phase-3", "phase-4",
        ]),
    )
    if store.has("Organization"):
        r["sponsor"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["principalInvestigator"] = store.get_reference("Practitioner", rng)
    if store.has("Location"):
        r["site"] = [store.get_reference("Location", rng)]
    r["description"] = t.p.gen_markdown()
    r["period"] = t.gen_Period()
    return r


def enrich_ResearchSubject(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "candidate", "eligible", "follow-up", "ineligible",
        "not-registered", "off-study", "on-study",
        "on-study-intervention", "on-study-observation",
        "pending-on-study", "potential-candidate",
        "screening", "withdrawn",
    ])
    if store.has("ResearchStudy"):
        r["study"] = store.get_reference("ResearchStudy", rng)
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    r["period"] = t.gen_Period()
    r["assignedComparisonGroup"] = t.p.gen_id()
    r["actualComparisonGroup"] = t.p.gen_id()
    return r


def enrich_QuestionnaireResponse(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "in-progress", "completed", "amended", "entered-in-error", "stopped",
    ])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["author"] = store.get_reference("Practitioner", rng)
    if store.has("Questionnaire"):
        r["questionnaire"] = store.get_reference("Questionnaire", rng)["reference"]
    r["authored"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["item"] = [{
        "linkId": f"question-{i + 1}",
        "text": t.p.gen_string(
            resource_type="QuestionnaireResponse", field_name="text", max_length=120
        ),
        "answer": [{
            "valueString": t.p.gen_string(
                resource_type="QuestionnaireResponse",
                field_name="answer",
                max_length=40,
            ),
        }],
    } for i in range(rng.randint(2, 6))]
    return r


def enrich_AuditEvent(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["action"] = rng.choice(["C", "R", "U", "D", "E"])
    r["recorded"] = t.p.gen_instant()
    r["outcome"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/audit-event-outcome",
        code=rng.choice(["0", "4", "8", "12"]),
        display=rng.choice(["Success", "Minor failure", "Serious failure", "Major failure"]),
    )
    r["type"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/audit-event-type",
        code=rng.choice(["rest", "hl7-v2", "hl7-v3", "dicom"]),
        display="Audit event type",
    )
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/audit-event-type",
        code=rng.choice(["rest", "hl7-v2", "hl7-v3", "dicom"]),
    )]
    r["code"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/audit-event-sub-type",
        code=rng.choice(["read", "create", "update", "delete", "search"]),
    )
    who = (
        store.get_reference("Practitioner", rng)
        if store.has("Practitioner")
        else t.gen_Reference()
    )
    r["agent"] = [{
        "type": t.gen_CodeableConcept(
            system=get_system("participation_type"),
            code="IRCP",
            display="information recipient",
        ),
        "requestor": True,
        "who": who,
        "network": {
            "address": t.p.faker.ipv4(),
            "type": t.gen_CodeableConcept(
                system="http://hl7.org/fhir/network-type",
                code="2",
                display="IP Address",
            ),
        },
    }]
    site = (
        store.get_reference("Location", rng)
        if store.has("Location")
        else t.gen_Reference(resource_type="Location")
    )
    r["source"] = {
        "site": site,
        "observer": t.gen_Reference(resource_type="Device"),
        "type": [t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/audit-source-type",
            code="4",
            display="Application Server",
        )],
    }
    entity_what = (
        store.get_reference("Patient", rng)
        if store.has("Patient")
        else t.gen_Reference()
    )
    r["entity"] = [{
        "what": entity_what,
        "role": t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/object-role",
            code=rng.choice(["1", "2", "3", "4"]),
            display=rng.choice(["Patient", "Location", "Report", "Domain Resource"]),
        ),
    }]
    return r


def enrich_EpisodeOfCare(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "planned", "waitlist", "active", "onhold",
        "finished", "cancelled", "entered-in-error",
    ])
    r["type"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/episodeofcare-type",
        code=rng.choice(["hacc", "pac", "diab", "da", "cacp", "posad", "oncol"]),
    )]
    if store.has("Patient"):
        patient_ref = store.get_reference("Patient", rng)
        r["patient"] = patient_ref
        r["subject"] = patient_ref
    if store.has("Organization"):
        r["managingOrganization"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["careManager"] = store.get_reference("Practitioner", rng)
    r["period"] = t.gen_Period()
    if store.has("Condition"):
        r["diagnosis"] = [{
            "condition": [store.get_reference("Condition", rng)],
            "use": t.gen_CodeableConcept(
                system="http://snomed.info/sct",
                code="8319008",
                display="Principal diagnosis",
            ),
        }]
    return r


def enrich_HealthcareService(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["active"] = rng.random() > 0.1
    if store.has("Organization"):
        r["providedBy"] = store.get_reference("Organization", rng)
    if store.has("Location"):
        r["location"] = [store.get_reference("Location", rng)]
    sc = random_code("service_category", rng)
    r["category"] = [t.gen_CodeableConcept(
        system=get_system("service_category"),
        code=sc["code"] if sc else "17",
        display=sc["display"] if sc else "General Practice",
    )]
    st = random_code("service_type", rng)
    r["type"] = [t.gen_CodeableConcept(
        system=get_system("service_type"),
        code=st["code"] if st else "124",
        display=st["display"] if st else "General Practice",
    )]
    r["name"] = rng.choice([
        "General Practice Clinic", "Outpatient Diabetes Clinic",
        "Cardiac Rehabilitation", "Physical Therapy",
        "Mental Health Services", "Oncology Outpatient",
    ])
    r["comment"] = t.p.gen_string(
        resource_type=r.get("resourceType"), field_name="comment"
    )
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    return r


def enrich_RelatedPerson(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["active"] = True
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    rel = random_code("contact_relationship", rng)
    r["relationship"] = [t.gen_CodeableConcept(
        system=get_system("contact_relationship"),
        code=rel["code"] if rel else "N",
        display=rel["display"] if rel else "Next-of-Kin",
    )]
    r["name"] = [t.gen_HumanName(use="official")]
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    r["gender"] = rng.choice(["male", "female", "other", "unknown"])
    r["address"] = [t.gen_Address()]
    r["period"] = t.gen_Period()
    return r


def enrich_Group(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["type"] = rng.choice([
        "person", "animal", "practitioner", "device",
        "careteam", "healthcareservice", "location",
        "organization", "relatedperson", "specimen",
    ])
    r["membership"] = rng.choice(["definitional", "enumerated"])
    r["active"] = True
    r["quantity"] = rng.randint(5, 500)
    r["name"] = rng.choice([
        "Diabetes Patients Group", "Hypertension Cohort",
        "Oncology Trial Participants", "Post-Surgical Patients",
    ])
    r["code"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["73211009", "38341003"]),
        display=rng.choice(["Diabetes mellitus", "Hypertensive disorder"]),
    )
    if store.has("Patient") and r["type"] == "person":
        count = min(3, store.count("Patient"))
        r["member"] = [{
            "entity": store.get_reference("Patient", rng),
            "period": t.gen_Period(),
            "inactive": False,
        } for _ in range(count)]
    return r


def enrich_DetectedIssue(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["preliminary", "final", "entered-in-error", "mitigated"])
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code=rng.choice(["DRG", "DI", "TIME", "DOSE", "DOSEIND", "ALG"]),
        display=rng.choice([
            "Drug Interaction Alert", "Drug Intolerance Alert",
            "Timing Detected Issue", "Dosage Alert",
            "Dose Indicator", "Allergy Alert",
        ]),
    )]
    r["severity"] = rng.choice(["high", "moderate", "low"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["author"] = store.get_reference("Practitioner", rng)
    r["identifiedDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["detail"] = t.p.gen_string(
        resource_type=r.get("resourceType"), field_name="detail"
    )
    r["mitigation"] = [{
        "action": t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
            code=rng.choice(["13", "2", "4", "5", "6", "7", "10", "11"]),
            display=rng.choice([
                "Consulted Prescriber", "Assessed Patient",
                "Consulted other prescriber", "Substituted different drug",
                "Provided patient education", "Instituted ongoing monitoring program",
            ]),
        ),
        "date": t.p.gen_dateTime(min_year=2023, max_year=2024),
    }]
    return r


def enrich_Substance(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["active", "inactive", "entered-in-error"])
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/substance-category",
        code=rng.choice([
            "allergen", "biological", "body", "chemical", "food", "drug", "material",
        ]),
    )]
    r["code"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["372687004", "387207008", "7980", "1191"]),
        display=rng.choice(["Amoxicillin", "Ibuprofen", "Penicillin", "Aspirin"]),
    )
    r["description"] = t.p.gen_string(
        resource_type=r.get("resourceType"), field_name="description"
    )
    return r


def enrich_DeviceUsage(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = pick_code("device_usage_status", rng, "active")
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    if store.has("Device"):
        r["device"] = {
            "reference": store.get_reference("Device", rng),
            "concept": concept_from_section("snomed_devices", rng, t),
        }
    return r


def enrich_DeviceDispense(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = pick_code("device_dispense_status", rng, "completed")
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    r["code"] = codeable_reference_from_section("snomed_devices", rng, t)
    return r


def enrich_BiologicallyDerivedProduct(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["productCategory"] = pick_code("biologically_derived_product_category", rng, "fluid")
    r["productStatus"] = pick_code("biologically_derived_product_status", rng, "available")
    r["code"] = concept_from_section("biologically_derived_product_codes", rng, t)
    r["identifier"] = [t.gen_Identifier(value=f"BDP-{rng.randint(1000, 9999)}")]
    if store.has("Practitioner"):
        r["collection"] = {"collector": store.get_reference("Practitioner", rng)}
    return r


def enrich_OrganizationAffiliation(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["active"] = True
    if store.has("Organization"):
        r["organization"] = store.get_reference("Organization", rng)
        r["participatingOrganization"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["practitioner"] = store.get_reference("Practitioner", rng)
    if store.has("Location"):
        r["location"] = [store.get_reference("Location", rng)]
    r["period"] = t.gen_Period()
    return r


def enrich_Endpoint(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = "active"
    r["connectionType"] = concept_from_section("endpoint_connection_type", rng, t)
    r["payloadType"] = [concept_from_section("mime_types", rng, t)]
    r["address"] = "https://fhir.example.org/r5"
    if store.has("Organization"):
        r["managingOrganization"] = store.get_reference("Organization", rng)
    return r


def enrich_GenomicStudy(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = pick_code("genomic_study_status", rng, "registered")
    if store.has("Patient"):
        r["subject"] = [store.get_reference("Patient", rng)]
    r["identifier"] = [t.gen_Identifier(value=f"GS-{rng.randint(1000, 9999)}")]
    return r


def enrich_Measure(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = pick_code("measure_status", rng, "active")
    r["title"] = rng.choice([
        "Diabetes HbA1c Control",
        "Hypertension Control",
        "Breast Cancer Screening",
    ])
    r["url"] = f"http://example.org/Measure/{r.get('id', 'meas')}"
    r["version"] = "1.0.0"
    r["publisher"] = "National Quality Collaborative"
    r["versionAlgorithmCoding"] = t.gen_Coding(
        system="http://terminology.hl7.org/CodeSystem/version-algorithm",
        code="semver",
        display="Semantic Versioning (semver.org)",
    )
    r.pop("versionAlgorithmString", None)
    return r


def enrich_MeasureReport(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = pick_code("measure_report_status", rng, "complete")
    r["type"] = pick_code("measure_report_type", rng, "individual")
    if store.has("Measure"):
        r["measure"] = store.get_reference("Measure", rng)
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    r["period"] = t.gen_Period()
    if store.has("Practitioner"):
        r["reporter"] = store.get_reference("Practitioner", rng)
    return r


ENRICHERS: dict[str, Any] = {
    "Specimen": enrich_Specimen,
    "ImagingStudy": enrich_ImagingStudy,
    "Device": enrich_Device,
    "ResearchStudy": enrich_ResearchStudy,
    "ResearchSubject": enrich_ResearchSubject,
    "QuestionnaireResponse": enrich_QuestionnaireResponse,
    "AuditEvent": enrich_AuditEvent,
    "EpisodeOfCare": enrich_EpisodeOfCare,
    "HealthcareService": enrich_HealthcareService,
    "RelatedPerson": enrich_RelatedPerson,
    "Group": enrich_Group,
    "DetectedIssue": enrich_DetectedIssue,
    "Substance": enrich_Substance,
    "DeviceUsage": enrich_DeviceUsage,
    "DeviceDispense": enrich_DeviceDispense,
    "BiologicallyDerivedProduct": enrich_BiologicallyDerivedProduct,
    "OrganizationAffiliation": enrich_OrganizationAffiliation,
    "Endpoint": enrich_Endpoint,
    "GenomicStudy": enrich_GenomicStudy,
    "Measure": enrich_Measure,
    "MeasureReport": enrich_MeasureReport,
}
