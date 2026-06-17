"""Clinical core resource enrichers (Prompt 9)."""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Any

from faker import Faker

from ...codes import codeable_from_section, pick_code, concept_from_section
from ...codes.loader import get_system, random_code
from ...resolvers.reference import ReferenceStore
from ..special_types import SpecialTypeGenerator


def enrich_Patient(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    gender = rng.choice(["male", "female", "other", "unknown"])
    r["gender"] = gender
    days_ago = rng.randint(5 * 365, 95 * 365)
    r["birthDate"] = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    r["name"] = [t.gen_HumanName(use="official"), t.gen_HumanName(use="nickname")]
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    r["address"] = [t.gen_Address()]
    r["identifier"] = [
        t.gen_Identifier(system="http://hospital.example.org/mrn"),
        t.gen_Identifier(system="http://hl7.org/fhir/sid/us-ssn"),
    ]
    ms = random_code("marital_status", rng)
    if ms:
        r["maritalStatus"] = t.gen_CodeableConcept(
            system=get_system("marital_status"),
            code=ms["code"],
            display=ms["display"],
        )
    lang = random_code("languages", rng)
    if lang:
        r["communication"] = [{
            "language": t.gen_CodeableConcept(
                system=lang.get("system") or get_system("languages"),
                code=lang["code"],
                display=lang.get("display"),
            ),
            "preferred": True,
        }]
    if "active" not in r:
        r["active"] = True
    if store.has("Organization"):
        r["managingOrganization"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["generalPractitioner"] = [store.get_reference("Practitioner", rng)]
    from ..field_fill import backbone_filler_for

    contact_fill = backbone_filler_for("Patient_Contact")
    if contact_fill:
        r["contact"] = [contact_fill(t, store, rng)]
    return r


def enrich_Practitioner(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["active"] = True
    r["name"] = [t.gen_HumanName(use="official")]
    r["name"][0]["prefix"] = [rng.choice(["Dr.", "Prof.", "Mr.", "Ms.", "Mrs."])]
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    r["address"] = [t.gen_Address()]
    r["gender"] = rng.choice(["male", "female", "other", "unknown"])
    r["identifier"] = [t.gen_Identifier(
        system="http://hl7.org/fhir/sid/us-npi",
        value=str(rng.randint(1000000000, 9999999999)),
    )]
    spec = random_code("specialties", rng)
    if spec:
        r["qualification"] = [{
            "code": t.gen_CodeableConcept(
                system=get_system("specialties"),
                code=spec["code"],
                display=spec["display"],
            ),
            "identifier": [t.gen_Identifier()],
            "period": t.gen_Period(),
        }]
    return r


def enrich_PractitionerRole(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["active"] = True
    r["period"] = t.gen_Period()
    if store.has("Practitioner"):
        r["practitioner"] = store.get_reference("Practitioner", rng)
    if store.has("Organization"):
        r["organization"] = store.get_reference("Organization", rng)
    role = random_code("practitioner_roles", rng)
    if role:
        r["code"] = [t.gen_CodeableConcept(
            system=get_system("practitioner_roles"),
            code=role["code"],
            display=role["display"],
        )]
    spec = random_code("specialties", rng)
    if spec:
        r["specialty"] = [t.gen_CodeableConcept(
            system=get_system("specialties"),
            code=spec["code"],
            display=spec["display"],
        )]
    if store.has("Location"):
        r["location"] = [store.get_reference("Location", rng)]
    return r


def enrich_Organization(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    f = Faker()
    r["active"] = True
    r["name"] = rng.choice([
        f"{f.last_name()} Medical Center",
        f"{f.city()} General Hospital",
        f"{f.last_name()} Clinic",
        f"St. {f.first_name()} Healthcare",
        f"{f.city()} Health System",
    ])
    org_type = random_code("organization_types", rng)
    if org_type:
        r["type"] = [t.gen_CodeableConcept(
            system=get_system("organization_types"),
            code=org_type["code"],
            display=org_type["display"],
        )]
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    r["address"] = [t.gen_Address()]
    r["identifier"] = [t.gen_Identifier(
        system="http://hl7.org/fhir/sid/us-npi",
        value=str(rng.randint(1000000000, 9999999999)),
    )]
    return r


def enrich_Location(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    loc_type = random_code("location_types", rng)
    r["status"] = rng.choice(["active", "inactive", "suspended"])
    r["mode"] = "instance"
    r["name"] = t.p.gen_string(
        resource_type="Location", field_name="name", max_length=80
    )
    if loc_type:
        r["type"] = [t.gen_CodeableConcept(
            system=get_system("location_types"),
            code=loc_type["code"],
            display=loc_type["display"],
        )]
    r["address"] = t.gen_Address()
    r["telecom"] = [t.gen_ContactPoint("phone")]
    if store.has("Organization"):
        r["managingOrganization"] = store.get_reference("Organization", rng)
    r["physicalType"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/location-physical-type",
        code=rng.choice(["si", "bu", "wi", "wa", "lvl", "ro", "bd", "ve"]),
        display=rng.choice(["Site", "Building", "Wing", "Ward", "Level", "Room", "Bed", "Vehicle"]),
    )
    return r


def enrich_Encounter(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    status = random_code("encounter_status", rng)
    r["status"] = status["code"] if status else "finished"
    enc_class = random_code("encounter_class", rng)
    r["class"] = [t.gen_CodeableConcept(
        system=get_system("encounter_class"),
        code=enc_class["code"] if enc_class else "AMB",
        display=enc_class["display"] if enc_class else "ambulatory",
    )]
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["participant"] = [{
            "type": [t.gen_CodeableConcept(
                system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                code="ATND",
                display="attender",
            )],
            "actor": store.get_reference("Practitioner", rng),
        }]
    r["actualPeriod"] = t.gen_Period()
    if store.has("Organization"):
        r["serviceProvider"] = store.get_reference("Organization", rng)
    if store.has("Location"):
        r["location"] = [{
            "location": store.get_reference("Location", rng),
            "status": rng.choice(["planned", "active", "reserved", "completed"]),
        }]
    r["identifier"] = [t.gen_Identifier(system="http://hospital.example.org/encounters")]
    return r


def enrich_Condition(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    cs = random_code("condition_clinical_status", rng)
    r["clinicalStatus"] = t.gen_CodeableConcept(
        system=get_system("condition_clinical_status"),
        code=cs["code"] if cs else "active",
    )
    vs = random_code("condition_verification_status", rng)
    r["verificationStatus"] = t.gen_CodeableConcept(
        system=get_system("condition_verification_status"),
        code=vs["code"] if vs else "confirmed",
    )
    sev = random_code("condition_severity", rng)
    if sev:
        r["severity"] = t.gen_CodeableConcept(
            system=get_system("condition_severity"),
            code=sev["code"],
            display=sev["display"],
        )
    cond = random_code("snomed_conditions", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("snomed_conditions"),
        code=cond["code"] if cond else "73211009",
        display=cond["display"] if cond else "Diabetes mellitus",
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["recorder"] = store.get_reference("Practitioner", rng)
    r["onsetDateTime"] = t.p.gen_dateTime(min_year=2015, max_year=2023)
    r["recordedDate"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    cat = random_code("condition_category", rng)
    r["category"] = [t.gen_CodeableConcept(
        system=get_system("condition_category"),
        code=cat["code"] if cat else "problem-list-item",
        display=cat["display"] if cat else "Problem List Item",
    )]
    return r


def _clear_value_polymorphs(r: dict[str, Any], keep: str) -> None:
    for key in list(r.keys()):
        if key.startswith("value") and key != keep:
            r.pop(key, None)


def enrich_Observation(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["registered", "preliminary", "final", "amended"])
    cat = random_code("observation_categories", rng)
    r["category"] = [t.gen_CodeableConcept(
        system=get_system("observation_categories"),
        code=cat["code"] if cat else "vital-signs",
        display=cat["display"] if cat else "Vital Signs",
    )]
    loinc = random_code("loinc_observations", rng)
    if loinc:
        r["code"] = t.gen_CodeableConcept(
            system=get_system("loinc_observations"),
            code=loinc["code"],
            display=loinc["display"],
        )
        if not loinc.get("is_panel") and "low" in loinc and "high" in loinc:
            val = round(
                rng.uniform(
                    loinc.get("typical_low", loinc["low"]),
                    loinc.get("typical_high", loinc["high"]),
                ),
                2,
            )
            _clear_value_polymorphs(r, "valueQuantity")
            r["valueQuantity"] = t.gen_Quantity(
                value=val,
                unit=loinc.get("unit"),
                code=loinc.get("ucum"),
                system="http://unitsofmeasure.org",
            )
            r["referenceRange"] = [{
                "low": t.gen_SimpleQuantity(value=loinc["low"], unit=loinc.get("unit")),
                "high": t.gen_SimpleQuantity(value=loinc["high"], unit=loinc.get("unit")),
            }]
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [store.get_reference("Practitioner", rng)]
    r["effectiveDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["issued"] = t.p.gen_instant()
    return r


def enrich_AllergyIntolerance(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    cs = random_code("allergy_clinical_status", rng)
    r["clinicalStatus"] = t.gen_CodeableConcept(
        system=get_system("allergy_clinical_status"),
        code=cs["code"] if cs else "active",
    )
    vs = random_code("allergy_verification_status", rng)
    r["verificationStatus"] = t.gen_CodeableConcept(
        system=get_system("allergy_verification_status"),
        code=vs["code"] if vs else "confirmed",
    )
    r["type"] = t.gen_CodeableConcept(
        system="http://hl7.org/fhir/allergy-intolerance-type",
        code=rng.choice(["allergy", "intolerance"]),
    )
    r["category"] = [rng.choice(["food", "medication", "environment", "biologic"])]
    r["criticality"] = rng.choice(["low", "high", "unable-to-assess"])
    sub = random_code("allergy_substances", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("allergy_substances"),
        code=sub["code"] if sub else "7980",
        display=sub["display"] if sub else "Penicillin",
    )
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    r["onsetDateTime"] = t.p.gen_dateTime(min_year=2010, max_year=2023)
    r["recordedDate"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    sev = random_code("reaction_severity", rng)
    r["reaction"] = [{
        "substance": t.gen_CodeableConcept(
            system=get_system("allergy_substances"),
            code=sub["code"] if sub else "7980",
            display=sub["display"] if sub else "Penicillin",
        ),
        "manifestation": [t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code=rng.choice(["271807003", "39579001", "418290006", "49727002"]),
            display=rng.choice(["Rash", "Anaphylaxis", "Itching", "Cough"]),
        )],
        "severity": sev["code"] if sev else "moderate",
        "onset": t.p.gen_dateTime(min_year=2020, max_year=2023),
    }]
    return r


def enrich_Procedure(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    proc = random_code("snomed_procedures", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("snomed_procedures"),
        code=proc["code"] if proc else "71388002",
        display=proc["display"] if proc else "Procedure",
    )
    r["status"] = rng.choice(["preparation", "in-progress", "completed", "stopped"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    r["occurredDateTime"] = t.p.gen_dateTime(min_year=2022, max_year=2024)
    r["category"] = [t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["387713003", "103693007", "46947000"]),
        display=rng.choice(["Surgical procedure", "Diagnostic procedure", "Chiropractic manipulation"]),
    )]
    body = random_code("body_sites", rng)
    if body:
        r["bodySite"] = [t.gen_CodeableConcept(
            system=get_system("body_sites"),
            code=body["code"],
            display=body["display"],
        )]
    return r


def enrich_DiagnosticReport(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    status = random_code("diagnostic_report_status", rng)
    r["status"] = status["code"] if status else "final"
    cat = random_code("diagnostic_report_categories", rng)
    r["category"] = [t.gen_CodeableConcept(
        system=get_system("diagnostic_report_categories"),
        code=cat["code"] if cat else "LAB",
        display=cat["display"] if cat else "Laboratory",
    )]
    loinc = random_code("loinc_observations", rng)
    r["code"] = t.gen_CodeableConcept(
        system="http://loinc.org",
        code=loinc["code"] if loinc else "58410-2",
        display=loinc["display"] if loinc else "CBC panel",
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [store.get_reference("Practitioner", rng)]
    r["effectiveDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["issued"] = t.p.gen_instant()
    if store.has("Observation"):
        r["result"] = [
            store.get_reference("Observation", rng)
            for _ in range(rng.randint(1, 4))
        ]
    r["conclusion"] = t.p.gen_string(
        resource_type="DiagnosticReport", field_name="conclusion"
    )
    return r


def enrich_Immunization(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    vaccine = random_code("vaccines", rng)
    r["vaccineCode"] = t.gen_CodeableConcept(
        system=get_system("vaccines"),
        code=vaccine["code"] if vaccine else "88",
        display=vaccine["display"] if vaccine else "Influenza, unspecified formulation",
    )
    r["status"] = rng.choice(["completed", "not-done"])
    r["primarySource"] = rng.random() > 0.2
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    if store.has("Location"):
        r["location"] = store.get_reference("Location", rng)
    r["occurrenceDateTime"] = t.p.gen_dateTime(min_year=2020, max_year=2024)
    body = random_code("body_sites", rng)
    if body:
        r["site"] = t.gen_CodeableConcept(
            system=get_system("body_sites"),
            code=body["code"],
            display=body["display"],
        )
    r["route"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-RouteOfAdministration",
        code="IM",
        display="Injection, intramuscular",
    )
    r["doseQuantity"] = t.gen_Quantity(value=0.5, unit="mL", code="mL")
    return r


def enrich_FamilyMemberHistory(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    f = Faker()
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    r["status"] = rng.choice(["partial", "completed", "entered-in-error", "health-unknown"])
    r["relationship"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        code=rng.choice(["MTH", "FTH", "SIB", "CHILD", "GRPRN", "AUNT", "UNCLE"]),
        display=rng.choice(["Mother", "Father", "Sibling", "Child", "Grandparent", "Aunt", "Uncle"]),
    )
    cond = random_code("snomed_conditions", rng)
    r["condition"] = [{
        "code": t.gen_CodeableConcept(
            system=get_system("snomed_conditions"),
            code=cond["code"] if cond else "73211009",
            display=cond["display"] if cond else "Diabetes mellitus",
        ),
        "outcome": t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code=rng.choice(["182992009", "370996005", "418715001"]),
            display=rng.choice(["Treatment completed", "Patient well", "Symptom resolved"]),
        ),
        "onsetAge": t.gen_Age(value=float(rng.randint(30, 80))),
    }]
    r["name"] = f.first_name()
    r["sex"] = t.gen_CodeableConcept(
        system="http://hl7.org/fhir/administrative-gender",
        code=rng.choice(["male", "female", "other", "unknown"]),
    )
    return r


def enrich_ClinicalImpression(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["in-progress", "completed", "entered-in-error"])
    r["description"] = t.p.gen_string(
        resource_type="ClinicalImpression", field_name="description"
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = store.get_reference("Practitioner", rng)
    r["effectiveDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    cond = random_code("snomed_conditions", rng)
    r["finding"] = [{
        "item": t.gen_CodeableConcept(
            system=get_system("snomed_conditions"),
            code=cond["code"] if cond else "73211009",
            display=cond["display"] if cond else "Diabetes mellitus",
        ),
    }]
    return r


def enrich_RiskAssessment(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["registered", "preliminary", "final", "amended"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = store.get_reference("Practitioner", rng)
    r["occurrenceDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["method"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code="413556000",
        display="Risk assessment using assessment tool",
    )
    prob = round(rng.uniform(0.0, 1.0), 2)
    r["prediction"] = [{
        "outcome": t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code=rng.choice(["363346000", "414545008", "230690007"]),
            display=rng.choice(["Malignant neoplasm", "Ischemic heart disease", "Stroke"]),
        ),
        "probabilityDecimal": prob,
        "whenPeriod": t.gen_Period(),
        "relativeRisk": round(rng.uniform(0.5, 5.0), 2),
    }]
    return r


def enrich_Composition(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = pick_code("composition_status", rng, "final")
    r["type"] = concept_from_section("loinc_composition_types", rng, t)
    r["date"] = t.p.gen_date(min_year=2023, max_year=2024)
    r["title"] = t.p.gen_string(
        resource_type="Composition", field_name="title", max_length=80
    )
    if store.has("Patient"):
        r["subject"] = [store.get_reference("Patient", rng)]
    if store.has("Practitioner"):
        r["author"] = [store.get_reference("Practitioner", rng)]
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    return r


def enrich_AdverseEvent(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = pick_code("adverse_event_status", rng, "completed")
    r["actuality"] = pick_code("adverse_event_actuality", rng, "actual")
    r["code"] = concept_from_section("snomed_conditions", rng, t)
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    r["recordedDate"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    return r


def enrich_BodyStructure(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["active"] = True
    site = codeable_from_section("body_sites", rng)
    r["morphology"] = site
    r["includedStructure"] = [{"structure": site}]
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    return r


def enrich_Person(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["active"] = True
    r["name"] = [t.gen_HumanName(use="official")]
    r["gender"] = pick_code("gender", rng, "unknown")
    r["birthDate"] = t.p.gen_date(min_year=1950, max_year=2005)
    if store.has("Patient"):
        r["link"] = [{"target": store.get_reference("Patient", rng)}]
    return r


def enrich_ImmunizationRecommendation(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["date"] = t.p.gen_date(min_year=2023, max_year=2024)
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    forecast = codeable_from_section("immunization_forecast_status", rng)
    vaccine = codeable_from_section("vaccines", rng)
    r["recommendation"] = [{
        "forecastStatus": forecast,
        "vaccineCode": [vaccine],
        "dateCriterion": [{"code": forecast, "value": "2024-09-01"}],
    }]
    return r


ENRICHERS: dict[str, Any] = {
    "Patient": enrich_Patient,
    "Practitioner": enrich_Practitioner,
    "PractitionerRole": enrich_PractitionerRole,
    "Organization": enrich_Organization,
    "Location": enrich_Location,
    "Encounter": enrich_Encounter,
    "Condition": enrich_Condition,
    "Observation": enrich_Observation,
    "AllergyIntolerance": enrich_AllergyIntolerance,
    "Procedure": enrich_Procedure,
    "DiagnosticReport": enrich_DiagnosticReport,
    "Immunization": enrich_Immunization,
    "FamilyMemberHistory": enrich_FamilyMemberHistory,
    "ClinicalImpression": enrich_ClinicalImpression,
    "RiskAssessment": enrich_RiskAssessment,
    "Composition": enrich_Composition,
    "AdverseEvent": enrich_AdverseEvent,
    "BodyStructure": enrich_BodyStructure,
    "Person": enrich_Person,
    "ImmunizationRecommendation": enrich_ImmunizationRecommendation,
}
