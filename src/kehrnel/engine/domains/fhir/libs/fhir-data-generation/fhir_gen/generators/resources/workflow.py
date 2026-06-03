"""Workflow, scheduling, and administrative resource enrichers (Prompt 11)."""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Any

from ...codes.loader import get_system, random_code
from ...resolvers.reference import ReferenceStore
from ..special_types import SpecialTypeGenerator


def _end_from_start(start_str: str, minutes: int) -> str:
    start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
    return (start + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def enrich_Appointment(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    status = random_code("appointment_status", rng)
    r["status"] = status["code"] if status else "booked"
    r["start"] = t.p.gen_instant()
    duration = rng.choice([15, 30, 45, 60])
    start_dt = datetime.fromisoformat(r["start"].replace("Z", "+00:00"))
    r["end"] = (start_dt + timedelta(minutes=duration)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    r["minutesDuration"] = duration
    st = random_code("service_type", rng)
    r["serviceType"] = [
        t.gen_CodeableReference(
            system=get_system("service_type"),
            code=st["code"] if st else "124",
            display=st["display"] if st else "General Practice",
        )
    ]
    participants: list[dict[str, Any]] = []
    if store.has("Patient"):
        participants.append({
            "actor": store.get_reference("Patient", rng),
            "status": "accepted",
            "type": [t.gen_CodeableConcept(
                system=get_system("participation_type"),
                code="SBJ",
                display="subject",
            )],
        })
    if store.has("Practitioner"):
        participants.append({
            "actor": store.get_reference("Practitioner", rng),
            "status": rng.choice(["accepted", "tentative"]),
            "type": [t.gen_CodeableConcept(
                system=get_system("participation_type"),
                code="ATND",
                display="attender",
            )],
        })
    if store.has("Location"):
        participants.append({
            "actor": store.get_reference("Location", rng),
            "status": "accepted",
        })
    r["participant"] = participants
    r["description"] = t.p.faker.sentence(nb_words=6)
    return r


def enrich_CarePlan(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["draft", "active", "on-hold", "revoked", "completed"])
    r["intent"] = rng.choice(["proposal", "plan", "order", "option"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["author"] = store.get_reference("Practitioner", rng)
    r["period"] = t.gen_Period()
    r["title"] = rng.choice([
        "Diabetes Management Plan",
        "Hypertension Care Plan",
        "Post-Surgery Recovery Plan",
        "Cardiac Care Plan",
        "Mental Health Care Plan",
    ])
    r["description"] = t.p.faker.paragraph(nb_sentences=2)
    r["category"] = [t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["736055001", "735321000", "734163000"]),
        display=rng.choice(["Nursing care plan", "Medication management plan", "Care plan"]),
    )]
    if store.has("CareTeam"):
        r["careTeam"] = [store.get_reference("CareTeam", rng)]
    if store.has("Condition"):
        cond = random_code("snomed_conditions", rng)
        r["addresses"] = [t.gen_CodeableConcept(
            system=get_system("snomed_conditions"),
            code=cond["code"] if cond else "73211009",
            display=cond["display"] if cond else "Diabetes mellitus",
        )]
    if store.has("Goal"):
        r["goal"] = [store.get_reference("Goal", rng)]
    return r


def enrich_CareTeam(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["proposed", "active", "suspended", "inactive", "entered-in-error"])
    r["name"] = rng.choice([
        "Primary Care Team",
        "Oncology Team",
        "Cardiology Team",
        "Diabetes Management Team",
        "Mental Health Team",
    ])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    r["period"] = t.gen_Period()
    participants: list[dict[str, Any]] = []
    if store.has("Practitioner"):
        for _ in range(rng.randint(1, 3)):
            role = random_code("practitioner_roles", rng)
            participants.append({
                "role": t.gen_CodeableConcept(
                    system=get_system("practitioner_roles"),
                    code=role["code"] if role else "112247003",
                    display=role["display"] if role else "Medical doctor",
                ),
                "member": store.get_reference("Practitioner", rng),
                "period": t.gen_Period(),
            })
    r["participant"] = participants
    if store.has("Organization"):
        r["managingOrganization"] = [store.get_reference("Organization", rng)]
    return r


def enrich_Goal(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["lifecycleStatus"] = rng.choice([
        "proposed", "planned", "accepted", "active",
        "on-hold", "completed", "cancelled",
    ])
    r["achievementStatus"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/goal-achievement",
        code=rng.choice([
            "in-progress", "improving", "worsening", "no-change",
            "achieved", "sustaining", "not-achieved",
        ]),
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    r["description"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["281078001", "395082007", "386490003"]),
        display=rng.choice([
            "Maintain blood pressure", "Weight reduction", "Blood glucose control",
        ]),
    )
    r["startDate"] = t.p.gen_date(min_year=2023, max_year=2024)
    r["target"] = [{
        "measure": t.gen_CodeableConcept(
            system="http://loinc.org",
            code=rng.choice(["8480-6", "29463-7", "4548-4"]),
            display=rng.choice(["Systolic BP", "Body weight", "HbA1c"]),
        ),
        "detailQuantity": t.gen_Quantity(value=round(rng.uniform(50, 150), 1), unit="mm[Hg]"),
        "dueDate": t.p.gen_date(min_year=2024, max_year=2025),
    }]
    if store.has("Practitioner"):
        r["expressedBy"] = store.get_reference("Practitioner", rng)
    return r


def enrich_ServiceRequest(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["draft", "active", "on-hold", "revoked", "completed", "unknown"])
    r["intent"] = rng.choice(["proposal", "plan", "order", "original-order"])
    r["priority"] = rng.choice(["routine", "urgent", "asap", "stat"])
    proc = random_code("snomed_procedures", rng)
    r["code"] = t.gen_CodeableReference(
        system=get_system("snomed_procedures"),
        code=proc["code"] if proc else "71388002",
        display=proc["display"] if proc else "Procedure",
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["requester"] = store.get_reference("Practitioner", rng)
        r["performer"] = [store.get_reference("Practitioner", rng)]
    r["authoredOn"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["occurrenceDateTime"] = t.p.gen_dateTime(min_year=2024, max_year=2025)
    return r


def enrich_Task(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "draft", "requested", "received", "accepted",
        "in-progress", "completed", "cancelled",
    ])
    r["intent"] = rng.choice(["unknown", "proposal", "plan", "order", "original-order"])
    r["priority"] = rng.choice(["routine", "urgent", "asap", "stat"])
    r["code"] = t.gen_CodeableConcept(
        system="http://hl7.org/fhir/CodeSystem/task-code",
        code=rng.choice(["approve", "fulfill", "abort", "replace", "change", "suspend", "resume"]),
    )
    r["description"] = t.p.faker.sentence()
    if store.has("Patient"):
        r["for"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["requester"] = store.get_reference("Practitioner", rng)
        r["owner"] = store.get_reference("Practitioner", rng)
    r["authoredOn"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["lastModified"] = t.p.gen_dateTime(min_year=2024, max_year=2025)
    r["executionPeriod"] = t.gen_Period()
    return r


def enrich_Communication(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "preparation", "in-progress", "not-done", "on-hold",
        "stopped", "completed", "entered-in-error", "unknown",
    ])
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/communication-category",
        code=rng.choice(["alert", "notification", "reminder", "instruction"]),
    )]
    r["priority"] = rng.choice(["routine", "urgent", "asap", "stat"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["sender"] = store.get_reference("Practitioner", rng)
        r["recipient"] = [store.get_reference("Practitioner", rng)]
    r["sent"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["payload"] = [{"contentString": t.p.faker.paragraph(nb_sentences=2)}]
    return r


def enrich_DocumentReference(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["current", "superseded", "entered-in-error"])
    r["docStatus"] = rng.choice(["preliminary", "final", "amended", "entered-in-error"])
    r["type"] = t.gen_CodeableConcept(
        system="http://loinc.org",
        code=rng.choice(["11488-4", "34117-2", "51847-2", "11506-3"]),
        display=rng.choice([
            "Consultation Note", "History and Physical Note",
            "Assessment Note", "Progress Note",
        ]),
    )
    loinc_cat = random_code("loinc_observations", rng)
    r["category"] = [t.gen_CodeableConcept(
        system=get_system("loinc_observations"),
        code=loinc_cat["code"] if loinc_cat else "11488-4",
        display=loinc_cat["display"] if loinc_cat else "Consultation note",
    )]
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["author"] = [store.get_reference("Practitioner", rng)]
    if store.has("Organization"):
        r["custodian"] = store.get_reference("Organization", rng)
    r["date"] = t.p.gen_instant()
    r["content"] = [{
        "attachment": t.gen_Attachment(content_type="text/plain"),
        "profile": [{
            "valueCoding": t.gen_Coding(
                system="http://ihe.net/fhir/ihe.formatcode.fhir/CodeSystem/formatcode",
                code="urn:ihe:iti:xds:2017:mimeTypeSufficient",
            ),
        }],
    }]
    if store.has("Encounter"):
        r["context"] = [{"encounter": [store.get_reference("Encounter", rng)]}]
    return r


def enrich_Schedule(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["active"] = rng.random() > 0.1
    st = random_code("service_type", rng)
    r["serviceType"] = [
        t.gen_CodeableReference(
            system=get_system("service_type"),
            code=st["code"] if st else "124",
            display=st["display"] if st else "General Practice",
        )
    ]
    spec = random_code("specialties", rng)
    if spec:
        r["specialty"] = [t.gen_CodeableConcept(
            system=get_system("specialties"),
            code=spec["code"],
            display=spec["display"],
        )]
    actors: list[dict[str, Any]] = []
    if store.has("Practitioner"):
        actors.append(store.get_reference("Practitioner", rng))
    if store.has("Location"):
        actors.append(store.get_reference("Location", rng))
    r["actor"] = actors or [t.gen_Reference(resource_type="Practitioner")]
    r["planningHorizon"] = t.gen_Period()
    r["comment"] = t.p.faker.sentence()
    return r


def enrich_Slot(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "busy", "free", "busy-unavailable", "busy-tentative", "entered-in-error",
    ])
    if store.has("Schedule"):
        r["schedule"] = store.get_reference("Schedule", rng)
    r["start"] = t.p.gen_instant()
    duration = rng.choice([15, 30, 45, 60])
    start_dt = datetime.fromisoformat(r["start"].replace("Z", "+00:00"))
    r["end"] = (start_dt + timedelta(minutes=duration)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    st = random_code("service_type", rng)
    r["serviceType"] = [
        t.gen_CodeableReference(
            system=get_system("service_type"),
            code=st["code"] if st else "124",
            display=st["display"] if st else "General Practice",
        )
    ]
    spec = random_code("specialties", rng)
    if spec:
        r["specialty"] = [t.gen_CodeableConcept(
            system=get_system("specialties"),
            code=spec["code"],
            display=spec["display"],
        )]
    r["appointmentType"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v2-0276",
        code=rng.choice(["ROUTINE", "FOLLOWUP", "WALKIN"]),
        display=rng.choice([
            "Routine appointment",
            "A follow up visit from a previous appointment",
            "A previously unscheduled walk-in visit",
        ]),
    )]
    r["serviceCategory"] = [t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["408443003", "11429006", "185349003"]),
        display=rng.choice([
            "General medical practice",
            "Consultation",
            "Encounter for check up",
        ]),
    )]
    return r


def enrich_Flag(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["active", "inactive", "entered-in-error"])
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/flag-category",
        code=rng.choice([
            "diet", "drug", "lab", "admin", "contact", "clinical",
            "behavioral", "research", "advance-directive", "safety",
        ]),
    )]
    fc = random_code("flag_codes", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("flag_codes"),
        code=fc["code"] if fc else "386472006",
        display=fc["display"] if fc else "Fall risk",
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["author"] = store.get_reference("Practitioner", rng)
    r["period"] = t.gen_Period()
    return r


def enrich_Consent(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "draft", "active", "inactive", "not-done", "entered-in-error", "unknown",
    ])
    r["date"] = t.p.gen_date(min_year=2023, max_year=2024)
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code=rng.choice(["59284-0", "64292-6", "57016-8"]),
        display=rng.choice(["Patient Consent", "Privacy Consent", "Privacy policy"]),
    )]
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Organization"):
        r["controller"] = [store.get_reference("Organization", rng)]
    elif store.has("Patient"):
        r["controller"] = [store.get_reference("Patient", rng)]
    return r


def enrich_NutritionOrder(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "draft", "active", "on-hold", "revoked", "completed",
        "entered-in-error", "unknown",
    ])
    r["intent"] = rng.choice(["proposal", "plan", "order"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["orderer"] = store.get_reference("Practitioner", rng)
    r["dateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["oralDiet"] = {
        "type": [t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code=rng.choice(["435801000124108", "437421000124105", "182922004"]),
            display=rng.choice(["Low sodium diet", "Low fat diet", "Diabetic diet"]),
        )],
        "texture": [{
            "modifier": t.gen_CodeableConcept(
                system="http://snomed.info/sct",
                code="228055009",
                display="Regular diet",
            ),
        }],
    }
    return r


ENRICHERS: dict[str, Any] = {
    "Appointment": enrich_Appointment,
    "CarePlan": enrich_CarePlan,
    "CareTeam": enrich_CareTeam,
    "Goal": enrich_Goal,
    "ServiceRequest": enrich_ServiceRequest,
    "Task": enrich_Task,
    "Communication": enrich_Communication,
    "DocumentReference": enrich_DocumentReference,
    "Schedule": enrich_Schedule,
    "Slot": enrich_Slot,
    "Flag": enrich_Flag,
    "Consent": enrich_Consent,
    "NutritionOrder": enrich_NutritionOrder,
}
