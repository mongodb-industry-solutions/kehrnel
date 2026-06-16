"""Medication resource enrichers (Prompt 10)."""

from __future__ import annotations

import random
from typing import Any

from ...codes import pick_code
from ...codes.loader import get_system, random_code
from ...resolvers.reference import ReferenceStore
from ..special_types import SpecialTypeGenerator


def _rxnorm_concept(t: SpecialTypeGenerator, med: dict[str, Any] | None) -> dict[str, Any]:
    return t.gen_CodeableConcept(
        system=get_system("rxnorm_medications"),
        code=med["code"] if med else "313782",
        display=med["display"] if med else "Acetaminophen 325 MG",
    )


def _set_medication(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
    med: dict[str, Any] | None,
) -> None:
    """FHIR R5 CodeableReference for medication[x]-style fields."""
    concept = _rxnorm_concept(t, med)
    if store.has("Medication"):
        r["medication"] = {
            "reference": store.get_reference("Medication", rng),
            "concept": concept,
        }
    else:
        r["medication"] = {"concept": concept}


def enrich_Medication(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    med = random_code("rxnorm_medications", rng)
    r["code"] = _rxnorm_concept(t, med)
    status = random_code("medication_status", rng)
    r["status"] = status["code"] if status else "active"
    if store.has("Organization"):
        r["marketingAuthorizationHolder"] = store.get_reference("Organization", rng)
    r["doseForm"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["385055001", "385061003", "385049006", "385229008"]),
        display=rng.choice([
            "Tablet dose form", "Capsule dose form", "Oral solution", "Injection",
        ]),
    )
    r["ingredient"] = [{
        "item": t.gen_CodeableConcept(
            system="http://www.nlm.nih.gov/research/umls/rxnorm",
            code=med["code"] if med else "161",
            display=rng.choice(["Acetaminophen", "Amoxicillin", "Ibuprofen", "Metformin"]),
        ),
        "isActive": True,
        "strengthRatio": t.gen_Ratio(),
    }]
    return r


def enrich_MedicationRequest(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    status = random_code("medication_request_status", rng)
    r["status"] = status["code"] if status else "active"
    intent = random_code("medication_request_intent", rng)
    r["intent"] = intent["code"] if intent else "order"
    med = random_code("rxnorm_medications", rng)
    _set_medication(r, t, store, rng, med)
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["requester"] = store.get_reference("Practitioner", rng)
        r["recorder"] = store.get_reference("Practitioner", rng)
    r["authoredOn"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["dosageInstruction"] = [t.gen_Dosage()]
    r["dispenseRequest"] = {
        "validityPeriod": t.gen_Period(),
        "numberOfRepeatsAllowed": rng.randint(0, 5),
        "quantity": t.gen_SimpleQuantity(value=float(rng.choice([30, 60, 90])), unit="each"),
        "expectedSupplyDuration": t.gen_Duration(
            value=float(rng.choice([30, 60, 90])),
            unit="d",
        ),
    }
    return r


def enrich_MedicationAdministration(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "in-progress", "not-done", "on-hold", "completed", "entered-in-error", "stopped",
    ])
    med = random_code("rxnorm_medications", rng)
    _set_medication(r, t, store, rng, med)
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    if store.has("MedicationRequest"):
        r["basedOn"] = [store.get_reference("MedicationRequest", rng)]
    r["occurredDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    route = random_code("dosage_routes", rng)
    r["dosage"] = {
        "route": t.gen_CodeableConcept(
            system=get_system("dosage_routes"),
            code=route["code"] if route else "26643006",
            display=route["display"] if route else "Oral route",
        ),
        "dose": t.gen_Quantity(value=round(rng.uniform(100, 500), 1), unit="mg", code="mg"),
    }
    return r


def enrich_MedicationDispense(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "preparation", "in-progress", "cancelled", "on-hold",
        "completed", "entered-in-error", "stopped", "declined", "unknown",
    ])
    med = random_code("rxnorm_medications", rng)
    _set_medication(r, t, store, rng, med)
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    if store.has("MedicationRequest"):
        r["authorizingPrescription"] = [store.get_reference("MedicationRequest", rng)]
    r["quantity"] = t.gen_SimpleQuantity(value=float(rng.choice([30, 60, 90])), unit="each")
    r["daysSupply"] = t.gen_SimpleQuantity(value=float(rng.choice([30, 60, 90])), unit="d")
    r["whenHandedOver"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["dosageInstruction"] = [t.gen_Dosage()]
    return r


def enrich_MedicationStatement(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = pick_code("medication_statement_status", rng, "recorded")
    med = random_code("rxnorm_medications", rng)
    _set_medication(r, t, store, rng, med)
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["informationSource"] = [store.get_reference("Practitioner", rng)]
    r["effectiveDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["dateAsserted"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["dosage"] = [t.gen_Dosage()]
    return r


def enrich_MedicationKnowledge(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    med = random_code("rxnorm_medications", rng)
    r["code"] = _rxnorm_concept(t, med)
    status = random_code("medication_status", rng)
    r["status"] = t.gen_CodeableConcept(
        system=get_system("medication_status"),
        code=status["code"] if status else "active",
    )
    r["doseForm"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code="385055001",
        display="Tablet dose form",
    )
    cond = random_code("snomed_conditions", rng)
    r["indicationGuideline"] = [{
        "indication": [t.gen_CodeableConcept(
            system=get_system("snomed_conditions"),
            code=cond["code"] if cond else "73211009",
            display=cond["display"] if cond else "Diabetes mellitus",
        )],
        "dosingGuideline": [{
            "dosage": [{
                "type": t.gen_CodeableConcept(
                    system=get_system("dosage_timing"),
                    code="QD",
                    display="Every day",
                ),
                "dosage": [t.gen_Dosage()],
            }],
        }],
    }]
    return r


ENRICHERS: dict[str, Any] = {
    "Medication": enrich_Medication,
    "MedicationRequest": enrich_MedicationRequest,
    "MedicationAdministration": enrich_MedicationAdministration,
    "MedicationDispense": enrich_MedicationDispense,
    "MedicationStatement": enrich_MedicationStatement,
    "MedicationKnowledge": enrich_MedicationKnowledge,
}
