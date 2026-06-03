"""Financial, insurance, and billing resource enrichers (Prompt 12)."""

from __future__ import annotations

import random
from typing import Any

from ...codes.loader import get_system, random_code
from ...resolvers.reference import ReferenceStore
from ..special_types import SpecialTypeGenerator


def enrich_Coverage(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["active", "cancelled", "draft", "entered-in-error"])
    cov_type = random_code("coverage_type", rng)
    r["type"] = t.gen_CodeableConcept(
        system=get_system("coverage_type"),
        code=cov_type["code"] if cov_type else "EHCPOL",
        display=cov_type["display"] if cov_type else "Extended healthcare",
    )
    if store.has("Patient"):
        r["beneficiary"] = store.get_reference("Patient", rng)
        r["subscriber"] = store.get_reference("Patient", rng)
    if store.has("Organization"):
        r["insurer"] = store.get_reference("Organization", rng)
    r["subscriberId"] = [t.gen_Identifier(
        system="http://insurance.example.org/subscribers",
        value=f"SUB{rng.randint(100000, 999999)}",
    )]
    r["relationship"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/subscriber-relationship",
        code=rng.choice(["self", "spouse", "child", "parent", "common"]),
    )
    r["period"] = t.gen_Period()
    r["class"] = [{
        "type": t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/coverage-class",
            code=rng.choice(["group", "plan", "subplan", "class"]),
        ),
        "value": t.gen_Identifier(value=f"GRP{rng.randint(10000, 99999)}"),
    }]
    r["order"] = rng.randint(1, 3)
    return r


def enrich_Claim(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["active", "cancelled", "draft", "entered-in-error"])
    r["type"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/claim-type",
        code=rng.choice(["institutional", "oral", "pharmacy", "professional", "vision"]),
    )
    r["use"] = rng.choice(["claim", "preauthorization", "predetermination"])
    if store.has("Patient"):
        patient_ref = store.get_reference("Patient", rng)
        r["patient"] = patient_ref
        r["subject"] = patient_ref
    r["created"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    if store.has("Organization"):
        r["insurer"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["provider"] = store.get_reference("Practitioner", rng)
    r["priority"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/processpriority",
        code=rng.choice(["stat", "normal", "deferred"]),
    )
    if store.has("Coverage"):
        r["insurance"] = [{
            "sequence": 1,
            "focal": True,
            "coverage": store.get_reference("Coverage", rng),
            "identifier": t.gen_Identifier(),
        }]
    if store.has("Encounter"):
        r["item"] = [{
            "sequence": i + 1,
            "encounter": [store.get_reference("Encounter", rng)],
            "productOrService": t.gen_CodeableConcept(
                system="http://snomed.info/sct",
                code=rng.choice(["371883000", "308335008"]),
                display=rng.choice(["Outpatient procedure", "Patient encounter"]),
            ),
            "unitPrice": t.gen_Money(),
            "net": t.gen_Money(),
        } for i in range(rng.randint(1, 3))]
    r["total"] = t.gen_Money()
    return r


def enrich_ClaimResponse(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["active", "cancelled", "draft", "entered-in-error"])
    r["type"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/claim-type",
        code=rng.choice(["institutional", "professional"]),
    )
    r["use"] = rng.choice(["claim", "preauthorization", "predetermination"])
    if store.has("Patient"):
        patient_ref = store.get_reference("Patient", rng)
        r["patient"] = patient_ref
        r["subject"] = patient_ref
    r["created"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    if store.has("Organization"):
        r["insurer"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["requestor"] = store.get_reference("Practitioner", rng)
    r["outcome"] = rng.choice(["queued", "complete", "error", "partial"])
    r["disposition"] = rng.choice([
        "Claim adjudicated as submitted",
        "Partial payment approved",
        "Claim denied",
        "Approved for processing",
    ])
    r["total"] = [{
        "category": t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/adjudication",
            code=rng.choice(["submitted", "copay", "eligible", "deductible", "benefit"]),
        ),
        "amount": t.gen_Money(),
    }]
    return r


def enrich_Account(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["active", "inactive", "entered-in-error", "on-hold", "unknown"])
    r["type"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code=rng.choice(["PBILLACCT", "PUBLICPOL"]),
        display=rng.choice(["patient billing account", "public healthcare policy"]),
    )
    r["name"] = f"Account-{rng.randint(10000, 99999)}"
    if store.has("Patient"):
        r["subject"] = [store.get_reference("Patient", rng)]
    r["servicePeriod"] = t.gen_Period()
    if store.has("Organization"):
        r["owner"] = store.get_reference("Organization", rng)
    r["description"] = t.p.faker.sentence()
    if store.has("Coverage"):
        r["coverage"] = [{
            "coverage": store.get_reference("Coverage", rng),
            "priority": 1,
        }]
    return r


def enrich_Invoice(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["draft", "issued", "balanced", "cancelled", "entered-in-error"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["participant"] = [{"actor": store.get_reference("Practitioner", rng)}]
    if store.has("Organization"):
        r["issuer"] = store.get_reference("Organization", rng)
    if store.has("Account"):
        r["account"] = store.get_reference("Account", rng)
    r["date"] = t.p.gen_date(min_year=2023, max_year=2024)
    r["lineItem"] = [{
        "sequence": i + 1,
        "chargeItem": t.gen_CodeableConcept(
            system=get_system("snomed_procedures"),
            code=(proc := random_code("snomed_procedures", rng) or {"code": "80146002"})["code"],
            display=proc.get("display", "Appendectomy"),
        ),
        "priceComponent": [{
            "type": rng.choice([
                "base", "surcharge", "deduction", "discount", "tax", "informational",
            ]),
            "amount": t.gen_Money(),
        }],
    } for i in range(rng.randint(1, 4))]
    r["totalNet"] = t.gen_Money()
    r["totalGross"] = t.gen_Money()
    return r


def enrich_ChargeItem(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice([
        "planned", "billable", "not-billable", "aborted",
        "billed", "entered-in-error", "unknown",
    ])
    r["code"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["308335008", "371883000"]),
        display=rng.choice(["Patient encounter procedure", "Outpatient procedure"]),
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    r["occurrenceDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["quantity"] = t.gen_Quantity(value=1.0, unit="each")
    r["unitPriceComponent"] = [{
        "type": "base",
        "amount": t.gen_Money(),
    }]
    return r


def enrich_CoverageEligibilityRequest(
    r: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    r["status"] = rng.choice(["active", "cancelled", "draft", "entered-in-error"])
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    r["created"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    if store.has("Practitioner"):
        r["provider"] = store.get_reference("Practitioner", rng)
    if store.has("Organization"):
        r["insurer"] = store.get_reference("Organization", rng)
    if store.has("Coverage"):
        r["insurance"] = [{"coverage": store.get_reference("Coverage", rng), "focal": True}]
    r["purpose"] = [rng.choice(["auth-requirements", "benefits", "discovery", "validation"])]
    return r


ENRICHERS: dict[str, Any] = {
    "Coverage": enrich_Coverage,
    "Claim": enrich_Claim,
    "ClaimResponse": enrich_ClaimResponse,
    "Account": enrich_Account,
    "Invoice": enrich_Invoice,
    "ChargeItem": enrich_ChargeItem,
    "CoverageEligibilityRequest": enrich_CoverageEligibilityRequest,
}
