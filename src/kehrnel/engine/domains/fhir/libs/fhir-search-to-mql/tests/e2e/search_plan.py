"""
Build full FHIR search test plans per E2E scenario.

Includes:
- One **convert** + **search** per bundled resource (from scenario + fallbacks)
- **Secondary** search queries where defined in scenario_searches
- **Compartment** searches (Patient / Practitioner / Device / Encounter) when applicable
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence

from .cli_scenarios_mql import MqlPipelineScenario, SearchStep
from .resource_search_queries import compartment_query

SearchKind = Literal["convert", "search", "compartment_search"]

# Resources that support Patient-compartment REST-style search in our configs
PATIENT_COMPARTMENT_RESOURCES: frozenset[str] = frozenset({
    "Observation",
    "Condition",
    "Encounter",
    "MedicationRequest",
    "MedicationAdministration",
    "MedicationStatement",
    "MedicationDispense",
    "AllergyIntolerance",
    "Immunization",
    "DiagnosticReport",
    "DocumentReference",
    "Composition",
    "CarePlan",
    "Goal",
    "ServiceRequest",
    "Procedure",
    "DeviceRequest",
    "ClinicalImpression",
    "FamilyMemberHistory",
    "Flag",
    "RiskAssessment",
    "Communication",
    "NutritionOrder",
    "NutritionIntake",
    "VisionPrescription",
    "Claim",
    "Coverage",
    "QuestionnaireResponse",
})

PRACTITIONER_COMPARTMENT_RESOURCES: frozenset[str] = frozenset({"Schedule"})

DEVICE_COMPARTMENT_RESOURCES: frozenset[str] = frozenset({"Observation"})

ENCOUNTER_COMPARTMENT_RESOURCES: frozenset[str] = frozenset({
    "Condition",
    "Observation",
})

# Extra non-compartment queries per scenario (resource -> list of query strings)
SCENARIO_EXTRA_SEARCHES: dict[str, dict[str, tuple[str, ...]]] = {
    "hc01": {
        "Patient": ("gender=male", "birthdate=ge1980-01-01"),
    },
    "hc04": {
        "Appointment": ("status=booked&date=ge2024-07-01",),
    },
    "hc07": {
        "Observation": ("category=vital-signs&date=ge2024-06-01",),
    },
}

@dataclass(frozen=True)
class PlannedSearch:
    kind: SearchKind
    resource: str
    query: str
    extra_args: tuple[str, ...] = ()

    @property
    def is_compartment(self) -> bool:
        return self.kind == "compartment_search"

    def as_search_step(self) -> SearchStep:
        return (self.resource, self.query, self.extra_args)


def _planned(
    kind: SearchKind,
    resource: str,
    query: str,
    *extra: str,
) -> PlannedSearch:
    return PlannedSearch(kind=kind, resource=resource, query=query, extra_args=extra)


def _compartment_steps(
    resources: Sequence[str],
    *,
    patient_id: str | None,
    practitioner_id: str | None,
    encounter_id: str | None,
    device_id: str | None,
) -> list[PlannedSearch]:
    steps: list[PlannedSearch] = []
    for resource in resources:
        query = compartment_query(resource)
        if patient_id and resource in PATIENT_COMPARTMENT_RESOURCES:
            steps.append(
                _planned(
                    "compartment_search",
                    resource,
                    query,
                    "--compartment-type",
                    "Patient",
                    "--compartment-id",
                    patient_id,
                )
            )
        if practitioner_id and resource in PRACTITIONER_COMPARTMENT_RESOURCES:
            steps.append(
                _planned(
                    "compartment_search",
                    resource,
                    query,
                    "--compartment-type",
                    "Practitioner",
                    "--compartment-id",
                    practitioner_id,
                )
            )
        if device_id and resource in DEVICE_COMPARTMENT_RESOURCES:
            steps.append(
                _planned(
                    "compartment_search",
                    resource,
                    "code=8480-6",
                    "--compartment-type",
                    "Device",
                    "--compartment-id",
                    device_id,
                )
            )
        if encounter_id and resource in ENCOUNTER_COMPARTMENT_RESOURCES:
            steps.append(
                _planned(
                    "compartment_search",
                    resource,
                    query,
                    "--compartment-type",
                    "Encounter",
                    "--compartment-id",
                    encounter_id,
                )
            )
    return steps


def build_search_plan(
    scenario: MqlPipelineScenario,
    *,
    patient_id: str | None = None,
    practitioner_id: str | None = None,
    encounter_id: str | None = None,
    device_id: str | None = None,
    include_compartments: bool = True,
) -> tuple[PlannedSearch, ...]:
    """
    Full ordered list of convert/search/compartment tests for one scenario.

    Order: convert+search per resource query, then compartment passes.
    """
    planned: list[PlannedSearch] = []
    seen: set[tuple[SearchKind, str, str, tuple[str, ...]]] = set()

    def add(kind: SearchKind, resource: str, query: str, *extra: str) -> None:
        key = (kind, resource, query, extra)
        if key in seen:
            return
        seen.add(key)
        planned.append(_planned(kind, resource, query, *extra))

    # Primary searches from scenario (already covers each resource at least once)
    for resource, query, extra in scenario.searches:
        search_kind: SearchKind = (
            "compartment_search" if "--compartment-type" in extra else "search"
        )
        add("convert", resource, query, *extra)
        add(search_kind, resource, query, *extra)

    # Secondary queries from SCENARIO_EXTRA_SEARCHES
    extras = SCENARIO_EXTRA_SEARCHES.get(scenario.id, {})
    for resource, queries in extras.items():
        if resource not in scenario.resources:
            continue
        for q in queries:
            add("convert", resource, q)
            add("search", resource, q)

    # Explicit compartment rows already in SCENARIO_SEARCHES (e.g. hc20) — keep as search
    if include_compartments:
        for step in _compartment_steps(
            scenario.resources,
            patient_id=patient_id,
            practitioner_id=practitioner_id,
            encounter_id=encounter_id,
            device_id=device_id,
        ):
            add("convert", step.resource, step.query, *step.extra_args)
            add("compartment_search", step.resource, step.query, *step.extra_args)

    return tuple(planned)


def sample_reference_ids(
    mongo_uri: str,
    db_name: str,
) -> dict[str, str | None]:
    """Load sample ids from generated data for compartment searches."""
    from pymongo import MongoClient

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[db_name]

    def _id(collection: str) -> str | None:
        doc = db[collection].find_one({}, {"id": 1})
        if doc and doc.get("id"):
            return str(doc["id"])
        return None

    refs = {
        "patient_id": _id("Patient"),
        "practitioner_id": _id("Practitioner"),
        "encounter_id": _id("Encounter"),
        "device_id": _id("Device"),
    }
    client.close()
    return refs
