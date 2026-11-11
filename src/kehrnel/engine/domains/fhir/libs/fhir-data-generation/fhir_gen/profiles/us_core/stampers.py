"""US Core profile stampers — apply meta.profile and required extensions."""

from __future__ import annotations

import random
from typing import Any

from .terminology import US_CORE_PROFILES
from .extensions import (
    gen_us_core_birthsex,
    gen_us_core_ethnicity,
    gen_us_core_gender_identity,
    gen_us_core_race,
)


def stamp_meta_profile(resource: dict[str, Any], profile_url: str) -> dict[str, Any]:
    """Add the US Core profile URL to resource.meta.profile."""
    if "meta" not in resource:
        resource["meta"] = {}
    meta = resource["meta"]
    profiles = list(meta.get("profile", []))
    if profile_url not in profiles:
        profiles.append(profile_url)
    meta["profile"] = profiles
    return resource


def stamp_patient(resource: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    """Apply US Core Patient profile: meta.profile + race/ethnicity/birthsex extensions."""
    stamp_meta_profile(resource, US_CORE_PROFILES["Patient"])
    extensions = list(resource.get("extension", []))
    # Add race extension (~85% of records have race data)
    if rng.random() < 0.85:
        extensions.append(gen_us_core_race(rng))
    # Add ethnicity extension (~80% of records)
    if rng.random() < 0.80:
        extensions.append(gen_us_core_ethnicity(rng))
    # Add birthsex extension (~90% of records)
    if rng.random() < 0.90:
        extensions.append(gen_us_core_birthsex(rng))
    # Add genderIdentity extension (~60% of records)
    if rng.random() < 0.60:
        extensions.append(gen_us_core_gender_identity(rng))
    if extensions:
        resource["extension"] = extensions
    return resource


def stamp_observation(resource: dict[str, Any], rng: random.Random) -> dict[str, Any]:
    """Apply US Core Observation profile: meta.profile."""
    # Choose the most appropriate US Core Observation profile based on category
    category_codes = []
    for cat in resource.get("category", []):
        for coding in cat.get("coding", []):
            category_codes.append(coding.get("code", ""))
    if "laboratory" in category_codes:
        profile_url = US_CORE_PROFILES["Observation"]  # us-core-observation-lab
    elif "vital-signs" in category_codes:
        profile_url = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-vital-signs"
    else:
        profile_url = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-simple-observation"
    stamp_meta_profile(resource, profile_url)
    return resource


# Dispatch table: resource_type -> stamper function
STAMPERS: dict[str, Any] = {
    "Patient": stamp_patient,
    "Observation": stamp_observation,
}

# Resources that only get meta.profile stamped (no extra extensions)
_PROFILE_ONLY = [
    "Practitioner", "PractitionerRole", "Organization", "Location",
    "Encounter", "Condition", "AllergyIntolerance", "DiagnosticReport",
    "Immunization", "MedicationRequest", "MedicationDispense", "Medication",
    "Procedure", "DocumentReference", "CarePlan", "CareTeam", "Device",
    "Goal", "ServiceRequest", "Coverage", "RelatedPerson", "Specimen",
    "FamilyMemberHistory", "Provenance", "QuestionnaireResponse",
]
for _rt in _PROFILE_ONLY:
    if _rt in US_CORE_PROFILES:
        def _make_stamper(url: str):
            def _stamper(resource: dict[str, Any], rng: random.Random) -> dict[str, Any]:
                return stamp_meta_profile(resource, url)
            return _stamper
        STAMPERS[_rt] = _make_stamper(US_CORE_PROFILES[_rt])
