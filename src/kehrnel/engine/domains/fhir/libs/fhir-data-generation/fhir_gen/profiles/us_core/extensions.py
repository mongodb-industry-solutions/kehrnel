"""US Core R4 extension generators."""

from __future__ import annotations

import random
from typing import Any

from .terminology import (
    EXT_BIRTHSEX,
    EXT_ETHNICITY,
    EXT_GENDER_IDENTITY,
    EXT_RACE,
    BIRTHSEX_CODES,
    ETHNICITY_OMB_CODES,
    RACE_OMB_CODES,
)


def gen_us_core_race(rng: random.Random) -> dict[str, Any]:
    """Generate US Core race extension (us-core-race)."""
    race = rng.choice(RACE_OMB_CODES)
    return {
        "url": EXT_RACE,
        "extension": [
            {
                "url": "ombCategory",
                "valueCoding": {
                    "system": race["system"],
                    "code": race["code"],
                    "display": race["display"],
                },
            },
            {
                "url": "text",
                "valueString": race["display"],
            },
        ],
    }


def gen_us_core_ethnicity(rng: random.Random) -> dict[str, Any]:
    """Generate US Core ethnicity extension (us-core-ethnicity)."""
    eth = rng.choice(ETHNICITY_OMB_CODES)
    return {
        "url": EXT_ETHNICITY,
        "extension": [
            {
                "url": "ombCategory",
                "valueCoding": {
                    "system": eth["system"],
                    "code": eth["code"],
                    "display": eth["display"],
                },
            },
            {
                "url": "text",
                "valueString": eth["display"],
            },
        ],
    }


def gen_us_core_birthsex(rng: random.Random) -> dict[str, Any]:
    """Generate US Core birthsex extension (us-core-birthsex)."""
    return {
        "url": EXT_BIRTHSEX,
        "valueCode": rng.choice(BIRTHSEX_CODES),
    }


def gen_us_core_gender_identity(rng: random.Random) -> dict[str, Any]:
    """Generate US Core genderIdentity extension."""
    codes = [
        {"code": "446151000124109", "display": "Identifies as male gender"},
        {"code": "446141000124107", "display": "Identifies as female gender"},
        {"code": "33791000087105", "display": "Identifies as nonbinary gender"},
        {"code": "asked-declined", "display": "Asked but declined to answer"},
    ]
    choice = rng.choice(codes)
    return {
        "url": EXT_GENDER_IDENTITY,
        "valueCodeableConcept": {
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": choice["code"],
                "display": choice["display"],
            }],
            "text": choice["display"],
        },
    }


def gen_us_core_sex(rng: random.Random) -> dict[str, Any]:
    """Generate US Core sex extension (us-core-sex — documented sex, USCDI v3+)."""
    from .terminology import EXT_SEX
    return {
        "url": EXT_SEX,
        "valueCodeableConcept": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/v3-AdministrativeGender",
                "code": rng.choice(["M", "F", "UN"]),
                "display": rng.choice(["Male", "Female", "Undifferentiated"]),
            }],
        },
    }


def gen_us_core_interpreter_required(rng: random.Random) -> dict[str, Any]:
    """Generate US Core interpreter-required extension."""
    from .terminology import EXT_INTERPRETER_REQUIRED
    return {
        "url": EXT_INTERPRETER_REQUIRED,
        "valueBoolean": rng.choice([True, False]),
    }


def gen_us_core_tribal_affiliation(rng: random.Random) -> dict[str, Any]:
    """Generate US Core tribal affiliation extension."""
    from .terminology import EXT_TRIBAL_AFFILIATION
    tribes = [
        {"code": "1002-5", "display": "American Indian or Alaska Native"},
        {"code": "1004-1", "display": "Abenaki"},
        {"code": "1006-6", "display": "Algonquian"},
    ]
    tribe = rng.choice(tribes)
    return {
        "url": EXT_TRIBAL_AFFILIATION,
        "extension": [
            {
                "url": "tribalAffiliation",
                "valueCodeableConcept": {
                    "coding": [{
                        "system": "urn:oid:2.16.840.1.113883.6.238",
                        "code": tribe["code"],
                        "display": tribe["display"],
                    }],
                    "text": tribe["display"],
                },
            },
            {
                "url": "isEnrolled",
                "valueBoolean": rng.choice([True, False]),
            },
        ],
    }
