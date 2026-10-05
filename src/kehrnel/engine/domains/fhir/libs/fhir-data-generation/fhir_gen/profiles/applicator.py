"""Profile pack applicator — post-enricher hook in ResourceGenerator."""

from __future__ import annotations

import random
from typing import Any


def apply_profile_pack(
    resource: dict[str, Any],
    profile_pack: str | None,
    schema_version: str,
    rng: random.Random,
) -> dict[str, Any]:
    """
    Apply a profile pack to a generated resource.

    Args:
        resource: The generated FHIR resource dict.
        profile_pack: Profile pack name (e.g. "us_core") or None.
        schema_version: Active FHIR version ("R4", "R5", "R6").
        rng: Random instance for probabilistic extension generation.

    Returns:
        Resource with profile stamps applied (mutates in place, returns same dict).

    Raises:
        ValueError: If profile_pack is set but schema_version is not R4
                    (US Core requires R4).
    """
    if not profile_pack:
        return resource

    if profile_pack == "us_core":
        if schema_version != "R4":
            raise ValueError(
                f"profile_pack='us_core' requires schema_version='R4', "
                f"got schema_version='{schema_version}'. "
                "US Core v9.0.0 targets FHIR R4 (4.0.1) only."
            )
        from .us_core.stampers import STAMPERS
        resource_type = resource.get("resourceType", "")
        stamper = STAMPERS.get(resource_type)
        if stamper:
            resource = stamper(resource, rng)
    else:
        raise ValueError(
            f"Unknown profile_pack: {profile_pack!r}. "
            "Supported packs: 'us_core'"
        )

    return resource
