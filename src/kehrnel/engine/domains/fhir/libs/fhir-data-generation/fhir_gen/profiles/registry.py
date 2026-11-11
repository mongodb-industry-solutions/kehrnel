"""Profile pack registry — lists available packs and their metadata."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProfilePackSpec:
    """Metadata for a profile pack."""
    name: str
    description: str
    fhir_version: str  # Required FHIR version
    ig_name: str
    ig_version: str
    canonical_base: str


PROFILE_PACKS: dict[str, ProfilePackSpec] = {
    "us_core": ProfilePackSpec(
        name="us_core",
        description="US Core Implementation Guide v9.0.0 (USCDI v6, FHIR R4)",
        fhir_version="R4",
        ig_name="hl7.fhir.us.core",
        ig_version="9.0.0",
        canonical_base="http://hl7.org/fhir/us/core/StructureDefinition",
    ),
}


def get_pack(name: str) -> ProfilePackSpec:
    """Get a profile pack spec by name."""
    if name not in PROFILE_PACKS:
        raise ValueError(
            f"Unknown profile pack: {name!r}. "
            f"Available: {list(PROFILE_PACKS.keys())}"
        )
    return PROFILE_PACKS[name]


def list_packs() -> list[str]:
    """List available profile pack names."""
    return list(PROFILE_PACKS.keys())
