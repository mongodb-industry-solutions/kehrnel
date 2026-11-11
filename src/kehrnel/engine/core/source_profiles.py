"""Load and integrity-check the bundled source-profile catalog."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.source_architecture import SourceProfile, SourceProfileCatalog


DEFAULT_SOURCE_PROFILE_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "source_profiles" / "catalog.json"
)


class SourceProfileCatalogError(ValueError):
    """The source-profile catalog is malformed or fails integrity checks."""


def source_profile_content_digest(profile: SourceProfile | dict[str, Any]) -> str:
    """Digest a profile while excluding its self-referential digest value."""

    try:
        model = profile if isinstance(profile, SourceProfile) else SourceProfile.model_validate(profile)
        payload = model.to_wire()
        payload["artifactRef"].pop("digest", None)
        return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    except ValueError as exc:
        raise SourceProfileCatalogError("source profile content cannot be digested") from exc


def load_source_profile_catalog(
    path: str | Path = DEFAULT_SOURCE_PROFILE_CATALOG_PATH,
) -> SourceProfileCatalog:
    catalog_path = Path(path)
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog = SourceProfileCatalog.model_validate(payload)
    except (OSError, UnicodeError, json.JSONDecodeError, ValidationError) as exc:
        raise SourceProfileCatalogError(
            f"invalid source profile catalog at {catalog_path}: {exc}"
        ) from exc

    for profile in catalog.profiles:
        expected = source_profile_content_digest(profile)
        if profile.artifact_ref.digest != expected:
            raise SourceProfileCatalogError(
                "source profile digest mismatch for "
                f"{profile.artifact_ref.id}@{profile.artifact_ref.version}"
            )
    return catalog


def source_profile_index(
    catalog: SourceProfileCatalog,
) -> dict[tuple[str, str, str], SourceProfile]:
    return {
        (
            profile.artifact_ref.id,
            profile.artifact_ref.version,
            profile.artifact_ref.digest,
        ): profile
        for profile in catalog.profiles
    }
