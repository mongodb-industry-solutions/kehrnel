"""Load and integrity-check the product-neutral source-category catalog."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from kehrnel.contracts.package_digest import canonical_json_bytes
from kehrnel.contracts.source_architecture import (
    SourceCategory,
    SourceCategoryCatalog,
    SourceProfileCatalog,
)


DEFAULT_SOURCE_CATEGORY_CATALOG_PATH = (
    Path(__file__).resolve().parents[1] / "source_categories" / "catalog.json"
)


class SourceCategoryCatalogError(ValueError):
    """The source-category catalog is malformed or fails integrity checks."""


def source_category_content_digest(category: SourceCategory | dict[str, Any]) -> str:
    """Digest a normalized category without its self-referential digest."""

    if isinstance(category, SourceCategory):
        payload = category.to_wire()
    else:
        payload = deepcopy(category)
    artifact_ref = payload.get("artifactRef") or payload.get("artifact_ref")
    if not isinstance(artifact_ref, dict):
        raise SourceCategoryCatalogError("source category is missing artifactRef")
    artifact_ref.pop("digest", None)
    return "sha256:" + hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def load_source_category_catalog(
    path: str | Path = DEFAULT_SOURCE_CATEGORY_CATALOG_PATH,
) -> SourceCategoryCatalog:
    catalog_path = Path(path)
    try:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog = SourceCategoryCatalog.model_validate(payload)
    except (OSError, json.JSONDecodeError, ValidationError) as exc:
        raise SourceCategoryCatalogError(
            f"invalid source category catalog at {catalog_path}: {exc}"
        ) from exc

    for category in catalog.categories:
        expected = source_category_content_digest(category)
        if category.artifact_ref.digest != expected:
            raise SourceCategoryCatalogError(
                "source category digest mismatch for "
                f"{category.artifact_ref.id}@{category.artifact_ref.version}"
            )
    return catalog


def assert_source_catalog_links(
    categories: SourceCategoryCatalog,
    profiles: SourceProfileCatalog,
) -> None:
    """Ensure category/profile relationships resolve exact immutable artifacts."""

    category_ids = {category.artifact_ref.id for category in categories.categories}
    profile_refs = {
        (
            profile.artifact_ref.id,
            profile.artifact_ref.version,
            profile.artifact_ref.digest,
        )
        for profile in profiles.profiles
    }
    for profile in profiles.profiles:
        missing = sorted(set(profile.category_ids) - category_ids)
        if missing:
            raise SourceCategoryCatalogError(
                f"source profile '{profile.artifact_ref.id}' references unknown categories: {missing}"
            )
    for category in categories.categories:
        for reference in category.reference_profile_refs:
            key = (reference.id, reference.version, reference.digest)
            if key not in profile_refs:
                raise SourceCategoryCatalogError(
                    "source category reference does not resolve exactly: "
                    f"{category.artifact_ref.id} -> {reference.id}@{reference.version}"
                )
