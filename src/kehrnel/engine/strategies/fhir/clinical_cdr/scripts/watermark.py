"""Kehrnel synthetic watermark markers on generated FHIR resources."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

KEHRNEL_FHIR_SYSTEM = "https://kehrnel.dev/fhir"
KEHRNEL_SYNTHETIC_EXTENSION_URL = "https://kehrnel.dev/fhir/synthetic"
SYNTHETIC_TAG_CODE = "synthetic"
GENERATED_VALUE_CODE = "generated"


def watermark_enabled(strategy_config: dict[str, Any] | None) -> bool:
    """Read ``generation.watermark.enabled`` (default true)."""
    generation = (strategy_config or {}).get("generation") or {}
    watermark = generation.get("watermark")
    if isinstance(watermark, dict):
        return bool(watermark.get("enabled", True))
    return True


def apply_watermark(
    resource: dict[str, Any],
    *,
    system: str = KEHRNEL_FHIR_SYSTEM,
    extension_url: str = KEHRNEL_SYNTHETIC_EXTENSION_URL,
) -> dict[str, Any]:
    """
    Mark a resource as Kehrnel synthetic output.

    Adds ``meta.tag`` (system + code ``synthetic``) and ``meta.extension``
    (url + ``valueCode`` ``generated``) when not already present.
    """
    doc = deepcopy(resource)
    meta = dict(doc.get("meta") or {})

    tags = [dict(t) for t in (meta.get("tag") or []) if isinstance(t, dict)]
    if not any(t.get("system") == system and t.get("code") == SYNTHETIC_TAG_CODE for t in tags):
        tags.append({"system": system, "code": SYNTHETIC_TAG_CODE})
    meta["tag"] = tags

    extensions = [dict(e) for e in (meta.get("extension") or []) if isinstance(e, dict)]
    if not any(e.get("url") == extension_url for e in extensions):
        extensions.append({"url": extension_url, "valueCode": GENERATED_VALUE_CODE})
    meta["extension"] = extensions

    doc["meta"] = meta
    return doc


def apply_watermark_many(
    resources: list[dict[str, Any]],
    *,
    enabled: bool = True,
    system: str = KEHRNEL_FHIR_SYSTEM,
    extension_url: str = KEHRNEL_SYNTHETIC_EXTENSION_URL,
) -> list[dict[str, Any]]:
    if not enabled:
        return resources
    return [
        apply_watermark(doc, system=system, extension_url=extension_url)
        for doc in resources
    ]


def has_synthetic_watermark(resource: dict[str, Any], *, system: str = KEHRNEL_FHIR_SYSTEM) -> bool:
    """True when ``meta.tag`` contains the Kehrnel synthetic marker."""
    tags = (resource.get("meta") or {}).get("tag") or []
    return any(
        isinstance(tag, dict) and tag.get("system") == system and tag.get("code") == SYNTHETIC_TAG_CODE
        for tag in tags
    )
