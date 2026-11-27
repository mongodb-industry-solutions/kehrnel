"""Normalize CanonicalResource / MetadataResource fields on generated FHIR data."""

from __future__ import annotations

import random
from typing import Any

from ..resolvers.reference import ReferenceStore
from .special_types import SpecialTypeGenerator

_VERSION_ALGORITHM_SYSTEM = "http://terminology.hl7.org/CodeSystem/version-algorithm"

# Resources whose ``publisher`` element is a string (not Reference) in FHIR R5.
_STRING_PUBLISHER_RESOURCES: frozenset[str] = frozenset({
    "ActivityDefinition",
    "CapabilityStatement",
    "ChargeItemDefinition",
    "Citation",
    "CodeSystem",
    "CompartmentDefinition",
    "ConceptMap",
    "ConditionDefinition",
    "EventDefinition",
    "ExampleScenario",
    "GraphDefinition",
    "ImplementationGuide",
    "Library",
    "Measure",
    "MedicationKnowledge",
    "MessageDefinition",
    "NamingSystem",
    "OperationDefinition",
    "PlanDefinition",
    "Questionnaire",
    "Requirements",
    "SearchParameter",
    "StructureDefinition",
    "StructureMap",
    "SubscriptionTopic",
    "TerminologyCapabilities",
    "TestPlan",
    "TestScript",
    "ValueSet",
})


def _default_publisher_name(rng: random.Random) -> str:
    return rng.choice([
        "Regional Health Authority",
        "Acme Clinical Content",
        "National Quality Collaborative",
    ])


def _plausible_publisher_name(text: str) -> bool:
    """Reject clinical narrative accidentally stored in ``publisher``."""
    if len(text) > 80 or "\n" in text:
        return False
    lowered = text.lower()
    clinical_markers = (
        "patient", "clinical", "medication", "assessment", "vital", "care team",
    )
    return not any(marker in lowered for marker in clinical_markers)


def _publisher_label(
    publisher: Any,
    store: ReferenceStore,
    rng: random.Random,
) -> str:
    if isinstance(publisher, str) and publisher.strip():
        cleaned = publisher.strip()
        if _plausible_publisher_name(cleaned):
            return cleaned
    if isinstance(publisher, dict):
        display = publisher.get("display")
        if isinstance(display, str) and display.strip():
            return display.strip()
        ref = publisher.get("reference", "")
        if isinstance(ref, str) and "/" in ref:
            rtype, _rid = ref.split("/", 1)
            if store.has(rtype):
                linked = store.get_resource(rtype, rng)
                if linked:
                    label = store._extract_display(linked, rtype)
                    if label:
                        return label
            return rtype
    return _default_publisher_name(rng)


def _valid_coding(coding: Any) -> bool:
    return (
        isinstance(coding, dict)
        and bool(coding.get("code"))
        and bool(coding.get("system") or coding.get("code"))
    )


def _normalize_version_algorithm(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    rng: random.Random,
) -> None:
    coding_key = "versionAlgorithmCoding"
    string_key = "versionAlgorithmString"
    coding = resource.get(coding_key)
    string_val = resource.get(string_key)

    if isinstance(coding, dict) and not _valid_coding(coding):
        resource.pop(coding_key, None)
        coding = None

    if isinstance(string_val, str):
        stripped = string_val.strip()
        # Reject clinical narrative mistaken for a version algorithm token.
        if not stripped or len(stripped) > 64 or " " in stripped:
            resource.pop(string_key, None)
            string_val = None
        else:
            resource[string_key] = stripped

    if coding and string_val:
        resource.pop(string_key, None)
    elif not coding and not string_val:
        resource[coding_key] = t.gen_Coding(
            system=_VERSION_ALGORITHM_SYSTEM,
            code=rng.choice(["semver", "integer", "alpha"]),
            display=rng.choice([
                "Semantic Versioning (semver.org)",
                "Integer versioning",
                "Alphabetical versioning",
            ]),
        )


def normalize_canonical_resource(
    resource: dict[str, Any],
    t: SpecialTypeGenerator,
    store: ReferenceStore,
    rng: random.Random,
) -> dict[str, Any]:
    """Coerce publisher and versionAlgorithm[x] to valid FHIR shapes."""
    rtype = resource.get("resourceType", "")
    if not rtype:
        return resource

    if rtype in _STRING_PUBLISHER_RESOURCES or "publisher" in resource:
        pub = resource.get("publisher")
        if rtype in _STRING_PUBLISHER_RESOURCES or pub is not None:
            resource["publisher"] = _publisher_label(pub, store, rng)

    if any(k in resource for k in ("versionAlgorithmCoding", "versionAlgorithmString")):
        _normalize_version_algorithm(resource, t, rng)
    elif rtype in _STRING_PUBLISHER_RESOURCES:
        # Canonical artifacts commonly carry a version algorithm when version is set.
        if resource.get("version") and rng.random() < 0.85:
            _normalize_version_algorithm(resource, t, rng)

    return resource
