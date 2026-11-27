"""In-memory store for cross-resource FHIR references."""

from __future__ import annotations

import re
from typing import Any

_REFERENCE_RE = re.compile(r"^([A-Za-z]+)/([A-Za-z0-9\-\.]{1,64})$")

_REFERENCE_FIELD_TARGETS: dict[str, list[str]] = {
    "actor": ["Practitioner", "PractitionerRole", "Patient", "RelatedPerson", "Device", "CareTeam"],
    "party": ["Patient", "Organization", "Practitioner", "RelatedPerson", "PractitionerRole"],
    "coverage": ["Coverage"],
    "appointment": ["Appointment"],
    "insurer": ["Organization"],
    "request": ["CoverageEligibilityRequest", "ServiceRequest", "CommunicationRequest"],
    "link": ["DocumentReference", "DiagnosticReport"],
    "resource": ["Patient", "Observation", "Condition", "Encounter"],
    "entity": ["Patient", "Practitioner", "Device", "Medication", "Substance"],
    "subject": ["Patient", "Group"],
    "product": ["Medication", "BiologicallyDerivedProduct", "Device"],
    "manufacturer": ["Organization"],
    "organization": ["Organization"],
    "observer": ["Practitioner", "Device", "Organization"],
    "contributor": ["Practitioner", "Organization", "Patient"],
    "variableDefinition": ["EvidenceVariable", "Group"],
    "characteristic": ["EvidenceVariable"],
    "information": ["Coverage", "DocumentReference"],
    "account": ["Account"],
    "item": ["Patient", "Encounter", "Appointment", "Observation"],
}


class ReferenceStore:
    """
    Session-scoped store of generated resources for cross-referencing.
    Maps resource_type -> list of {id, reference, display, resource}.
    """

    def __init__(self) -> None:
        self._store: dict[str, list[dict[str, Any]]] = {}

    def register(self, resource: dict[str, Any]) -> None:
        """Register a generated resource for future referencing."""
        rtype = resource.get("resourceType")
        rid = resource.get("id")
        if not rtype or not rid:
            return
        entry = {
            "id": rid,
            "reference": f"{rtype}/{rid}",
            "display": self._extract_display(resource, rtype),
            "resource": resource,
        }
        self._store.setdefault(rtype, []).append(entry)

    def get_reference(self, resource_type: str, rng) -> dict[str, Any] | None:
        """Return a FHIR Reference for a random registered resource of the given type."""
        entries = self._store.get(resource_type, [])
        if not entries:
            return None
        entry = rng.choice(entries)
        return {
            "reference": entry["reference"],
            "type": resource_type,
            "display": entry["display"],
        }

    def get_id(self, resource_type: str, rng) -> str | None:
        entries = self._store.get(resource_type, [])
        if not entries:
            return None
        return rng.choice(entries)["id"]

    def has(self, resource_type: str) -> bool:
        return bool(self._store.get(resource_type))

    def has_id(self, resource_type: str, resource_id: str) -> bool:
        return any(e["id"] == resource_id for e in self._store.get(resource_type, []))

    def count(self, resource_type: str) -> int:
        return len(self._store.get(resource_type, []))

    def get_resource(self, resource_type: str, rng) -> dict[str, Any] | None:
        """Return full resource dict for a random entry of the given type."""
        entries = self._store.get(resource_type, [])
        if not entries:
            return None
        return rng.choice(entries)["resource"]

    def _extract_display(self, resource: dict[str, Any], rtype: str) -> str:
        if rtype == "Patient":
            names = resource.get("name", [])
            if names:
                n = names[0]
                given = " ".join(n.get("given", []))
                family = n.get("family", "")
                return f"{given} {family}".strip() or "Patient"
        elif rtype in ("Practitioner", "RelatedPerson"):
            names = resource.get("name", [])
            if names:
                n = names[0]
                return f"Dr. {n.get('family', '')}".strip()
        elif rtype == "Organization":
            return resource.get("name", "Unknown Organization")
        elif rtype == "Medication":
            cc = resource.get("code", {})
            codings = cc.get("coding", [{}])
            if codings:
                return codings[0].get("display", "Unknown Medication")
            return "Unknown Medication"
        elif rtype == "Location":
            return resource.get("name", "Unknown Location")
        return f"{rtype}/{resource.get('id', 'unknown')}"

    def clear(self, resource_type: str | None = None) -> None:
        if resource_type:
            self._store.pop(resource_type, None)
        else:
            self._store.clear()

    def all_resources(self) -> list[dict[str, Any]]:
        """Every registered resource document in generation order."""
        resources: list[dict[str, Any]] = []
        for entries in self._store.values():
            resources.extend(entry["resource"] for entry in entries)
        return resources

    def reference_is_valid(self, reference: str) -> bool:
        """True if ``Type/id`` points at a registered resource."""
        match = _REFERENCE_RE.match(reference)
        if not match:
            return reference.startswith("urn:")
        rtype, rid = match.group(1), match.group(2)
        return self.has_id(rtype, rid)

    def repair_reference(self, ref_obj: dict[str, Any], rng) -> dict[str, Any] | None:
        """
        Ensure a Reference object points at a registered resource.
        Returns a valid Reference, or None if the field should be removed.
        """
        if not isinstance(ref_obj, dict):
            return None
        reference = ref_obj.get("reference")
        if not isinstance(reference, str):
            return None
        if reference.startswith("urn:"):
            return None
        match = _REFERENCE_RE.match(reference)
        if not match:
            return None
        rtype, rid = match.group(1), match.group(2)
        if self.has_id(rtype, rid):
            return ref_obj
        fixed = self.get_reference(rtype, rng)
        return fixed

    @staticmethod
    def _is_fhir_reference_element(node: dict[str, Any]) -> bool:
        """True when ``node`` is a Reference element, not a resource with a ``reference`` URI field."""
        if "resourceType" in node:
            return False
        if ReferenceStore._is_identifier_element(node):
            return False
        ref = node.get("reference")
        return isinstance(ref, str)

    @staticmethod
    def _is_identifier_element(node: dict[str, Any]) -> bool:
        """True when ``node`` is an Identifier (e.g. Claim.related.reference), not a FHIR Reference."""
        if not isinstance(node, dict):
            return False
        if "value" in node and not isinstance(node.get("reference"), str):
            return True
        if node.keys() <= {"id", "extension", "modifierExtension", "type", "system", "use", "period", "assigner"}:
            return "value" in node or "system" in node or "type" in node
        return False

    @staticmethod
    def _is_backbone_element(node: dict[str, Any]) -> bool:
        """True when ``node`` is a nested backbone object, not a Reference placeholder."""
        if not isinstance(node, dict) or "resourceType" in node:
            return False
        if ReferenceStore._is_fhir_reference_element(node):
            return False
        meta = {"id", "extension", "modifierExtension"}
        keys = set(node.keys()) - meta
        if not keys:
            return False
        if keys & {
            "coverage", "sequence", "focal", "relationship", "claim", "identifier",
            "businessArrangement", "preAuthRef", "claimResponse", "party", "role",
        }:
            return True
        return len(keys) >= 2

    def fill_missing_references(self, resource: dict[str, Any], rng) -> dict[str, Any]:
        """Populate Reference elements that are missing ``reference`` when targets exist in the store."""

        def fill_ref(field_name: str, node: dict[str, Any]) -> dict[str, Any] | None:
            if node.get("reference"):
                return node
            for candidate in _REFERENCE_FIELD_TARGETS.get(field_name, []):
                if self.has(candidate):
                    filled = self.get_reference(candidate, rng)
                    if filled:
                        return filled
            return node if node.get("reference") else None

        def walk(node: Any, parent_key: str | None = None) -> Any:
            if isinstance(node, dict):
                if self._is_backbone_element(node):
                    pass
                elif self._is_fhir_reference_element(node) or (
                    parent_key in _REFERENCE_FIELD_TARGETS
                    and not self._is_identifier_element(node)
                ):
                    if not node.get("reference") and parent_key:
                        filled = fill_ref(parent_key, node)
                        if filled:
                            return filled
                result: dict[str, Any] = {}
                for key, value in node.items():
                    new_value = walk(value, key)
                    if new_value is not None:
                        result[key] = new_value
                return result
            if isinstance(node, list):
                return [
                    item
                    for item in (walk(entry, parent_key) for entry in node)
                    if item is not None
                ]
            return node

        return walk(resource)

    def repair_resource(self, resource: dict[str, Any], rng) -> dict[str, Any]:
        """Rebind broken ``Type/id`` references to registered resources."""

        def walk(node: Any) -> Any:
            if isinstance(node, dict):
                if self._is_fhir_reference_element(node):
                    repaired = self.repair_reference(node, rng)
                    if repaired is not None:
                        return repaired
                    ref = node["reference"]
                    if ref.startswith("urn:") or (
                        "/" in ref and not self.reference_is_valid(ref)
                    ):
                        return None
                    return node
                cleaned: dict[str, Any] = {}
                for key, value in node.items():
                    new_value = walk(value)
                    if new_value is not None:
                        cleaned[key] = new_value
                return cleaned
            if isinstance(node, list):
                return [
                    item
                    for item in (walk(entry) for entry in node)
                    if item is not None
                ]
            return node

        return walk(resource)
