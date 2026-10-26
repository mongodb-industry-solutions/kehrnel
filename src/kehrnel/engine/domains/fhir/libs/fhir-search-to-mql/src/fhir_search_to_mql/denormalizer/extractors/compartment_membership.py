"""
Generic compartment-membership extractor.

Implements Approach 3 ("Hybrid") from
``analysis_documents/FHIR_TO_MQL_COMPARTMENT.md``: precompute a
``_compartments.<CompartmentType>: [id, ...]`` field on every resource
that belongs to one of the FHIR R5 compartments (Patient, Encounter,
Practitioner, Device, RelatedPerson). This lets ``CompartmentResolver``
collapse a multi-parameter ``$or`` (subject ∪ performer ∪ ...) down to a
single indexed field lookup for the highest-volume case (Patient
compartment), while leaving the other compartments to dynamic
translation.

Why a generic capability extractor (not a resource-specific one):

* The fields it walks are declared per-rule in YAML using FHIRPath-lite
  expressions resolved by :mod:`path_resolver`. Any resource that
  contributes to a compartment can use the same extractor by listing the
  appropriate linking parameters from the canonical
  ``CompartmentDefinition`` (e.g. ``subject`` / ``performer`` for
  Observation, ``actor`` for Appointment, ``link.other`` for Patient).
* Multiple compartments per rule are supported — emit one mapping per
  compartment with the matching ``compartment`` discriminator and the
  extractor populates ``_compartments.Patient``,
  ``_compartments.Practitioner``, etc., in a single pass.

Configuration shape (one entry per compartment per resource)::

    compartment_membership:
      source: $resource
      target: _compartments
      extractor: CompartmentMembershipExtractor
      field_mappings:
        - target_field: Patient            # writes _compartments.Patient
          source_paths:                    # FHIRPath-lite expressions
            - subject
            - performer
          reference_type: Patient          # filter: only Patient/* refs
          include_self: false              # see below
          datatype: array[string]

``include_self: true`` makes the extractor add the resource's own
``id`` to the membership list when the resource's ``resourceType`` is
the same as the compartment's root type. This implements the FHIR
``[base]`` linking-parameter token (e.g. a ``Patient`` resource is in
its own Patient compartment).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import resolve_path


class CompartmentMembershipExtractor(FieldExtractor):
    """Aggregate compartment IDs into a single per-compartment list."""

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if not isinstance(value, dict) or not field_mappings:
            return {}

        result: Dict[str, Any] = {}
        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            if not target_field:
                continue

            ids = list(self._collect_ids(value, mapping))
            if not ids:
                continue

            # Deduplicate while preserving first-seen order so query
            # plans don't depend on dict iteration order.
            seen = set()
            deduped: List[str] = []
            for rid in ids:
                if rid not in seen:
                    seen.add(rid)
                    deduped.append(rid)
            result[target_field] = deduped

        return result

    def _collect_ids(
        self,
        resource: Dict[str, Any],
        mapping: Dict[str, Any],
    ) -> Iterable[str]:
        """Yield compartment-member IDs for a single mapping."""
        reference_type: Optional[str] = mapping.get("reference_type") or mapping.get("compartment")
        # ``[base]`` linking parameter — the resource itself participates
        # in the compartment if its resourceType matches.
        if mapping.get("include_self"):
            self_id = resource.get("id")
            if self_id and (
                reference_type is None
                or resource.get("resourceType") == reference_type
            ):
                yield self_id

        source_paths = self._normalize_paths(mapping)
        for path in source_paths:
            for item in resolve_path(resource, path):
                ref_str = self._extract_reference(item)
                if not ref_str:
                    continue
                rt, rid = self._split_reference(ref_str)
                if rid is None:
                    continue
                # Filter by compartment root type when the rule asks
                # for it. This is the FHIR rule that, for example, the
                # Patient compartment for Observation only includes
                # subject/performer references that point to a Patient
                # (a Group or Practitioner subject does NOT add the
                # resource to the Patient compartment).
                if reference_type and rt is not None and rt != reference_type:
                    continue
                yield rid

    @staticmethod
    def _normalize_paths(mapping: Dict[str, Any]) -> List[str]:
        """Accept either ``source_paths: [...]`` or a single ``source_path``."""
        paths = mapping.get("source_paths")
        if paths is None:
            single = mapping.get("source_path")
            paths = [single] if single else []
        return [p for p in paths if isinstance(p, str) and p]

    @staticmethod
    def _extract_reference(item: Any) -> Optional[str]:
        """Pull the ``reference`` string off a Reference dict, or pass through strings."""
        if isinstance(item, dict):
            ref = item.get("reference")
            return ref if isinstance(ref, str) and ref else None
        if isinstance(item, str) and item:
            return item
        return None

    @staticmethod
    def _split_reference(reference: str) -> tuple[Optional[str], Optional[str]]:
        """
        Split ``ResourceType/id`` (or absolute URL ending in ``/Type/id``)
        into ``(resourceType, id)``. ``#contained`` and bare ids are
        returned as ``(None, id)`` so callers can decide whether to
        include them.
        """
        if reference.startswith("#"):
            return (None, reference[1:])
        # Strip query/fragment: not part of FHIR canonical references but
        # defensive against unexpected inputs.
        ref = reference.split("?", 1)[0].split("#", 1)[0]
        parts = ref.rstrip("/").split("/")
        if len(parts) >= 2 and parts[-2] and parts[-1]:
            return (parts[-2], parts[-1])
        return (None, parts[-1] if parts else None)
