"""
Reference extractor for FHIR reference structures.

Extracts:
- {resourceType}Id: Extracted ID (e.g., patientId, practitionerId)
- {resourceType}Name: Display name if present
- {resourceType}Type: Resource type
- references: Array of full reference strings
- ids: Array of extracted IDs
- types: Array of resource types

Two invocation modes (mirrors :class:`CodeableConceptExtractor`):

1. **Pre-resolved**: ``value`` is a Reference (or list of References) that
   the denormalizer already navigated via a top-level field. The legacy
   ``nested_path`` heuristic still kicks in for the ``link[*].other.reference``
   shape so existing configs keep working unchanged.

2. **Resource-rooted** (``source: $resource``): ``value`` is the entire
   FHIR resource and each field_mapping's ``source_path`` is a path
   expression evaluated by :mod:`path_resolver`. The path may target the
   ``Reference`` object itself (``component[*].valueReference``) or its
   ``.reference`` string directly — both shapes are accepted.
"""

from typing import Any, Dict, List, Optional
import re

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import (
    looks_like_resource,
    resolve_path,
)


class ReferenceExtractor(FieldExtractor):
    """Extract Reference FHIR structure to searchable fields."""
    
    # Pattern to parse reference: ResourceType/id or full URL
    REFERENCE_PATTERN = re.compile(r'(?:.*/)?([\w]+)/([\w-]+)$')

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Reference structure.
        
        Args:
            value: Reference or list of Reference structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted reference fields
        """
        # Resource-rooted mode: walk source_path against the full resource
        # using the generic FHIRPath-lite resolver. Each mapping resolves
        # independently so a single rule can populate per-component arrays.
        if looks_like_resource(value) and field_mappings:
            return self._extract_from_resource(value, field_mappings)

        result = {}
        references = self._ensure_list(value)

        if not references:
            return result

        # Check if source_path indicates nested structure (e.g., "link[*].other.reference")
        nested_path = None
        if field_mappings and len(field_mappings) > 0:
            source_path = field_mappings[0].get('source_path', '')
            # Extract nested path after [*]. (e.g., "link[*].other.reference" -> "other.reference")
            if '[*].' in source_path:
                nested_path = source_path.split('[*].', 1)[1]
                # If nested_path is just "reference", treat as standard (not nested)
                if nested_path == 'reference':
                    nested_path = None
        
        all_ids = []
        all_types = []
        all_references = []
        all_displays = []
        
        # Store specific resource type references
        typed_references = {}
        
        for ref in references:
            if not isinstance(ref, dict):
                continue
            
            # Handle nested paths (e.g., link[*].other.reference)
            if nested_path and '.' in nested_path:
                # Navigate nested structure to get to the reference object
                # For "other.reference", we navigate to "other" and then get "reference"
                nested_ref = ref
                path_parts = nested_path.rsplit('.', 1)[0].split('.')  # Get all parts except final "reference"
                
                for part in path_parts:
                    if isinstance(nested_ref, dict):
                        nested_ref = nested_ref.get(part)
                    else:
                        nested_ref = None
                        break
                
                if not nested_ref or not isinstance(nested_ref, dict):
                    continue
                
                reference_string = nested_ref.get('reference')
                display = nested_ref.get('display')
            else:
                # Standard reference structure
                reference_string = ref.get('reference')
                display = ref.get('display')
            
            if not reference_string:
                continue
            
            # Parse the reference
            parsed = self._parse_reference(reference_string)
            if not parsed:
                continue
            
            resource_type, resource_id = parsed
            
            all_ids.append(resource_id)
            all_types.append(resource_type)
            all_references.append(reference_string)
            
            if display:
                all_displays.append(display)
            
            # Store typed reference
            type_key = f"{resource_type.lower()}Id"
            if type_key not in typed_references:
                typed_references[type_key] = []
            typed_references[type_key].append(resource_id)
            
            # Store display for this type
            if display:
                display_key = f"{resource_type.lower()}Name"
                if display_key not in typed_references:
                    typed_references[display_key] = []
                typed_references[display_key].append(display)
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                normalize = mapping.get('normalize')
                reference_type = mapping.get('referenceType')
                extract_type = mapping.get('extractType')  # specify id, type, full, display
                # `filterType` restricts the projection to references whose
                # parsed resource type matches (case-insensitive). This is
                # what `_search.patientId` / `_search.practitionerId` /
                # `_search.locationId` need so an Appointment with three
                # participants of three different types doesn't end up with
                # the same array stuffed into all three fields.
                filter_type = mapping.get('filterType')
                datatype = mapping.get('datatype', 'string')  # array vs single value

                if not target_field:
                    continue

                # Build the per-mapping pool, filtered by resource type when
                # requested. We zip the parallel lists so id/type/full all
                # stay aligned to the same underlying reference.
                if filter_type:
                    ft = filter_type.lower()
                    pool_ids: List[str] = []
                    pool_types: List[str] = []
                    pool_refs: List[str] = []
                    pool_displays: List[str] = []
                    # `all_displays` is sparse (only appended when display
                    # is present), so we can't zip it positionally. Use
                    # the typed_references map for filtered displays.
                    for rid, rt, rfull in zip(all_ids, all_types, all_references):
                        if rt.lower() == ft:
                            pool_ids.append(rid)
                            pool_types.append(rt)
                            pool_refs.append(rfull)
                    pool_displays = typed_references.get(
                        f"{ft}Name", []
                    )
                else:
                    pool_ids = all_ids
                    pool_types = all_types
                    pool_refs = all_references
                    pool_displays = all_displays

                if extract_type:
                    if extract_type == 'id':
                        values = pool_ids
                    elif extract_type == 'type':
                        values = pool_types
                    elif extract_type == 'full':
                        values = pool_refs
                    elif extract_type == 'display':
                        values = pool_displays
                    else:
                        values = pool_ids
                elif reference_type:
                    # Legacy: `referenceType` was overloaded as both a
                    # filter and a projection-by-type selector. Preserve
                    # that contract — tests rely on it.
                    type_key = f"{reference_type.lower()}Id"
                    values = typed_references.get(type_key, [])
                elif 'display' in source_path:
                    values = pool_displays
                elif 'reference' in source_path:
                    values = pool_refs
                else:
                    values = pool_ids

                if normalize == 'lowercase':
                    values = [v.lower() if isinstance(v, str) else v for v in values]

                if 'array' in datatype:
                    # Sparse output: skip empty arrays so a typed bucket
                    # like `patientId` is omitted entirely on Appointments
                    # that have no Patient participant, instead of
                    # writing `[]` (which clutters indexes for no
                    # benefit and breaks $exists-style coverage queries).
                    if values:
                        result[target_field] = values
                else:
                    if len(values) > 1:
                        result[target_field] = values
                    elif len(values) == 1:
                        result[target_field] = values[0]
        else:
            # Default extraction (backward compatibility)
            result['ids'] = all_ids if all_ids else []
            result['types'] = all_types if all_types else []
            result['references'] = all_references if all_references else []
            if all_displays:
                result['displays'] = all_displays
            
            # Add typed references
            result.update(typed_references)
        
        return result
    
    def _extract_from_resource(
        self,
        resource: Dict[str, Any],
        field_mappings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Resource-rooted extraction: each mapping declares a path expression
        in ``source_path`` that is resolved against ``resource`` to a flat
        list of references (or reference strings). Each mapping's
        ``extractType`` (or ``referenceType`` for legacy configs) selects
        which projection — id, type, full reference, or display — is
        written to the target field.
        """
        result: Dict[str, Any] = {}
        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = mapping.get("source_path") or ""
            if not target_field or not source_path:
                continue

            resolved = resolve_path(resource, source_path)
            ids: List[str] = []
            types: List[str] = []
            full_refs: List[str] = []
            displays: List[str] = []

            for item in resolved:
                ref_str: Optional[str] = None
                display: Optional[str] = None
                if isinstance(item, dict):
                    # Item may be a Reference object or a string-only path.
                    ref_str = item.get("reference") if "reference" in item else None
                    display = item.get("display") if "display" in item else None
                elif isinstance(item, str):
                    ref_str = item
                if not ref_str:
                    continue
                parsed = self._parse_reference(ref_str)
                if not parsed:
                    continue
                rt, rid = parsed
                ids.append(rid)
                types.append(rt)
                full_refs.append(ref_str)
                if display:
                    displays.append(display)

            # Optional resource-type filter — see the pre-resolved
            # branch above for the rationale (the typed-id buckets
            # like `_search.patientId` should hold IDs of `Patient/*`
            # references only, not every participant).
            filter_type = mapping.get("filterType")
            if filter_type:
                ft = filter_type.lower()
                f_ids: List[str] = []
                f_types: List[str] = []
                f_refs: List[str] = []
                f_displays: List[str] = []
                # `displays` is sparse — we can't zip it positionally with
                # ids/types/refs, so collect from a parallel sweep.
                for idx, rt in enumerate(types):
                    if rt.lower() == ft:
                        f_ids.append(ids[idx])
                        f_types.append(rt)
                        f_refs.append(full_refs[idx])
                # Re-walk to pull displays for the matching subset. This is
                # cheap relative to denormalization overall and avoids a
                # second parallel array on the hot path.
                for item in resolved:
                    if isinstance(item, dict):
                        ref_str = item.get("reference")
                        display = item.get("display")
                        if not ref_str or not display:
                            continue
                        parsed = self._parse_reference(ref_str)
                        if parsed and parsed[0].lower() == ft:
                            f_displays.append(display)
                ids, types, full_refs, displays = f_ids, f_types, f_refs, f_displays

            extract_type = mapping.get("extractType") or mapping.get("referenceType")
            if extract_type == "id":
                values: List[str] = ids
            elif extract_type == "type":
                values = types
            elif extract_type == "full":
                values = full_refs
            elif extract_type == "display":
                values = displays
            else:
                values = ids

            # Skip the write when nothing resolved — see CodeableConcept's
            # `_assign` for the equivalent rationale (resource-rooted rules
            # always fire so we filter empties here to keep output sparse).
            if not values:
                continue

            normalize = mapping.get("normalize")
            if normalize == "lowercase":
                values = [v.lower() if isinstance(v, str) else v for v in values]

            datatype = mapping.get("datatype", "string")
            if "array" in datatype:
                result[target_field] = values
            else:
                if len(values) > 1:
                    result[target_field] = values
                elif len(values) == 1:
                    result[target_field] = values[0]

        return result

    def _parse_reference(self, reference: str) -> Optional[tuple]:
        """
        Parse a FHIR reference string.
        
        Args:
            reference: Reference string (e.g., "Patient/123", "http://example.org/fhir/Patient/123")
            
        Returns:
            Tuple of (resource_type, resource_id) or None if invalid
        """
        if not reference:
            return None
        
        # Handle contained references (#id)
        if reference.startswith('#'):
            return ('Contained', reference[1:])
        
        # Parse using regex
        match = self.REFERENCE_PATTERN.match(reference)
        if match:
            return (match.group(1), match.group(2))
        
        return None
