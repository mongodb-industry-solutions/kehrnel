"""
Identifier extractor for FHIR identifier structures.

Extracts:
- values: Array of identifier values
- systems: Array of identifier systems
- systemValues: Array of "system|value" pairs
- types: Array of identifier types

Two invocation modes (mirrors :class:`CodeableConceptExtractor` and
:class:`ReferenceExtractor`):

1. **Pre-resolved**: ``value`` is an Identifier (or list) already
   navigated by the denormalizer via a top-level field on the resource.

2. **Resource-rooted** (``source: $resource``): ``value`` is the entire
   FHIR resource; each mapping's ``source_path`` is a path expression
   evaluated by :mod:`path_resolver` (supports dot navigation, ``[*]``
   array iteration, and ``|`` union — e.g.
   ``identifier | qualification[*].identifier``). This lets a single
   denormalization rule populate one set of target fields from
   identifiers that live in multiple sub-resources, which is exactly
   the FHIR R5 pattern for ``Organization.identifier``.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor
from fhir_search_to_mql.denormalizer.path_resolver import (
    looks_like_resource,
    resolve_path,
)


class IdentifierExtractor(FieldExtractor):
    """Extract Identifier FHIR structure to searchable fields."""

    def extract(
        self,
        value: Any,
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Identifier structure.

        Args:
            value: Identifier, list of Identifier, or full FHIR resource
                (with ``resourceType``). Resource-mode requires
                ``field_mappings`` to declare paths via ``source_path``.
            field_mappings: Field mapping configuration

        Returns:
            Dictionary with extracted identifier fields
        """
        if looks_like_resource(value) and field_mappings:
            return self._extract_from_resource(value, field_mappings)
        result = {}
        identifiers = self._ensure_list(value)
        
        if not identifiers:
            return result
        
        values = []
        systems = []
        system_values = []
        types = []
        uses = []
        
        for identifier in identifiers:
            if not isinstance(identifier, dict):
                continue
            
            ident_value = identifier.get('value')
            ident_system = identifier.get('system')
            ident_type = identifier.get('type')
            ident_use = identifier.get('use')
            
            if ident_value:
                values.append(ident_value)
            
            if ident_system:
                systems.append(ident_system)
            
            if ident_system and ident_value:
                system_values.append(f"{ident_system}|{ident_value}")
            elif ident_value:
                system_values.append(f"|{ident_value}")
            
            if ident_type:
                # Type is usually a CodeableConcept
                if isinstance(ident_type, dict) and 'coding' in ident_type:
                    codings = ident_type['coding']
                    if isinstance(codings, list):
                        for coding in codings:
                            if isinstance(coding, dict) and 'code' in coding:
                                types.append(coding['code'])
            
            if ident_use:
                uses.append(ident_use)
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                normalize = mapping.get('normalize')
                
                if not target_field:
                    continue
                
                # Target-field-first projection (mirrors the same
                # ordering in :class:`CodeableConceptExtractor`): an
                # explicit ``*_systemCode`` / ``*_systemValue`` /
                # ``*_systems`` / ``*_values`` / ``*_types`` / ``*_uses``
                # target name is the most reliable signal of caller
                # intent and wins over source-path substring
                # heuristics. The check is case-insensitive so both
                # camelCase and snake_case naming conventions work.
                target_lower = target_field.lower()
                if "system" in target_lower and "value" in target_lower:
                    extracted = system_values
                elif "system" in target_lower and "code" in target_lower:
                    # ``systemCode`` is the codebase synonym for
                    # ``systemValue`` (system|value pair output).
                    extracted = system_values
                elif "system" in target_lower:
                    extracted = systems
                elif "type" in target_lower:
                    extracted = types
                elif "use" in target_lower:
                    extracted = uses
                elif "value" in target_lower:
                    extracted = values
                # Fallback: source-path heuristics for rules with
                # generic target field names.
                elif 'value' in source_path and 'system' not in source_path:
                    extracted = values
                elif 'system' in source_path and 'value' not in source_path:
                    extracted = systems
                elif 'system' in source_path and 'value' in source_path:
                    extracted = system_values
                elif 'type' in source_path:
                    extracted = types
                elif 'use' in source_path:
                    extracted = uses
                else:
                    extracted = values  # Default
                
                # Apply normalization
                if normalize == 'lowercase':
                    extracted = [v.lower() if isinstance(v, str) else v for v in extracted]
                
                # Set the field based on datatype.
                # Sparse output for scalar datatypes: when nothing was
                # projected, omit the target_field entirely so the
                # denormalizer's ``datatype: string`` validation won't
                # reject ``None`` and roll back the whole rule.
                datatype = mapping.get('datatype', 'string')
                if 'array' in datatype:
                    result[target_field] = extracted
                else:
                    if len(extracted) > 1:
                        result[target_field] = extracted
                    elif len(extracted) == 1:
                        result[target_field] = extracted[0]
                    # else: leave target_field absent
        else:
            # Default extraction
            result['values'] = values if values else []
            result['systems'] = systems if systems else []
            result['systemValues'] = system_values if system_values else []
            if types:
                result['types'] = types
            if uses:
                result['uses'] = uses
        
        return result

    def _extract_from_resource(
        self,
        resource: Dict[str, Any],
        field_mappings: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Resource-rooted extraction: each mapping's ``source_path`` is
        resolved against the full resource and the resulting flat list
        of Identifier dicts is fed through the same projection logic as
        the pre-resolved path. Empty resolutions are skipped to keep
        output sparse (the legacy denormalizer simply never invoked the
        rule when the source field was absent — we replicate that here).
        """
        result: Dict[str, Any] = {}
        for mapping in field_mappings:
            target_field = mapping.get("target_field")
            source_path = mapping.get("source_path") or ""
            if not target_field or not source_path:
                continue

            identifiers = [
                item for item in resolve_path(resource, source_path)
                if isinstance(item, dict)
            ]
            if not identifiers:
                continue

            # Re-use the pre-resolved logic for one mapping at a time so
            # the projection rules (value / system / systemValue / type /
            # use) stay in a single place.
            sub = self.extract(identifiers, field_mappings=[mapping])
            if target_field in sub and sub[target_field] not in (None, [], ""):
                result[target_field] = sub[target_field]
        return result
