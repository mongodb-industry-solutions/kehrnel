"""
Coding extractor for single FHIR Coding elements.

Extracts individual coding elements (simpler than CodeableConcept).
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class CodingExtractor(FieldExtractor):
    """Extract Coding FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Coding structure.
        
        Args:
            value: Coding or list of Coding structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted coding fields
        """
        result = {}
        codings = self._ensure_list(value)
        
        if not codings:
            return result
        
        codes = []
        systems = []
        system_values = []
        displays = []
        versions = []
        user_selected = []
        
        for coding in codings:
            if not isinstance(coding, dict):
                continue
            
            code = coding.get('code')
            system = coding.get('system')
            display = coding.get('display')
            version = coding.get('version')
            is_user_selected = coding.get('userSelected', False)
            
            if code:
                codes.append(code)
            if system:
                systems.append(system)
            if display:
                displays.append(display)
            if version:
                versions.append(version)
            if is_user_selected:
                user_selected.append(True)
            
            # Create system|code pair
            if system and code:
                system_values.append(f"{system}|{code}")
            elif code:
                system_values.append(f"|{code}")  # Empty system
        
        # Apply field mappings if provided.
        #
        # Target-field-first projection (mirrors the same ordering in
        # :class:`CodeableConceptExtractor` / :class:`IdentifierExtractor`):
        # an explicit ``*_systemCode`` / ``*_systems`` / ``*_codes`` /
        # ``*_displays`` target name wins over source-path substring
        # heuristics — otherwise a hint like ``operationalStatus`` (no
        # "code" / "system" tokens) silently falls through to no
        # branch and the field is never assigned. The check is
        # case-insensitive so both camelCase and snake_case work.
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')

                if not target_field:
                    continue

                target_lower = target_field.lower()
                if "system" in target_lower and (
                    "code" in target_lower or "value" in target_lower
                ):
                    if system_values:
                        result[target_field] = system_values
                    continue
                if "system" in target_lower:
                    if systems:
                        result[target_field] = systems
                    continue
                if "display" in target_lower:
                    if displays:
                        result[target_field] = displays
                    continue
                if "version" in target_lower:
                    if versions:
                        result[target_field] = versions
                    continue
                if "userselected" in target_lower or "user_selected" in target_lower:
                    if user_selected:
                        result[target_field] = user_selected
                    continue
                if "code" in target_lower:
                    if codes:
                        result[target_field] = codes
                    continue

                # Fallback: source-path heuristics for rules that name
                # the target field generically.
                if 'code' in source_path and 'system' not in source_path:
                    if codes:
                        result[target_field] = codes
                elif 'system' in source_path and 'code' not in source_path:
                    if systems:
                        result[target_field] = systems
                elif 'system' in source_path and 'code' in source_path:
                    if system_values:
                        result[target_field] = system_values
                elif 'display' in source_path:
                    if displays:
                        result[target_field] = displays
                elif 'version' in source_path:
                    if versions:
                        result[target_field] = versions
                elif 'userSelected' in source_path:
                    if user_selected:
                        result[target_field] = user_selected
        else:
            # Default extraction without mappings
            if codes:
                result['codingCodes'] = codes
            if systems:
                result['codingSystems'] = systems
            if system_values:
                result['codingSystemValues'] = system_values
            if displays:
                result['codingDisplays'] = displays
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
