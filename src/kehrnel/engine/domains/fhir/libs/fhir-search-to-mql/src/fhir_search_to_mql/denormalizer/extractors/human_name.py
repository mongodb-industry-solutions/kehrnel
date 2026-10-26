"""
HumanName extractor for FHIR name structures.

Extracts:
- familyName: Family name from first official name
- givenNames: All given names
- fullName: Constructed full name
- nameText: Text representation if present
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class HumanNameExtractor(FieldExtractor):
    """Extract HumanName FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract HumanName structure.
        
        Args:
            value: HumanName or list of HumanName structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted name fields
        """
        result = {}
        names = self._ensure_list(value)
        
        if not names:
            return result
        
        # Extract family names — HumanName.family is 0..1 string per entry,
        # but Patient/Practitioner.name is 0..* HumanName, so we collect
        # one family string from each name element (FHIR R5).
        family_names = []
        for name in names:
            if not isinstance(name, dict) or 'family' not in name:
                continue
            fam = name['family']
            if isinstance(fam, list):
                family_names.extend(v for v in fam if isinstance(v, str))
            elif isinstance(fam, str):
                family_names.append(fam)
        
        # Extract given names
        given_names = []
        for name in names:
            if isinstance(name, dict) and 'given' in name:
                givens = name['given'] if isinstance(name['given'], list) else [name['given']]
                given_names.extend(givens)
        
        # Extract text representations
        name_texts = []
        for name in names:
            if isinstance(name, dict) and 'text' in name:
                name_texts.append(name['text'])
        
        # Construct full names
        full_names = []
        for name in names:
            if not isinstance(name, dict):
                continue
            
            parts = []
            if 'prefix' in name:
                prefixes = name['prefix'] if isinstance(name['prefix'], list) else [name['prefix']]
                parts.extend(prefixes)
            if 'given' in name:
                givens = name['given'] if isinstance(name['given'], list) else [name['given']]
                parts.extend(givens)
            if 'family' in name:
                fam = name['family']
                if isinstance(fam, list):
                    parts.extend(fam)
                else:
                    parts.append(fam)
            if 'suffix' in name:
                suffixes = name['suffix'] if isinstance(name['suffix'], list) else [name['suffix']]
                parts.extend(suffixes)
            
            if parts:
                full_names.append(' '.join(parts))
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                normalize = mapping.get('normalize')
                
                if not target_field:
                    continue
                
                # Determine what to extract based on source_path
                if 'family' in source_path:
                    values = family_names
                elif 'given' in source_path:
                    values = given_names
                elif 'text' in source_path:
                    values = name_texts
                else:
                    # Full name construction
                    values = full_names
                
                # Apply normalization
                if normalize == 'lowercase':
                    values = [v.lower() if isinstance(v, str) else v for v in values]
                
                # Set the field based on datatype. Sparse-output rules:
                # array datatypes are omitted entirely when no values
                # are projected (writing `[]` would leak `nameText: []`
                # into `_search` for every name without `.text`, which
                # both pollutes indexes and breaks `:missing` coverage —
                # mirrors the same contract enforced in
                # CodeableConceptExtractor and ReferenceExtractor).
                datatype = mapping.get('datatype', 'string')
                if 'array' in datatype:
                    if values:
                        result[target_field] = values
                else:
                    if len(values) == 1:
                        result[target_field] = values[0]
                    elif len(values) > 1:
                        # Scalar datatype with multiple values — keep list so
                        # callers see every token; config should use array[string].
                        result[target_field] = values
                    # No values + scalar datatype: omit (sparse).
        else:
            # Default extraction (backward compatibility)
            result['familyName'] = family_names[0] if family_names else None
            result['givenNames'] = given_names if given_names else []
            result['fullName'] = full_names if full_names else []
            if name_texts:
                result['nameText'] = name_texts
        
        return result
