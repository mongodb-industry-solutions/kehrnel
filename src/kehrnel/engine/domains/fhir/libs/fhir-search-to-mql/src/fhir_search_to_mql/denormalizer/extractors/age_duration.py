"""
Age/Duration extractor for FHIR age and duration values.

Extracts age and duration information (both use Quantity structure).
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class AgeDurationExtractor(FieldExtractor):
    """Extract Age or Duration FHIR structures to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Age or Duration structure.
        
        Both Age and Duration use the Quantity structure with specific units.
        
        Args:
            value: Age/Duration or list of Age/Duration structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted age/duration fields
        """
        result = {}
        items = self._ensure_list(value)
        
        if not items:
            return result
        
        values = []
        units = []
        systems = []
        codes = []
        
        for item in items:
            if not isinstance(item, dict):
                continue
            
            # Extract value
            if 'value' in item:
                values.append(item['value'])
            
            # Extract unit
            if 'unit' in item:
                units.append(item['unit'])
            
            # Extract system
            if 'system' in item:
                systems.append(item['system'])
            
            # Extract code
            if 'code' in item:
                codes.append(item['code'])
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                
                if not target_field:
                    continue
                
                # Map to appropriate extracted values
                if 'value' in source_path:
                    if values:
                        result[target_field] = values[0] if len(values) == 1 else values
                elif 'unit' in source_path and 'code' not in source_path:
                    if units:
                        result[target_field] = units[0] if len(units) == 1 else units
                elif 'system' in source_path:
                    if systems:
                        result[target_field] = systems[0] if len(systems) == 1 else systems
                elif 'code' in source_path:
                    if codes:
                        result[target_field] = codes[0] if len(codes) == 1 else codes
        else:
            # Default extraction without mappings
            if values:
                result['value'] = values[0] if len(values) == 1 else values
            if units:
                result['unit'] = units[0] if len(units) == 1 else units
            if systems:
                result['system'] = systems[0] if len(systems) == 1 else systems
            if codes:
                result['code'] = codes[0] if len(codes) == 1 else codes
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
