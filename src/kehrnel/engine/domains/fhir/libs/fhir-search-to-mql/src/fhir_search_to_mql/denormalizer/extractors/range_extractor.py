"""
Range extractor for FHIR value ranges.

Extracts range information for values with low/high bounds.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class RangeExtractor(FieldExtractor):
    """Extract Range FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Range structure.
        
        Args:
            value: Range or list of Range structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted range fields
        """
        result = {}
        ranges = self._ensure_list(value)
        
        if not ranges:
            return result
        
        low_values = []
        low_units = []
        high_values = []
        high_units = []
        low_systems = []
        high_systems = []
        
        for range_obj in ranges:
            if not isinstance(range_obj, dict):
                continue
            
            # Extract low bound
            if 'low' in range_obj:
                low = range_obj['low']
                if isinstance(low, dict):
                    if 'value' in low:
                        low_values.append(low['value'])
                    if 'unit' in low:
                        low_units.append(low['unit'])
                    if 'system' in low:
                        low_systems.append(low['system'])
            
            # Extract high bound
            if 'high' in range_obj:
                high = range_obj['high']
                if isinstance(high, dict):
                    if 'value' in high:
                        high_values.append(high['value'])
                    if 'unit' in high:
                        high_units.append(high['unit'])
                    if 'system' in high:
                        high_systems.append(high['system'])
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                
                if not target_field:
                    continue
                
                # Map to appropriate extracted values
                if 'low.value' in source_path or 'lowValue' in target_field:
                    if low_values:
                        result[target_field] = low_values[0] if len(low_values) == 1 else low_values
                elif 'low.unit' in source_path or 'lowUnit' in target_field:
                    if low_units:
                        result[target_field] = low_units[0] if len(low_units) == 1 else low_units
                elif 'high.value' in source_path or 'highValue' in target_field:
                    if high_values:
                        result[target_field] = high_values[0] if len(high_values) == 1 else high_values
                elif 'high.unit' in source_path or 'highUnit' in target_field:
                    if high_units:
                        result[target_field] = high_units[0] if len(high_units) == 1 else high_units
        else:
            # Default extraction without mappings
            if low_values:
                result['rangeLowValue'] = low_values[0] if len(low_values) == 1 else low_values
            if low_units:
                result['rangeLowUnit'] = low_units[0] if len(low_units) == 1 else low_units
            if high_values:
                result['rangeHighValue'] = high_values[0] if len(high_values) == 1 else high_values
            if high_units:
                result['rangeHighUnit'] = high_units[0] if len(high_units) == 1 else high_units
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
