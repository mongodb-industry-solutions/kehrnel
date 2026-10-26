"""
RatioRange extractor for FHIR ratio range values.

Extracts ratio range information with low/high ratio bounds.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class RatioRangeExtractor(FieldExtractor):
    """Extract RatioRange FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract RatioRange structure.
        
        Args:
            value: RatioRange or list of RatioRange structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted ratio range fields
        """
        result = {}
        ratio_ranges = self._ensure_list(value)
        
        if not ratio_ranges:
            return result
        
        low_numerator_values = []
        low_denominator_values = []
        high_numerator_values = []
        high_denominator_values = []
        low_ratio_values = []
        high_ratio_values = []
        
        for ratio_range in ratio_ranges:
            if not isinstance(ratio_range, dict):
                continue
            
            # Extract low ratio
            if 'lowNumerator' in ratio_range:
                low_num = ratio_range['lowNumerator']
                if isinstance(low_num, dict) and 'value' in low_num:
                    low_numerator_values.append(low_num['value'])
            
            if 'lowDenominator' in ratio_range:
                low_den = ratio_range['lowDenominator']
                if isinstance(low_den, dict) and 'value' in low_den:
                    low_denominator_values.append(low_den['value'])
            
            # Compute low ratio
            if low_numerator_values and low_denominator_values:
                if low_denominator_values[-1] != 0:
                    low_ratio_values.append(low_numerator_values[-1] / low_denominator_values[-1])
            
            # Extract high ratio
            if 'highNumerator' in ratio_range:
                high_num = ratio_range['highNumerator']
                if isinstance(high_num, dict) and 'value' in high_num:
                    high_numerator_values.append(high_num['value'])
            
            if 'highDenominator' in ratio_range:
                high_den = ratio_range['highDenominator']
                if isinstance(high_den, dict) and 'value' in high_den:
                    high_denominator_values.append(high_den['value'])
            
            # Compute high ratio
            if high_numerator_values and high_denominator_values:
                if high_denominator_values[-1] != 0:
                    high_ratio_values.append(high_numerator_values[-1] / high_denominator_values[-1])
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                
                if not target_field:
                    continue
                
                # Map to appropriate extracted values
                if 'low' in target_field.lower() and 'numerator' in target_field.lower():
                    if low_numerator_values:
                        result[target_field] = low_numerator_values[0] if len(low_numerator_values) == 1 else low_numerator_values
                elif 'low' in target_field.lower() and 'denominator' in target_field.lower():
                    if low_denominator_values:
                        result[target_field] = low_denominator_values[0] if len(low_denominator_values) == 1 else low_denominator_values
                elif 'low' in target_field.lower() and 'ratio' in target_field.lower():
                    if low_ratio_values:
                        result[target_field] = low_ratio_values[0] if len(low_ratio_values) == 1 else low_ratio_values
                elif 'high' in target_field.lower() and 'numerator' in target_field.lower():
                    if high_numerator_values:
                        result[target_field] = high_numerator_values[0] if len(high_numerator_values) == 1 else high_numerator_values
                elif 'high' in target_field.lower() and 'denominator' in target_field.lower():
                    if high_denominator_values:
                        result[target_field] = high_denominator_values[0] if len(high_denominator_values) == 1 else high_denominator_values
                elif 'high' in target_field.lower() and 'ratio' in target_field.lower():
                    if high_ratio_values:
                        result[target_field] = high_ratio_values[0] if len(high_ratio_values) == 1 else high_ratio_values
        else:
            # Default extraction without mappings
            if low_ratio_values:
                result['ratioRangeLowValue'] = low_ratio_values[0] if len(low_ratio_values) == 1 else low_ratio_values
            if high_ratio_values:
                result['ratioRangeHighValue'] = high_ratio_values[0] if len(high_ratio_values) == 1 else high_ratio_values
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
