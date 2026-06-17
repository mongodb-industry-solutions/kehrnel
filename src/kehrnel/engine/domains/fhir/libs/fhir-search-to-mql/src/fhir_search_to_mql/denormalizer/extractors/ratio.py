"""
Ratio extractor for FHIR ratio values.

Extracts ratio information (numerator/denominator).
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class RatioExtractor(FieldExtractor):
    """Extract Ratio FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Ratio structure.
        
        Args:
            value: Ratio or list of Ratio structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted ratio fields
        """
        result = {}
        ratios = self._ensure_list(value)
        
        if not ratios:
            return result
        
        numerator_values = []
        numerator_units = []
        denominator_values = []
        denominator_units = []
        ratio_values = []  # Computed value
        
        for ratio in ratios:
            if not isinstance(ratio, dict):
                continue
            
            # Extract numerator
            numerator_value = None
            if 'numerator' in ratio:
                numerator = ratio['numerator']
                if isinstance(numerator, dict):
                    if 'value' in numerator:
                        numerator_value = numerator['value']
                        numerator_values.append(numerator_value)
                    if 'unit' in numerator:
                        numerator_units.append(numerator['unit'])
            
            # Extract denominator
            denominator_value = None
            if 'denominator' in ratio:
                denominator = ratio['denominator']
                if isinstance(denominator, dict):
                    if 'value' in denominator:
                        denominator_value = denominator['value']
                        denominator_values.append(denominator_value)
                    if 'unit' in denominator:
                        denominator_units.append(denominator['unit'])
            
            # Compute ratio value if both present
            if numerator_value is not None and denominator_value is not None and denominator_value != 0:
                ratio_values.append(numerator_value / denominator_value)
        
        # Apply field mappings if provided
        if field_mappings:
            for mapping in field_mappings:
                target_field = mapping.get('target_field')
                source_path = mapping.get('source_path', '')
                
                if not target_field:
                    continue
                
                # Map to appropriate extracted values
                if 'numerator.value' in source_path or 'numeratorValue' in target_field:
                    if numerator_values:
                        result[target_field] = numerator_values[0] if len(numerator_values) == 1 else numerator_values
                elif 'numerator.unit' in source_path or 'numeratorUnit' in target_field:
                    if numerator_units:
                        result[target_field] = numerator_units[0] if len(numerator_units) == 1 else numerator_units
                elif 'denominator.value' in source_path or 'denominatorValue' in target_field:
                    if denominator_values:
                        result[target_field] = denominator_values[0] if len(denominator_values) == 1 else denominator_values
                elif 'denominator.unit' in source_path or 'denominatorUnit' in target_field:
                    if denominator_units:
                        result[target_field] = denominator_units[0] if len(denominator_units) == 1 else denominator_units
                elif 'ratio' in target_field.lower() and 'value' in target_field.lower():
                    if ratio_values:
                        result[target_field] = ratio_values[0] if len(ratio_values) == 1 else ratio_values
        else:
            # Default extraction without mappings
            if numerator_values:
                result['ratioNumeratorValue'] = numerator_values[0] if len(numerator_values) == 1 else numerator_values
            if numerator_units:
                result['ratioNumeratorUnit'] = numerator_units[0] if len(numerator_units) == 1 else numerator_units
            if denominator_values:
                result['ratioDenominatorValue'] = denominator_values[0] if len(denominator_values) == 1 else denominator_values
            if denominator_units:
                result['ratioDenominatorUnit'] = denominator_units[0] if len(denominator_units) == 1 else denominator_units
            if ratio_values:
                result['ratioValue'] = ratio_values[0] if len(ratio_values) == 1 else ratio_values
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
