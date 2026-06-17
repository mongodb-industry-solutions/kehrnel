"""
Quantity extractor for FHIR quantity structures.

Extracts and preserves:
- value: Numeric value
- unit: Unit of measure
- system: Code system URI
- code: Coded unit
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class QuantityExtractor(FieldExtractor):
    """Extract Quantity FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Quantity structure.
        
        Args:
            value: Quantity or list of Quantity structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted quantity fields
        """
        result = {}
        quantities = self._ensure_list(value)
        
        if not quantities:
            return result
        
        # For single quantity, return structured object
        # For multiple, return array
        if len(quantities) == 1 and isinstance(quantities[0], dict):
            qty = quantities[0]
            
            quantity_obj = {}
            if 'value' in qty:
                quantity_obj['value'] = qty['value']
            if 'unit' in qty:
                quantity_obj['unit'] = qty['unit']
            if 'system' in qty:
                quantity_obj['system'] = qty['system']
            if 'code' in qty:
                quantity_obj['code'] = qty['code']
            if 'comparator' in qty:
                quantity_obj['comparator'] = qty['comparator']
            
            return quantity_obj if quantity_obj else result
        
        # Multiple quantities
        quantity_array = []
        for qty in quantities:
            if not isinstance(qty, dict):
                continue
            
            qty_obj = {}
            if 'value' in qty:
                qty_obj['value'] = qty['value']
            if 'unit' in qty:
                qty_obj['unit'] = qty['unit']
            if 'system' in qty:
                qty_obj['system'] = qty['system']
            if 'code' in qty:
                qty_obj['code'] = qty['code']
            if 'comparator' in qty:
                qty_obj['comparator'] = qty['comparator']
            
            if qty_obj:
                quantity_array.append(qty_obj)
        
        return {'quantities': quantity_array} if quantity_array else result
