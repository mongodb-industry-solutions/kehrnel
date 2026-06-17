"""
Money extractor for FHIR monetary amounts.

Extracts monetary information with value and currency.
"""

from typing import Any, Dict, List, Optional

from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor


class MoneyExtractor(FieldExtractor):
    """Extract Money FHIR structure to searchable fields."""
    
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract Money structure.
        
        Args:
            value: Money or list of Money structures
            field_mappings: Field mapping configuration
            
        Returns:
            Dictionary with extracted money fields
        """
        result = {}
        monies = self._ensure_list(value)
        
        if not monies:
            return result
        
        values = []
        currencies = []
        
        for money in monies:
            if not isinstance(money, dict):
                continue
            
            # Extract value
            if 'value' in money:
                values.append(money['value'])
            
            # Extract currency
            if 'currency' in money:
                currencies.append(money['currency'])
        
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
                elif 'currency' in source_path:
                    if currencies:
                        result[target_field] = currencies[0] if len(currencies) == 1 else currencies
        else:
            # Default extraction without mappings
            if values:
                result['moneyValue'] = values[0] if len(values) == 1 else values
            if currencies:
                result['moneyCurrency'] = currencies[0] if len(currencies) == 1 else currencies
        
        return result
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
