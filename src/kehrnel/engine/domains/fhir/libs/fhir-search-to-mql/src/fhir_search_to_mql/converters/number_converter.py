"""
Number parameter converter.

Converts FHIR number searches to MongoDB queries with implicit range based on significant figures:
- "100" → {"field": {"$gte": 99.5, "$lt": 100.5}}
- "100.0" → {"field": {"$gte": 99.95, "$lt": 100.05}}
- "1e2" → {"field": {"$gte": 50, "$lt": 150}}

Supports prefixes: eq, ne, gt, lt, ge, le, ap (approximately)
"""

from typing import Dict, Any, Optional
import re
from decimal import Decimal, getcontext

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core.constants import PREFIXES, NUMBER_MODIFIERS

# Set precision for Decimal calculations
getcontext().prec = 28


class NumberConverter(BaseConverter):
    """
    Convert FHIR number parameters to MongoDB queries.
    
    FHIR number searches have implicit ranges based on significant figures:
    - Whole numbers: ±0.5
    - Decimal numbers: ±0.5 × 10^(-decimal_places)
    - Scientific notation: ±50% of value
    
    Prefixes:
    - eq: Equals (default, implicit range)
    - ne: Not equals
    - gt: Greater than
    - ge: Greater than or equal
    - lt: Less than
    - le: Less than or equal
    - ap: Approximately (±10%)
    """
    
    # Allowed prefixes for number searches
    ALLOWED_PREFIXES = ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'ap']
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert number parameter to MongoDB query.
        
        Args:
            value: Numeric value as string
            modifier: Optional modifier (:missing)
            prefix: Optional prefix (eq, ne, ge, gt, le, lt, ap)
            
        Returns:
            MongoDB query dictionary
            
        Raises:
            ConversionError: If conversion fails
        """
        # Validate modifier
        self._validate_modifier(modifier, NUMBER_MODIFIERS)
        
        # Handle :missing modifier
        if modifier == 'missing':
            return self._handle_missing(value)
        
        # Validate prefix
        self._validate_prefix(prefix, self.ALLOWED_PREFIXES)
        
        # Parse numeric value and calculate range
        try:
            number_range = self._parse_number_range(value)
        except Exception as e:
            raise ConversionError(f"Invalid number format '{value}': {str(e)}")
        
        # Get fields to query
        fields = self._get_fields_for_modifier(modifier)
        
        if not fields:
            raise ConversionError("No fields configured for number parameter")
        
        # Build query for each field
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            
            query = self._build_number_query(
                field_name,
                number_range,
                prefix or 'eq'
            )
            
            field_queries.append(query)
        
        # Combine with OR if multiple fields
        return self._create_or_query(field_queries)
    
    def _parse_number_range(self, value: str) -> Dict[str, float]:
        """
        Parse a number string and calculate implicit range based on precision.
        
        FHIR Specification:
        - Implicit range based on significant figures
        - "100" has precision ±0.5
        - "100.0" has precision ±0.05
        - "100.00" has precision ±0.005
        - "1e2" has 50% range (50-150)
        
        Args:
            value: Number as string
            
        Returns:
            Dictionary with 'value', 'lower', 'upper', 'precision'
        """
        # Remove whitespace
        value = value.strip()
        
        # Parse the number
        try:
            numeric_value = float(value)
        except ValueError:
            raise ConversionError(f"Cannot parse number '{value}'")
        
        # Detect if scientific notation
        is_scientific = 'e' in value.lower()
        
        if is_scientific:
            # Scientific notation: ±50% range
            # "1e2" (100) → 50 to 150
            abs_value = abs(numeric_value)
            precision = abs_value * 0.5
            lower = numeric_value - precision
            upper = numeric_value + precision
        else:
            # Determine precision based on decimal places
            if '.' in value:
                # Count decimal places
                decimal_places = len(value.split('.')[1])
                # Precision is ±0.5 × 10^(-decimal_places)
                # E.g., 100.0 (1 decimal) → precision = 0.5 × 0.1 = 0.05
                precision = 0.5 * (10 ** -decimal_places)
            else:
                # Whole number: precision is ±0.5
                precision = 0.5
            
            lower = numeric_value - precision
            upper = numeric_value + precision
        
        return {
            'value': numeric_value,
            'lower': lower,
            'upper': upper,
            'precision': precision
        }
    
    def _build_number_query(
        self,
        field_name: str,
        number_range: Dict[str, float],
        prefix: str
    ) -> Dict[str, Any]:
        """
        Build MongoDB query for a number field.
        
        Args:
            field_name: Field to query
            number_range: Number range dict with value, lower, upper
            prefix: Comparison prefix
            
        Returns:
            MongoDB query
        """
        value = number_range['value']
        lower = number_range['lower']
        upper = number_range['upper']
        
        if prefix == 'eq':
            # Equals with implicit range
            return {
                "$and": [
                    {field_name: {"$gte": lower}},
                    {field_name: {"$lt": upper}}
                ]
            }
        elif prefix == 'ne':
            # Not equals (outside the range)
            return {
                "$or": [
                    {field_name: {"$lt": lower}},
                    {field_name: {"$gte": upper}}
                ]
            }
        elif prefix == 'gt':
            # Greater than (above upper bound)
            return {field_name: {"$gt": upper}}
        elif prefix == 'ge':
            # Greater than or equal (at or above lower bound)
            return {field_name: {"$gte": lower}}
        elif prefix == 'lt':
            # Less than (below lower bound)
            return {field_name: {"$lt": lower}}
        elif prefix == 'le':
            # Less than or equal (at or below upper bound)
            return {field_name: {"$lte": upper}}
        elif prefix == 'ap':
            # Approximately (±10%)
            approx_lower = value * 0.9
            approx_upper = value * 1.1
            return {
                "$and": [
                    {field_name: {"$gte": approx_lower}},
                    {field_name: {"$lte": approx_upper}}
                ]
            }
        else:
            # Default to equals
            return {
                "$and": [
                    {field_name: {"$gte": lower}},
                    {field_name: {"$lt": upper}}
                ]
            }
    
    def _handle_missing(self, value: str) -> Dict[str, Any]:
        """
        Handle :missing modifier.
        
        Args:
            value: "true" or "false"
            
        Returns:
            MongoDB query
        """
        is_missing = value.lower() == 'true'
        
        fields = self._get_fields_for_modifier(None)  # Use default fields
        
        if not fields:
            raise ConversionError("No fields configured for missing check")
        
        field_name = fields[0].get('field') if isinstance(fields[0], dict) else fields[0]
        
        if is_missing:
            return {"$or": [
                {field_name: {"$exists": False}},
                {field_name: None}
            ]}
        else:
            return {field_name: {"$exists": True, "$ne": None}}
