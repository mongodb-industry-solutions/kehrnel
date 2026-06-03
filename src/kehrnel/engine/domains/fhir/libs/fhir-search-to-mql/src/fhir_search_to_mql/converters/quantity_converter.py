"""
Quantity parameter converter.

Converts FHIR quantity searches to MongoDB queries.
Handles value, system, and code components with prefixes.

Format: [prefix][value]|[system]|[code]
Examples:
- "5.4" → value only with implicit range
- "5.4||mg" → value + code
- "5.4|http://unitsofmeasure.org|mg" → full specification
- "gt140|http://unitsofmeasure.org|mm[Hg]" → with prefix
"""

from typing import Dict, Any, Optional, Tuple

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core.constants import PREFIXES, QUANTITY_MODIFIERS


class QuantityConverter(BaseConverter):
    """
    Convert FHIR quantity parameters to MongoDB queries.
    
    Quantity Format:
    - [prefix][value]|[system]|[code]
    - All components except value are optional
    - Value has implicit range like number parameters
    
    Examples:
    - "5.4" → value only
    - "5.4||mg" → value and unit code
    - "5.4|http://unitsofmeasure.org|mg" → full
    - "gt5.4||mg" → with prefix
    - "ap100||mg" → approximately 100mg
    
    Generated Query:
    {
      "$and": [
        {"field.value": {comparison}},
        {"field.system": "system"},  # if specified
        {"field.code": "code"}       # if specified
      ]
    }
    """
    
    # Allowed prefixes for quantity searches
    ALLOWED_PREFIXES = ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'ap']
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert quantity parameter to MongoDB query.
        
        Args:
            value: Quantity value string (may include prefix, system, code)
            modifier: Optional modifier (:missing)
            prefix: Optional prefix (can also be embedded in value)
            
        Returns:
            MongoDB query dictionary
            
        Raises:
            ConversionError: If conversion fails
        """
        # Validate modifier
        self._validate_modifier(modifier, QUANTITY_MODIFIERS)
        
        # Handle :missing modifier
        if modifier == 'missing':
            return self._handle_missing(value)
        
        # Parse quantity value (may extract embedded prefix)
        try:
            quantity_info = self._parse_quantity(value)
        except Exception as e:
            raise ConversionError(f"Invalid quantity format '{value}': {str(e)}")
        
        # Use embedded prefix if no explicit prefix
        if prefix is None and quantity_info['prefix']:
            prefix = quantity_info['prefix']
        
        # Validate prefix
        self._validate_prefix(prefix, self.ALLOWED_PREFIXES)
        
        # Get fields to query
        fields = self._get_fields_for_modifier(modifier)
        
        if not fields:
            raise ConversionError("No fields configured for quantity parameter")
        
        # Build query for each field
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            
            query = self._build_quantity_query(
                field_name,
                quantity_info,
                prefix or 'eq'
            )
            
            field_queries.append(query)
        
        # Combine with OR if multiple fields
        return self._create_or_query(field_queries)
    
    def _parse_quantity(self, value: str) -> Dict[str, Any]:
        """
        Parse quantity string to extract prefix, value, system, and code.
        
        Format: [prefix][value]|[system]|[code]
        
        Args:
            value: Quantity string
            
        Returns:
            Dictionary with parsed components
        """
        # Check for embedded prefix
        prefix = None
        numeric_value = value
        
        # Extract prefix if present
        for p in self.ALLOWED_PREFIXES:
            if value.startswith(p):
                prefix = p
                numeric_value = value[len(p):]
                break
        
        # Parse system|code parts
        parts = numeric_value.split('|')
        
        if len(parts) == 1:
            # Value only
            num_str = parts[0]
            system = None
            code = None
        elif len(parts) == 2:
            # Could be value|system or value|code (ambiguous)
            # Assume value|code (no system)
            num_str = parts[0]
            system = None
            code = parts[1] if parts[1] else None
        elif len(parts) >= 3:
            # value|system|code
            num_str = parts[0]
            system = parts[1] if parts[1] else None
            code = parts[2] if parts[2] else None
        else:
            raise ConversionError(f"Invalid quantity format: {value}")
        
        # Parse numeric value
        try:
            parsed_value = float(num_str.strip())
        except ValueError:
            raise ConversionError(f"Cannot parse quantity value: {num_str}")
        
        # Calculate implicit range (same as number converter)
        if '.' in num_str:
            decimal_places = len(num_str.split('.')[1])
            precision = 0.5 * (10 ** -decimal_places)
        else:
            precision = 0.5
        
        lower = parsed_value - precision
        upper = parsed_value + precision
        
        return {
            'prefix': prefix,
            'value': parsed_value,
            'lower': lower,
            'upper': upper,
            'precision': precision,
            'system': system,
            'code': code,
            'has_system': system is not None,
            'has_code': code is not None,
        }
    
    def _build_quantity_query(
        self,
        field_name: str,
        quantity_info: Dict[str, Any],
        prefix: str
    ) -> Dict[str, Any]:
        """
        Build MongoDB query for a quantity field.
        
        Args:
            field_name: Base field name (e.g., "_search.valueQuantity")
            quantity_info: Parsed quantity info
            prefix: Comparison prefix
            
        Returns:
            MongoDB query
        """
        conditions = []
        
        # Build value comparison
        value = quantity_info['value']
        lower = quantity_info['lower']
        upper = quantity_info['upper']
        
        value_field = f"{field_name}.value"
        
        if prefix == 'eq':
            # Equals with implicit range
            conditions.append({value_field: {"$gte": lower}})
            conditions.append({value_field: {"$lt": upper}})
        elif prefix == 'ne':
            # Not equals (outside the range)
            return {
                "$or": [
                    {value_field: {"$lt": lower}},
                    {value_field: {"$gte": upper}}
                ]
            }
        elif prefix == 'gt':
            # Greater than (above upper bound)
            conditions.append({value_field: {"$gt": upper}})
        elif prefix == 'ge':
            # Greater than or equal (at or above lower bound)
            conditions.append({value_field: {"$gte": lower}})
        elif prefix == 'lt':
            # Less than (below lower bound)
            conditions.append({value_field: {"$lt": lower}})
        elif prefix == 'le':
            # Less than or equal (at or below upper bound)
            conditions.append({value_field: {"$lte": upper}})
        elif prefix == 'ap':
            # Approximately (±10%)
            approx_lower = value * 0.9
            approx_upper = value * 1.1
            conditions.append({value_field: {"$gte": approx_lower}})
            conditions.append({value_field: {"$lte": approx_upper}})
        else:
            # Default to equals
            conditions.append({value_field: {"$gte": lower}})
            conditions.append({value_field: {"$lt": upper}})
        
        # Add system constraint if specified
        if quantity_info['has_system']:
            system_field = f"{field_name}.system"
            conditions.append({system_field: quantity_info['system']})
        
        # Add code constraint if specified
        if quantity_info['has_code']:
            code_field = f"{field_name}.code"
            conditions.append({code_field: quantity_info['code']})
        
        # Combine conditions
        if len(conditions) == 0:
            return {}
        elif len(conditions) == 1:
            return conditions[0]
        else:
            return {"$and": conditions}
    
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
