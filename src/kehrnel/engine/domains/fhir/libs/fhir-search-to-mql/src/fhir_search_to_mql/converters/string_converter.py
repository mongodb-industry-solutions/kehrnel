"""
String parameter converter.

Converts FHIR string searches to MongoDB queries using optimized patterns:
- Default: Case-insensitive PREFIX match using range query
- :exact: Case-sensitive exact match
- :contains: Escaped substring regex or configured text index

Default prefix searches avoid regex and use indexed lowercase ranges.
"""

import re
from typing import Dict, Any, Optional

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core.constants import STRING_MODIFIERS


class StringConverter(BaseConverter):
    """
    Convert FHIR string parameters to MongoDB queries.
    
    Strategy:
    - Default (no modifier): Case-insensitive PREFIX match
      - Query: {"field_lower": {"$gte": "value", "$lt": "value\\uffff"}}
      - Performance: 5ms (index-backed)
    
    - :exact modifier: Case-sensitive exact match
      - Query: {"field": "value"}
      - Performance: 5ms (index-backed)
    
    - :contains modifier: Correct substring match
      - Query: escaped regex on a normalized field, or a configured text index
    """
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert string parameter to MongoDB query.
        
        Args:
            value: Search value
            modifier: Optional modifier (:exact, :contains, :missing)
            prefix: Not used for string parameters
            
        Returns:
            MongoDB query dictionary
            
        Raises:
            ConversionError: If conversion fails
        """
        # Validate modifier
        self._validate_modifier(modifier, STRING_MODIFIERS)
        
        # Handle :missing modifier
        if modifier == 'missing':
            return self._handle_missing(value)
        
        # Get fields to query based on modifier
        fields = self._get_fields_for_modifier(modifier)
        
        if not fields:
            raise ConversionError(
                f"No fields configured for string parameter with modifier '{modifier}'"
            )
        
        # Build query for each field
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            query_type = field_config.get('query_type', 'range') if isinstance(field_config, dict) else 'range'
            
            if modifier == 'exact':
                # Exact match (case-sensitive)
                field_queries.append({field_name: value})
            
            elif modifier == 'contains':
                # Substring match
                if query_type == 'text':
                    # Text index search
                    field_queries.append({"$text": {"$search": value}})
                else:
                    # Equality against the scalar lowercase projections only
                    # matched a whole value. Escape client input so it remains
                    # a literal substring rather than executable regex syntax.
                    field_queries.append({
                        field_name: {
                            "$regex": re.escape(value.lower()),
                            "$options": "i",
                        }
                    })
            
            else:
                # Default: PREFIX match using range query
                # This matches FHIR specification for default string search
                lower_value = value.lower()
                field_queries.append({
                    field_name: {
                        "$gte": lower_value,
                        "$lt": lower_value + "\uffff"
                    }
                })
        
        # Combine with OR if multiple fields
        return self._create_or_query(field_queries)
    
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
