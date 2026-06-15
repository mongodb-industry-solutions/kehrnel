"""
Token parameter converter.

Converts FHIR token searches (codes, identifiers, booleans) to MongoDB queries.
Handles system|code pairs, code-only, and boolean values.
"""

from typing import Dict, Any, Optional

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core.constants import TOKEN_MODIFIERS


class TokenConverter(BaseConverter):
    """
    Convert FHIR token parameters to MongoDB queries.
    
    Token Formats:
    - code=8480-6 (code only)
    - code=http://loinc.org|8480-6 (system|code)
    - code=http://loinc.org| (system only)
    - code=|8480-6 (empty system)
    - gender=male (simple token)
    - active=true (boolean)
    """
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert token parameter to MongoDB query.
        
        Args:
            value: Search value (may contain system|code)
            modifier: Optional modifier (:not, :text, :missing)
            prefix: Not used for token parameters
            
        Returns:
            MongoDB query dictionary
            
        Raises:
            ConversionError: If conversion fails
        """
        # Validate modifier
        self._validate_modifier(modifier, TOKEN_MODIFIERS)
        
        # Handle :missing modifier
        if modifier == 'missing':
            return self._handle_missing(value)
        
        # Handle :text modifier
        if modifier == 'text':
            return self._handle_text_search(value)
        
        # Parse token value
        token_info = self._parse_token(value)
        
        # Get fields to query
        fields = self._get_fields_for_modifier(modifier)
        
        if not fields:
            raise ConversionError("No fields configured for token parameter")
        
        # Build query
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            token_type = field_config.get('tokenType', 'code') if isinstance(field_config, dict) else 'code'
            
            if token_info['has_system'] and token_info['has_code']:
                # System|code pair
                if token_type == 'systemCode':
                    query_value = f"{token_info['system']}|{token_info['code']}"
                    field_queries.append({field_name: query_value})
                else:
                    # Use element match if stored as objects
                    field_queries.append({
                        "$and": [
                            {field_name + ".system": token_info['system']},
                            {field_name + ".code": token_info['code']}
                        ]
                    })
            
            elif token_info['has_code']:
                # Code only
                if token_type == 'boolean':
                    # Convert to boolean
                    bool_value = value.lower() == 'true'
                    field_queries.append({field_name: bool_value})
                else:
                    field_queries.append({field_name: token_info['code']})
            
            elif token_info['has_system']:
                # System only
                field_queries.append({field_name + ".system": token_info['system']})
        
        # Apply :not modifier
        if modifier == 'not':
            if len(field_queries) == 1:
                # Negate single query
                field_name = list(field_queries[0].keys())[0]
                value = field_queries[0][field_name]
                return {field_name: {"$ne": value}}
            else:
                # Negate OR query
                return {"$nor": field_queries}
        
        # Combine with OR if multiple fields
        return self._create_or_query(field_queries)
    
    def _parse_token(self, value: str) -> Dict[str, Any]:
        """
        Parse token value to extract system and code.
        
        Args:
            value: Token value (may contain |)
            
        Returns:
            Dictionary with system, code, and flags
        """
        if '|' in value:
            parts = value.split('|', 1)
            system = parts[0]
            code = parts[1] if len(parts) > 1 else ''
            
            return {
                'system': system,
                'code': code,
                'has_system': bool(system),
                'has_code': bool(code),
            }
        else:
            # No system, just code
            return {
                'system': None,
                'code': value,
                'has_system': False,
                'has_code': bool(value),
            }
    
    def _handle_text_search(self, value: str) -> Dict[str, Any]:
        """
        Handle :text modifier for searching display/text fields.
        
        Args:
            value: Search text
            
        Returns:
            MongoDB query for text search
        """
        # Use lowercase + range query for PREFIX match (NO REGEX)
        lower_value = value.lower()
        
        fields = self._get_fields_for_modifier('text')
        if not fields:
            fields = self._get_fields_for_modifier(None)
        
        field_queries = []
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            
            # Assume text fields have _lower variant
            if not field_name.endswith('_lower'):
                field_name = field_name + '_lower'
            
            field_queries.append({
                field_name: {
                    "$gte": lower_value,
                    "$lt": lower_value + "\uffff"
                }
            })
        
        return self._create_or_query(field_queries)
    
    def _handle_missing(self, value: str) -> Dict[str, Any]:
        """Handle :missing modifier."""
        is_missing = value.lower() == 'true'
        
        fields = self._get_fields_for_modifier(None)
        field_name = fields[0].get('field') if isinstance(fields[0], dict) else fields[0]
        
        if is_missing:
            return {"$or": [
                {field_name: {"$exists": False}},
                {field_name: None}
            ]}
        else:
            return {field_name: {"$exists": True, "$ne": None}}
