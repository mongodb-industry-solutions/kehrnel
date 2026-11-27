"""
Reference parameter converter.

Converts FHIR reference searches to MongoDB queries.
Handles various reference formats and modifiers including :identifier and :text.

Reference Formats:
- "Patient/123" → Type and ID
- "123" → ID only
- "https://example.org/fhir/Patient/123" → Full URL
- "subject:Patient=123" → Type-specific reference
- "subject:identifier=system|value" → Reference by identifier (multi-step)
- "subject:text=John Smith" → Reference by display text
"""

from typing import Dict, Any, Optional, List
import re
from urllib.parse import urlparse

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.converters.multi_step_query import MultiStepQuery, create_simple_multi_step_query
from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core.constants import REFERENCE_MODIFIERS


class ReferenceConverter(BaseConverter):
    """
    Convert FHIR reference parameters to MongoDB queries.
    
    Handles:
    - Reference parsing (Patient/123, URLs, IDs)
    - Type modifiers (:Patient, :Practitioner)
    - :identifier modifier (multi-step query)
    - :text modifier (display name search)
    - :missing modifier
    
    Examples:
        # Simple reference by ID
        converter.convert("123")
        → {"_search.patientId": "123"}
        
        # Reference with type
        converter.convert("Patient/123")
        → {"_search.patientId": "123"}
        
        # Type modifier
        converter.convert("123", modifier="Patient")
        → {"_search.patientId": "123"}
        
        # Identifier modifier (multi-step)
        converter.convert("system|value", modifier="identifier")
        → MultiStepQuery(...)
        
        # Text modifier (display name)
        converter.convert("John Smith", modifier="text")
        → {"_search.patientName_lower": {"$gte": "john smith", "$lt": "john smith\\uffff"}}
    """
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert reference parameter to MongoDB query.
        
        Args:
            value: Reference value (ID, Type/ID, URL, identifier, text)
            modifier: Optional modifier (:Patient, :identifier, :text, :missing)
            prefix: Not used for reference parameters
            
        Returns:
            MongoDB query dictionary or MultiStepQuery object
            
        Raises:
            ConversionError: If conversion fails
        """
        # Validate modifier
        self._validate_modifier(modifier, REFERENCE_MODIFIERS)
        
        # Handle :missing modifier
        if modifier == 'missing':
            return self._handle_missing(value)
        
        # Handle :identifier modifier (multi-step query)
        if modifier == 'identifier':
            return self._handle_identifier_search(value)
        
        # Handle :text modifier (display name search)
        if modifier == 'text':
            return self._handle_text_search(value)
        
        # Parse reference value
        ref_info = self._parse_reference(value, modifier)
        
        # Get fields to query based on reference type
        fields = self._get_fields_for_reference_type(ref_info['type'], modifier)
        
        if not fields:
            raise ConversionError(
                f"No fields configured for reference to type '{ref_info['type']}'"
            )
        
        # Build query for each field
        field_queries = []
        
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            
            # Use ID from parsed reference
            field_queries.append({field_name: ref_info['id']})
        
        # Combine with OR if multiple fields
        return self._create_or_query(field_queries)
    
    def _parse_reference(self, value: str, modifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Parse a reference value to extract type and ID.
        
        Formats:
        - "Patient/123" → {type: "Patient", id: "123"}
        - "123" → {type: None, id: "123"}
        - "https://example.org/fhir/Patient/123" → {type: "Patient", id: "123"}
        - With :Patient modifier → {type: "Patient", id: value}
        
        Args:
            value: Reference value
            modifier: Type modifier if present
            
        Returns:
            Dictionary with 'type' and 'id'
        """
        ref_type = None
        ref_id = value
        
        # Check if modifier is a resource type (e.g., :Patient)
        if modifier and modifier[0].isupper():
            # Modifier is a resource type
            ref_type = modifier
            ref_id = value
        elif '/' in value:
            # Check if it's a URL
            if value.startswith('http://') or value.startswith('https://'):
                # Parse URL
                parsed = urlparse(value)
                path_parts = parsed.path.strip('/').split('/')
                
                # Look for ResourceType/ID pattern in path
                for i in range(len(path_parts) - 1):
                    if path_parts[i][0].isupper():  # Resource type starts with uppercase
                        ref_type = path_parts[i]
                        ref_id = path_parts[i + 1]
                        break
                
                if not ref_type:
                    # Couldn't parse, use last part as ID
                    ref_id = path_parts[-1] if path_parts else value
            else:
                # Simple Type/ID format
                parts = value.split('/', 1)
                if len(parts) == 2:
                    ref_type = parts[0]
                    ref_id = parts[1]
        
        return {
            'type': ref_type,
            'id': ref_id,
            'original': value
        }
    
    def _get_fields_for_reference_type(
        self,
        ref_type: Optional[str],
        modifier: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get fields to query based on reference type.
        
        For reference searches, we query against fields with referenceType: id.
        The ref_type parameter indicates the resource type being referenced (e.g., "Patient"),
        but referenceType in config indicates the field purpose ("id", "type", "full").
        
        Args:
            ref_type: Reference resource type (e.g., "Patient", "Practitioner")
            modifier: Modifier if present
            
        Returns:
            List of field configurations with referenceType: id
        """
        # Get fields from configuration
        fields = self._get_fields_for_modifier(modifier)
        
        if not fields:
            return []
        
        # For reference searches, we want fields with referenceType: id
        # These are the fields that store the extracted ID portion of references
        id_fields = []
        
        for field_config in fields:
            if isinstance(field_config, dict):
                ref_field_type = field_config.get('referenceType')
                # Use fields marked as 'id' type - these store the extracted ID
                if ref_field_type == 'id':
                    id_fields.append(field_config)
            else:
                # If no referenceType specified, include it
                id_fields.append(field_config)
        
        # Return id fields if found, otherwise return all fields
        return id_fields if id_fields else fields
    
    def _handle_identifier_search(self, value: str) -> MultiStepQuery:
        """
        Handle :identifier modifier for reference search.
        
        This requires a multi-step query:
        1. Find resources with matching identifier
        2. Extract resource IDs
        3. Query with reference to those IDs
        
        Args:
            value: Identifier value (system|value or value)
            
        Returns:
            MultiStepQuery object
        """
        # Parse identifier value (same format as token)
        if '|' in value:
            parts = value.split('|', 1)
            system = parts[0]
            code = parts[1] if len(parts) > 1 else ''
        else:
            system = None
            code = value
        
        # Get reference target type from configuration
        # This would normally come from the parameter config
        target_type = self.param_config.get('referenceTarget', 'Patient')
        
        # Build identifier query
        if system and code:
            identifier_query = {
                "identifier": {
                    "$elemMatch": {
                        "system": system,
                        "value": code
                    }
                }
            }
        elif code:
            identifier_query = {
                "identifier.value": code
            }
        else:
            raise ConversionError(f"Invalid identifier format: {value}")
        
        # Get field name for final query
        fields = self._get_fields_for_modifier(None)
        if not fields:
            raise ConversionError("No fields configured for reference parameter")
        
        field_name = fields[0].get('field') if isinstance(fields[0], dict) else fields[0]
        
        # Create multi-step query
        multi_step = create_simple_multi_step_query(
            resource_type=target_type,
            query=identifier_query,
            extract_field="id",
            final_field=field_name,
            description=f"Find {target_type} by identifier, then query by reference"
        )
        
        return multi_step
    
    def _handle_text_search(self, value: str) -> Dict[str, Any]:
        """
        Handle :text modifier for searching cached display names.
        
        Uses PREFIX match with lowercase range query (NO REGEX).
        
        Args:
            value: Display text to search
            
        Returns:
            MongoDB query
        """
        # Use lowercase + range query for PREFIX match (NO REGEX)
        lower_value = value.lower()
        
        # Get text fields from configuration
        fields = self._get_fields_for_modifier('text')
        if not fields:
            # Fall back to default fields with _lower suffix
            fields = self._get_fields_for_modifier(None)
        
        if not fields:
            raise ConversionError("No fields configured for :text modifier")
        
        field_queries = []
        for field_config in fields:
            field_name = field_config.get('field') if isinstance(field_config, dict) else field_config
            
            # Assume text fields have _lower variant or are configured for text search
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
