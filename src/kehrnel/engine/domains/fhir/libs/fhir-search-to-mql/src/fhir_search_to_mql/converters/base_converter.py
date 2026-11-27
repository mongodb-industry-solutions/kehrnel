"""
Base converter class for all parameter type converters.
"""

from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod

from fhir_search_to_mql.core.exceptions import ConversionError


class BaseConverter(ABC):
    """
    Base class for all FHIR parameter converters.
    
    Each converter handles one FHIR search parameter type (string, token, date, etc.)
    and converts it to MongoDB query format.
    """
    
    def __init__(self, param_config: Dict[str, Any]):
        """
        Initialize converter with parameter configuration.
        
        Args:
            param_config: Parameter configuration from YAML
        """
        self.param_config = param_config
        self.param_type = param_config.get('type')
        self.fields = param_config.get('fields', [])
    
    @abstractmethod
    def convert(
        self, 
        value: str, 
        modifier: Optional[str] = None, 
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert a FHIR search parameter to MongoDB query.
        
        Args:
            value: Parameter value
            modifier: Optional modifier (:exact, :contains, etc.)
            prefix: Optional prefix (ge, le, etc.)
            
        Returns:
            MongoDB query dictionary
            
        Raises:
            ConversionError: If conversion fails
        """
        pass
    
    def _get_fields_for_modifier(self, modifier: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get the appropriate fields to query based on modifier.
        
        Args:
            modifier: Parameter modifier (e.g., 'exact', 'contains')
            
        Returns:
            List of field configurations to query
        """
        # Handle both list and dict format for fields
        if isinstance(self.fields, list):
            return self.fields
        elif isinstance(self.fields, dict):
            # Fields organized by modifier
            if modifier and modifier in self.fields:
                fields = self.fields[modifier]
            else:
                fields = self.fields.get('default', [])
            
            # Ensure it's a list
            if not isinstance(fields, list):
                fields = [fields]
            
            return fields
        else:
            return []
    
    def _create_or_query(self, field_queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create an OR query from multiple field queries.
        
        Args:
            field_queries: List of individual field queries
            
        Returns:
            MongoDB query (single field or $or)
        """
        if not field_queries:
            return {}
        
        if len(field_queries) == 1:
            return field_queries[0]
        
        return {"$or": field_queries}
    
    def _validate_modifier(self, modifier: Optional[str], allowed_modifiers: List[str]) -> None:
        """
        Validate that modifier is allowed for this parameter type.
        
        Args:
            modifier: Modifier to validate
            allowed_modifiers: List of allowed modifiers
            
        Raises:
            ConversionError: If modifier is not allowed
        """
        if modifier and modifier not in allowed_modifiers:
            raise ConversionError(
                f"Modifier ':{modifier}' not allowed for {self.param_type} parameter. "
                f"Allowed modifiers: {', '.join([':' + m for m in allowed_modifiers])}"
            )
    
    def _validate_prefix(self, prefix: Optional[str], allowed_prefixes: List[str]) -> None:
        """
        Validate that prefix is allowed for this parameter type.
        
        Args:
            prefix: Prefix to validate
            allowed_prefixes: List of allowed prefixes
            
        Raises:
            ConversionError: If prefix is not allowed
        """
        if prefix and prefix not in allowed_prefixes:
            raise ConversionError(
                f"Prefix '{prefix}' not allowed for {self.param_type} parameter. "
                f"Allowed prefixes: {', '.join(allowed_prefixes)}"
            )
