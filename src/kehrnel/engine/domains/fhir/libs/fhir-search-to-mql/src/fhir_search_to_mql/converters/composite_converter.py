"""
Composite parameter converter.

Converts FHIR composite searches to MongoDB queries.
Composite parameters combine multiple sub-parameters with different types.

Format: param=component1$component2$component3
Example: code-value-quantity=http://loinc.org|2093-3$le5
"""

from typing import Dict, Any, Optional, List

from fhir_search_to_mql.converters.base_converter import BaseConverter
from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core.constants import COMPOSITE_MODIFIERS


class CompositeConverter(BaseConverter):
    """
    Convert FHIR composite parameters to MongoDB queries.
    
    Composite parameters combine multiple search criteria of different types.
    Components are separated by "$" and combined with AND logic.
    
    Format: param=component1$component2$component3
    
    Examples:
        # Code-value-quantity composite
        # "code-value-quantity=http://loinc.org|2093-3$le5"
        # Means: code=http://loinc.org|2093-3 AND value<=5
        
        # Component-code-value-quantity composite
        # "component-code-value-quantity=http://loinc.org|8480-6$gt140"
        # Means: component.code=8480-6 AND component.value>140
    
    Configuration:
        Composite parameters need component definitions in configuration:
        ```yaml
        search_parameters:
          code-value-quantity:
            type: composite
            components:
              - name: code
                type: token
                converter: TokenConverter
                fields: [{field: "code.coding"}]
              - name: value
                type: quantity
                converter: QuantityConverter
                fields: [{field: "valueQuantity"}]
        ```
    """
    
    def convert(
        self,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert composite parameter to MongoDB query.
        
        Args:
            value: Composite value with $ separators
            modifier: Optional modifier (:missing)
            prefix: Not used for composite parameters
            
        Returns:
            MongoDB query dictionary with AND logic
            
        Raises:
            ConversionError: If conversion fails
        """
        # Validate modifier
        self._validate_modifier(modifier, COMPOSITE_MODIFIERS)
        
        # Handle :missing modifier
        if modifier == 'missing':
            return self._handle_missing(value)
        
        # Split composite value by $ separator
        component_values = value.split('$')
        
        # Get component definitions from configuration
        components = self.param_config.get('components', [])
        
        if not components:
            raise ConversionError(
                f"No component definitions found for composite parameter"
            )
        
        if len(component_values) != len(components):
            raise ConversionError(
                f"Expected {len(components)} components, got {len(component_values)}. "
                f"Composite value: {value}"
            )
        
        # Convert each component
        component_queries = []
        
        for i, component_def in enumerate(components):
            component_value = component_values[i]
            component_query = self._convert_component(
                component_value,
                component_def,
                i
            )
            component_queries.append(component_query)
        
        # Combine with AND logic
        if len(component_queries) == 0:
            return {}
        elif len(component_queries) == 1:
            return component_queries[0]
        else:
            return {"$and": component_queries}
    
    def _convert_component(
        self,
        value: str,
        component_def: Dict[str, Any],
        index: int
    ) -> Dict[str, Any]:
        """
        Convert a single component of the composite parameter.
        
        Args:
            value: Component value
            component_def: Component definition from configuration
            index: Component index (for error messages)
            
        Returns:
            MongoDB query for this component
            
        Raises:
            ConversionError: If component conversion fails
        """
        component_type = component_def.get('type')
        component_name = component_def.get('name', f'component_{index}')
        
        if not component_type:
            raise ConversionError(
                f"Component '{component_name}' has no type defined"
            )
        
        # Get converter class for this component
        converter_name = component_def.get('converter')
        
        if not converter_name:
            # Try to infer converter from type
            converter_name = self._infer_converter_name(component_type)
        
        # Import and instantiate the appropriate converter
        try:
            converter = self._get_converter_instance(converter_name, component_def)
        except Exception as e:
            raise ConversionError(
                f"Failed to create converter for component '{component_name}': {str(e)}"
            )
        
        # Parse prefix if embedded in value (for number/quantity components)
        prefix = None
        actual_value = value
        
        if component_type in ['number', 'quantity']:
            # Check for embedded prefix
            for p in ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'ap', 'sa', 'eb']:
                if value.startswith(p):
                    prefix = p
                    actual_value = value[len(p):]
                    break
        
        # Convert using the appropriate converter
        try:
            query = converter.convert(actual_value, modifier=None, prefix=prefix)
        except Exception as e:
            raise ConversionError(
                f"Failed to convert component '{component_name}' value '{value}': {str(e)}"
            )
        
        return query
    
    def _infer_converter_name(self, param_type: str) -> str:
        """
        Infer converter class name from parameter type.
        
        Args:
            param_type: Parameter type (string, token, date, etc.)
            
        Returns:
            Converter class name
        """
        type_to_converter = {
            'string': 'StringConverter',
            'token': 'TokenConverter',
            'date': 'DateConverter',
            'number': 'NumberConverter',
            'quantity': 'QuantityConverter',
            'reference': 'ReferenceConverter',
            'uri': 'URIConverter',
        }
        
        converter_name = type_to_converter.get(param_type)
        
        if not converter_name:
            raise ConversionError(
                f"Cannot infer converter for parameter type '{param_type}'"
            )
        
        return converter_name
    
    def _get_converter_instance(
        self,
        converter_name: str,
        component_config: Dict[str, Any]
    ):
        """
        Get an instance of the appropriate converter.
        
        Args:
            converter_name: Name of converter class
            component_config: Component configuration
            
        Returns:
            Converter instance
        """
        # Import converter classes
        from fhir_search_to_mql.converters.string_converter import StringConverter
        from fhir_search_to_mql.converters.token_converter import TokenConverter
        from fhir_search_to_mql.converters.date_converter import DateConverter
        from fhir_search_to_mql.converters.number_converter import NumberConverter
        from fhir_search_to_mql.converters.quantity_converter import QuantityConverter
        from fhir_search_to_mql.converters.reference_converter import ReferenceConverter
        from fhir_search_to_mql.converters.uri_converter import URIConverter
        
        converters = {
            'StringConverter': StringConverter,
            'TokenConverter': TokenConverter,
            'DateConverter': DateConverter,
            'NumberConverter': NumberConverter,
            'QuantityConverter': QuantityConverter,
            'ReferenceConverter': ReferenceConverter,
            'URIConverter': URIConverter,
        }
        
        converter_class = converters.get(converter_name)
        
        if not converter_class:
            raise ConversionError(f"Unknown converter: {converter_name}")
        
        return converter_class(component_config)
    
    def _handle_missing(self, value: str) -> Dict[str, Any]:
        """
        Handle :missing modifier.
        
        For composite parameters, :missing checks if ANY component is missing.
        
        Args:
            value: "true" or "false"
            
        Returns:
            MongoDB query
        """
        is_missing = value.lower() == 'true'
        
        # Get all component field names
        components = self.param_config.get('components', [])
        
        if not components:
            raise ConversionError("No components configured for composite parameter")
        
        missing_queries = []
        
        for component in components:
            fields = component.get('fields', [])
            if fields:
                field_name = fields[0].get('field') if isinstance(fields[0], dict) else fields[0]
                
                if is_missing:
                    # At least one component is missing
                    missing_queries.append({"$or": [
                        {field_name: {"$exists": False}},
                        {field_name: None}
                    ]})
                else:
                    # All components exist
                    missing_queries.append({
                        field_name: {"$exists": True, "$ne": None}
                    })
        
        if not missing_queries:
            return {}
        
        if is_missing:
            # At least one component missing (OR logic)
            return {"$or": missing_queries}
        else:
            # All components exist (AND logic)
            return {"$and": missing_queries}
