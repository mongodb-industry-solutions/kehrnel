"""
Query parser for FHIR search query strings.

Parses query strings like: "name=Smith&gender=male&birthdate=ge1980-01-01"
Full URLs like: "http://example.org/fhir/Patient?name=Smith"
"""

from typing import Dict, List, Any, Optional
from urllib.parse import parse_qs, urlparse, unquote
import re

from fhir_search_to_mql.core.exceptions import ParsingError, ValidationError
from fhir_search_to_mql.parser.parameter_parser import ParameterParser


class QueryParser:
    """
    Parse FHIR search query strings into structured parameter objects.
    
    Supports:
    - Query strings: "name=Smith&gender=male"
    - Full URLs: "http://example.org/fhir/Patient?name=Smith"
    - URL decoding
    - Multiple values: "name=Smith,Johnson"
    - Repeated parameters: "name=Smith&name=Johnson"
    """
    
    def __init__(self, validate_syntax: bool = True):
        """
        Initialize the query parser.
        
        Args:
            validate_syntax: Whether to validate parameter syntax (default: True)
        """
        self.param_parser = ParameterParser()
        self.validate_syntax = validate_syntax
    
    def parse(
        self, 
        query_string: Optional[str] = None, 
        url: Optional[str] = None,
        resource_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Parse FHIR search query into structured format.
        
        Args:
            query_string: Query string (e.g., "name=Smith&gender=male")
            url: Full URL (alternative to query_string)
            resource_type: Optional resource type (extracted from URL if not provided)
            
        Returns:
            Dictionary with:
            - resource_type: Extracted or provided resource type
            - parameters: List of parsed parameters
            - parameter_count: Total number of parameters
            - unique_parameters: Number of unique parameter names
            
        Raises:
            ParsingError: If parsing fails
            ValidationError: If syntax validation fails
        """
        if not query_string and not url:
            raise ParsingError("Either query_string or url must be provided")
        
        # Extract components from URL if provided
        extracted_resource_type = None
        if url:
            parsed_url = urlparse(url)
            query_string = parsed_url.query
            
            # Extract resource type from URL path
            # Format: /fhir/Patient?params or /Patient?params
            path_parts = [p for p in parsed_url.path.split('/') if p]
            if path_parts:
                # Last part before query is typically the resource type
                potential_resource = path_parts[-1]
                if potential_resource and potential_resource[0].isupper():
                    extracted_resource_type = potential_resource
            
            if not query_string:
                return {
                    'resource_type': resource_type or extracted_resource_type,
                    'parameters': [],
                    'parameter_count': 0,
                    'unique_parameters': 0,
                }
        
        # Use provided resource_type or extracted one
        final_resource_type = resource_type or extracted_resource_type
        
        # Parse query string into key-value pairs
        try:
            params_dict = parse_qs(query_string, keep_blank_values=True)
        except Exception as e:
            raise ParsingError(f"Failed to parse query string: {str(e)}")
        
        # Convert to structured parameters
        parameters = []
        errors = []
        
        for param_name, values in params_dict.items():
            # Handle repeated parameters: name=Smith&name=Johnson
            for value in values:
                # URL decode
                value = unquote(value)
                
                # Validate syntax if enabled
                if self.validate_syntax:
                    try:
                        self.param_parser.validate_syntax(param_name, value)
                    except ValidationError as e:
                        errors.append(str(e))
                        continue
                
                # Parse the parameter
                try:
                    parsed_param = self.param_parser.parse_parameter(param_name, value)
                    parameters.append(parsed_param)
                except Exception as e:
                    error_msg = f"Failed to parse parameter '{param_name}={value}': {str(e)}"
                    errors.append(error_msg)
                    print(f"Warning: {error_msg}")
                    continue
        
        # If all parameters failed to parse, raise error
        if params_dict and not parameters and errors:
            raise ParsingError(f"Failed to parse any parameters. Errors: {'; '.join(errors)}")
        
        return {
            'resource_type': final_resource_type,
            'parameters': parameters,
            'parameter_count': len(parameters),
            'unique_parameters': len(set(p['name'] for p in parameters)),
            'errors': errors if errors else None,
        }
    
    def parse_compartment_url(self, url: str) -> Dict[str, Any]:
        """
        Parse a compartment URL.
        
        Example: "/Patient/123/Observation?code=8480-6"
        
        Args:
            url: Compartment URL
            
        Returns:
            Dictionary with compartment info and parameters
            
        Raises:
            ParsingError: If URL format is invalid
        """
        parsed_url = urlparse(url)
        path_parts = [p for p in parsed_url.path.split('/') if p]
        
        if len(path_parts) < 3:
            raise ParsingError(
                f"Invalid compartment URL format: {url}. "
                f"Expected: /CompartmentType/id/ResourceType"
            )
        
        compartment_type = path_parts[0]
        compartment_id = path_parts[1]
        resource_type = path_parts[2]
        
        # Parse additional query parameters
        query_params = self.parse(url=url) if parsed_url.query else {'parameters': []}
        
        return {
            'compartment_type': compartment_type,
            'compartment_id': compartment_id,
            'resource_type': resource_type,
            'parameters': query_params['parameters'],
        }
