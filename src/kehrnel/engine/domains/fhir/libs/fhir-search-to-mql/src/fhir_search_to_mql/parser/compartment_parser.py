"""
Compartment URL parser for FHIR search queries.

Parses compartment-based URLs like:
- "/Patient/123/Observation"
- "/Patient/123/Observation?code=8480-6&date=ge2024-01-01"
- "/Encounter/456/Condition"
"""

from typing import Dict, List, Any, Optional
from urllib.parse import urlparse, parse_qs, unquote

from fhir_search_to_mql.core.exceptions import ParsingError, ValidationError
from fhir_search_to_mql.core.constants import COMPARTMENT_TYPES
from fhir_search_to_mql.parser.parameter_parser import ParameterParser


class CompartmentParser:
    """
    Parse FHIR compartment URLs into structured format.
    
    Compartments allow searching for resources that are related to a specific
    resource instance (e.g., all Observations for Patient/123).
    
    Supported compartment types:
    - Patient
    - Encounter
    - Practitioner
    - Device
    - RelatedPerson
    """
    
    def __init__(self):
        """Initialize the compartment parser."""
        self.param_parser = ParameterParser()
    
    def parse(self, url: str) -> Dict[str, Any]:
        """
        Parse a compartment URL.
        
        Example URLs:
        - "/Patient/123/Observation"
        - "/Patient/123/Observation?code=8480-6&date=ge2024-01-01"
        - "/Encounter/456/Condition"
        
        Args:
            url: Compartment URL to parse
            
        Returns:
            Dictionary containing:
            - compartment_type: The compartment resource type (e.g., "Patient")
            - compartment_id: The compartment instance ID (e.g., "123")
            - resource_type: The resource type being searched (e.g., "Observation")
            - parameters: List of parsed query parameters
            
        Raises:
            ParsingError: If URL format is invalid
            ValidationError: If compartment type or resource type is invalid
        """
        # Parse the URL
        parsed_url = urlparse(url)
        
        # Extract path components
        path_parts = [p for p in parsed_url.path.split('/') if p]
        
        # Validate path structure
        if len(path_parts) < 3:
            raise ParsingError(
                f"Invalid compartment URL format: {url}. "
                f"Expected format: /CompartmentType/id/ResourceType or "
                f"/CompartmentType/id/ResourceType?params"
            )
        
        # Extract compartment components
        compartment_type = path_parts[0]
        compartment_id = path_parts[1]
        resource_type = path_parts[2]
        
        # Validate compartment type
        self._validate_compartment_type(compartment_type)
        
        # Validate resource type (basic check - should be capitalized)
        self._validate_resource_type(resource_type)
        
        # Validate compartment ID is present
        if not compartment_id:
            raise ValidationError(
                f"Compartment ID is required in compartment URL: {url}"
            )
        
        # Parse query parameters
        parameters = []
        if parsed_url.query:
            try:
                params_dict = parse_qs(parsed_url.query, keep_blank_values=True)
                
                for param_name, values in params_dict.items():
                    for value in values:
                        # URL decode
                        value = unquote(value)
                        
                        # Parse the parameter
                        try:
                            parsed_param = self.param_parser.parse_parameter(
                                param_name, value
                            )
                            parameters.append(parsed_param)
                        except Exception as e:
                            # Log warning but continue
                            print(
                                f"Warning: Failed to parse parameter "
                                f"'{param_name}={value}': {str(e)}"
                            )
                            continue
            except Exception as e:
                raise ParsingError(
                    f"Failed to parse query parameters in compartment URL: {str(e)}"
                )
        
        return {
            'compartment_type': compartment_type,
            'compartment_id': compartment_id,
            'resource_type': resource_type,
            'parameters': parameters,
            'parameter_count': len(parameters),
            'query_string': parsed_url.query or '',
        }
    
    def _validate_compartment_type(self, compartment_type: str) -> None:
        """
        Validate that the compartment type is supported.
        
        Args:
            compartment_type: The compartment type to validate
            
        Raises:
            ValidationError: If compartment type is not valid
        """
        if compartment_type not in COMPARTMENT_TYPES:
            raise ValidationError(
                f"Invalid compartment type: {compartment_type}. "
                f"Valid compartment types: {', '.join(COMPARTMENT_TYPES)}"
            )
    
    def _validate_resource_type(self, resource_type: str) -> None:
        """
        Validate that the resource type appears valid.
        
        Basic validation - checks that it starts with uppercase letter.
        Full resource type validation should be done by the converter
        against the configuration.
        
        Args:
            resource_type: The resource type to validate
            
        Raises:
            ValidationError: If resource type format is invalid
        """
        if not resource_type:
            raise ValidationError("Resource type is required in compartment URL")
        
        if not resource_type[0].isupper():
            raise ValidationError(
                f"Invalid resource type: {resource_type}. "
                f"Resource types must start with an uppercase letter."
            )
    
    def is_compartment_url(self, url: str) -> bool:
        """
        Check if a URL is a compartment URL.
        
        A compartment URL has the format: /ResourceType/id/SearchType
        
        Args:
            url: URL to check
            
        Returns:
            True if the URL appears to be a compartment URL
        """
        try:
            parsed_url = urlparse(url)
            path_parts = [p for p in parsed_url.path.split('/') if p]
            
            # Must have at least 3 parts
            if len(path_parts) < 3:
                return False
            
            # First part should be a valid compartment type
            if path_parts[0] not in COMPARTMENT_TYPES:
                return False
            
            # Second part should be an ID (non-empty)
            if not path_parts[1]:
                return False
            
            # Third part should be a resource type (starts with uppercase)
            if not path_parts[2] or not path_parts[2][0].isupper():
                return False
            
            return True
        except Exception:
            return False
    
    def extract_compartment_info(self, url: str) -> Optional[Dict[str, str]]:
        """
        Extract just the compartment info without parsing parameters.
        
        Args:
            url: Compartment URL
            
        Returns:
            Dictionary with compartment_type, compartment_id, resource_type
            or None if not a valid compartment URL
        """
        try:
            if not self.is_compartment_url(url):
                return None
            
            parsed_url = urlparse(url)
            path_parts = [p for p in parsed_url.path.split('/') if p]
            
            return {
                'compartment_type': path_parts[0],
                'compartment_id': path_parts[1],
                'resource_type': path_parts[2],
            }
        except Exception:
            return None
    
    def get_supported_compartments(self) -> List[str]:
        """
        Get list of supported compartment types.
        
        Returns:
            List of compartment type names
        """
        return list(COMPARTMENT_TYPES)
