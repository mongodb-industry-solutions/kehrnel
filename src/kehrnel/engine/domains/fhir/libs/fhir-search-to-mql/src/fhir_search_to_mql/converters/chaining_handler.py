"""
Reference chaining handler.

Implements FHIR reference chaining for complex queries across resources.
Supports forward chaining and deep chaining.

Forward chaining: subject:Patient.name=Smith
Deep chaining: subject:Patient.organization:Organization.name=Hospital
"""

from typing import Dict, Any, Optional, List, Tuple

from fhir_search_to_mql.converters.multi_step_query import MultiStepQuery, QueryStep
from fhir_search_to_mql.core.exceptions import ConversionError


class ChainingHandler:
    """
    Handle reference chaining in FHIR searches.
    
    Reference chaining allows searching for resources based on properties of
    referenced resources.
    
    Syntax: referenceParam:ResourceType.searchParam=value
    
    Examples:
        # Forward chaining
        subject:Patient.name=Smith
        → Find resources whose subject references Patients with name Smith
        
        # Deep chaining
        subject:Patient.organization:Organization.name=Hospital
        → Find resources whose subject references Patients whose organization
          references Organizations with name Hospital
    
    Implementation:
        Creates multi-step queries that:
        1. Query target resource with search criteria
        2. Extract reference IDs
        3. Query source resource with those reference IDs
    """
    
    def __init__(self, converter_registry: Optional[Dict[str, Any]] = None):
        """
        Initialize chaining handler.
        
        Args:
            converter_registry: Registry of parameter converters (optional)
        """
        self.converter_registry = converter_registry or {}
    
    def parse_chain(
        self,
        parameter_name: str,
        value: str,
        source_resource_type: str
    ) -> MultiStepQuery:
        """
        Parse chaining syntax and create multi-step query.
        
        Args:
            parameter_name: Full parameter with chain (e.g., "subject:Patient.name")
            value: Search value
            source_resource_type: Source resource type being queried
            
        Returns:
            MultiStepQuery object
            
        Raises:
            ConversionError: If chain syntax is invalid
            
        Examples:
            # Simple chain
            parse_chain("subject:Patient.name", "Smith", "Observation")
            
            # Deep chain
            parse_chain("subject:Patient.organization:Organization.name", "Hospital", "Observation")
        """
        # Parse the chain
        chain_parts = self._parse_chain_syntax(parameter_name)
        
        if not chain_parts:
            raise ConversionError(f"Invalid chain syntax: {parameter_name}")
        
        # Create multi-step query
        multi_step = MultiStepQuery(
            description=f"Chain: {parameter_name}={value} on {source_resource_type}"
        )
        
        # Process chain from right to left (deepest to shallowest)
        current_value = value
        
        for i in range(len(chain_parts) - 1, -1, -1):
            chain_part = chain_parts[i]
            
            resource_type = chain_part['resource_type']
            search_param = chain_part['search_param']
            
            # Build query for this level
            # In practice, would use appropriate converter based on parameter type
            query = {search_param: current_value}
            
            # Add step
            if i == 0:
                # First level - extract reference to use in source query
                extract_field = "id"
                description = f"Find {resource_type} where {search_param}={current_value}"
            else:
                # Intermediate level - extract reference to use in next level
                ref_param = chain_parts[i - 1]['reference_param']
                extract_field = f"{ref_param}.reference"
                description = f"Find {resource_type} where {search_param}={current_value}"
            
            multi_step.add_step(
                resource_type=resource_type,
                query=query,
                extract_field=extract_field,
                description=description
            )
            
            # For next iteration, we're looking for resources that reference
            # the IDs we'll extract from this step
            current_value = None  # Will be populated from previous step results
        
        # Build final query that uses extracted IDs
        base_ref_param = chain_parts[0]['reference_param']
        
        # Map reference parameter to field name
        # In practice, would use configuration to map parameter to field
        final_field = f"_search.{base_ref_param}Id"
        
        multi_step.set_final_query_builder(
            lambda ids: {final_field: {"$in": ids}} if ids else {final_field: None}
        )
        
        return multi_step
    
    def _parse_chain_syntax(self, parameter_name: str) -> List[Dict[str, Any]]:
        """
        Parse chaining syntax into structured format.
        
        Syntax: refParam:ResourceType.searchParam:ResourceType.searchParam...
        
        Args:
            parameter_name: Parameter name with chain
            
        Returns:
            List of chain parts, each containing:
            - reference_param: Reference parameter name
            - resource_type: Target resource type
            - search_param: Search parameter on target resource
            
        Examples:
            "subject:Patient.name"
            → [{
                'reference_param': 'subject',
                'resource_type': 'Patient',
                'search_param': 'name'
            }]
            
            "subject:Patient.organization:Organization.name"
            → [
                {
                    'reference_param': 'subject',
                    'resource_type': 'Patient',
                    'search_param': 'organization:Organization.name'  # Will be parsed recursively
                },
                {
                    'reference_param': 'organization',
                    'resource_type': 'Organization',
                    'search_param': 'name'
                }
            ]
        """
        chain_parts = []
        remaining = parameter_name
        
        while ':' in remaining and '.' in remaining:
            # Find the first reference and type
            colon_pos = remaining.find(':')
            if colon_pos == -1:
                break
            
            ref_param = remaining[:colon_pos]
            remaining = remaining[colon_pos + 1:]
            
            # Find the resource type (ends at .)
            dot_pos = remaining.find('.')
            if dot_pos == -1:
                break
            
            resource_type = remaining[:dot_pos]
            remaining = remaining[dot_pos + 1:]
            
            # Check if there's another chain
            next_colon = remaining.find(':')
            next_dot = remaining.find('.')
            
            if next_colon != -1 and next_dot != -1 and next_colon < next_dot:
                # There's another chain level
                # Find the search param (up to next colon)
                search_param = remaining[:next_colon]
                remaining = remaining[next_colon:]
            else:
                # This is the last level
                search_param = remaining
                remaining = ""
            
            chain_parts.append({
                'reference_param': ref_param,
                'resource_type': resource_type,
                'search_param': search_param
            })
            
            if not remaining:
                break
        
        return chain_parts
    
    def supports_chaining(self, parameter_name: str) -> bool:
        """
        Check if a parameter name contains chaining syntax.
        
        Args:
            parameter_name: Parameter name to check
            
        Returns:
            True if parameter uses chaining syntax
        """
        # Chaining syntax: contains both : and .
        # But :exact, :contains, etc. are modifiers, not chaining
        if ':' not in parameter_name or '.' not in parameter_name:
            return False
        
        # Check if : comes before .
        colon_pos = parameter_name.find(':')
        dot_pos = parameter_name.find('.')
        
        return colon_pos < dot_pos
    
    def extract_base_parameter(self, parameter_name: str) -> str:
        """
        Extract base parameter name from chaining syntax.
        
        Args:
            parameter_name: Parameter name with chain
            
        Returns:
            Base parameter name (before first :)
            
        Examples:
            "subject:Patient.name" → "subject"
            "name:exact" → "name" (modifier, not chain)
        """
        if ':' in parameter_name:
            return parameter_name.split(':', 1)[0]
        return parameter_name
    
    def to_aggregation_pipeline(
        self,
        multi_step_query: MultiStepQuery,
        base_collection: str
    ) -> List[Dict[str, Any]]:
        """
        Convert multi-step query to MongoDB aggregation pipeline.
        
        Uses $lookup to join collections instead of multiple queries.
        
        Args:
            multi_step_query: Multi-step query to convert
            base_collection: Starting collection name
            
        Returns:
            List of aggregation pipeline stages
            
        Note:
            This is an alternative execution strategy that may be more
            efficient for some queries but less flexible than multi-step execution.
        """
        return multi_step_query.to_aggregation_pipeline(base_collection)


def is_chained_parameter(parameter_name: str) -> bool:
    """
    Check if a parameter name uses chaining syntax.
    
    Args:
        parameter_name: Parameter name to check
        
    Returns:
        True if parameter uses chaining syntax
    """
    handler = ChainingHandler()
    return handler.supports_chaining(parameter_name)


def parse_chained_parameter(
    parameter_name: str,
    value: str,
    source_resource_type: str
) -> MultiStepQuery:
    """
    Parse a chained parameter and create multi-step query.
    
    Convenience function that creates a ChainingHandler and parses the chain.
    
    Args:
        parameter_name: Parameter name with chain
        value: Search value
        source_resource_type: Source resource type
        
    Returns:
        MultiStepQuery object
    """
    handler = ChainingHandler()
    return handler.parse_chain(parameter_name, value, source_resource_type)
