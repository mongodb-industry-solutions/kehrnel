"""
Query validator for FHIR search parameters.

Validates parameters, types, modifiers, prefixes, values, and provides helpful error messages.
"""

from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field

from fhir_search_to_mql.core.exceptions import ConversionError
from fhir_search_to_mql.core import constants


@dataclass
class ValidationResult:
    """
    Result of query validation.
    
    Attributes:
        is_valid: Whether validation passed
        errors: List of error messages (blocking)
        warnings: List of warning messages (non-blocking)
    """
    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)


class QueryValidator:
    """
    Validate FHIR search parameters and queries.
    
    Provides comprehensive validation with helpful error messages:
    - Parameter exists in resource configuration
    - Parameter type matches configuration
    - Modifier is allowed for parameter type
    - Prefix is allowed for parameter type
    - Value format is correct
    - Reference types are allowed
    - Field paths exist
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize query validator.
        
        Args:
            config: Resource configuration (optional)
        """
        self.config = config or {}
    
    def validate_parameter(
        self,
        parameter_name: str,
        value: str,
        modifier: Optional[str] = None,
        prefix: Optional[str] = None,
        resource_type: Optional[str] = None
    ) -> ValidationResult:
        """
        Validate a search parameter.
        
        Args:
            parameter_name: Parameter name (e.g., "name", "birthdate")
            value: Parameter value
            modifier: Optional modifier (e.g., "exact", "contains")
            prefix: Optional prefix (e.g., "eq", "gt", "le")
            resource_type: Resource type (e.g., "Patient")
            
        Returns:
            ValidationResult with errors and warnings
        """
        result = ValidationResult()
        
        # Check if parameter exists
        if not self._parameter_exists(parameter_name):
            result.add_error(
                f"Parameter '{parameter_name}' not defined for resource "
                f"'{resource_type or 'Unknown'}'"
            )
            return result
        
        # Get parameter configuration
        param_config = self.config.get(parameter_name, {})
        param_type = param_config.get('type', 'unknown')
        
        # Validate modifier
        if modifier:
            if not self._is_modifier_allowed(param_type, modifier):
                result.add_error(
                    f"Modifier ':{modifier}' not allowed for parameter type '{param_type}'"
                )
        
        # Validate prefix
        if prefix:
            if not self._is_prefix_allowed(param_type, prefix):
                result.add_error(
                    f"Prefix '{prefix}' not allowed for parameter type '{param_type}'"
                )
        
        # Validate value format
        value_validation = self._validate_value_format(param_type, value, modifier)
        if not value_validation['valid']:
            result.add_error(value_validation['message'])
        
        # Validate reference type
        if param_type == 'reference' and modifier:
            ref_validation = self._validate_reference_type(param_config, modifier)
            if not ref_validation['valid']:
                result.add_error(ref_validation['message'])
        
        # Check for missing indexes (warning)
        index_check = self._check_indexes(param_config)
        if not index_check['has_index']:
            result.add_warning(
                f"No index found for field '{index_check['field']}', query may be slow"
            )
        
        return result
    
    def validate_query(
        self,
        query: Dict[str, Any],
        resource_type: Optional[str] = None
    ) -> ValidationResult:
        """
        Validate a MongoDB query structure.
        
        Args:
            query: MongoDB query to validate
            resource_type: Resource type (optional)
            
        Returns:
            ValidationResult with errors and warnings
        """
        result = ValidationResult()
        
        # Check query complexity
        complexity = self._estimate_complexity(query)
        
        if complexity['num_conditions'] > 10:
            result.add_warning(
                f"Complex query with {complexity['num_conditions']} conditions, "
                f"consider splitting"
            )
        
        if complexity['max_depth'] > 5:
            result.add_warning(
                f"Deep query nesting (depth {complexity['max_depth']}), "
                f"may impact performance"
            )
        
        if complexity['has_regex']:
            result.add_warning(
                "Query uses regex which may be slow without proper indexes"
            )
        
        return result
    
    def _parameter_exists(self, parameter_name: str) -> bool:
        """Check if parameter exists in configuration."""
        # Special parameters always exist
        if parameter_name.startswith('_'):
            return True
        
        return parameter_name in self.config
    
    def _is_modifier_allowed(self, param_type: str, modifier: str) -> bool:
        """Check if modifier is allowed for parameter type."""
        allowed_modifiers = {
            'string': constants.STRING_MODIFIERS,
            'token': constants.TOKEN_MODIFIERS,
            'reference': constants.REFERENCE_MODIFIERS,
            'uri': constants.URI_MODIFIERS,
            'date': ['missing'],
            'number': ['missing'],
            'quantity': ['missing'],
            'composite': ['missing'],
        }
        
        if param_type not in allowed_modifiers:
            return True  # Unknown type, allow
        
        return modifier in allowed_modifiers[param_type]
    
    def _is_prefix_allowed(self, param_type: str, prefix: str) -> bool:
        """Check if prefix is allowed for parameter type."""
        # Prefixes are allowed for date, number, quantity
        allowed_types = ['date', 'number', 'quantity']
        
        if param_type not in allowed_types:
            return False
        
        return prefix in constants.PREFIXES
    
    def _validate_value_format(
        self,
        param_type: str,
        value: str,
        modifier: Optional[str] = None
    ) -> Dict[str, Any]:
        """Validate value format for parameter type."""
        if param_type == 'date':
            return self._validate_date_format(value)
        elif param_type == 'number':
            return self._validate_number_format(value)
        elif param_type == 'quantity':
            return self._validate_quantity_format(value)
        elif param_type == 'token':
            return self._validate_token_format(value)
        elif param_type == 'uri':
            return self._validate_uri_format(value)
        
        # Default: allow all values
        return {'valid': True}
    
    def _validate_date_format(self, value: str) -> Dict[str, Any]:
        """Validate date format."""
        # Accept: YYYY, YYYY-MM, YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS
        import re
        
        patterns = [
            r'^\d{4}$',  # YYYY
            r'^\d{4}-\d{2}$',  # YYYY-MM
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # YYYY-MM-DDTHH:MM:SS
        ]
        
        for pattern in patterns:
            if re.match(pattern, value):
                return {'valid': True}
        
        return {
            'valid': False,
            'message': f"Invalid date format: '{value}'. Expected YYYY, YYYY-MM, YYYY-MM-DD, or YYYY-MM-DDTHH:MM:SS"
        }
    
    def _validate_number_format(self, value: str) -> Dict[str, Any]:
        """Validate number format."""
        try:
            float(value)
            return {'valid': True}
        except ValueError:
            return {
                'valid': False,
                'message': f"Invalid number format: '{value}'"
            }
    
    def _validate_quantity_format(self, value: str) -> Dict[str, Any]:
        """Validate quantity format (value|system|code)."""
        # Can be: "5", "5||mg", "5|http://unitsofmeasure.org|mg"
        parts = value.split('|')
        
        if len(parts) > 3:
            return {
                'valid': False,
                'message': f"Invalid quantity format: '{value}'. Expected value|system|code"
            }
        
        # Validate numeric value
        if parts[0]:
            try:
                float(parts[0])
            except ValueError:
                return {
                    'valid': False,
                    'message': f"Invalid quantity value: '{parts[0]}'"
                }
        
        return {'valid': True}
    
    def _validate_token_format(self, value: str) -> Dict[str, Any]:
        """Validate token format (system|code or code)."""
        # Can be: "code" or "system|code"
        parts = value.split('|')
        
        if len(parts) > 2:
            return {
                'valid': False,
                'message': f"Invalid token format: '{value}'. Expected system|code or code"
            }
        
        return {'valid': True}
    
    def _validate_uri_format(self, value: str) -> Dict[str, Any]:
        """Validate URI format."""
        # Basic check - should start with http:// or https:// or urn:
        if not (value.startswith('http://') or value.startswith('https://') or value.startswith('urn:')):
            return {
                'valid': False,
                'message': f"Invalid URI format: '{value}'. Expected http://, https://, or urn:"
            }
        
        return {'valid': True}
    
    def _validate_reference_type(
        self,
        param_config: Dict[str, Any],
        modifier: str
    ) -> Dict[str, Any]:
        """Validate reference type modifier."""
        # If modifier is a resource type, check if it's allowed
        allowed_types = param_config.get('referenceTarget', [])
        
        if isinstance(allowed_types, str):
            allowed_types = [allowed_types]
        
        # Special modifiers
        if modifier in ['identifier', 'text', 'missing']:
            return {'valid': True}
        
        # Check if resource type
        if allowed_types and modifier not in allowed_types:
            return {
                'valid': False,
                'message': f"Reference type '{modifier}' not allowed. Expected one of: {', '.join(allowed_types)}"
            }
        
        return {'valid': True}
    
    def _check_indexes(self, param_config: Dict[str, Any]) -> Dict[str, Any]:
        """Check if parameter has index hints."""
        fields = param_config.get('fields', [])
        
        if not fields:
            return {'has_index': True}  # No fields to check
        
        first_field = fields[0]
        field_name = first_field.get('field', '')
        has_index = first_field.get('indexed', False)
        
        return {
            'has_index': has_index,
            'field': field_name
        }
    
    def _estimate_complexity(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate query complexity."""
        metrics = {
            'num_conditions': 0,
            'max_depth': 0,
            'has_regex': False
        }
        
        self._analyze_query(query, metrics, depth=0)
        
        return metrics
    
    def _analyze_query(
        self,
        query: Any,
        metrics: Dict[str, Any],
        depth: int
    ) -> None:
        """Recursively analyze query structure."""
        if depth > metrics['max_depth']:
            metrics['max_depth'] = depth
        
        if not isinstance(query, dict):
            return
        
        for key, value in query.items():
            if key in ['$and', '$or', '$nor']:
                if isinstance(value, list):
                    metrics['num_conditions'] += len(value)
                    for item in value:
                        self._analyze_query(item, metrics, depth + 1)
            elif key == '$regex':
                metrics['has_regex'] = True
            elif not key.startswith('$'):
                metrics['num_conditions'] += 1
                if isinstance(value, dict):
                    self._analyze_query(value, metrics, depth + 1)
