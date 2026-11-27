"""
Base class for field extractors.

All field extractors inherit from FieldExtractor and implement the extract() method
to transform FHIR data structures into denormalized search fields.
"""

from typing import Any, Dict, List, Optional
from abc import ABC, abstractmethod

from fhir_search_to_mql.core.exceptions import DenormalizationError


class FieldExtractor(ABC):
    """
    Base class for all field extractors.
    
    Each extractor knows how to transform a specific FHIR data type into
    denormalized fields optimized for MongoDB queries.
    """
    
    def __init__(self):
        """Initialize the extractor."""
        self.name = self.__class__.__name__
    
    @abstractmethod
    def extract(
        self, 
        value: Any, 
        field_mappings: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Extract and denormalize a FHIR field value.
        
        Args:
            value: The FHIR field value to extract
            field_mappings: Optional list of field mapping configurations
            
        Returns:
            Dictionary of denormalized fields
            
        Raises:
            DenormalizationError: If extraction fails
        """
        pass
    
    def validate(self, value: Any) -> bool:
        """
        Validate that a value can be extracted.
        
        Args:
            value: The value to validate
            
        Returns:
            True if valid, False otherwise
        """
        return value is not None
    
    def _ensure_list(self, value: Any) -> List[Any]:
        """Ensure value is a list."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        return [value]
    
    def _extract_field(
        self, 
        obj: Dict[str, Any], 
        path: str, 
        default: Any = None
    ) -> Any:
        """
        Extract a field from a nested dictionary using dot notation.
        
        Args:
            obj: Dictionary to extract from
            path: Dot-notation path (e.g., "name.family")
            default: Default value if path not found
            
        Returns:
            Extracted value or default
        """
        parts = path.split('.')
        current = obj
        
        for part in parts:
            if not isinstance(current, dict):
                return default
            current = current.get(part, default)
            if current is default:
                return default
        
        return current
    
    def _apply_transformation(
        self, 
        value: Any, 
        transformation: Optional[str] = None
    ) -> Any:
        """
        Apply transformation to a value.
        
        Args:
            value: Value to transform
            transformation: Transformation type ('lowercase', 'uppercase', etc.)
            
        Returns:
            Transformed value
        """
        if not transformation or not value:
            return value
        
        if transformation == 'lowercase' and isinstance(value, str):
            return value.lower()
        elif transformation == 'uppercase' and isinstance(value, str):
            return value.upper()
        
        return value
