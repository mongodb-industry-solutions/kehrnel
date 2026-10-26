"""
Modifier validation and handling for FHIR search parameters.

Validates modifiers against parameter types and provides modifier information.
"""

from typing import Dict, List, Optional, Set
from fhir_search_to_mql.core.constants import (
    STRING_MODIFIERS,
    TOKEN_MODIFIERS,
    REFERENCE_MODIFIERS,
    DATE_MODIFIERS,
    NUMBER_MODIFIERS,
    QUANTITY_MODIFIERS,
    URI_MODIFIERS,
    COMPOSITE_MODIFIERS,
)
from fhir_search_to_mql.core.exceptions import InvalidModifierError


class ModifierValidator:
    """
    Validate and handle FHIR search parameter modifiers.
    
    Ensures modifiers are valid for the parameter type and provides
    information about modifier behavior.
    """
    
    # Map parameter types to their valid modifiers
    VALID_MODIFIERS_BY_TYPE = {
        "string": STRING_MODIFIERS,
        "token": TOKEN_MODIFIERS,
        "reference": REFERENCE_MODIFIERS,
        "date": DATE_MODIFIERS,
        "number": NUMBER_MODIFIERS,
        "quantity": QUANTITY_MODIFIERS,
        "uri": URI_MODIFIERS,
        "composite": COMPOSITE_MODIFIERS,
        "special": [],  # Special parameters have their own rules
    }
    
    # Common modifiers that apply to all types
    COMMON_MODIFIERS = ["missing"]
    
    # Modifiers that are resource type references (e.g., :Patient, :Practitioner)
    RESOURCE_TYPE_MODIFIERS = [
        "Patient", "Practitioner", "Organization", "Device", "RelatedPerson",
        "Observation", "Condition", "Procedure", "Medication", "Location",
        "Encounter", "Appointment", "Schedule", "Slot", "PractitionerRole",
        "CareTeam", "Group", "EpisodeOfCare", "MedicationRequest",
        "ServiceRequest", "DiagnosticReport", "Specimen"
    ]
    
    def __init__(self):
        """Initialize the modifier validator."""
        pass
    
    def is_valid_modifier(
        self, 
        modifier: str, 
        param_type: str
    ) -> bool:
        """
        Check if a modifier is valid for a given parameter type.
        
        Args:
            modifier: The modifier to validate
            param_type: The parameter type (string, token, reference, etc.)
            
        Returns:
            True if the modifier is valid, False otherwise
        """
        if param_type not in self.VALID_MODIFIERS_BY_TYPE:
            return False
        
        # Check if it's a common modifier
        if modifier in self.COMMON_MODIFIERS:
            return True
        
        # Check if it's a resource type modifier (for references)
        if param_type == "reference" and modifier in self.RESOURCE_TYPE_MODIFIERS:
            return True
        
        # Check if it's in the type-specific modifiers
        valid_modifiers = self.VALID_MODIFIERS_BY_TYPE[param_type]
        return modifier in valid_modifiers
    
    def validate_modifier(
        self, 
        modifier: Optional[str], 
        param_name: str, 
        param_type: str
    ) -> None:
        """
        Validate a modifier and raise exception if invalid.
        
        Args:
            modifier: The modifier to validate (None if no modifier)
            param_name: The parameter name (for error messages)
            param_type: The parameter type
            
        Raises:
            InvalidModifierError: If the modifier is invalid for the parameter type
        """
        if modifier is None:
            return  # No modifier to validate
        
        if not self.is_valid_modifier(modifier, param_type):
            valid_mods = self.get_valid_modifiers(param_type)
            raise InvalidModifierError(
                f"Invalid modifier '{modifier}' for parameter '{param_name}' "
                f"of type '{param_type}'. Valid modifiers: {', '.join(valid_mods)}"
            )
    
    def get_valid_modifiers(self, param_type: str) -> List[str]:
        """
        Get all valid modifiers for a parameter type.
        
        Args:
            param_type: The parameter type
            
        Returns:
            List of valid modifier names
        """
        if param_type not in self.VALID_MODIFIERS_BY_TYPE:
            return []
        
        modifiers = list(self.VALID_MODIFIERS_BY_TYPE[param_type])
        
        # Add common modifiers
        for mod in self.COMMON_MODIFIERS:
            if mod not in modifiers:
                modifiers.append(mod)
        
        # Add resource type modifiers for reference parameters
        if param_type == "reference":
            modifiers.extend(self.RESOURCE_TYPE_MODIFIERS)
        
        return modifiers
    
    def is_type_modifier(self, modifier: str) -> bool:
        """
        Check if a modifier is a resource type modifier.
        
        Args:
            modifier: The modifier to check
            
        Returns:
            True if it's a resource type modifier
        """
        return modifier in self.RESOURCE_TYPE_MODIFIERS
    
    def get_modifier_description(
        self, 
        modifier: str, 
        param_type: str
    ) -> str:
        """
        Get a human-readable description of what a modifier does.
        
        Args:
            modifier: The modifier
            param_type: The parameter type
            
        Returns:
            Description of the modifier's behavior
        """
        descriptions = {
            "exact": "Exact match, case-sensitive",
            "contains": "Substring match, case-insensitive",
            "missing": "Check if parameter is missing or present",
            "not": "Negation - find resources NOT matching the value",
            "text": "Search in text representation",
            "identifier": "Search by identifier",
            "in": "Value must be in specified ValueSet",
            "not-in": "Value must not be in specified ValueSet",
            "of-type": "Token type-specific search",
            "below": "URI is below the specified URI (hierarchical)",
            "above": "URI is above the specified URI (hierarchical)",
        }
        
        # Check for resource type modifiers
        if self.is_type_modifier(modifier):
            return f"Restrict reference to {modifier} resources only"
        
        return descriptions.get(modifier, f"Modifier for {param_type} parameter")
    
    def requires_special_handling(self, modifier: str) -> bool:
        """
        Check if a modifier requires special query handling.
        
        Args:
            modifier: The modifier to check
            
        Returns:
            True if special handling is needed
        """
        special_modifiers = ["missing", "in", "not-in", "identifier"]
        return modifier in special_modifiers
    
    def get_all_modifiers(self) -> Dict[str, List[str]]:
        """
        Get all modifiers organized by parameter type.
        
        Returns:
            Dictionary mapping parameter types to their valid modifiers
        """
        result = {}
        for param_type in self.VALID_MODIFIERS_BY_TYPE:
            result[param_type] = self.get_valid_modifiers(param_type)
        return result
