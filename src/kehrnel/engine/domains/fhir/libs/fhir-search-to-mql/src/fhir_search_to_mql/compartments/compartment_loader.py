"""
Compartment Definition Loader.

Loads and validates FHIR CompartmentDefinition files.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass

from fhir_search_to_mql.core.exceptions import ConfigurationError


@dataclass
class ResourceEntry:
    """Resource entry in compartment definition."""
    
    code: str
    params: List[str]


@dataclass
class CompartmentDefinition:
    """FHIR CompartmentDefinition structure."""
    
    id: str
    url: str
    name: str
    code: str
    status: str
    description: str
    resources: Dict[str, ResourceEntry]  # resource_type -> ResourceEntry
    
    @classmethod
    def from_dict(cls, data: dict) -> 'CompartmentDefinition':
        """Create CompartmentDefinition from dictionary."""
        # Validate required fields
        required_fields = ['id', 'url', 'name', 'code', 'status', 'resource']
        for field in required_fields:
            if field not in data:
                raise ConfigurationError(
                    f"Missing required field '{field}' in CompartmentDefinition"
                )
        
        # Parse resources
        resources = {}
        for resource_entry in data.get('resource', []):
            if 'code' not in resource_entry:
                raise ConfigurationError(
                    "Missing 'code' in resource entry"
                )
            
            code = resource_entry['code']
            params = resource_entry.get('param', [])
            
            resources[code] = ResourceEntry(
                code=code,
                params=params
            )
        
        return cls(
            id=data['id'],
            url=data['url'],
            name=data['name'],
            code=data['code'],
            status=data['status'],
            description=data.get('description', ''),
            resources=resources
        )


class CompartmentLoader:
    """
    Load and validate CompartmentDefinition files.
    
    Loads all JSON files from the definitions directory and
    validates their structure.
    """
    
    def __init__(self, definitions_dir: Optional[str] = None):
        """
        Initialize loader.
        
        Args:
            definitions_dir: Path to compartment definitions directory.
                If None, uses default location relative to this file.
        """
        if definitions_dir is None:
            # Default to definitions subdirectory
            this_dir = Path(__file__).parent
            definitions_dir = this_dir / 'definitions'
        
        self.definitions_dir = Path(definitions_dir)
        self.compartments: Dict[str, CompartmentDefinition] = {}
        
        if not self.definitions_dir.exists():
            raise ConfigurationError(
                f"Compartment definitions directory not found: {self.definitions_dir}"
            )
    
    def load_all(self) -> Dict[str, CompartmentDefinition]:
        """
        Load all compartment definitions.
        
        Returns:
            Dictionary mapping compartment code to definition
            
        Raises:
            ConfigurationError: If definitions cannot be loaded or validated
        """
        self.compartments = {}
        
        # Find all JSON files
        json_files = list(self.definitions_dir.glob('*.json'))
        
        if not json_files:
            raise ConfigurationError(
                f"No compartment definition files found in {self.definitions_dir}"
            )
        
        # Load each file
        for json_file in json_files:
            try:
                definition = self._load_file(json_file)
                self.compartments[definition.code] = definition
            except Exception as e:
                raise ConfigurationError(
                    f"Failed to load compartment definition from {json_file}: {e}"
                )
        
        return self.compartments
    
    def _load_file(self, file_path: Path) -> CompartmentDefinition:
        """
        Load single compartment definition file.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            CompartmentDefinition object
            
        Raises:
            ConfigurationError: If file cannot be loaded or validated
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON in {file_path}: {e}"
            )
        except IOError as e:
            raise ConfigurationError(
                f"Failed to read {file_path}: {e}"
            )
        
        # Validate resourceType
        if data.get('resourceType') != 'CompartmentDefinition':
            raise ConfigurationError(
                f"Invalid resourceType in {file_path}: "
                f"expected 'CompartmentDefinition', got '{data.get('resourceType')}'"
            )
        
        # Create definition
        definition = CompartmentDefinition.from_dict(data)
        
        # Validate
        self._validate_definition(definition)
        
        return definition
    
    def _validate_definition(self, definition: CompartmentDefinition):
        """
        Validate compartment definition.
        
        Args:
            definition: CompartmentDefinition to validate
            
        Raises:
            ConfigurationError: If validation fails
        """
        # Check code is valid
        valid_codes = ['Patient', 'Encounter', 'Practitioner', 'Device', 'RelatedPerson']
        if definition.code not in valid_codes:
            raise ConfigurationError(
                f"Invalid compartment code '{definition.code}'. "
                f"Expected one of: {', '.join(valid_codes)}"
            )
        
        # Check status
        if definition.status not in ['draft', 'active', 'retired']:
            raise ConfigurationError(
                f"Invalid status '{definition.status}' for compartment {definition.code}"
            )
        
        # Check resources
        if not definition.resources:
            raise ConfigurationError(
                f"No resources defined in compartment {definition.code}"
            )
        
        # Validate each resource entry
        for resource_type, entry in definition.resources.items():
            if not entry.params:
                raise ConfigurationError(
                    f"No parameters defined for resource {resource_type} "
                    f"in compartment {definition.code}"
                )
    
    def get_compartment(self, code: str) -> Optional[CompartmentDefinition]:
        """
        Get compartment definition by code.
        
        Args:
            code: Compartment code (Patient, Encounter, etc.)
            
        Returns:
            CompartmentDefinition or None if not found
        """
        return self.compartments.get(code)
    
    def get_all_compartments(self) -> Dict[str, CompartmentDefinition]:
        """
        Get all loaded compartment definitions.
        
        Returns:
            Dictionary mapping compartment code to definition
        """
        return self.compartments
    
    def get_resource_entry(
        self,
        compartment_code: str,
        resource_type: str
    ) -> Optional[ResourceEntry]:
        """
        Get resource entry from compartment definition.
        
        Args:
            compartment_code: Compartment code
            resource_type: Resource type
            
        Returns:
            ResourceEntry or None if not found
        """
        compartment = self.get_compartment(compartment_code)
        if not compartment:
            return None
        
        return compartment.resources.get(resource_type)
    
    def is_resource_in_compartment(
        self,
        compartment_code: str,
        resource_type: str
    ) -> bool:
        """
        Check if resource type is in compartment.

        Per FHIR R5 §3.5.5 every resource is a member of the compartment
        rooted at its own type (the implicit ``[base]`` linking parameter
        — see https://www.hl7.org/fhir/compartmentdefinition.html). The
        FHIR R5 source CompartmentDefinition documents are inconsistent
        about listing the root resource in their own ``resource[]`` array
        (Patient and Encounter list themselves; Practitioner, Device, and
        RelatedPerson do not). We treat self-membership uniformly so
        ``Practitioner/<id>/Practitioner``, ``Device/<id>/Device``, etc.
        are all valid queries — the precompute layer in the YAML
        configs (``compartment_membership`` rule with
        ``include_self: true``) already populates the matching
        ``_compartments.<Type>`` field, so the resolver has a precomputed
        target either way.

        Args:
            compartment_code: Compartment code
            resource_type: Resource type

        Returns:
            True if resource is in compartment
        """
        # Implicit self-membership — every compartment-defining resource
        # is itself a member of its own compartment.
        if compartment_code == resource_type:
            return True
        return self.get_resource_entry(compartment_code, resource_type) is not None
    
    def get_linking_parameters(
        self,
        compartment_code: str,
        resource_type: str
    ) -> List[str]:
        """
        Get linking parameters for resource in compartment.
        
        Args:
            compartment_code: Compartment code
            resource_type: Resource type
            
        Returns:
            List of parameter names that link to compartment
        """
        entry = self.get_resource_entry(compartment_code, resource_type)
        if not entry:
            return []
        
        return entry.params
