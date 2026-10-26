"""
FHIR Compartment Support.

Provides compartment-based query resolution for FHIR resources.
"""

from fhir_search_to_mql.compartments.compartment_loader import (
    CompartmentLoader,
    CompartmentDefinition,
    ResourceEntry,
)
from fhir_search_to_mql.compartments.compartment_resolver import (
    CompartmentResolver,
    CompartmentQuery,
)

__all__ = [
    'CompartmentLoader',
    'CompartmentDefinition',
    'ResourceEntry',
    'CompartmentResolver',
    'CompartmentQuery',
]
