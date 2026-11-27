"""
Core module for FHIR Search to MQL library.

Provides exception classes and core utilities.
"""

from fhir_search_to_mql.core.exceptions import (
    FHIRSearchToMQLError,
    ConfigurationError,
    ValidationError,
    ConversionError,
    ParsingError,
    ResourceNotInCompartmentError,
    UnsupportedParameterError,
    InvalidModifierError,
    InvalidPrefixError,
    MissingConfigurationError,
    DenormalizationError,
)

__all__ = [
    "FHIRSearchToMQLError",
    "ConfigurationError",
    "ValidationError",
    "ConversionError",
    "ParsingError",
    "ResourceNotInCompartmentError",
    "UnsupportedParameterError",
    "InvalidModifierError",
    "InvalidPrefixError",
    "MissingConfigurationError",
    "DenormalizationError",
]
