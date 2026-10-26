"""
Custom exception classes for the FHIR Search to MQL library.
"""


class FHIRSearchToMQLError(Exception):
    """Base exception for all FHIR Search to MQL errors."""
    pass


class ConfigurationError(FHIRSearchToMQLError):
    """Raised when there's an error in configuration files or loading."""
    pass


class ValidationError(FHIRSearchToMQLError):
    """Raised when validation fails (query, resource, configuration)."""
    pass


class ConversionError(FHIRSearchToMQLError):
    """Raised when conversion from FHIR to MQL fails."""
    pass


class ParsingError(FHIRSearchToMQLError):
    """Raised when parsing FHIR query strings or URLs fails."""
    pass


class ResourceNotInCompartmentError(FHIRSearchToMQLError):
    """Raised when a resource type is not found in a compartment definition."""
    pass


class UnsupportedParameterError(FHIRSearchToMQLError):
    """Raised when an unsupported search parameter is encountered."""
    pass


class InvalidModifierError(FHIRSearchToMQLError):
    """Raised when an invalid modifier is used with a parameter."""
    pass


class InvalidPrefixError(FHIRSearchToMQLError):
    """Raised when an invalid prefix is used with a parameter."""
    pass


class MissingConfigurationError(ConfigurationError):
    """Raised when required configuration is missing."""
    pass


class DenormalizationError(FHIRSearchToMQLError):
    """Raised when denormalization fails."""
    pass
