"""
FHIR Search to MQL Conversion Library

A production-ready library for converting FHIR search queries to MongoDB Query Language
with optimized denormalization for high-performance healthcare data search.
"""

__version__ = "1.2.1"
__author__ = "FHIR-GEN Team"

from fhir_search_to_mql.core.config_loader import ConfigLoader
from fhir_search_to_mql.denormalizer.resource_denormalizer import ResourceDenormalizer
from fhir_search_to_mql.denormalizer.mongodb_handler import MongoDBHandler
from fhir_search_to_mql.fhir_search_converter import FHIRSearchConverter
from fhir_search_to_mql.parser.query_parser import QueryParser
from fhir_search_to_mql.parser.search_request_parser import (
    criteria_dict_to_query_string,
    parse_fhir_search,
    parse_fhir_search_parts,
)
from fhir_search_to_mql.builder.mql_builder import MQLBuilder
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
    "__version__",
    "ConfigLoader",
    "ResourceDenormalizer",
    "MongoDBHandler",
    "FHIRSearchConverter",
    "QueryParser",
    "criteria_dict_to_query_string",
    "parse_fhir_search",
    "parse_fhir_search_parts",
    "MQLBuilder",
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
