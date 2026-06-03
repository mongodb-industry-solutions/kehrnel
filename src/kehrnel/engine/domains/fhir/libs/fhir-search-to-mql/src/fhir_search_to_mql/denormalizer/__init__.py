"""
Denormalizer package for extracting and transforming FHIR data structures.

Provides 18 extractors for complete FHIR R4/R5/R6 datatype coverage:
- Basic: HumanName, Identifier, ContactPoint, Address, Quantity, Period
- Complex: CodeableConcept, Reference, Coding
- Advanced: Timing, Range, Ratio, RatioRange, Extension, Money, AgeDuration, Dosage, Availability
"""

# Base class and orchestrator
from fhir_search_to_mql.denormalizer.extractors import FieldExtractor
from fhir_search_to_mql.denormalizer.resource_denormalizer import ResourceDenormalizer

# Handlers
from fhir_search_to_mql.denormalizer.file_handler import FileHandler
from fhir_search_to_mql.denormalizer.mongodb_handler import MongoDBHandler

# All extractors from extractors package
from fhir_search_to_mql.denormalizer.extractors import (
    # Basic extractors
    HumanNameExtractor,
    IdentifierExtractor,
    ContactPointExtractor,
    AddressExtractor,
    QuantityExtractor,
    PeriodExtractor,
    # Complex extractors
    CodeableConceptExtractor,
    ReferenceExtractor,
    CodingExtractor,
    # Advanced extractors
    TimingExtractor,
    RangeExtractor,
    RatioExtractor,
    RatioRangeExtractor,
    ExtensionExtractor,
    MoneyExtractor,
    AgeDurationExtractor,
    DosageExtractor,
    AvailabilityExtractor,
)

__all__ = [
    # Base classes
    "FieldExtractor",
    "ResourceDenormalizer",
    
    # Handlers
    "FileHandler",
    "MongoDBHandler",
    
    # Basic extractors
    "HumanNameExtractor",
    "IdentifierExtractor",
    "ContactPointExtractor",
    "AddressExtractor",
    "QuantityExtractor",
    "PeriodExtractor",
    
    # Complex extractors
    "CodeableConceptExtractor",
    "ReferenceExtractor",
    "CodingExtractor",
    
    # Advanced extractors
    "TimingExtractor",
    "RangeExtractor",
    "RatioExtractor",
    "RatioRangeExtractor",
    "ExtensionExtractor",
    "MoneyExtractor",
    "AgeDurationExtractor",
    "DosageExtractor",
    "AvailabilityExtractor",
]
