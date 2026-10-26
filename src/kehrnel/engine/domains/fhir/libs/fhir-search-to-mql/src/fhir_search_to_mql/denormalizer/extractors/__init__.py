"""
Extractors package for FHIR resource denormalization.

Contains all field extractors for different FHIR data types.
"""

# Base class from parent package
from fhir_search_to_mql.denormalizer.base_denormalizer import FieldExtractor

# Basic extractors (1-6)
from fhir_search_to_mql.denormalizer.extractors.human_name import HumanNameExtractor
from fhir_search_to_mql.denormalizer.extractors.identifier import IdentifierExtractor
from fhir_search_to_mql.denormalizer.extractors.contact_point import ContactPointExtractor
from fhir_search_to_mql.denormalizer.extractors.address import AddressExtractor
from fhir_search_to_mql.denormalizer.extractors.quantity import QuantityExtractor
from fhir_search_to_mql.denormalizer.extractors.period import PeriodExtractor

# Complex extractors (7-9)
from fhir_search_to_mql.denormalizer.extractors.codeable_concept import CodeableConceptExtractor
from fhir_search_to_mql.denormalizer.extractors.reference import ReferenceExtractor
from fhir_search_to_mql.denormalizer.extractors.coding import CodingExtractor

# Advanced extractors (10-18)
from fhir_search_to_mql.denormalizer.extractors.timing import TimingExtractor
from fhir_search_to_mql.denormalizer.extractors.range_extractor import RangeExtractor
from fhir_search_to_mql.denormalizer.extractors.ratio import RatioExtractor
from fhir_search_to_mql.denormalizer.extractors.ratio_range import RatioRangeExtractor
from fhir_search_to_mql.denormalizer.extractors.extension import ExtensionExtractor
from fhir_search_to_mql.denormalizer.extractors.money import MoneyExtractor
from fhir_search_to_mql.denormalizer.extractors.age_duration import AgeDurationExtractor
from fhir_search_to_mql.denormalizer.extractors.dosage import DosageExtractor
from fhir_search_to_mql.denormalizer.extractors.availability import AvailabilityExtractor

# Phonetic encoding (Soundex) for HumanName values
from fhir_search_to_mql.denormalizer.extractors.phonetic import PhoneticExtractor

# Generic free-text concat / lowercase extractor (resource-agnostic).
from fhir_search_to_mql.denormalizer.extractors.text import TextExtractor

# Direct scalar / polymorphic-scalar copy (e.g. Patient.deceased[x]).
from fhir_search_to_mql.denormalizer.extractors.direct_field import (
    DirectFieldExtractor,
)

# Compartment membership precompute (resource-agnostic, drives the
# `_compartments.<Type>` fast-path in CompartmentResolver).
from fhir_search_to_mql.denormalizer.extractors.compartment_membership import (
    CompartmentMembershipExtractor,
)

__all__ = [
    # Base class
    "FieldExtractor",

    # Basic extractors (1-6)
    "HumanNameExtractor",
    "IdentifierExtractor",
    "ContactPointExtractor",
    "AddressExtractor",
    "QuantityExtractor",
    "PeriodExtractor",

    # Complex extractors (7-9)
    "CodeableConceptExtractor",
    "ReferenceExtractor",
    "CodingExtractor",

    # Advanced extractors (10-18)
    "TimingExtractor",
    "RangeExtractor",
    "RatioExtractor",
    "RatioRangeExtractor",
    "ExtensionExtractor",
    "MoneyExtractor",
    "AgeDurationExtractor",
    "DosageExtractor",
    "AvailabilityExtractor",

    # Phonetic encoding
    "PhoneticExtractor",

    # Generic text concat
    "TextExtractor",

    # Direct scalar copy
    "DirectFieldExtractor",

    # Compartment membership precompute
    "CompartmentMembershipExtractor",
]
