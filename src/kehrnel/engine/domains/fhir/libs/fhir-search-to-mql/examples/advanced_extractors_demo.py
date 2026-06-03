"""
Example: Using Advanced FHIR Extractors (Phase 2).

Demonstrates all 18 extractors for complete FHIR datatype coverage.
"""

from fhir_search_to_mql.denormalizer import (
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


def demonstrate_basic_extractors():
    """Demonstrate basic extractors."""
    print("\n" + "="*80)
    print("BASIC EXTRACTORS (1-6)")
    print("="*80)
    
    # 1. HumanName
    print("\n1. HumanNameExtractor:")
    name_extractor = HumanNameExtractor()
    name = {"family": "Smith", "given": ["John", "Michael"], "prefix": ["Dr."]}
    result = name_extractor.extract(name)
    print(f"   Input: {name}")
    print(f"   Output: {result}")
    
    # 2. Identifier
    print("\n2. IdentifierExtractor:")
    identifier_extractor = IdentifierExtractor()
    identifier = {"system": "http://hospital.org/mrn", "value": "MRN-12345"}
    result = identifier_extractor.extract(identifier)
    print(f"   Input: {identifier}")
    print(f"   Output: {result}")
    
    # 3. ContactPoint
    print("\n3. ContactPointExtractor:")
    contact_extractor = ContactPointExtractor()
    contact = [
        {"system": "phone", "value": "555-1234"},
        {"system": "email", "value": "john@example.com"}
    ]
    result = contact_extractor.extract(contact)
    print(f"   Input: {contact}")
    print(f"   Output: {result}")
    
    # 4. Address
    print("\n4. AddressExtractor:")
    address_extractor = AddressExtractor()
    address = {
        "line": ["123 Main St", "Apt 4B"],
        "city": "Boston",
        "state": "MA",
        "postalCode": "02134"
    }
    result = address_extractor.extract(address)
    print(f"   Input: {address}")
    print(f"   Output: {result}")
    
    # 5. Quantity
    print("\n5. QuantityExtractor:")
    quantity_extractor = QuantityExtractor()
    quantity = {"value": 120, "unit": "mmHg", "system": "http://unitsofmeasure.org"}
    result = quantity_extractor.extract(quantity)
    print(f"   Input: {quantity}")
    print(f"   Output: {result}")
    
    # 6. Period
    print("\n6. PeriodExtractor:")
    period_extractor = PeriodExtractor()
    period = {"start": "2024-01-01", "end": "2024-12-31"}
    result = period_extractor.extract(period)
    print(f"   Input: {period}")
    print(f"   Output: {result}")


def demonstrate_complex_extractors():
    """Demonstrate complex extractors."""
    print("\n" + "="*80)
    print("COMPLEX EXTRACTORS (7-9)")
    print("="*80)
    
    # 7. CodeableConcept
    print("\n7. CodeableConceptExtractor:")
    codeable_extractor = CodeableConceptExtractor()
    codeable_concept = {
        "coding": [
            {"system": "http://loinc.org", "code": "8480-6", "display": "Systolic BP"}
        ],
        "text": "Blood pressure systolic"
    }
    result = codeable_extractor.extract(codeable_concept)
    print(f"   Input: {codeable_concept}")
    print(f"   Output: {result}")
    
    # 8. Reference
    print("\n8. ReferenceExtractor:")
    reference_extractor = ReferenceExtractor()
    reference = {"reference": "Patient/pat-123", "display": "John Smith"}
    result = reference_extractor.extract(reference)
    print(f"   Input: {reference}")
    print(f"   Output: {result}")
    
    # 9. Coding
    print("\n9. CodingExtractor:")
    coding_extractor = CodingExtractor()
    coding = {"system": "http://snomed.info/sct", "code": "271649006", "display": "Systolic BP"}
    result = coding_extractor.extract(coding)
    print(f"   Input: {coding}")
    print(f"   Output: {result}")


def demonstrate_advanced_extractors():
    """Demonstrate advanced extractors for complete FHIR coverage."""
    print("\n" + "="*80)
    print("ADVANCED EXTRACTORS (10-18)")
    print("="*80)
    
    # 10. Timing
    print("\n10. TimingExtractor:")
    timing_extractor = TimingExtractor()
    timing = {
        "event": ["2024-01-01T10:00:00Z"],
        "repeat": {"frequency": 2, "period": 1, "periodUnit": "d"}
    }
    result = timing_extractor.extract(timing)
    print(f"   Input: {timing}")
    print(f"   Output: {result}")
    
    # 11. Range
    print("\n11. RangeExtractor:")
    range_extractor = RangeExtractor()
    range_obj = {
        "low": {"value": 120, "unit": "mmHg"},
        "high": {"value": 140, "unit": "mmHg"}
    }
    result = range_extractor.extract(range_obj)
    print(f"   Input: {range_obj}")
    print(f"   Output: {result}")
    
    # 12. Ratio
    print("\n12. RatioExtractor:")
    ratio_extractor = RatioExtractor()
    ratio = {
        "numerator": {"value": 1, "unit": "tablet"},
        "denominator": {"value": 2, "unit": "day"}
    }
    result = ratio_extractor.extract(ratio)
    print(f"   Input: {ratio}")
    print(f"   Output: {result}")
    
    # 13. RatioRange
    print("\n13. RatioRangeExtractor:")
    ratio_range_extractor = RatioRangeExtractor()
    ratio_range = {
        "lowNumerator": {"value": 1},
        "lowDenominator": {"value": 10},
        "highNumerator": {"value": 1},
        "highDenominator": {"value": 5}
    }
    result = ratio_range_extractor.extract(ratio_range)
    print(f"   Input: {ratio_range}")
    print(f"   Output: {result}")
    
    # 14. Extension
    print("\n14. ExtensionExtractor:")
    extension_extractor = ExtensionExtractor()
    extension = {"url": "http://example.org/ethnicity", "valueString": "Hispanic"}
    result = extension_extractor.extract(extension)
    print(f"   Input: {extension}")
    print(f"   Output: {result}")
    
    # 15. Money
    print("\n15. MoneyExtractor:")
    money_extractor = MoneyExtractor()
    money = {"value": 100.50, "currency": "USD"}
    result = money_extractor.extract(money)
    print(f"   Input: {money}")
    print(f"   Output: {result}")
    
    # 16. Age/Duration
    print("\n16. AgeDurationExtractor:")
    age_duration_extractor = AgeDurationExtractor()
    age = {"value": 45, "unit": "years", "code": "a"}
    result = age_duration_extractor.extract(age)
    print(f"   Input: {age}")
    print(f"   Output: {result}")
    
    # 17. Dosage
    print("\n17. DosageExtractor:")
    dosage_extractor = DosageExtractor()
    dosage = {
        "text": "Take 1 tablet by mouth daily",
        "route": {"coding": [{"code": "PO"}], "text": "Oral"},
        "doseQuantity": {"value": 1, "unit": "tablet"}
    }
    result = dosage_extractor.extract(dosage)
    print(f"   Input: {dosage}")
    print(f"   Output: {result}")
    
    # 18. Availability
    print("\n18. AvailabilityExtractor:")
    availability_extractor = AvailabilityExtractor()
    availability = {
        "daysOfWeek": ["mon", "tue", "wed", "thu", "fri"],
        "availableStartTime": "09:00:00",
        "availableEndTime": "17:00:00"
    }
    result = availability_extractor.extract(availability)
    print(f"   Input: {availability}")
    print(f"   Output: {result}")


def demonstrate_field_mappings():
    """Demonstrate using field_mappings for custom extraction."""
    print("\n" + "="*80)
    print("FIELD MAPPINGS: Configuration-Driven Extraction")
    print("="*80)
    
    # Example: CodeableConcept with field mappings
    print("\nCodeableConceptExtractor with field_mappings:")
    
    extractor = CodeableConceptExtractor()
    
    codeable_concept = {
        "coding": [
            {"system": "http://loinc.org", "code": "8480-6", "display": "Systolic BP"},
            {"system": "http://snomed.info/sct", "code": "271649006"}
        ],
        "text": "Blood pressure"
    }
    
    field_mappings = [
        {
            "source_path": "coding[*].code",
            "target_field": "codes",
            "datatype": "array[string]",
            "transformation": "Extract all code values",
            "description": "Array of all codes from all codings"
        },
        {
            "source_path": "coding[*].system|code",
            "target_field": "systemValues",
            "datatype": "array[string]",
            "transformation": "Combine system and code with | delimiter",
            "description": "Array of system|code pairs"
        }
    ]
    
    result = extractor.extract(codeable_concept, field_mappings=field_mappings)
    
    print(f"\nInput: {codeable_concept}")
    print(f"\nField Mappings: {len(field_mappings)} mappings")
    for mapping in field_mappings:
        print(f"  - {mapping['source_path']} → {mapping['target_field']}")
    print(f"\nOutput: {result}")


if __name__ == "__main__":
    print("\n" + "="*80)
    print("FHIR EXTRACTORS - COMPLETE COVERAGE (18 EXTRACTORS)")
    print("="*80)
    
    demonstrate_basic_extractors()
    demonstrate_complex_extractors()
    demonstrate_advanced_extractors()
    demonstrate_field_mappings()
    
    print("\n" + "="*80)
    print("Coverage: 18/18 FHIR searchable datatypes ✓")
    print("="*80)
