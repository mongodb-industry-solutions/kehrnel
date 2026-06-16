"""
Unit tests for additional FHIR extractors (Phase 2).

Tests the 10 new extractors for complete FHIR coverage.
"""

import pytest
from fhir_search_to_mql.denormalizer.extractors import (
    TimingExtractor,
    RangeExtractor,
    RatioExtractor,
    RatioRangeExtractor,
    CodingExtractor,
    ExtensionExtractor,
    MoneyExtractor,
    AgeDurationExtractor,
    DosageExtractor,
    AvailabilityExtractor,
)


class TestTimingExtractor:
    """Test TimingExtractor functionality."""
    
    def test_extract_timing_with_events(self):
        """Test extracting timing with event dates."""
        extractor = TimingExtractor()
        
        timing = {
            "event": ["2024-01-01T10:00:00Z", "2024-01-02T10:00:00Z"],
            "repeat": {
                "frequency": 2,
                "period": 1,
                "periodUnit": "d"
            }
        }
        
        result = extractor.extract(timing)
        
        assert "timingEvents" in result
        assert len(result["timingEvents"]) == 2
        assert result["timingFrequencies"] == [2]
        assert result["timingPeriods"] == [1]
        assert result["timingPeriodUnits"] == ["d"]
    
    def test_extract_timing_with_bounds(self):
        """Test extracting timing with bounds period."""
        extractor = TimingExtractor()
        
        timing = {
            "repeat": {
                "boundsPeriod": {
                    "start": "2024-01-01",
                    "end": "2024-12-31"
                }
            }
        }
        
        result = extractor.extract(timing)
        
        assert result["timingBoundsStart"] == "2024-01-01"
        assert result["timingBoundsEnd"] == "2024-12-31"


class TestRangeExtractor:
    """Test RangeExtractor functionality."""
    
    def test_extract_range_with_low_and_high(self):
        """Test extracting range with both bounds."""
        extractor = RangeExtractor()
        
        range_obj = {
            "low": {"value": 120, "unit": "mmHg"},
            "high": {"value": 140, "unit": "mmHg"}
        }
        
        result = extractor.extract(range_obj)
        
        assert result["rangeLowValue"] == 120
        assert result["rangeLowUnit"] == "mmHg"
        assert result["rangeHighValue"] == 140
        assert result["rangeHighUnit"] == "mmHg"
    
    def test_extract_range_with_only_low(self):
        """Test extracting range with only low bound."""
        extractor = RangeExtractor()
        
        range_obj = {
            "low": {"value": 0, "unit": "kg"}
        }
        
        result = extractor.extract(range_obj)
        
        assert result["rangeLowValue"] == 0
        assert result["rangeLowUnit"] == "kg"
        assert "rangeHighValue" not in result


class TestRatioExtractor:
    """Test RatioExtractor functionality."""
    
    def test_extract_ratio(self):
        """Test extracting ratio with numerator and denominator."""
        extractor = RatioExtractor()
        
        ratio = {
            "numerator": {"value": 1, "unit": "tablet"},
            "denominator": {"value": 2, "unit": "day"}
        }
        
        result = extractor.extract(ratio)
        
        assert result["ratioNumeratorValue"] == 1
        assert result["ratioNumeratorUnit"] == "tablet"
        assert result["ratioDenominatorValue"] == 2
        assert result["ratioDenominatorUnit"] == "day"
        assert result["ratioValue"] == 0.5


class TestRatioRangeExtractor:
    """Test RatioRangeExtractor functionality."""
    
    def test_extract_ratio_range(self):
        """Test extracting ratio range."""
        extractor = RatioRangeExtractor()
        
        ratio_range = {
            "lowNumerator": {"value": 1},
            "lowDenominator": {"value": 10},
            "highNumerator": {"value": 1},
            "highDenominator": {"value": 5}
        }
        
        result = extractor.extract(ratio_range)
        
        assert result["ratioRangeLowValue"] == 0.1
        assert result["ratioRangeHighValue"] == 0.2


class TestCodingExtractor:
    """Test CodingExtractor functionality."""
    
    def test_extract_coding(self):
        """Test extracting coding structure."""
        extractor = CodingExtractor()
        
        coding = {
            "system": "http://loinc.org",
            "code": "8480-6",
            "display": "Systolic blood pressure"
        }
        
        result = extractor.extract(coding)
        
        assert result["codingCodes"] == ["8480-6"]
        assert result["codingSystems"] == ["http://loinc.org"]
        assert result["codingSystemValues"] == ["http://loinc.org|8480-6"]
        assert result["codingDisplays"] == ["Systolic blood pressure"]
    
    def test_extract_coding_array(self):
        """Test extracting array of codings."""
        extractor = CodingExtractor()
        
        codings = [
            {"system": "http://loinc.org", "code": "8480-6"},
            {"system": "http://snomed.info/sct", "code": "271649006"}
        ]
        
        result = extractor.extract(codings)
        
        assert len(result["codingCodes"]) == 2
        assert len(result["codingSystemValues"]) == 2


class TestExtensionExtractor:
    """Test ExtensionExtractor functionality."""
    
    def test_extract_extension_with_string_value(self):
        """Test extracting extension with string value."""
        extractor = ExtensionExtractor()
        
        extension = {
            "url": "http://example.org/ethnicity",
            "valueString": "Hispanic"
        }
        
        result = extractor.extract(extension)
        
        assert result["extensionUrls"] == ["http://example.org/ethnicity"]
        assert result["extensionStringValues"] == ["Hispanic"]
    
    def test_extract_extension_with_boolean_value(self):
        """Test extracting extension with boolean value."""
        extractor = ExtensionExtractor()
        
        extension = {
            "url": "http://example.org/active",
            "valueBoolean": True
        }
        
        result = extractor.extract(extension)
        
        assert result["extensionBooleanValues"] == [True]


class TestMoneyExtractor:
    """Test MoneyExtractor functionality."""
    
    def test_extract_money(self):
        """Test extracting money value and currency."""
        extractor = MoneyExtractor()
        
        money = {
            "value": 100.50,
            "currency": "USD"
        }
        
        result = extractor.extract(money)
        
        assert result["moneyValue"] == 100.50
        assert result["moneyCurrency"] == "USD"


class TestAgeDurationExtractor:
    """Test AgeDurationExtractor functionality."""
    
    def test_extract_age(self):
        """Test extracting age value."""
        extractor = AgeDurationExtractor()
        
        age = {
            "value": 45,
            "unit": "years",
            "system": "http://unitsofmeasure.org",
            "code": "a"
        }
        
        result = extractor.extract(age)
        
        assert result["value"] == 45
        assert result["unit"] == "years"
        assert result["code"] == "a"
    
    def test_extract_duration(self):
        """Test extracting duration value."""
        extractor = AgeDurationExtractor()
        
        duration = {
            "value": 30,
            "unit": "minutes",
            "system": "http://unitsofmeasure.org",
            "code": "min"
        }
        
        result = extractor.extract(duration)
        
        assert result["value"] == 30
        assert result["unit"] == "minutes"


class TestDosageExtractor:
    """Test DosageExtractor functionality."""
    
    def test_extract_dosage(self):
        """Test extracting dosage information."""
        extractor = DosageExtractor()
        
        dosage = {
            "text": "Take 1 tablet by mouth daily",
            "route": {
                "coding": [{"code": "PO", "display": "Oral"}],
                "text": "Oral"
            },
            "doseQuantity": {
                "value": 1,
                "unit": "tablet"
            }
        }
        
        result = extractor.extract(dosage)
        
        assert result["dosageText"] == ["Take 1 tablet by mouth daily"]
        assert result["dosageRoute"] == ["Oral"]
        assert result["dosageRouteCodes"] == ["PO"]
        assert result["dosageDoseValue"] == [1]
        assert result["dosageDoseUnit"] == ["tablet"]


class TestAvailabilityExtractor:
    """Test AvailabilityExtractor functionality."""
    
    def test_extract_availability(self):
        """Test extracting availability schedule."""
        extractor = AvailabilityExtractor()
        
        availability = {
            "daysOfWeek": ["mon", "tue", "wed", "thu", "fri"],
            "allDay": False,
            "availableStartTime": "09:00:00",
            "availableEndTime": "17:00:00"
        }
        
        result = extractor.extract(availability)
        
        assert len(result["availabilityDaysOfWeek"]) == 5
        assert result["availabilityAllDay"] == False
        assert result["availabilityStartTime"] == ["09:00:00"]
        assert result["availabilityEndTime"] == ["17:00:00"]
    
    def test_extract_availability_all_day(self):
        """Test extracting all-day availability."""
        extractor = AvailabilityExtractor()
        
        availability = {
            "daysOfWeek": ["sat", "sun"],
            "allDay": True
        }
        
        result = extractor.extract(availability)
        
        assert result["availabilityAllDay"] == True
        assert len(result["availabilityDaysOfWeek"]) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
