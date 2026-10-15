"""
Comprehensive tests for Phase 4 Basic Converters.

Tests all converters:
- StringConverter
- TokenConverter
- DateConverter
- NumberConverter
- QuantityConverter
"""

import pytest
from datetime import datetime, timedelta
from fhir_search_to_mql.converters import (
    StringConverter,
    TokenConverter,
    DateConverter,
    NumberConverter,
    QuantityConverter,
)
from fhir_search_to_mql.core.exceptions import ConversionError


# ==================== STRING CONVERTER TESTS ====================

class TestStringConverter:
    """Test StringConverter with all modifiers and cases."""
    
    @pytest.fixture
    def string_config(self):
        """Sample configuration for string parameter."""
        return {
            'type': 'string',
            'fields': {
                'default': [{'field': 'name_lower'}],
                'exact': [{'field': 'name'}],
                'contains': [{'field': 'name_tokens'}]
            }
        }
    
    @pytest.fixture
    def converter(self, string_config):
        """Create StringConverter instance."""
        return StringConverter(string_config)
    
    def test_default_prefix_match(self, converter):
        """Test default case-insensitive PREFIX match."""
        query = converter.convert("Smith")
        
        assert 'name_lower' in query
        assert '$gte' in query['name_lower']
        assert query['name_lower']['$gte'] == 'smith'
        assert '$lt' in query['name_lower']
        assert query['name_lower']['$lt'] == 'smith\uffff'
    
    def test_exact_modifier(self, converter):
        """Test :exact modifier for case-sensitive exact match."""
        query = converter.convert("Smith", modifier='exact')
        
        assert 'name' in query
        assert query['name'] == 'Smith'
    
    def test_contains_modifier(self, converter):
        """Test :contains modifier for substring match."""
        query = converter.convert("mit", modifier='contains')

        assert 'name_tokens' in query
        assert query['name_tokens'] == {'$regex': 'mit', '$options': 'i'}
    
    def test_missing_true(self, converter):
        """Test :missing=true for missing fields."""
        query = converter.convert("true", modifier='missing')
        
        assert '$or' in query
        assert {'name_lower': {'$exists': False}} in query['$or']
        assert {'name_lower': None} in query['$or']
    
    def test_missing_false(self, converter):
        """Test :missing=false for existing fields."""
        query = converter.convert("false", modifier='missing')
        
        assert 'name_lower' in query
        assert '$exists' in query['name_lower']
        assert query['name_lower']['$exists'] == True
        assert '$ne' in query['name_lower']
        assert query['name_lower']['$ne'] == None
    
    def test_multiple_fields(self):
        """Test OR query with multiple fields."""
        config = {
            'type': 'string',
            'fields': {
                'default': [
                    {'field': 'familyName_lower'},
                    {'field': 'givenNames_lower'},
                    {'field': 'fullName_lower'}
                ]
            }
        }
        converter = StringConverter(config)
        query = converter.convert("Smith")
        
        assert '$or' in query
        assert len(query['$or']) == 3
    
    def test_invalid_modifier(self, converter):
        """Test error on invalid modifier."""
        with pytest.raises(ConversionError):
            converter.convert("Smith", modifier='invalid')


# ==================== TOKEN CONVERTER TESTS ====================

class TestTokenConverter:
    """Test TokenConverter with all formats and modifiers."""
    
    @pytest.fixture
    def token_config(self):
        """Sample configuration for token parameter."""
        return {
            'type': 'token',
            'fields': [
                {'field': '_search.codeCodes', 'tokenType': 'code'},
                {'field': '_search.codeSystemValues', 'tokenType': 'systemCode'}
            ]
        }
    
    @pytest.fixture
    def converter(self, token_config):
        """Create TokenConverter instance."""
        return TokenConverter(token_config)
    
    def test_code_only(self, converter):
        """Test code-only format."""
        query = converter.convert("8480-6")
        
        assert '$or' in query
        # Should match code field
        found_code = False
        for q in query['$or']:
            if '_search.codeCodes' in q:
                assert q['_search.codeCodes'] == '8480-6'
                found_code = True
        assert found_code
    
    def test_system_and_code(self, converter):
        """Test system|code format."""
        query = converter.convert("http://loinc.org|8480-6")
        
        assert '$or' in query
        # Should find systemCode match
        found = False
        for q in query['$or']:
            if '_search.codeSystemValues' in q:
                assert q['_search.codeSystemValues'] == 'http://loinc.org|8480-6'
                found = True
        assert found
    
    def test_system_only(self, converter):
        """Test system-only format (system|)."""
        query = converter.convert("http://loinc.org|")
        
        # Should query for system
        assert '$or' in query or '_search.codeCodes.system' in str(query)
    
    def test_empty_system(self, converter):
        """Test empty system format (|code)."""
        query = converter.convert("|8480-6")
        
        # Should match with empty/no system
        assert query is not None
    
    def test_not_modifier(self, converter):
        """Test :not modifier."""
        query = converter.convert("cancelled", modifier='not')
        
        # Should have $ne or $nor
        assert '$ne' in str(query) or '$nor' in str(query)
    
    def test_text_modifier(self, converter):
        """Test :text modifier for display text search."""
        query = converter.convert("blood pressure", modifier='text')
        
        # Should search lowercase with prefix match
        assert '_lower' in str(query) or '$gte' in str(query)
    
    def test_boolean_token(self):
        """Test boolean value token."""
        config = {
            'type': 'token',
            'fields': [{'field': 'active', 'tokenType': 'boolean'}]
        }
        converter = TokenConverter(config)
        query = converter.convert("true")
        
        assert 'active' in query
        assert query['active'] == True
    
    def test_simple_token(self):
        """Test simple token (no system)."""
        config = {
            'type': 'token',
            'fields': [{'field': 'gender'}]
        }
        converter = TokenConverter(config)
        query = converter.convert("male")
        
        assert 'gender' in query
        assert query['gender'] == 'male'


# ==================== DATE CONVERTER TESTS ====================

class TestDateConverter:
    """Test DateConverter with all prefixes and precision levels."""
    
    @pytest.fixture
    def date_config(self):
        """Sample configuration for date parameter."""
        return {
            'type': 'date',
            'fields': [{'field': 'birthDate', 'type': 'date'}]
        }
    
    @pytest.fixture
    def converter(self, date_config):
        """Create DateConverter instance."""
        return DateConverter(date_config)
    
    def test_full_date(self, converter):
        """Test full date with implicit range."""
        query = converter.convert("1980-05-15")
        
        assert 'birthDate' in str(query)
        assert '$and' in query
    
    def test_year_month(self, converter):
        """Test partial date (year-month)."""
        query = converter.convert("1980-05")
        
        # Should have range from 1980-05-01 to 1980-05-31
        assert query is not None
    
    def test_year_only(self, converter):
        """Test year-only date."""
        query = converter.convert("1980")
        
        # Should have range for entire year
        assert query is not None
    
    def test_ge_prefix(self, converter):
        """Test ge (greater than or equal) prefix."""
        query = converter.convert("1980-01-01", prefix='ge')
        
        assert 'birthDate' in query
        assert '$gte' in query['birthDate']
    
    def test_gt_prefix(self, converter):
        """Test gt (greater than) prefix."""
        query = converter.convert("1980-01-01", prefix='gt')
        
        assert 'birthDate' in query
        assert '$gt' in query['birthDate']
    
    def test_le_prefix(self, converter):
        """Test le (less than or equal) prefix."""
        query = converter.convert("1980-12-31", prefix='le')
        
        assert 'birthDate' in query
        assert '$lte' in query['birthDate']
    
    def test_lt_prefix(self, converter):
        """Test lt (less than) prefix."""
        query = converter.convert("1980-12-31", prefix='lt')
        
        assert 'birthDate' in query
        assert '$lt' in query['birthDate']
    
    def test_ne_prefix(self, converter):
        """Test ne (not equal) prefix."""
        query = converter.convert("1980-05-15", prefix='ne')
        
        assert '$or' in query
    
    def test_sa_prefix(self, converter):
        """Test sa (starts after) prefix."""
        query = converter.convert("1980-01-01", prefix='sa')
        
        assert 'birthDate' in query
        assert '$gt' in query['birthDate']
    
    def test_eb_prefix(self, converter):
        """Test eb (ends before) prefix."""
        query = converter.convert("1980-12-31", prefix='eb')
        
        assert 'birthDate' in query
        assert '$lt' in query['birthDate']
    
    def test_ap_prefix(self, converter):
        """Test ap (approximately) prefix."""
        query = converter.convert("1980-06-15", prefix='ap')
        
        # Should have range of ±1 day
        assert '$and' in query or '$gte' in str(query)
    
    def test_period_field(self):
        """Test Period field (start/end)."""
        config = {
            'type': 'date',
            'fields': [{'field': '_search.period', 'type': 'period'}]
        }
        converter = DateConverter(config)
        query = converter.convert("2024-01-01")
        
        # Should query both start and end
        assert '.start' in str(query) or '.end' in str(query) or '$or' in query


# ==================== NUMBER CONVERTER TESTS ====================

class TestNumberConverter:
    """Test NumberConverter with implicit ranges and prefixes."""
    
    @pytest.fixture
    def number_config(self):
        """Sample configuration for number parameter."""
        return {
            'type': 'number',
            'fields': [{'field': 'value'}]
        }
    
    @pytest.fixture
    def converter(self, number_config):
        """Create NumberConverter instance."""
        return NumberConverter(number_config)
    
    def test_whole_number(self, converter):
        """Test whole number with implicit ±0.5 range."""
        query = converter.convert("100")
        
        assert 'value' in str(query)
        assert '$and' in query
        # Should be [99.5, 100.5)
        assert query['$and'][0]['value']['$gte'] == 99.5
        assert query['$and'][1]['value']['$lt'] == 100.5
    
    def test_one_decimal(self, converter):
        """Test number with one decimal place."""
        query = converter.convert("100.0")
        
        # Should be [99.95, 100.05)
        assert '$and' in query
        assert abs(query['$and'][0]['value']['$gte'] - 99.95) < 0.01
        assert abs(query['$and'][1]['value']['$lt'] - 100.05) < 0.01
    
    def test_two_decimals(self, converter):
        """Test number with two decimal places."""
        query = converter.convert("100.00")
        
        # Should be [99.995, 100.005)
        assert '$and' in query
        assert abs(query['$and'][0]['value']['$gte'] - 99.995) < 0.001
    
    def test_scientific_notation(self, converter):
        """Test scientific notation (±50% range)."""
        query = converter.convert("1e2")
        
        # 100 with ±50 range: [50, 150)
        assert '$and' in query
        assert query['$and'][0]['value']['$gte'] == 50.0
        assert query['$and'][1]['value']['$lt'] == 150.0
    
    def test_gt_prefix(self, converter):
        """Test gt (greater than) prefix."""
        query = converter.convert("100", prefix='gt')
        
        assert 'value' in query
        assert '$gt' in query['value']
        # Should be > 100.5 (upper bound)
        assert query['value']['$gt'] == 100.5
    
    def test_ge_prefix(self, converter):
        """Test ge (greater than or equal) prefix."""
        query = converter.convert("100", prefix='ge')
        
        assert 'value' in query
        assert '$gte' in query['value']
        # Should be >= 99.5 (lower bound)
        assert query['value']['$gte'] == 99.5
    
    def test_lt_prefix(self, converter):
        """Test lt (less than) prefix."""
        query = converter.convert("100", prefix='lt')
        
        assert 'value' in query
        assert '$lt' in query['value']
        assert query['value']['$lt'] == 99.5
    
    def test_le_prefix(self, converter):
        """Test le (less than or equal) prefix."""
        query = converter.convert("100", prefix='le')
        
        assert 'value' in query
        assert '$lte' in query['value']
        assert query['value']['$lte'] == 100.5
    
    def test_ne_prefix(self, converter):
        """Test ne (not equal) prefix."""
        query = converter.convert("100", prefix='ne')
        
        assert '$or' in query
    
    def test_ap_prefix(self, converter):
        """Test ap (approximately ±10%) prefix."""
        query = converter.convert("100", prefix='ap')
        
        # Should be [90, 110]
        assert '$and' in query
        assert query['$and'][0]['value']['$gte'] == pytest.approx(90.0)
        assert query['$and'][1]['value']['$lte'] == pytest.approx(110.0)


# ==================== QUANTITY CONVERTER TESTS ====================

class TestQuantityConverter:
    """Test QuantityConverter with all formats and components."""
    
    @pytest.fixture
    def quantity_config(self):
        """Sample configuration for quantity parameter."""
        return {
            'type': 'quantity',
            'fields': [{'field': '_search.valueQuantity'}]
        }
    
    @pytest.fixture
    def converter(self, quantity_config):
        """Create QuantityConverter instance."""
        return QuantityConverter(quantity_config)
    
    def test_value_only(self, converter):
        """Test value-only format."""
        query = converter.convert("5.4")
        
        assert '_search.valueQuantity.value' in str(query)
        assert '$and' in query or '$gte' in str(query)
    
    def test_value_and_code(self, converter):
        """Test value||code format."""
        query = converter.convert("5.4||mg")
        
        # Should have both value and code conditions
        assert '$and' in query
        assert '_search.valueQuantity.value' in str(query)
        assert '_search.valueQuantity.code' in str(query)
        
        # Find code condition
        found_code = False
        for condition in query['$and']:
            if '_search.valueQuantity.code' in condition:
                assert condition['_search.valueQuantity.code'] == 'mg'
                found_code = True
        assert found_code
    
    def test_full_specification(self, converter):
        """Test full system|code format."""
        query = converter.convert("5.4|http://unitsofmeasure.org|mg")
        
        # Should have value, system, and code
        assert '$and' in query
        conditions_str = str(query['$and'])
        assert '_search.valueQuantity.value' in conditions_str
        assert '_search.valueQuantity.system' in conditions_str
        assert '_search.valueQuantity.code' in conditions_str
    
    def test_embedded_prefix(self, converter):
        """Test embedded prefix in value."""
        query = converter.convert("gt140||mm[Hg]")
        
        # Should parse gt prefix
        assert '$gt' in str(query)
        assert '_search.valueQuantity.code' in str(query)
    
    def test_explicit_prefix(self, converter):
        """Test explicit prefix parameter."""
        query = converter.convert("5.4||mg", prefix='ge')
        
        assert '$gte' in str(query)
    
    def test_ap_prefix_with_unit(self, converter):
        """Test approximately with unit."""
        query = converter.convert("100||mg", prefix='ap')
        
        # Should have ±10% on value and code match
        assert '$and' in query
        conditions_str = str(query)
        assert '_search.valueQuantity.value' in conditions_str
        assert '_search.valueQuantity.code' in conditions_str
    
    def test_quantity_precision(self, converter):
        """Test quantity with multiple decimal places."""
        query = converter.convert("5.40||mg")
        
        # Should have narrower precision
        assert query is not None


# ==================== INTEGRATION TESTS ====================

class TestConverterIntegration:
    """Integration tests across multiple converters."""
    
    def test_all_converters_handle_missing(self):
        """Test all converters handle :missing modifier."""
        configs = [
            {'type': 'string', 'fields': [{'field': 'field'}]},
            {'type': 'token', 'fields': [{'field': 'field'}]},
            {'type': 'date', 'fields': [{'field': 'field'}]},
            {'type': 'number', 'fields': [{'field': 'field'}]},
            {'type': 'quantity', 'fields': [{'field': 'field'}]},
        ]
        converters = [
            StringConverter(configs[0]),
            TokenConverter(configs[1]),
            DateConverter(configs[2]),
            NumberConverter(configs[3]),
            QuantityConverter(configs[4]),
        ]
        
        for converter in converters:
            query = converter.convert("true", modifier='missing')
            assert '$or' in query or '$exists' in str(query)
    
    def test_invalid_modifiers_raise_errors(self):
        """Test invalid modifiers raise ConversionError."""
        config = {'type': 'string', 'fields': [{'field': 'field'}]}
        converter = StringConverter(config)
        
        with pytest.raises(ConversionError):
            converter.convert("value", modifier='not_a_real_modifier')
    
    def test_invalid_prefixes_raise_errors(self):
        """Test invalid prefixes raise ConversionError."""
        config = {'type': 'number', 'fields': [{'field': 'field'}]}
        converter = NumberConverter(config)
        
        with pytest.raises(ConversionError):
            converter.convert("100", prefix='invalid')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
