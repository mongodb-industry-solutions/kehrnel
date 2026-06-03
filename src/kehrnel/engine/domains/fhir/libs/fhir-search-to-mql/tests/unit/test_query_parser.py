"""
Unit tests for Phase 3: Query Parser

Tests all parser components:
- QueryParser
- ParameterParser
- ModifierValidator
- CompartmentParser
"""

import pytest
from fhir_search_to_mql.parser import (
    QueryParser,
    ParameterParser,
    ModifierValidator,
    CompartmentParser,
)
from fhir_search_to_mql.core.exceptions import (
    ParsingError,
    ValidationError,
    InvalidModifierError,
)


class TestParameterParser:
    """Test ParameterParser functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.parser = ParameterParser()
    
    def test_parse_basic_parameter(self):
        """Test parsing a basic parameter without modifiers or prefixes."""
        result = self.parser.parse_parameter("name", "Smith")
        
        assert result['name'] == "name"
        assert result['value'] == "Smith"
        assert result['modifier'] is None
        assert result['prefix'] is None
        assert result['values'] == ["Smith"]
        assert result['type'] == "string"
    
    def test_parse_parameter_with_modifier(self):
        """Test parsing a parameter with a modifier."""
        result = self.parser.parse_parameter("name:exact", "Smith")
        
        assert result['name'] == "name"
        assert result['modifier'] == "exact"
        assert result['value'] == "Smith"
        assert result['prefix'] is None
    
    def test_parse_parameter_with_prefix(self):
        """Test parsing a parameter with a prefix."""
        result = self.parser.parse_parameter("birthdate", "ge1980-01-01")
        
        assert result['name'] == "birthdate"
        assert result['prefix'] == "ge"
        assert result['value'] == "1980-01-01"
        assert result['modifier'] is None
        assert result['type'] == "date"
    
    def test_parse_parameter_with_comma_separated_values(self):
        """Test parsing parameter with multiple comma-separated values."""
        result = self.parser.parse_parameter("name", "Smith,Johnson,Williams")
        
        assert result['name'] == "name"
        assert result['values'] == ["Smith", "Johnson", "Williams"]
        assert len(result['values']) == 3
    
    def test_parse_parameter_type_inference_string(self):
        """Test parameter type inference for string parameters."""
        result = self.parser.parse_parameter("name", "Smith")
        assert result['type'] == "string"
        
        result = self.parser.parse_parameter("family", "Doe")
        assert result['type'] == "string"
    
    def test_parse_parameter_type_inference_token(self):
        """Test parameter type inference for token parameters."""
        result = self.parser.parse_parameter("code", "8480-6")
        assert result['type'] == "token"
        
        result = self.parser.parse_parameter("status", "active")
        assert result['type'] == "token"
    
    def test_parse_parameter_type_inference_reference(self):
        """Test parameter type inference for reference parameters."""
        result = self.parser.parse_parameter("patient", "Patient/123")
        assert result['type'] == "reference"
        
        result = self.parser.parse_parameter("subject", "Patient/456")
        assert result['type'] == "reference"
    
    def test_parse_parameter_type_inference_date(self):
        """Test parameter type inference for date parameters."""
        result = self.parser.parse_parameter("birthdate", "ge1980-01-01")
        assert result['type'] == "date"
        
        result = self.parser.parse_parameter("date", "lt2024-01-01")
        assert result['type'] == "date"
    
    def test_parse_special_parameter(self):
        """Test parsing special parameters."""
        result = self.parser.parse_parameter("_id", "123")
        assert result['type'] == "special"
        assert self.parser.is_special_parameter("_id")
    
    def test_validate_syntax_valid(self):
        """Test syntax validation for valid parameters."""
        # Should not raise exception
        self.parser.validate_syntax("name", "Smith")
        self.parser.validate_syntax("name:exact", "Smith")
        self.parser.validate_syntax("_id", "123")
    
    def test_validate_syntax_empty_name(self):
        """Test syntax validation fails for empty parameter name."""
        with pytest.raises(ValidationError, match="Parameter name cannot be empty"):
            self.parser.validate_syntax("", "value")
    
    def test_validate_syntax_none_value(self):
        """Test syntax validation fails for None value."""
        with pytest.raises(ValidationError, match="has no value"):
            self.parser.validate_syntax("name", None)
    
    def test_validate_syntax_invalid_characters(self):
        """Test syntax validation fails for invalid characters."""
        with pytest.raises(ValidationError, match="Invalid characters"):
            self.parser.validate_syntax("name@#$", "value")
    
    def test_extract_chaining(self):
        """Test extracting chaining information from parameter names."""
        result = self.parser.extract_chaining("subject:Patient.name")
        assert result is not None
        assert result['base'] == "subject"
        assert result['type'] == "Patient"
        assert result['chain'] == "name"
    
    def test_extract_chaining_none(self):
        """Test extract_chaining returns None for non-chained parameters."""
        result = self.parser.extract_chaining("name")
        assert result is None
        
        result = self.parser.extract_chaining("name:exact")
        assert result is None


class TestQueryParser:
    """Test QueryParser functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.parser = QueryParser()
    
    def test_parse_simple_query_string(self):
        """Test parsing a simple query string."""
        result = self.parser.parse(query_string="name=Smith&gender=male")
        
        assert result['parameter_count'] == 2
        assert result['unique_parameters'] == 2
        
        # Check first parameter
        param1 = result['parameters'][0]
        assert param1['name'] == "name"
        assert param1['value'] == "Smith"
        
        # Check second parameter
        param2 = result['parameters'][1]
        assert param2['name'] == "gender"
        assert param2['value'] == "male"
    
    def test_parse_query_with_prefix(self):
        """Test parsing query with prefixes."""
        result = self.parser.parse(query_string="birthdate=ge1980-01-01")
        
        assert result['parameter_count'] == 1
        param = result['parameters'][0]
        assert param['name'] == "birthdate"
        assert param['prefix'] == "ge"
        assert param['value'] == "1980-01-01"
        assert param['type'] == "date"
    
    def test_parse_query_with_modifier(self):
        """Test parsing query with modifiers."""
        result = self.parser.parse(query_string="name:exact=Smith")
        
        assert result['parameter_count'] == 1
        param = result['parameters'][0]
        assert param['name'] == "name"
        assert param['modifier'] == "exact"
        assert param['value'] == "Smith"
    
    def test_parse_full_url(self):
        """Test parsing a full URL."""
        url = "http://example.org/fhir/Patient?name=Smith&gender=male"
        result = self.parser.parse(url=url)
        
        assert result['resource_type'] == "Patient"
        assert result['parameter_count'] == 2
        assert result['parameters'][0]['name'] == "name"
        assert result['parameters'][1]['name'] == "gender"
    
    def test_parse_url_with_path(self):
        """Test parsing URL with path components."""
        url = "https://fhir.example.com/base/Observation?code=8480-6"
        result = self.parser.parse(url=url)
        
        assert result['resource_type'] == "Observation"
        assert result['parameter_count'] == 1
        assert result['parameters'][0]['name'] == "code"
    
    def test_parse_multiple_values_comma_separated(self):
        """Test parsing comma-separated values."""
        result = self.parser.parse(query_string="name=Smith,Johnson,Williams")
        
        assert result['parameter_count'] == 1
        param = result['parameters'][0]
        assert param['values'] == ["Smith", "Johnson", "Williams"]
    
    def test_parse_repeated_parameters(self):
        """Test parsing repeated parameters."""
        result = self.parser.parse(query_string="name=Smith&name=Johnson")
        
        assert result['parameter_count'] == 2
        assert result['unique_parameters'] == 1
        assert result['parameters'][0]['value'] == "Smith"
        assert result['parameters'][1]['value'] == "Johnson"
    
    def test_parse_complex_query(self):
        """Test parsing complex query with multiple features."""
        query = "name:contains=Smith&gender=male&birthdate=ge1980-01-01&status=active"
        result = self.parser.parse(query_string=query)
        
        assert result['parameter_count'] == 4
        assert result['unique_parameters'] == 4
        
        # Check name parameter
        name_param = next(p for p in result['parameters'] if p['name'] == 'name')
        assert name_param['modifier'] == "contains"
        assert name_param['value'] == "Smith"
        
        # Check birthdate parameter
        bd_param = next(p for p in result['parameters'] if p['name'] == 'birthdate')
        assert bd_param['prefix'] == "ge"
        assert bd_param['value'] == "1980-01-01"
    
    def test_parse_url_decoding(self):
        """Test URL decoding of values."""
        result = self.parser.parse(query_string="name=Smith%20John&address=123%20Main%20St")
        
        assert result['parameter_count'] == 2
        assert result['parameters'][0]['value'] == "Smith John"
        assert result['parameters'][1]['value'] == "123 Main St"
    
    def test_parse_empty_query_string(self):
        """Test parsing URL with no query parameters."""
        result = self.parser.parse(url="http://example.org/fhir/Patient")
        
        assert result['resource_type'] == "Patient"
        assert result['parameter_count'] == 0
        assert result['parameters'] == []
    
    def test_parse_no_input_raises_error(self):
        """Test that parsing without input raises error."""
        with pytest.raises(ParsingError, match="Either query_string or url must be provided"):
            self.parser.parse()
    
    def test_parse_with_resource_type_override(self):
        """Test parsing with explicit resource_type override."""
        result = self.parser.parse(
            query_string="name=Smith",
            resource_type="Patient"
        )
        
        assert result['resource_type'] == "Patient"


class TestModifierValidator:
    """Test ModifierValidator functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.validator = ModifierValidator()
    
    def test_is_valid_modifier_string_exact(self):
        """Test valid string modifier."""
        assert self.validator.is_valid_modifier("exact", "string") is True
        assert self.validator.is_valid_modifier("contains", "string") is True
    
    def test_is_valid_modifier_token_not(self):
        """Test valid token modifier."""
        assert self.validator.is_valid_modifier("not", "token") is True
        assert self.validator.is_valid_modifier("text", "token") is True
    
    def test_is_valid_modifier_reference_identifier(self):
        """Test valid reference modifier."""
        assert self.validator.is_valid_modifier("identifier", "reference") is True
    
    def test_is_valid_modifier_resource_type(self):
        """Test resource type modifiers for references."""
        assert self.validator.is_valid_modifier("Patient", "reference") is True
        assert self.validator.is_valid_modifier("Practitioner", "reference") is True
        assert self.validator.is_valid_modifier("Organization", "reference") is True
    
    def test_is_valid_modifier_common_missing(self):
        """Test common 'missing' modifier works for all types."""
        assert self.validator.is_valid_modifier("missing", "string") is True
        assert self.validator.is_valid_modifier("missing", "token") is True
        assert self.validator.is_valid_modifier("missing", "reference") is True
        assert self.validator.is_valid_modifier("missing", "date") is True
    
    def test_is_valid_modifier_invalid(self):
        """Test invalid modifiers."""
        assert self.validator.is_valid_modifier("exact", "token") is False
        assert self.validator.is_valid_modifier("not", "string") is False
        assert self.validator.is_valid_modifier("invalid", "string") is False
    
    def test_validate_modifier_valid(self):
        """Test validate_modifier doesn't raise for valid modifiers."""
        # Should not raise
        self.validator.validate_modifier("exact", "name", "string")
        self.validator.validate_modifier("not", "status", "token")
        self.validator.validate_modifier(None, "name", "string")
    
    def test_validate_modifier_invalid_raises(self):
        """Test validate_modifier raises for invalid modifiers."""
        with pytest.raises(InvalidModifierError, match="Invalid modifier"):
            self.validator.validate_modifier("exact", "status", "token")
    
    def test_get_valid_modifiers_string(self):
        """Test getting valid modifiers for string parameters."""
        modifiers = self.validator.get_valid_modifiers("string")
        assert "exact" in modifiers
        assert "contains" in modifiers
        assert "missing" in modifiers
    
    def test_get_valid_modifiers_token(self):
        """Test getting valid modifiers for token parameters."""
        modifiers = self.validator.get_valid_modifiers("token")
        assert "not" in modifiers
        assert "text" in modifiers
        assert "missing" in modifiers
    
    def test_get_valid_modifiers_reference(self):
        """Test getting valid modifiers for reference parameters."""
        modifiers = self.validator.get_valid_modifiers("reference")
        assert "identifier" in modifiers
        assert "missing" in modifiers
        assert "Patient" in modifiers
        assert "Practitioner" in modifiers
    
    def test_is_type_modifier(self):
        """Test identifying resource type modifiers."""
        assert self.validator.is_type_modifier("Patient") is True
        assert self.validator.is_type_modifier("Observation") is True
        assert self.validator.is_type_modifier("exact") is False
        assert self.validator.is_type_modifier("not") is False
    
    def test_get_modifier_description(self):
        """Test getting modifier descriptions."""
        desc = self.validator.get_modifier_description("exact", "string")
        assert "Exact match" in desc
        
        desc = self.validator.get_modifier_description("contains", "string")
        assert "Substring" in desc
        
        desc = self.validator.get_modifier_description("Patient", "reference")
        assert "Patient" in desc
    
    def test_requires_special_handling(self):
        """Test identifying modifiers requiring special handling."""
        assert self.validator.requires_special_handling("missing") is True
        assert self.validator.requires_special_handling("in") is True
        assert self.validator.requires_special_handling("identifier") is True
        assert self.validator.requires_special_handling("exact") is False
    
    def test_get_all_modifiers(self):
        """Test getting all modifiers organized by type."""
        all_modifiers = self.validator.get_all_modifiers()
        
        assert "string" in all_modifiers
        assert "token" in all_modifiers
        assert "reference" in all_modifiers
        assert isinstance(all_modifiers["string"], list)


class TestCompartmentParser:
    """Test CompartmentParser functionality."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.parser = CompartmentParser()
    
    def test_parse_basic_compartment_url(self):
        """Test parsing a basic compartment URL."""
        result = self.parser.parse("/Patient/123/Observation")
        
        assert result['compartment_type'] == "Patient"
        assert result['compartment_id'] == "123"
        assert result['resource_type'] == "Observation"
        assert result['parameter_count'] == 0
        assert result['parameters'] == []
    
    def test_parse_compartment_url_with_query(self):
        """Test parsing compartment URL with query parameters."""
        result = self.parser.parse("/Patient/123/Observation?code=8480-6&date=ge2024-01-01")
        
        assert result['compartment_type'] == "Patient"
        assert result['compartment_id'] == "123"
        assert result['resource_type'] == "Observation"
        assert result['parameter_count'] == 2
        
        # Check parameters
        code_param = next(p for p in result['parameters'] if p['name'] == 'code')
        assert code_param['value'] == "8480-6"
        
        date_param = next(p for p in result['parameters'] if p['name'] == 'date')
        assert date_param['prefix'] == "ge"
        assert date_param['value'] == "2024-01-01"
    
    def test_parse_encounter_compartment(self):
        """Test parsing Encounter compartment URL."""
        result = self.parser.parse("/Encounter/456/Condition")
        
        assert result['compartment_type'] == "Encounter"
        assert result['compartment_id'] == "456"
        assert result['resource_type'] == "Condition"
    
    def test_parse_practitioner_compartment(self):
        """Test parsing Practitioner compartment URL."""
        result = self.parser.parse("/Practitioner/789/Appointment?status=booked")
        
        assert result['compartment_type'] == "Practitioner"
        assert result['compartment_id'] == "789"
        assert result['resource_type'] == "Appointment"
        assert result['parameter_count'] == 1
    
    def test_parse_invalid_format_raises_error(self):
        """Test that invalid format raises ParsingError."""
        with pytest.raises(ParsingError, match="Invalid compartment URL format"):
            self.parser.parse("/Patient/123")
        
        with pytest.raises(ParsingError, match="Invalid compartment URL format"):
            self.parser.parse("/Patient")
    
    def test_parse_invalid_compartment_type_raises_error(self):
        """Test that invalid compartment type raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid compartment type"):
            self.parser.parse("/InvalidType/123/Observation")
    
    def test_parse_invalid_resource_type_raises_error(self):
        """Test that invalid resource type format raises ValidationError."""
        with pytest.raises(ValidationError, match="Invalid resource type"):
            self.parser.parse("/Patient/123/observation")
    
    def test_is_compartment_url_valid(self):
        """Test identifying valid compartment URLs."""
        assert self.parser.is_compartment_url("/Patient/123/Observation") is True
        assert self.parser.is_compartment_url("/Encounter/456/Condition?status=active") is True
    
    def test_is_compartment_url_invalid(self):
        """Test identifying invalid compartment URLs."""
        assert self.parser.is_compartment_url("/Patient/123") is False
        assert self.parser.is_compartment_url("/InvalidType/123/Observation") is False
        assert self.parser.is_compartment_url("/Patient//Observation") is False
    
    def test_extract_compartment_info(self):
        """Test extracting compartment info without full parsing."""
        info = self.parser.extract_compartment_info("/Patient/123/Observation?code=test")
        
        assert info is not None
        assert info['compartment_type'] == "Patient"
        assert info['compartment_id'] == "123"
        assert info['resource_type'] == "Observation"
    
    def test_extract_compartment_info_invalid_returns_none(self):
        """Test extract_compartment_info returns None for invalid URLs."""
        info = self.parser.extract_compartment_info("/Patient/123")
        assert info is None
        
        info = self.parser.extract_compartment_info("/InvalidType/123/Observation")
        assert info is None
    
    def test_get_supported_compartments(self):
        """Test getting list of supported compartments."""
        compartments = self.parser.get_supported_compartments()
        
        assert "Patient" in compartments
        assert "Encounter" in compartments
        assert "Practitioner" in compartments
        assert "Device" in compartments
        assert "RelatedPerson" in compartments


class TestIntegration:
    """Integration tests combining multiple parser components."""
    
    def test_full_workflow_query_parsing(self):
        """Test complete workflow from URL to parsed parameters."""
        query_parser = QueryParser()
        modifier_validator = ModifierValidator()
        
        # Parse a complex query
        url = "http://example.org/fhir/Patient?name:contains=Smith&gender=male&birthdate=ge1980-01-01"
        result = query_parser.parse(url=url)
        
        assert result['resource_type'] == "Patient"
        assert result['parameter_count'] == 3
        
        # Validate modifiers for each parameter
        for param in result['parameters']:
            if param['modifier']:
                # Should not raise
                modifier_validator.validate_modifier(
                    param['modifier'],
                    param['name'],
                    param['type']
                )
    
    def test_full_workflow_compartment_parsing(self):
        """Test complete workflow for compartment URL parsing."""
        compartment_parser = CompartmentParser()
        
        # Parse compartment URL with parameters
        url = "/Patient/123/Observation?code=8480-6&status=final"
        result = compartment_parser.parse(url)
        
        assert result['compartment_type'] == "Patient"
        assert result['compartment_id'] == "123"
        assert result['resource_type'] == "Observation"
        assert result['parameter_count'] == 2
        
        # Verify parameters were parsed correctly
        params_dict = {p['name']: p['value'] for p in result['parameters']}
        assert params_dict['code'] == "8480-6"
        assert params_dict['status'] == "final"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
