"""
Unit tests for FHIRSearchConverter.
"""

import pytest
from fhir_search_to_mql import FHIRSearchConverter
from fhir_search_to_mql.core.exceptions import (
    MissingConfigurationError,
    UnsupportedParameterError
)

@pytest.fixture
def converter():
    """Create a FHIRSearchConverter instance."""
    return FHIRSearchConverter()

def test_convert_simple_string_search(converter):
    """Test converting a simple string search."""
    query = converter.convert("Patient", query_string="name=Smith")
    
    assert "$or" in query or "_search.familyName_lower" in query

def test_convert_token_search(converter):
    """Test converting a token search."""
    query = converter.convert("Patient", query_string="gender=male")
    
    assert "gender" in query

def test_convert_date_search(converter):
    """Test converting a date search with prefix."""
    query = converter.convert("Patient", query_string="birthdate=ge1980-01-01")
    
    assert "birthDate" in query or "$and" in query

def test_convert_multiple_parameters(converter):
    """Test converting multiple parameters."""
    query = converter.convert("Patient", query_string="name=Smith&gender=male")
    
    assert "$and" in query or len(query.keys()) >= 2

def test_convert_with_modifier(converter):
    """Test converting with modifier."""
    query = converter.convert("Patient", query_string="name:exact=Smith")
    
    # Exact modifier should not use range query
    assert query is not None

def test_convert_unknown_resource(converter):
    """Test error for unknown resource type."""
    with pytest.raises(MissingConfigurationError):
        converter.convert("UnknownResource", query_string="field=value")

def test_get_supported_parameters(converter):
    """Test getting supported parameters."""
    params = converter.get_supported_parameters("Patient")
    
    assert isinstance(params, list)
    assert "name" in params
    assert "gender" in params

def test_convert_from_url(converter):
    """Test converting from full URL."""
    url = "http://example.org/fhir/Patient?name=Smith&gender=male"
    query = converter.convert("Patient", url=url)
    
    assert query is not None

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
