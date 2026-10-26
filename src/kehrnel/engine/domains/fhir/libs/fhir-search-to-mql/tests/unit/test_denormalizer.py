"""
Unit tests for ResourceDenormalizer.
"""

import pytest
import json
from fhir_search_to_mql import ResourceDenormalizer
from fhir_search_to_mql.core.exceptions import DenormalizationError

@pytest.fixture
def denormalizer():
    """Create a ResourceDenormalizer instance."""
    return ResourceDenormalizer()

@pytest.fixture
def sample_patient():
    """Sample Patient resource."""
    return {
        "resourceType": "Patient",
        "id": "example",
        "name": [
            {
                "use": "official",
                "family": "Smith",
                "given": ["John"]
            }
        ],
        "gender": "male",
        "birthDate": "1980-01-01",
        "active": True
    }

@pytest.mark.skip(reason="Requires valid Patient configuration file")
def test_denormalize_patient(denormalizer, sample_patient):
    """Test denormalizing a Patient resource."""
    result = denormalizer.denormalize(sample_patient)
    
    # Check that _search field was added
    assert "_search" in result
    
    # Check denormalized fields
    search_fields = result["_search"]
    assert "familyName" in search_fields
    assert "familyName_lower" in search_fields
    assert search_fields["familyName"] == ["Smith"]
    assert search_fields["familyName_lower"] == ["smith"]

def test_denormalize_missing_resource_type(denormalizer):
    """Test error handling for missing resourceType."""
    resource = {"id": "example"}
    
    with pytest.raises(DenormalizationError):
        denormalizer.denormalize(resource)

def test_denormalize_unknown_resource(denormalizer):
    """Test denormalizing resource without configuration."""
    resource = {
        "resourceType": "UnknownResource",
        "id": "example"
    }
    
    # Should return resource unchanged (no config available)
    result = denormalizer.denormalize(resource)
    assert result == resource

def test_denormalize_empty_fields(denormalizer):
    """Test denormalizing resource with empty fields."""
    resource = {
        "resourceType": "Patient",
        "id": "example"
    }
    
    # Should succeed even with missing fields
    result = denormalizer.denormalize(resource)
    assert "resourceType" in result

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
