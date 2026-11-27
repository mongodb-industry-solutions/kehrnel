"""
Shared pytest fixtures and configuration for all tests.
"""

import json
import pytest
from pathlib import Path
from typing import Dict, Any
from datetime import datetime


# =============================================================================
# Test Data Directory
# =============================================================================

@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return path to test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(scope="session")
def configs_dir() -> Path:
    """
    Return the path to the YAML configs bundled inside the package.

    Configs are now packaged inside ``src/fhir_search_to_mql/configs/``
    (instead of the repo-root ``configs/`` directory) so they ship as
    package data when the library is installed via pip. Tests that
    need a filesystem path to those configs (e.g. for assertions
    about file presence or for explicit override paths) pull from
    here.
    """
    return (
        Path(__file__).parent.parent
        / "src"
        / "fhir_search_to_mql"
        / "configs"
    )


# =============================================================================
# Sample FHIR Resources
# =============================================================================

@pytest.fixture
def sample_patient() -> Dict[str, Any]:
    """Sample Patient resource for testing."""
    return {
        "resourceType": "Patient",
        "id": "example-patient",
        "identifier": [
            {
                "use": "official",
                "system": "http://hospital.example.org",
                "value": "MRN12345"
            }
        ],
        "active": True,
        "name": [
            {
                "use": "official",
                "family": "Smith",
                "given": ["John", "Michael"],
                "prefix": ["Mr."]
            },
            {
                "use": "nickname",
                "given": ["Johnny"]
            }
        ],
        "telecom": [
            {
                "system": "phone",
                "value": "555-1234",
                "use": "home"
            },
            {
                "system": "email",
                "value": "john.smith@example.com",
                "use": "work"
            }
        ],
        "gender": "male",
        "birthDate": "1980-05-15",
        "address": [
            {
                "use": "home",
                "type": "both",
                "text": "123 Main St, Springfield, IL 62701",
                "line": ["123 Main St"],
                "city": "Springfield",
                "state": "IL",
                "postalCode": "62701",
                "country": "US"
            }
        ]
    }


@pytest.fixture
def sample_observation() -> Dict[str, Any]:
    """Sample Observation resource for testing."""
    return {
        "resourceType": "Observation",
        "id": "example-observation",
        "status": "final",
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "vital-signs",
                        "display": "Vital Signs"
                    }
                ]
            }
        ],
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "8480-6",
                    "display": "Systolic blood pressure"
                }
            ],
            "text": "Systolic blood pressure"
        },
        "subject": {
            "reference": "Patient/example-patient",
            "display": "John Smith"
        },
        "encounter": {
            "reference": "Encounter/example-encounter"
        },
        "effectiveDateTime": "2024-01-15T10:30:00Z",
        "issued": "2024-01-15T10:35:00Z",
        "performer": [
            {
                "reference": "Practitioner/example-practitioner",
                "display": "Dr. Jane Doe"
            }
        ],
        "valueQuantity": {
            "value": 120,
            "unit": "mmHg",
            "system": "http://unitsofmeasure.org",
            "code": "mm[Hg]"
        }
    }


@pytest.fixture
def sample_appointment() -> Dict[str, Any]:
    """Sample Appointment resource for testing."""
    return {
        "resourceType": "Appointment",
        "id": "example-appointment",
        "status": "booked",
        "serviceCategory": [
            {
                "coding": [
                    {
                        "system": "http://example.org/service-category",
                        "code": "gp",
                        "display": "General Practice"
                    }
                ]
            }
        ],
        "serviceType": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "11429006",
                        "display": "Consultation"
                    }
                ]
            }
        ],
        "specialty": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "394814009",
                        "display": "General practice"
                    }
                ]
            }
        ],
        "appointmentType": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v2-0276",
                    "code": "ROUTINE",
                    "display": "Routine appointment"
                }
            ]
        },
        "reasonCode": [
            {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "25064002",
                        "display": "Headache"
                    }
                ],
                "text": "Annual checkup"
            }
        ],
        "priority": 5,
        "description": "Annual physical examination",
        "start": "2024-06-20T09:00:00Z",
        "end": "2024-06-20T10:00:00Z",
        "created": "2024-01-10T14:30:00Z",
        "comment": "Patient requested morning appointment",
        "participant": [
            {
                "actor": {
                    "reference": "Patient/example-patient",
                    "display": "John Smith"
                },
                "required": "required",
                "status": "accepted"
            },
            {
                "actor": {
                    "reference": "Practitioner/example-practitioner",
                    "display": "Dr. Jane Doe"
                },
                "required": "required",
                "status": "accepted"
            },
            {
                "actor": {
                    "reference": "Location/example-location",
                    "display": "Room 101"
                },
                "required": "required",
                "status": "accepted"
            }
        ]
    }


# =============================================================================
# Configuration Fixtures
# =============================================================================

@pytest.fixture
def sample_patient_config() -> Dict[str, Any]:
    """Sample Patient resource configuration."""
    return {
        "resource": "Patient",
        "fhir_version": "R5",
        "denormalization": {
            "identifier": {
                "source": "identifier",
                "extractor": "IdentifierExtractor",
                "target": "_search",
                "field_mappings": [
                    {
                        "source_path": "identifier[*].value",
                        "target_field": "identifierValues",
                        "datatype": "array[string]"
                    },
                    {
                        "source_path": "identifier[*]",
                        "target_field": "identifierSystem_value",
                        "datatype": "array[token]",
                        "format": "{system}|{value}"
                    }
                ]
            },
            "name": {
                "source": "name",
                "extractor": "HumanNameExtractor",
                "target": "_search",
                "field_mappings": [
                    {
                        "source_path": "name[*].family",
                        "target_field": "familyName",
                        "datatype": "array[string]"
                    },
                    {
                        "source_path": "name[*].family",
                        "target_field": "familyName_lower",
                        "datatype": "array[string]",
                        "normalize": "lowercase"
                    },
                    {
                        "source_path": "name[*].given",
                        "target_field": "givenNames",
                        "datatype": "array[string]"
                    },
                    {
                        "source_path": "name[*].given",
                        "target_field": "givenNames_lower",
                        "datatype": "array[string]",
                        "normalize": "lowercase"
                    }
                ]
            }
        },
        "search_parameters": {
            "identifier": {
                "type": "token",
                "fields": [
                    {
                        "field": "_search.identifierSystem_value",
                        "indexed": True
                    }
                ]
            },
            "name": {
                "type": "string",
                "fields": {
                    "default": [
                        {"field": "_search.familyName_lower"},
                        {"field": "_search.givenNames_lower"}
                    ],
                    "exact": [
                        {"field": "_search.familyName"},
                        {"field": "_search.givenNames"}
                    ]
                }
            },
            "family": {
                "type": "string",
                "fields": {
                    "default": [
                        {"field": "_search.familyName_lower"}
                    ],
                    "exact": [
                        {"field": "_search.familyName"}
                    ]
                }
            },
            "given": {
                "type": "string",
                "fields": {
                    "default": [
                        {"field": "_search.givenNames_lower"}
                    ],
                    "exact": [
                        {"field": "_search.givenNames"}
                    ]
                }
            },
            "gender": {
                "type": "token",
                "fields": [
                    {"field": "gender", "indexed": True}
                ]
            },
            "birthdate": {
                "type": "date",
                "fields": [
                    {"field": "birthDate", "indexed": True}
                ]
            }
        }
    }


@pytest.fixture
def sample_observation_config() -> Dict[str, Any]:
    """Sample Observation resource configuration."""
    return {
        "resource": "Observation",
        "fhir_version": "R5",
        "search_parameters": {
            "code": {
                "type": "token",
                "fields": [
                    {"field": "_search.codeSystem_code", "indexed": True}
                ]
            },
            "subject": {
                "type": "reference",
                "fields": [
                    {"field": "_search.subjectId", "indexed": True}
                ]
            },
            "patient": {
                "type": "reference",
                "fields": [
                    {"field": "_search.patientId", "indexed": True}
                ]
            },
            "encounter": {
                "type": "reference",
                "fields": [
                    {"field": "_search.encounterId", "indexed": True}
                ]
            },
            "status": {
                "type": "token",
                "fields": [
                    {"field": "status", "indexed": True}
                ]
            },
            "date": {
                "type": "date",
                "fields": [
                    {"field": "effectiveDateTime", "indexed": True}
                ]
            }
        }
    }


# =============================================================================
# Error and Edge Case Fixtures
# =============================================================================

@pytest.fixture
def edge_case_resources() -> Dict[str, Dict[str, Any]]:
    """Various edge case resources for testing."""
    return {
        "null_values": {
            "resourceType": "Patient",
            "id": "null-test",
            "name": None,
            "gender": None
        },
        "empty_arrays": {
            "resourceType": "Patient",
            "id": "empty-array-test",
            "identifier": [],
            "name": [],
            "telecom": []
        },
        "missing_fields": {
            "resourceType": "Patient",
            "id": "missing-fields-test"
            # No name, identifier, etc.
        },
        "special_characters": {
            "resourceType": "Patient",
            "id": "special-chars-test",
            "name": [
                {
                    "family": "O'Brien-Smith",
                    "given": ["Mary-Jane", "José"]
                }
            ]
        },
        "very_large_values": {
            "resourceType": "Observation",
            "id": "large-value-test",
            "valueQuantity": {
                "value": 999999999999.99,
                "unit": "kg"
            }
        }
    }


@pytest.fixture
def malformed_queries() -> Dict[str, str]:
    """Various malformed query strings for error testing."""
    return {
        "no_value": "name=",
        "no_param": "=Smith",
        "invalid_prefix": "birthdate=zz2024-01-01",
        "invalid_modifier": "name:invalid=Smith",
        "multiple_modifiers": "name:exact:contains=Smith",
        "missing_equals": "nameSmith",
        "special_chars": "name=<script>alert('xss')</script>",
        "very_long": "name=" + ("A" * 10000)
    }


# =============================================================================
# Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_mongodb_collection(mocker):
    """Mock MongoDB collection for testing without database."""
    mock_collection = mocker.Mock()
    mock_collection.find.return_value = []
    mock_collection.find_one.return_value = None
    mock_collection.insert_one.return_value = mocker.Mock(inserted_id="mock-id")
    mock_collection.insert_many.return_value = mocker.Mock(inserted_ids=["id1", "id2"])
    return mock_collection


# =============================================================================
# Parametrize Data
# =============================================================================

@pytest.fixture
def string_search_cases():
    """Test cases for string search parameter conversion."""
    return [
        # (query_string, expected_operator, expected_value, description)
        ("name=Smith", "$gte", "smith", "Default prefix search"),
        ("name:exact=Smith", "$eq", "Smith", "Exact match"),
        ("name:contains=mit", "$regex", "mit", "Contains search"),
        ("family=O'Brien", "$gte", "o'brien", "Special characters"),
        ("given=José", "$gte", "josé", "Unicode characters"),
    ]


@pytest.fixture
def token_search_cases():
    """Test cases for token search parameter conversion."""
    return [
        # (query_string, expected_field, expected_value, description)
        ("gender=male", "gender", "male", "Simple token"),
        ("identifier=MRN12345", "_search.identifierValues", "MRN12345", "Identifier value only"),
        ("identifier=http://hospital.example.org|MRN12345", 
         "_search.identifierSystem_value", 
         "http://hospital.example.org|MRN12345", 
         "Identifier with system"),
        ("code=http://loinc.org|8480-6", "_search.codeSystem_code", "http://loinc.org|8480-6", "Code with system"),
    ]


@pytest.fixture
def date_search_cases():
    """Test cases for date search parameter conversion."""
    return [
        # (query_string, expected_operator, expected_value, description)
        ("birthdate=2024-01-15", "$gte", "2024-01-15", "Equal (prefix) date"),
        ("birthdate=gt2024-01-15", "$gt", "2024-01-15T23:59:59.999999", "Greater than date"),
        ("birthdate=lt2024-01-15", "$lt", "2024-01-15", "Less than date"),
        ("birthdate=ge2024-01-15", "$gte", "2024-01-15", "Greater than or equal"),
        ("birthdate=le2024-01-15", "$lte", "2024-01-15T23:59:59.999999", "Less than or equal"),
        ("date=2024", "$gte", "2024-01-01", "Year only"),
        ("date=2024-06", "$gte", "2024-06-01", "Year-month only"),
    ]


@pytest.fixture
def reference_search_cases():
    """Test cases for reference search parameter conversion."""
    return [
        # (query_string, expected_field, expected_value, description)
        ("subject=Patient/patient-123", "_search.subjectId", "patient-123", "Reference with type"),
        ("patient=patient-123", "_search.patientId", "patient-123", "Reference ID only"),
        ("encounter=Encounter/enc-456", "_search.encounterId", "enc-456", "Encounter reference"),
    ]


@pytest.fixture
def number_search_cases():
    """Test cases for number search parameter conversion."""
    return [
        # (query_string, expected_operator, expected_value, description)
        ("value-quantity=120", "$gte", 119.5, "Implicit range (default 10%)"),
        ("value-quantity=gt100", "$gt", 100, "Greater than"),
        ("value-quantity=lt200", "$lt", 200, "Less than"),
        ("priority=5", "$gte", 4.5, "Integer with range"),
    ]


# =============================================================================
# Performance Testing Fixtures
# =============================================================================

@pytest.fixture
def large_resource_batch(sample_patient):
    """Generate a large batch of resources for performance testing."""
    resources = []
    for i in range(100):
        resource = sample_patient.copy()
        resource["id"] = f"patient-{i:04d}"
        resource["name"][0]["family"] = f"Patient{i:04d}"
        resources.append(resource)
    return resources


@pytest.fixture
def complex_query_string():
    """Complex query string with many parameters for performance testing."""
    params = [
        "name=Smith",
        "gender=male",
        "birthdate=ge1980-01-01",
        "birthdate=le2000-12-31",
        "address-city=Springfield",
        "address-state=IL",
        "address-postalcode=62701",
        "phone=555-1234",
        "email=john.smith@example.com",
        "active=true"
    ]
    return "&".join(params)


# =============================================================================
# Marker Configurations
# =============================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests for individual components")
    config.addinivalue_line("markers", "integration: Integration tests with dependencies")
    config.addinivalue_line("markers", "performance: Performance and load tests")
    config.addinivalue_line("markers", "mongodb: Tests requiring MongoDB connection")
    config.addinivalue_line("markers", "slow: Slow-running tests")
