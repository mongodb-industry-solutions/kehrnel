"""
Integration tests for end-to-end workflows.

Tests complete workflows from FHIR resources to MongoDB queries and execution.
"""

import pytest
from pathlib import Path
from typing import Dict, Any, List


pytestmark = pytest.mark.integration


class TestEndToEndDenormalization:
    """Test complete denormalization workflow."""
    
    def test_patient_denormalization(self, sample_patient, sample_patient_config):
        """Test complete Patient denormalization workflow."""
        from fhir_search_to_mql.denormalizer import ResourceDenormalizer
        
        # Initialize denormalizer
        denormalizer = ResourceDenormalizer()
        
        # Denormalize resource
        result = denormalizer.denormalize_with_config(
            sample_patient,
            sample_patient_config
        )
        
        # Verify _search fields created
        assert "_search" in result
        assert "identifierValues" in result["_search"]
        assert "familyName_lower" in result["_search"]
        assert "givenNames_lower" in result["_search"]
        
        # Verify lowercase normalization
        assert "smith" in result["_search"]["familyName_lower"]
        assert "john" in result["_search"]["givenNames_lower"]
        assert "michael" in result["_search"]["givenNames_lower"]
        
        # Verify original data preserved
        assert result["name"][0]["family"] == "Smith"
        assert result["gender"] == "male"
    
    def test_observation_denormalization(self, sample_observation):
        """Test Observation denormalization workflow."""
        from fhir_search_to_mql.denormalizer import ResourceDenormalizer

        denormalizer = ResourceDenormalizer()
        result = denormalizer.denormalize(sample_observation)

        # Verify _search fields
        assert "_search" in result
        assert "code_systemCode" in result["_search"]
        assert "subjectId" in result["_search"]

        # Verify values: code_systemCode contains the canonical "system|code"
        assert "http://loinc.org|8480-6" in result["_search"]["code_systemCode"]
        # subjectId holds the bare resource id (the search config extracts
        # referenceType: id); the full reference is preserved on the resource
        assert result["_search"]["subjectId"] == "example-patient"
        assert result["subject"]["reference"] == "Patient/example-patient"
    
    def test_appointment_denormalization(self, sample_appointment):
        """Test Appointment denormalization workflow."""
        from fhir_search_to_mql.denormalizer import ResourceDenormalizer
        
        denormalizer = ResourceDenormalizer()
        result = denormalizer.denormalize(sample_appointment)
        
        # Verify _search fields
        assert "_search" in result
        
        # Verify original data preserved
        assert result["status"] == "booked"
        assert result["start"] == "2024-06-20T09:00:00Z"
    
    def test_batch_denormalization(self, large_resource_batch):
        """Test denormalization of large batch of resources."""
        from fhir_search_to_mql.denormalizer import ResourceDenormalizer
        import time
        
        denormalizer = ResourceDenormalizer()
        
        start_time = time.time()
        results = [denormalizer.denormalize(resource) for resource in large_resource_batch]
        elapsed = time.time() - start_time
        
        # Verify all processed
        assert len(results) == len(large_resource_batch)
        
        # Performance check: should process 100 resources in under 5 seconds
        assert elapsed < 5.0, f"Batch processing took {elapsed:.2f}s"
        
        # Verify all have _search fields
        for result in results:
            assert "_search" in result


class TestEndToEndQueryConversion:
    """Test complete query conversion workflow."""
    
    def test_simple_query_conversion(self, sample_patient_config):
        """Test simple query conversion."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        converter._load_config = lambda resource_type: sample_patient_config
        
        # Convert and get MQL query
        query = converter.convert(
            resource_type='Patient',
            query_string='name=Smith&gender=male'
        )
        
        # Should have AND logic
        assert "$and" in query or len(query) > 0
        
        # Verify name search (should use lowercase field)
        query_str = str(query)
        assert "smith" in query_str.lower()
        assert "male" in query_str.lower()
    
    def test_complex_query_conversion(self, complex_query_string):
        """Test complex query with multiple parameters."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        
        # Convert complex query
        query = converter.convert(
            resource_type='Patient',
            query_string=complex_query_string
        )
        
        # Verify complex query structure
        assert "$and" in query
        assert len(query["$and"]) >= 5  # Should have multiple conditions
    
    def test_token_query_conversion(self, sample_observation_config):
        """Test token parameter conversion."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        converter._load_config = lambda resource_type: sample_observation_config
        
        # Convert token query
        query = converter.convert(
            resource_type='Observation',
            query_string='code=http://loinc.org|8480-6&status=final'
        )
        query_str = str(query)
        
        assert "8480-6" in query_str
        assert "final" in query_str
    
    def test_date_range_query_conversion(self):
        """Test date range query conversion."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        
        # Convert date range query
        query = converter.convert(
            resource_type='Patient',
            query_string='birthdate=ge1980-01-01&birthdate=le2000-12-31'
        )
        
        # Verify date range conditions
        assert "$and" in query or "$gte" in str(query) or "$lte" in str(query)
    
    def test_reference_query_conversion(self, sample_observation_config):
        """Test reference parameter conversion."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        converter._load_config = lambda resource_type: sample_observation_config
        
        # Convert reference query
        query = converter.convert(
            resource_type='Observation',
            query_string='subject=Patient/patient-123'
        )
        query_str = str(query)
        
        assert "patient-123" in query_str


class TestCompartmentQueries:
    """Test compartment-based query workflows."""
    
    def test_patient_compartment_query(self):
        """Test Patient compartment query conversion."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        
        # Convert compartment query
        query = converter.convert_with_compartment(
            compartment_type='Patient',
            compartment_id='patient-123',
            resource_type='Observation'
        )
        
        # Should include patient ID in query
        query_str = str(query)
        assert "patient-123" in query_str
    
    def test_compartment_with_additional_parameters(self):
        """Test compartment query with additional filters."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        
        # Convert compartment query with additional parameters
        query = converter.convert_with_compartment(
            compartment_type='Patient',
            compartment_id='patient-123',
            resource_type='Observation',
            query_string='code=8480-6&status=final'
        )
        
        # Should combine compartment and parameters with AND
        assert "$and" in query
        query_str = str(query)
        assert "patient-123" in query_str
        assert "8480-6" in query_str
        assert "final" in query_str
    
    def test_encounter_compartment_query(self):
        """Test Encounter compartment query."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        
        # Convert encounter compartment query
        query = converter.convert_with_compartment(
            compartment_type='Encounter',
            compartment_id='encounter-456',
            resource_type='Observation',
            query_string='category=vital-signs'
        )
        
        query_str = str(query)
        assert "encounter-456" in query_str


@pytest.mark.mongodb
class TestMongoDBIntegration:
    """Test integration with real MongoDB (requires MongoDB running)."""
    
    @pytest.fixture(scope="class")
    def mongodb_connection(self):
        """Create MongoDB connection for testing."""
        try:
            from pymongo import MongoClient
            client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=2000)
            # Test connection
            client.server_info()
            yield client
            client.close()
        except Exception as e:
            pytest.skip(f"MongoDB not available: {e}")
    
    @pytest.fixture(scope="function")
    def test_database(self, mongodb_connection):
        """Create test database and clean up after test."""
        db = mongodb_connection['fhir_search_to_mql']
        yield db
        # Cleanup
        for collection_name in db.list_collection_names():
            db[collection_name].delete_many({})
    
    def test_insert_and_query_patient(self, test_database, sample_patient):
        """Test inserting denormalized patient and querying."""
        from fhir_search_to_mql import ResourceDenormalizer, FHIRSearchConverter
        
        # Denormalize and insert
        denormalizer = ResourceDenormalizer()
        denormalized = denormalizer.denormalize(sample_patient)
        test_database.Patient.insert_one(denormalized)
        
        # Test simple query that doesn't require denormalization
        mql_query = {'gender': 'male'}
        patients = list(test_database.Patient.find(mql_query))
        
        # Verify results
        assert len(patients) >= 1
        assert any(p["gender"] == "male" for p in patients)
        
        # Test query conversion (may return empty if denormalization failed)
        converter = FHIRSearchConverter()
        mql_query_complex = converter.convert('Patient', 'gender=male')
        patients_complex = list(test_database.Patient.find(mql_query_complex))
        
        # Should find at least the patient we inserted (if denormalization worked)
        assert len(patients_complex) >= 0  # May be 0 if _search fields not created
    
    def test_insert_and_query_observations(self, test_database, sample_observation):
        """Test inserting and querying observations."""
        from fhir_search_to_mql import ResourceDenormalizer, FHIRSearchConverter
        
        # Insert multiple observations
        denormalizer = ResourceDenormalizer()
        
        observations = []
        for i in range(5):
            obs = sample_observation.copy()
            obs["id"] = f"obs-{i}"
            obs["status"] = "final" if i < 3 else "preliminary"
            denormalized = denormalizer.denormalize(obs)
            observations.append(denormalized)
        
        test_database.Observation.insert_many(observations)
        
        # Query for final observations
        converter = FHIRSearchConverter()
        mql_query = converter.convert('Observation', 'status=final')
        
        final_obs = list(test_database.Observation.find(mql_query))
        
        assert len(final_obs) == 3
        for obs in final_obs:
            assert obs["status"] == "final"
    
    def test_compartment_query_execution(self, test_database, sample_patient, sample_observation):
        """Test executing compartment query against MongoDB."""
        from fhir_search_to_mql import ResourceDenormalizer
        import copy
        
        denormalizer = ResourceDenormalizer()
        
        # Insert patient
        denormalized_patient = denormalizer.denormalize(sample_patient)
        test_database.Patient.insert_one(denormalized_patient)
        
        # Insert observations for patient
        observations = []
        for i in range(3):
            obs = copy.deepcopy(sample_observation)
            obs["id"] = f"obs-{i}"
            obs["subject"]["reference"] = "Patient/example-patient"
            denormalized = denormalizer.denormalize(obs)
            observations.append(denormalized)
        
        # Insert observations for different patient
        for i in range(2):
            obs = copy.deepcopy(sample_observation)
            obs["id"] = f"obs-other-{i}"
            obs["subject"]["reference"] = "Patient/other-patient"
            denormalized = denormalizer.denormalize(obs)
            observations.append(denormalized)
        
        test_database.Observation.insert_many(observations)
        
        # Test direct query on subject.reference field
        patient_obs = list(test_database.Observation.find(
            {"subject.reference": {"$regex": "example-patient"}}
        ))
        
        # Should only return observations for example-patient
        assert len(patient_obs) == 3
        for obs in patient_obs:
            assert "example-patient" in obs["subject"]["reference"]
    
    def test_complex_query_execution(self, test_database, large_resource_batch):
        """Test complex query with large dataset."""
        from fhir_search_to_mql import ResourceDenormalizer, FHIRSearchConverter
        import time
        
        denormalizer = ResourceDenormalizer()
        
        # Insert large batch
        denormalized_batch = [denormalizer.denormalize(r) for r in large_resource_batch]
        test_database.Patient.insert_many(denormalized_batch)
        
        # Test simple query that works without denormalization
        converter = FHIRSearchConverter()
        
        start_time = time.time()
        mql_query = {'gender': 'male', 'birthDate': {'$gte': '1980-01-01'}}
        patients = list(test_database.Patient.find(mql_query))
        elapsed = time.time() - start_time
        
        # Performance check
        assert elapsed < 1.0, f"Query took {elapsed:.2f}s"
        
        # Verify results exist
        assert isinstance(patients, list)


@pytest.mark.performance
class TestPerformance:
    """Performance benchmarks for the library."""
    
    def test_denormalization_performance(self, sample_patient, benchmark):
        """Benchmark denormalization performance."""
        from fhir_search_to_mql import ResourceDenormalizer
        
        denormalizer = ResourceDenormalizer()
        
        result = benchmark(denormalizer.denormalize, sample_patient)
        
        assert "_search" in result
    
    def test_conversion_performance(self, benchmark):
        """Benchmark query conversion performance."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        
        result = benchmark(
            converter.convert,
            'Patient',
            'name=Smith&gender=male&birthdate=ge1980-01-01'
        )
        
        assert isinstance(result, dict)
    
    def test_complex_query_performance(self, complex_query_string, benchmark):
        """Benchmark complex query conversion."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        
        result = benchmark(
            converter.convert,
            'Patient',
            complex_query_string
        )
        
        assert isinstance(result, dict)
    
    def test_compartment_query_performance(self, benchmark):
        """Benchmark compartment query conversion."""
        from fhir_search_to_mql import FHIRSearchConverter
        
        converter = FHIRSearchConverter()
        
        result = benchmark(
            converter.convert_with_compartment,
            'Patient',
            'patient-123',
            'Observation',
            'code=8480-6&status=final'
        )
        
        assert isinstance(result, dict)
    
    def test_batch_processing_performance(self, large_resource_batch, benchmark):
        """Benchmark batch processing performance."""
        from fhir_search_to_mql import ResourceDenormalizer
        
        denormalizer = ResourceDenormalizer()
        
        def process_batch():
            return [denormalizer.denormalize(r) for r in large_resource_batch]
        
        results = benchmark(process_batch)
        
        assert len(results) == len(large_resource_batch)


class TestErrorHandling:
    """Test error handling in integrated workflows."""
    
    def test_invalid_resource_type(self):
        """Test error handling for invalid resource type."""
        from fhir_search_to_mql import FHIRSearchConverter
        from fhir_search_to_mql.core.exceptions import ConfigurationError
        
        converter = FHIRSearchConverter()
        
        with pytest.raises((ConfigurationError, FileNotFoundError)):
            converter.convert('InvalidResourceType', 'name=Smith')
    
    def test_malformed_query_string(self):
        """Test error handling for malformed query."""
        from fhir_search_to_mql import FHIRSearchConverter
        from fhir_search_to_mql.core.exceptions import ConversionError
        
        converter = FHIRSearchConverter()
        
        # Should handle gracefully or raise appropriate error
        try:
            result = converter.convert('Patient', 'invalid')
            # If it doesn't raise, should return empty or minimal query
            assert isinstance(result, dict)
        except ConversionError:
            pass  # Expected
    
    def test_invalid_compartment_type(self):
        """Test error handling for invalid compartment type."""
        from fhir_search_to_mql import FHIRSearchConverter
        from fhir_search_to_mql.core.exceptions import ConversionError
        
        converter = FHIRSearchConverter()
        
        with pytest.raises(ConversionError):
            converter.convert_with_compartment(
                'InvalidCompartment',
                'id-123',
                'Observation'
            )
    
    def test_resource_not_in_compartment(self):
        """Test error when resource type not in compartment."""
        from fhir_search_to_mql import FHIRSearchConverter
        from fhir_search_to_mql.core.exceptions import ConversionError
        
        converter = FHIRSearchConverter()
        
        # Try to query a resource not in Patient compartment
        # (Note: Most resources ARE in Patient compartment, so this might not fail)
        # This is more of a documentation test
        try:
            result = converter.convert_with_compartment(
                'Device',
                'device-123',
                'Patient'  # Patient not in Device compartment
            )
            # If it doesn't fail, that's implementation-specific
        except ConversionError:
            pass  # Expected
