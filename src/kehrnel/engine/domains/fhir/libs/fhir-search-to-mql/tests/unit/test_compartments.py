"""
Tests for FHIR Compartment Support.

Tests CompartmentLoader, CompartmentResolver, and FHIRSearchConverter integration.
"""

import pytest
import json
from pathlib import Path

from fhir_search_to_mql.compartments import (
    CompartmentLoader,
    CompartmentDefinition,
    CompartmentResolver,
    CompartmentQuery)
from fhir_search_to_mql.core.exceptions import ConversionError, ConfigurationError


# ==================== COMPARTMENT LOADER TESTS ====================

class TestCompartmentLoader:
    """Test CompartmentLoader functionality."""
    
    @pytest.fixture
    def loader(self):
        """Create CompartmentLoader with default definitions."""
        return CompartmentLoader()
    
    def test_load_all_compartments(self, loader):
        """Test loading all compartment definitions."""
        compartments = loader.load_all()
        
        # Should have 5 standard compartments
        assert len(compartments) == 5
        assert 'Patient' in compartments
        assert 'Encounter' in compartments
        assert 'Practitioner' in compartments
        assert 'Device' in compartments
        assert 'RelatedPerson' in compartments
    
    def test_get_compartment(self, loader):
        """Test getting specific compartment."""
        loader.load_all()
        
        patient_comp = loader.get_compartment('Patient')
        assert patient_comp is not None
        assert patient_comp.code == 'Patient'
        assert patient_comp.status == 'active'
        assert len(patient_comp.resources) > 0
    
    def test_get_nonexistent_compartment(self, loader):
        """Test getting non-existent compartment."""
        loader.load_all()
        
        result = loader.get_compartment('NonExistent')
        assert result is None
    
    def test_get_resource_entry(self, loader):
        """Test getting resource entry from compartment."""
        loader.load_all()
        
        # Observation in Patient compartment
        entry = loader.get_resource_entry('Patient', 'Observation')
        assert entry is not None
        assert entry.code == 'Observation'
        assert 'subject' in entry.params or 'performer' in entry.params
    
    def test_is_resource_in_compartment(self, loader):
        """Test checking if resource is in compartment."""
        loader.load_all()
        
        # Observation is in Patient compartment
        assert loader.is_resource_in_compartment('Patient', 'Observation')
        
        # Organization is not in Patient compartment
        assert not loader.is_resource_in_compartment('Patient', 'Organization')
    
    def test_get_linking_parameters(self, loader):
        """Test getting linking parameters."""
        loader.load_all()
        
        # Observation in Patient compartment
        params = loader.get_linking_parameters('Patient', 'Observation')
        assert len(params) > 0
        assert 'subject' in params or 'performer' in params
    
    def test_validate_compartment_code(self, loader):
        """Test compartment code validation."""
        # Should succeed for valid code
        loader.load_all()
        patient_comp = loader.get_compartment('Patient')
        assert patient_comp.code == 'Patient'


# ==================== COMPARTMENT RESOLVER TESTS ====================

class TestCompartmentResolver:
    """Test CompartmentResolver functionality."""
    
    @pytest.fixture
    def resolver(self):
        """Create CompartmentResolver with default definitions."""
        return CompartmentResolver()
    
    @pytest.fixture
    def observation_config(self):
        """Sample Observation configuration."""
        return {
            'resource': 'Observation',
            'parameters': {
                'subject': {
                    'type': 'reference',
                    'fields': [
                        {'field': '_search.patientId'}
                    ]
                },
                'performer': {
                    'type': 'reference',
                    'fields': [
                        {'field': '_search.performerId'}
                    ]
                }
            }
        }
    
    @pytest.fixture
    def encounter_config(self):
        """Sample Encounter configuration."""
        return {
            'resource': 'Encounter',
            'parameters': {
                'patient': {
                    'type': 'reference',
                    'fields': [
                        {'field': '_search.patientId'}
                    ]
                },
                'subject': {
                    'type': 'reference',
                    'fields': [
                        {'field': '_search.subjectId'}
                    ]
                }
            }
        }
    
    def test_resolve_patient_compartment(self, resolver, observation_config):
        """Test resolving Patient compartment for Observation."""
        query = resolver.resolve(
            compartment_type='Patient',
            compartment_id='pat-123',
            resource_type='Observation',
            config=observation_config
        )
        
        # Should return OR query with both subject and performer
        assert '$or' in query or '_search.patientId' in query
        
        if '$or' in query:
            # Check that compartment ID is in query
            query_str = str(query)
            assert 'pat-123' in query_str
    
    def test_resolve_encounter_compartment(self, resolver, observation_config):
        """Test resolving Encounter compartment for Observation."""
        # Need config with encounter parameter
        config = {
            'resource': 'Observation',
            'parameters': {
                'encounter': {
                    'type': 'reference',
                    'fields': [
                        {'field': '_search.encounterId'}
                    ]
                }
            }
        }
        
        query = resolver.resolve(
            compartment_type='Encounter',
            compartment_id='enc-456',
            resource_type='Observation',
            config=config
        )
        
        # Should contain encounter ID
        query_str = str(query)
        assert 'enc-456' in query_str
    
    def test_resolve_invalid_compartment(self, resolver, observation_config):
        """Test resolving invalid compartment type."""
        with pytest.raises(ConversionError) as exc_info:
            resolver.resolve(
                compartment_type='InvalidType',
                compartment_id='123',
                resource_type='Observation',
                config=observation_config
            )
        
        assert 'Invalid compartment type' in str(exc_info.value)
    
    def test_resolve_resource_not_in_compartment(self, resolver, observation_config):
        """Test resolving resource not in compartment."""
        # Organization is not in Patient compartment
        org_config = {
            'resource': 'Organization',
            'parameters': {}
        }
        
        with pytest.raises(ConversionError) as exc_info:
            resolver.resolve(
                compartment_type='Patient',
                compartment_id='pat-123',
                resource_type='Organization',
                config=org_config
            )
        
        assert 'not in compartment' in str(exc_info.value)
    
    def test_combine_with_parameters(self, resolver):
        """Test combining compartment query with parameters."""
        compartment_query = {
            "$or": [
                {"_search.patientId": "pat-123"},
                {"_search.performerId": "pat-123"}
            ]
        }
        
        parameter_queries = [
            {"_search.codeSystem_code": "8480-6"},
            {"_search.status": "final"}
        ]
        
        combined = resolver.combine_with_parameters(
            compartment_query,
            parameter_queries
        )
        
        # Should use $and to combine
        assert "$and" in combined or all(
            key in combined for key in ["_search.codeSystem_code", "_search.status", "$or"]
        )
    
    def test_validate_compartment_query(self, resolver):
        """Test compartment query validation."""
        # Valid query
        is_valid, error = resolver.validate_compartment_query(
            'Patient',
            'pat-123',
            'Observation'
        )
        assert is_valid
        assert error is None
        
        # Invalid compartment type
        is_valid, error = resolver.validate_compartment_query(
            'InvalidType',
            'pat-123',
            'Observation'
        )
        assert not is_valid
        assert 'Invalid compartment type' in error
        
        # Missing ID
        is_valid, error = resolver.validate_compartment_query(
            'Patient',
            '',
            'Observation'
        )
        assert not is_valid
        assert 'required' in error.lower()
        
        # Resource not in compartment
        is_valid, error = resolver.validate_compartment_query(
            'Patient',
            'pat-123',
            'Organization'
        )
        assert not is_valid
        assert 'not in compartment' in error
    
    def test_get_compartment_resources(self, resolver):
        """Test getting resources in compartment."""
        resources = resolver.get_compartment_resources('Patient')
        
        assert len(resources) > 0
        assert 'Observation' in resources
        assert 'Condition' in resources
        assert 'Encounter' in resources
    
    def test_get_compartment_info(self, resolver):
        """Test getting compartment information."""
        info = resolver.get_compartment_info('Patient')
        
        assert info is not None
        assert info['code'] == 'Patient'
        assert info['name'] == 'Patient'
        assert info['resource_count'] > 0
        assert 'resources' in info
        assert 'Observation' in info['resources']


# ==================== INTEGRATION TESTS ====================

class TestCompartmentIntegration:
    """Integration tests for compartment support."""
    
    @pytest.fixture
    def sample_observation_config(self):
        """Sample Observation configuration with subject and performer."""
        return {
            'resource': 'Observation',
            'parameters': {
                'subject': {
                    'type': 'reference',
                    'fields': [
                        {'field': '_search.patientId', 'indexed': True}
                    ]
                },
                'performer': {
                    'type': 'reference',
                    'fields': [
                        {'field': '_search.performerId', 'indexed': True}
                    ]
                },
                'code': {
                    'type': 'token',
                    'fields': [
                        {'field': '_search.codeSystem_code', 'indexed': True}
                    ]
                }
            }
        }
    
    def test_full_compartment_workflow(self, sample_observation_config):
        """Test complete compartment query workflow."""
        # Load compartments
        loader = CompartmentLoader()
        loader.load_all()
        
        # Create resolver
        resolver = CompartmentResolver()
        
        # Resolve compartment query
        compartment_query = resolver.resolve(
            compartment_type='Patient',
            compartment_id='patient-123',
            resource_type='Observation',
            config=sample_observation_config
        )
        
        # Verify query structure
        assert compartment_query is not None
        query_str = str(compartment_query)
        assert 'patient-123' in query_str
        
        # Simulate additional parameter query
        parameter_query = {"_search.codeSystem_code": "8480-6"}
        
        # Combine
        final_query = resolver.combine_with_parameters(
            compartment_query,
            [parameter_query]
        )
        
        # Verify combined query
        assert final_query is not None
        final_str = str(final_query)
        assert 'patient-123' in final_str
        assert '8480-6' in final_str
    
    def test_multiple_resources_in_compartment(self):
        """Test multiple resource types in same compartment."""
        resolver = CompartmentResolver()
        
        # Get all resources in Patient compartment
        resources = resolver.get_compartment_resources('Patient')
        
        # Should include many common resources
        common_resources = [
            'Observation', 'Condition', 'Encounter', 
            'MedicationRequest', 'Procedure', 'AllergyIntolerance'
        ]
        
        for resource in common_resources:
            assert resource in resources, f"{resource} should be in Patient compartment"


# ==================== EDGE CASE TESTS ====================

class TestCompartmentEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_compartment_id(self):
        """Test handling empty compartment ID."""
        resolver = CompartmentResolver()
        
        is_valid, error = resolver.validate_compartment_query(
            'Patient', '', 'Observation'
        )
        
        assert not is_valid
        assert 'required' in error.lower()
    
    def test_missing_parameter_config(self):
        """Test handling missing parameter configuration."""
        resolver = CompartmentResolver()
        
        # Config without the required parameters
        incomplete_config = {
            'resource': 'Observation',
            'parameters': {
                'code': {
                    'type': 'token',
                    'fields': [{'field': '_search.code'}]
                }
            }
        }
        
        # Should raise error or return empty result
        try:
            query = resolver.resolve(
                'Patient',
                'pat-123',
                'Observation',
                incomplete_config
            )
            # If it succeeds, query should be empty or raise error
            assert query is not None
        except ConversionError:
            # Expected if no valid parameters found
            pass
    
    def test_all_compartment_types(self):
        """Test all 5 compartment types."""
        resolver = CompartmentResolver()
        
        compartment_types = [
            'Patient', 'Encounter', 'Practitioner', 'Device', 'RelatedPerson'
        ]
        
        for comp_type in compartment_types:
            info = resolver.get_compartment_info(comp_type)
            assert info is not None, f"Compartment {comp_type} should be loaded"
            assert info['code'] == comp_type
            assert len(info['resources']) > 0


# ==================== HYBRID PRECOMPUTE FAST-PATH ====================

class TestCompartmentMembershipExtractor:
    """
    Unit tests for the generic CompartmentMembershipExtractor — the
    denormalization side of the hybrid approach. Verifies path walking,
    reference-type filtering, include_self, and dedup behavior.
    """

    def _extractor(self):
        from fhir_search_to_mql.denormalizer.extractors import (
            CompartmentMembershipExtractor)
        return CompartmentMembershipExtractor()

    def test_collects_patient_ids_from_subject_and_performer(self):
        observation = {
            "resourceType": "Observation",
            "id": "obs-1",
            "subject": {"reference": "Patient/pat-1"},
            "performer": [
                {"reference": "Patient/pat-2"},
                {"reference": "Practitioner/dr-9"},
            ],
        }
        out = self._extractor().extract(
            observation,
            field_mappings=[
                {
                    "target_field": "Patient",
                    "source_paths": ["subject", "performer"],
                    "reference_type": "Patient",
                    "datatype": "array[string]",
                }
            ])
        assert out == {"Patient": ["pat-1", "pat-2"]}

    def test_filters_out_non_matching_reference_types(self):
        observation = {
            "resourceType": "Observation",
            "id": "obs-1",
            "subject": {"reference": "Group/g-1"},  # not Patient → excluded
            "performer": [{"reference": "Practitioner/dr-9"}],
        }
        out = self._extractor().extract(
            observation,
            field_mappings=[
                {
                    "target_field": "Patient",
                    "source_paths": ["subject", "performer"],
                    "reference_type": "Patient",
                }
            ])
        # No Patient-typed references → key suppressed.
        assert out == {}

    def test_include_self_adds_resource_own_id(self):
        patient = {"resourceType": "Patient", "id": "pat-1"}
        out = self._extractor().extract(
            patient,
            field_mappings=[
                {
                    "target_field": "Patient",
                    "source_paths": ["link[*].other"],
                    "reference_type": "Patient",
                    "include_self": True,
                }
            ])
        assert out == {"Patient": ["pat-1"]}

    def test_include_self_respects_resource_type(self):
        # A non-Patient resource with include_self should NOT be added
        # to the Patient compartment via the [base] rule.
        observation = {"resourceType": "Observation", "id": "obs-1"}
        out = self._extractor().extract(
            observation,
            field_mappings=[
                {
                    "target_field": "Patient",
                    "source_paths": [],
                    "reference_type": "Patient",
                    "include_self": True,
                }
            ])
        assert out == {}

    def test_dedupes_repeated_ids_preserving_first_seen_order(self):
        observation = {
            "resourceType": "Observation",
            "subject": {"reference": "Patient/pat-1"},
            "performer": [{"reference": "Patient/pat-1"}],
        }
        out = self._extractor().extract(
            observation,
            field_mappings=[
                {
                    "target_field": "Patient",
                    "source_paths": ["subject", "performer"],
                    "reference_type": "Patient",
                }
            ])
        assert out == {"Patient": ["pat-1"]}

    def test_walks_nested_path_to_actor(self):
        appointment = {
            "resourceType": "Appointment",
            "id": "appt-1",
            "participant": [
                {"actor": {"reference": "Patient/pat-1"}, "status": "accepted"},
                {"actor": {"reference": "Practitioner/dr-1"}, "status": "accepted"},
            ],
        }
        out = self._extractor().extract(
            appointment,
            field_mappings=[
                {
                    "target_field": "Patient",
                    "source_paths": ["participant[*].actor"],
                    "reference_type": "Patient",
                }
            ])
        assert out == {"Patient": ["pat-1"]}

    def test_handles_missing_paths_gracefully(self):
        out = self._extractor().extract(
            {"resourceType": "Observation"},
            field_mappings=[
                {
                    "target_field": "Patient",
                    "source_paths": ["subject", "performer"],
                    "reference_type": "Patient",
                }
            ])
        assert out == {}

    def test_returns_empty_for_non_dict_input(self):
        assert self._extractor().extract(None) == {}
        assert self._extractor().extract([]) == {}


class TestCompartmentResolverPrecomputedFastPath:
    """
    Unit tests for the CompartmentResolver fast-path: when a resource
    config opts into ``compartments.precomputed: [Patient]``, the resolver
    must emit the single-field equality query against
    ``_compartments.Patient`` rather than the dynamic ``$or``.
    """

    @pytest.fixture
    def resolver(self):
        return CompartmentResolver()

    @pytest.fixture
    def precomputed_observation_config(self):
        # Same as the dynamic-test config, plus the opt-in block. The
        # `parameters` section is intentionally kept so we can assert the
        # resolver did NOT walk it.
        return {
            "resource": "Observation",
            "compartments": {"precomputed": ["Patient"]},
            "parameters": {
                "subject": {
                    "type": "reference",
                    "fields": [{"field": "_search.patientId"}],
                },
                "performer": {
                    "type": "reference",
                    "fields": [{"field": "_search.performerId"}],
                },
            },
        }

    def test_patient_compartment_uses_precomputed_field(
        self, resolver, precomputed_observation_config
    ):
        query = resolver.resolve(
            compartment_type="Patient",
            compartment_id="pat-123",
            resource_type="Observation",
            config=precomputed_observation_config)
        assert query == {"_compartments.Patient": "pat-123"}

    def test_other_compartments_still_use_dynamic_path(
        self, resolver, precomputed_observation_config
    ):
        # `precomputed: [Patient]` must not affect the Encounter path.
        # Add an `encounter` parameter so the dynamic path can succeed.
        cfg = dict(precomputed_observation_config)
        cfg["parameters"] = dict(cfg["parameters"])
        cfg["parameters"]["encounter"] = {
            "type": "reference",
            "fields": [{"field": "_search.encounterId"}],
        }
        query = resolver.resolve(
            compartment_type="Encounter",
            compartment_id="enc-9",
            resource_type="Observation",
            config=cfg)
        # Dynamic path: emits the parameter-based query against
        # `_search.encounterId` — NOT against `_compartments.Encounter`.
        query_str = str(query)
        assert "_compartments.Encounter" not in query_str
        assert "encounterId" in query_str
        assert "enc-9" in query_str

    def test_no_opt_in_falls_through_to_dynamic(self, resolver):
        # Identical to the existing dynamic test, but without the
        # `compartments` block — confirms the fast-path is opt-in.
        cfg = {
            "resource": "Observation",
            "parameters": {
                "subject": {
                    "type": "reference",
                    "fields": [{"field": "_search.patientId"}],
                },
            },
        }
        query = resolver.resolve(
            compartment_type="Patient",
            compartment_id="pat-1",
            resource_type="Observation",
            config=cfg)
        assert "_compartments.Patient" not in str(query)
        assert "_search.patientId" in str(query)


class TestPrecomputedCompartmentEndToEnd:
    """
    End-to-end: denormalize a real resource through the YAML config,
    confirm `_compartments.Patient` is populated, then route a compartment
    query through FHIRSearchConverter and confirm the fast-path query
    matches the denormalized document.
    """

    @pytest.fixture
    def converter(self):
        from fhir_search_to_mql.fhir_search_converter import FHIRSearchConverter
        return FHIRSearchConverter()

    @pytest.fixture
    def denormalizer(self):
        from fhir_search_to_mql.denormalizer.resource_denormalizer import (
            ResourceDenormalizer)
        return ResourceDenormalizer()

    def test_observation_compartment_membership_populated(self, denormalizer):
        observation = {
            "resourceType": "Observation",
            "id": "obs-1",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "1234-5"}]},
            "subject": {"reference": "Patient/pat-1"},
            "performer": [
                {"reference": "Patient/pat-2"},
                {"reference": "Practitioner/dr-9"},
            ],
        }
        out = denormalizer.denormalize(observation)
        assert out["_compartments"]["Patient"] == ["pat-1", "pat-2"]

    def test_patient_compartment_membership_populated(self, denormalizer):
        patient = {
            "resourceType": "Patient",
            "id": "pat-1",
            "link": [{"other": {"reference": "Patient/pat-2"}, "type": "seealso"}],
        }
        out = denormalizer.denormalize(patient)
        members = out["_compartments"]["Patient"]
        assert members[0] == "pat-1"  # include_self
        assert "pat-2" in members      # link target

    def test_appointment_compartment_membership_populated(self, denormalizer):
        appointment = {
            "resourceType": "Appointment",
            "id": "appt-1",
            "status": "booked",
            "participant": [
                {
                    "actor": {"reference": "Patient/pat-1"},
                    "status": "accepted",
                },
                {
                    "actor": {"reference": "Practitioner/dr-1"},
                    "status": "accepted",
                },
            ],
        }
        out = denormalizer.denormalize(appointment)
        assert out["_compartments"]["Patient"] == ["pat-1"]

    def test_search_fast_path_matches_denormalized_doc(self, converter, denormalizer):
        observation = {
            "resourceType": "Observation",
            "id": "obs-1",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "8480-6"}]},
            "subject": {"reference": "Patient/pat-1"},
        }
        denorm = denormalizer.denormalize(observation)

        query = converter.convert_with_compartment(
            "Patient", "pat-1", "Observation"
        )
        # Fast-path: the query must collapse to a single indexed lookup.
        assert query == {"_compartments.Patient": "pat-1"}

        # And that lookup must hit the denormalized doc.
        members = denorm["_compartments"]["Patient"]
        assert "pat-1" in members

    def test_non_patient_subject_is_excluded(self, denormalizer):
        observation = {
            "resourceType": "Observation",
            "id": "obs-x",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "1234-5"}]},
            "subject": {"reference": "Group/g-1"},
            "performer": [{"reference": "Practitioner/dr-9"}],
        }
        out = denormalizer.denormalize(observation)
        # No Patient-typed refs → Patient bucket empty / absent.
        # The Practitioner performer feeds the precomputed
        # `_compartments.Practitioner` bucket (Hybrid Approach 3),
        # so `_compartments` itself is present.
        comp = out.get("_compartments", {})
        assert not comp.get("Patient")
        assert comp.get("Practitioner") == ["dr-9"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
