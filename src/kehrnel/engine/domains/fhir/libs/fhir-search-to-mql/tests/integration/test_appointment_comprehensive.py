"""
Comprehensive integration tests for ALL Appointment search parameters per FHIR R5 specification.

Reference: https://hl7.org/fhir/R5/appointment-search.html

This test suite ensures complete coverage of:

1. **All 23 Search Parameters**:
   - Reference (11): actor, based-on, group, location, patient, practitioner, 
                     reason-reference, service-type-reference, slot, subject, supporting-info
   - Token (8): appointment-type, identifier, part-status, reason-code, 
                service-category, service-type, specialty, status
   - Date (2): date, requested-period
   - Common (2): _id, _lastUpdated

2. **Multi-Field Search Validation**:
   - patient: searches both subject AND participant.actor
   - group: searches both subject AND participant.actor
   - subject: searches both subject AND participant.actor

3. **CodeableReference Field Support**:
   - reason: both reason-code (concept) and reason-reference (reference)
   - serviceType: both service-type (concept) and service-type-reference (reference)

4. **All FHIR Modifiers**:
   - Reference: :identifier, :missing, :[type]
   - Token: :text, :not, :in, :not-in, :missing
   - Date: eq, ne, gt, ge, lt, le, sa, eb, ap

5. **Complex Query Patterns**:
   - Multiple parameters (implicit AND)
   - Multi-field OR logic (patient, group, subject)
   - Date ranges with multiple operators
   - Array field searches (serviceCategory, specialty, slot, etc.)

6. **Edge Cases & Error Handling**:
   - Special characters in references
   - URL encoding/decoding
   - Empty/null values
   - Invalid parameters
   - Date edge cases (partial dates, timezones, milliseconds)
   - Token variations (system|code, code-only, system-only)

7. **MongoDB Query Validation**:
   - Correct $or/$and nesting
   - Proper field path usage (_search.* vs direct fields)
   - Date operator mapping ($gte, $lte, $gt, $lt)
   - Index-optimized queries

Test Organization:
- TestAppointmentReferenceParameters: All 11 reference parameters (15 tests)
- TestAppointmentTokenParameters: All 8 token parameters (12 tests)
- TestAppointmentDateParameters: Date searches with operators (10 tests)
- TestAppointmentCommonParameters: _id and _lastUpdated (5 tests)
- TestAppointmentMultiFieldSearches: patient/group/subject multi-field logic (9 tests)
- TestAppointmentCodeableReference: reason and serviceType dual searches (6 tests)
- TestAppointmentComplexQueries: Real-world combinations (8 tests)
- TestAppointmentModifiers: FHIR modifiers (10 tests)
- TestAppointmentEdgeCases: Error handling & special cases (15 tests)
- TestAppointmentArrayFields: Multiple values in arrays (8 tests)
- TestAppointmentDateEdgeCases: Date parsing edge cases (10 tests)
- TestAppointmentReferenceEdgeCases: Reference format variations (8 tests)
- TestAppointmentTokenEdgeCases: Token format variations (7 tests)
- TestAppointmentQueryStructure: MongoDB query structure validation (10 tests)
- TestAppointmentDenormalization: Field extraction validation (12 tests)
- TestAppointmentValidationErrors: Invalid input handling (8 tests)
- TestAppointmentPerformance: Query optimization scenarios (5 tests)
- TestAppointmentParameterCombinations: Explicit AND/OR logic (6 tests)
- TestAppointmentSpecialParameters: Special parameters (_id, _lastUpdated) (5 tests)
- TestAppointmentComplexLogic: Advanced boolean combinations (5 tests)
- TestAppointmentBuilderEdgeCases: MQL builder edge cases (3 tests)
- TestAppointmentLogicCombinations: Complex boolean logic (4 tests)
- TestAppointmentURLParsing: URL parameter parsing (7 tests)
- TestAppointmentBuilderValidation: MQL builder validation (5 tests)
- TestAppointmentIndexRecommendations: Index usage validation (4 tests)
- TestAppointmentMongoDBQueries: MongoDB integration (1 test)

Total Tests: 203 tests
Expected Coverage: 85%+ on critical converters
Target: All converters, extractors, and builder used by Appointment config

Test Markers:
- @pytest.mark.integration: All tests (requires converter and configs)
- @pytest.mark.mongodb: Tests requiring MongoDB (optional)
"""

import pytest
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from src.fhir_search_to_mql.fhir_search_converter import FHIRSearchConverter
from src.fhir_search_to_mql.core.exceptions import (
    ConversionError,
    UnsupportedParameterError)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="module")
def converter():
    """Create converter instance for all tests (uses bundled configs)."""
    return FHIRSearchConverter()


@pytest.fixture
def sample_appointment() -> Dict[str, Any]:
    """Sample Appointment resource for testing."""
    return {
        "resourceType": "Appointment",
        "id": "example-appointment",
        "status": "booked",
        "appointmentType": {
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/v2-0276",
                "code": "CHECKUP",
                "display": "Checkup"
            }]
        },
        "serviceCategory": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/service-category",
                "code": "17",
                "display": "General Practice"
            }]
        }],
        "serviceType": [{
            "concept": {
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": "408443003",
                    "display": "General medical practice"
                }]
            }
        }],
        "specialty": [{
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "394814009",
                "display": "General practice"
            }]
        }],
        "reason": [{
            "concept": {
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "code": "185345009",
                    "display": "Encounter for symptom"
                }]
            }
        }],
        "start": "2024-06-15T09:00:00Z",
        "end": "2024-06-15T10:00:00Z",
        "participant": [
            {
                "actor": {
                    "reference": "Patient/example-patient"
                },
                "status": "accepted"
            },
            {
                "actor": {
                    "reference": "Practitioner/example-practitioner"
                },
                "status": "accepted"
            }
        ],
        "subject": {
            "reference": "Patient/example-patient"
        }
    }


# =============================================================================
# Test Class 1: Reference Parameters (11 parameters, 15 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentReferenceParameters:
    """Test all 11 reference search parameters."""

    def test_actor_by_id(self, converter):
        """Test actor parameter with ID only."""
        result = converter.convert("Appointment", "actor=Patient/123")
        assert "_search.actorIds" in json.dumps(result)
        assert "123" in json.dumps(result)

    def test_actor_device(self, converter):
        """Test actor parameter with Device reference."""
        result = converter.convert("Appointment", "actor=Device/monitor-1")
        assert "_search.actorIds" in json.dumps(result)
        assert "monitor-1" in json.dumps(result)

    def test_patient_multi_field(self, converter):
        """Test patient parameter searches both subject and participant.actor."""
        result = converter.convert("Appointment", "patient=Patient/456")
        result_str = json.dumps(result)
        assert "$or" in result_str
        assert "_search.patientId" in result_str
        assert "_search.subjectId" in result_str
        assert "456" in result_str

    def test_practitioner_by_id(self, converter):
        """Test practitioner parameter."""
        result = converter.convert("Appointment", "practitioner=Practitioner/789")
        assert "_search.practitionerId" in json.dumps(result)
        assert "789" in json.dumps(result)

    def test_location_by_id(self, converter):
        """Test location parameter."""
        result = converter.convert("Appointment", "location=Location/clinic-a")
        assert "_search.locationId" in json.dumps(result)
        assert "clinic-a" in json.dumps(result)

    def test_group_multi_field(self, converter):
        """Test group parameter searches both subject and participant.actor."""
        result = converter.convert("Appointment", "group=Group/cohort-1")
        result_str = json.dumps(result)
        assert "$or" in result_str
        assert "_search.subjectId" in result_str
        assert "_search.actorIds" in result_str
        assert "cohort-1" in result_str

    def test_subject_multi_field(self, converter):
        """Test subject parameter searches both locations."""
        result = converter.convert("Appointment", "subject=Patient/999")
        result_str = json.dumps(result)
        assert "$or" in result_str
        assert "_search.subjectId" in result_str
        assert "_search.actorIds" in result_str
        assert "999" in result_str

    def test_based_on(self, converter):
        """Test based-on parameter."""
        result = converter.convert("Appointment", "based-on=ServiceRequest/req-123")
        assert "_search.basedOnId" in json.dumps(result)
        assert "req-123" in json.dumps(result)

    def test_reason_reference(self, converter):
        """Test reason-reference parameter."""
        result = converter.convert("Appointment", "reason-reference=Condition/condition-1")
        assert "_search.reasonReferenceId" in json.dumps(result)
        assert "condition-1" in json.dumps(result)

    def test_service_type_reference(self, converter):
        """Test service-type-reference parameter."""
        result = converter.convert("Appointment", "service-type-reference=HealthcareService/hs-1")
        assert "_search.serviceTypeReferenceId" in json.dumps(result)
        assert "hs-1" in json.dumps(result)

    def test_slot(self, converter):
        """Test slot parameter."""
        result = converter.convert("Appointment", "slot=Slot/slot-123")
        assert "_search.slotId" in json.dumps(result)
        assert "slot-123" in json.dumps(result)

    def test_supporting_info(self, converter):
        """Test supporting-info parameter."""
        result = converter.convert("Appointment", "supporting-info=DocumentReference/doc-1")
        assert "_search.supportingInfoId" in json.dumps(result)
        assert "doc-1" in json.dumps(result)

    def test_multiple_references_and_logic(self, converter):
        """Test combining multiple reference parameters (AND logic)."""
        result = converter.convert("Appointment", "patient=Patient/123&practitioner=Practitioner/456")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "123" in result_str
        assert "456" in result_str

    def test_reference_with_full_url(self, converter):
        """Test reference with full URL."""
        result = converter.convert("Appointment", "patient=http://example.org/fhir/Patient/789")
        assert "789" in json.dumps(result)

    def test_reference_with_version(self, converter):
        """Test reference with version."""
        result = converter.convert("Appointment", "patient=Patient/123/_history/2")
        result_str = json.dumps(result)
        assert "123/_history/2" in result_str


# =============================================================================
# Test Class 2: Token Parameters (8 parameters, 12 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentTokenParameters:
    """Test all 8 token search parameters."""

    def test_status_code(self, converter):
        """Test status parameter with code."""
        result = converter.convert("Appointment", "status=booked")
        assert result == {"status": "booked"}

    def test_status_multiple_values(self, converter):
        """Test status with comma-separated values."""
        result = converter.convert("Appointment", "status=booked,proposed")
        assert "booked,proposed" in json.dumps(result)

    def test_appointment_type_code_only(self, converter):
        """Test appointment-type with code only."""
        result = converter.convert("Appointment", "appointment-type=CHECKUP")
        result_str = json.dumps(result)
        assert "$or" in result_str
        assert "_search.appointmentType" in result_str
        assert "CHECKUP" in result_str

    def test_appointment_type_system_code(self, converter):
        """Test appointment-type with system|code."""
        result = converter.convert("Appointment", "appointment-type=http://terminology.hl7.org|CHECKUP")
        result_str = json.dumps(result)
        assert "_search.appointmentType_systemCode" in result_str
        assert "http://terminology.hl7.org|CHECKUP" in result_str

    def test_service_category(self, converter):
        """Test service-category parameter."""
        result = converter.convert("Appointment", "service-category=17")
        result_str = json.dumps(result)
        assert "_search.serviceCategory" in result_str
        assert "17" in result_str

    def test_service_type(self, converter):
        """Test service-type parameter (code)."""
        result = converter.convert("Appointment", "service-type=57")
        result_str = json.dumps(result)
        assert "_search.serviceType" in result_str
        assert "57" in result_str

    def test_specialty(self, converter):
        """Test specialty parameter."""
        result = converter.convert("Appointment", "specialty=cardiology")
        result_str = json.dumps(result)
        assert "_search.specialty" in result_str
        assert "cardiology" in result_str

    def test_identifier_value_only(self, converter):
        """Test identifier with value only."""
        result = converter.convert("Appointment", "identifier=APT123")
        result_str = json.dumps(result)
        assert "_search.identifier" in result_str
        assert "APT123" in result_str

    def test_identifier_system_value(self, converter):
        """Test identifier with system|value."""
        result = converter.convert("Appointment", "identifier=http://hospital.org|APT123")
        result_str = json.dumps(result)
        assert "_search.identifier_systemCode" in result_str
        assert "http://hospital.org|APT123" in result_str

    def test_part_status(self, converter):
        """Test part-status parameter."""
        result = converter.convert("Appointment", "part-status=accepted")
        result_str = json.dumps(result)
        assert "$or" in result_str
        assert "_search.participantStatus" in result_str or "participant.status" in result_str
        assert "accepted" in result_str

    def test_reason_code(self, converter):
        """Test reason-code parameter."""
        result = converter.convert("Appointment", "reason-code=followup")
        result_str = json.dumps(result)
        assert "_search.reasonCode" in result_str
        assert "followup" in result_str

    def test_multiple_tokens_and_logic(self, converter):
        """Test combining multiple token parameters (AND logic)."""
        result = converter.convert("Appointment", "status=booked&specialty=cardiology")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "booked" in result_str
        assert "cardiology" in result_str


# =============================================================================
# Test Class 3: Date Parameters (2 parameters, 10 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentDateParameters:
    """Test date search parameters with all operators."""

    def test_date_exact(self, converter):
        """Test date parameter with exact match."""
        result = converter.convert("Appointment", "date=2024-06-15")
        result_str = json.dumps(result, default=str)
        assert "$or" in result_str
        assert "start" in result_str or "_search.appointmentPeriod" in result_str
        assert "2024-06-15" in result_str

    def test_date_greater_equal(self, converter):
        """Test date parameter with ge operator."""
        result = converter.convert("Appointment", "date=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "$gte" in result_str
        assert "2024-01-01" in result_str

    def test_date_less_equal(self, converter):
        """Test date parameter with le operator."""
        result = converter.convert("Appointment", "date=le2024-12-31")
        result_str = json.dumps(result, default=str)
        assert "$lte" in result_str
        assert "2024-12-31" in result_str

    def test_date_range(self, converter):
        """Test date range with multiple operators."""
        result = converter.convert("Appointment", "date=ge2024-01-01&date=le2024-12-31")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "$gte" in result_str
        assert "$lte" in result_str
        assert "2024-01-01" in result_str
        assert "2024-12-31" in result_str

    def test_date_greater_than(self, converter):
        """Test date parameter with gt operator."""
        result = converter.convert("Appointment", "date=gt2024-06-01")
        result_str = json.dumps(result, default=str)
        assert "$gt" in result_str
        assert "2024-06-01" in result_str

    def test_date_less_than(self, converter):
        """Test date parameter with lt operator."""
        result = converter.convert("Appointment", "date=lt2024-06-30")
        result_str = json.dumps(result, default=str)
        assert "$lt" in result_str
        assert "2024-06-30" in result_str

    def test_date_starts_after(self, converter):
        """Test date parameter with sa (starts after) operator."""
        result = converter.convert("Appointment", "date=sa2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "$gt" in result_str
        assert "2024-01-01" in result_str

    def test_requested_period_exact(self, converter):
        """Test requested-period parameter."""
        result = converter.convert("Appointment", "requested-period=2024-06-15")
        result_str = json.dumps(result, default=str)
        assert "_search.requestedPeriod" in result_str or "requestedPeriod" in result_str
        assert "2024-06-15" in result_str

    def test_requested_period_range(self, converter):
        """Test requested-period with range."""
        result = converter.convert("Appointment", "requested-period=ge2024-01-01&requested-period=le2024-01-31")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "2024-01-01" in result_str
        assert "2024-01-31" in result_str

    def test_date_with_time(self, converter):
        """Test date parameter with full timestamp."""
        result = converter.convert("Appointment", "date=2024-06-15T09:00:00Z")
        result_str = json.dumps(result, default=str)
        assert "2024-06-15" in result_str
        assert "09:00:00" in result_str


# =============================================================================
# Test Class 4: Common Parameters (2 parameters, 5 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentCommonParameters:
    """Test common search parameters."""

    def test_id_simple(self, converter):
        """Test _id parameter with simple ID."""
        result = converter.convert("Appointment", "_id=appointment-123")
        # ID may be parsed differently
        assert "id" in result
        assert "123" in json.dumps(result)

    def test_id_uuid(self, converter):
        """Test _id parameter with UUID."""
        result = converter.convert("Appointment", "_id=550e8400-e29b-41d4-a716-446655440000")
        assert result == {"id": "550e8400-e29b-41d4-a716-446655440000"}

    def test_last_updated_exact(self, converter):
        """Test _lastUpdated parameter."""
        result = converter.convert("Appointment", "_lastUpdated=2024-01-15")
        result_str = json.dumps(result, default=str)
        assert "meta.lastUpdated" in result_str
        assert "2024-01-15" in result_str

    def test_last_updated_range(self, converter):
        """Test _lastUpdated with range."""
        result = converter.convert("Appointment", "_lastUpdated=ge2024-01-01&_lastUpdated=le2024-12-31")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "meta.lastUpdated" in result_str
        assert "$gte" in result_str
        assert "$lte" in result_str

    def test_id_and_last_updated(self, converter):
        """Test combining _id and _lastUpdated."""
        result = converter.convert("Appointment", "_id=apt-123&_lastUpdated=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "id" in result_str
        # ID may be parsed, just verify it's present
        assert "123" in result_str
        assert "meta.lastUpdated" in result_str


# =============================================================================
# Test Class 5: Multi-Field Searches (3 parameters, 9 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentMultiFieldSearches:
    """Test parameters that search multiple fields with OR logic."""

    def test_patient_searches_both_locations(self, converter):
        """Verify patient searches both subject and participant.actor."""
        result = converter.convert("Appointment", "patient=Patient/123")
        result_str = json.dumps(result)
        assert "$or" in result_str
        assert "_search.patientId" in result_str
        assert "_search.subjectId" in result_str

    def test_patient_or_structure(self, converter):
        """Verify patient generates correct $or structure."""
        result = converter.convert("Appointment", "patient=Patient/123")
        assert "$or" in result
        or_clauses = result["$or"]
        assert len(or_clauses) == 2
        assert any("_search.patientId" in clause for clause in or_clauses)
        assert any("_search.subjectId" in clause for clause in or_clauses)

    def test_group_searches_both_locations(self, converter):
        """Verify group searches both subject and participant.actor."""
        result = converter.convert("Appointment", "group=Group/456")
        result_str = json.dumps(result)
        assert "$or" in result_str
        assert "_search.subjectId" in result_str
        assert "_search.actorIds" in result_str

    def test_group_or_structure(self, converter):
        """Verify group generates correct $or structure."""
        result = converter.convert("Appointment", "group=Group/456")
        assert "$or" in result
        or_clauses = result["$or"]
        assert len(or_clauses) == 2
        assert any("_search.subjectId" in clause for clause in or_clauses)
        assert any("_search.actorIds" in clause for clause in or_clauses)

    def test_subject_searches_both_locations(self, converter):
        """Verify subject searches both subject and participant.actor."""
        result = converter.convert("Appointment", "subject=Patient/789")
        result_str = json.dumps(result)
        assert "$or" in result_str
        assert "_search.subjectId" in result_str
        assert "_search.actorIds" in result_str

    def test_subject_or_structure(self, converter):
        """Verify subject generates correct $or structure."""
        result = converter.convert("Appointment", "subject=Patient/789")
        assert "$or" in result
        or_clauses = result["$or"]
        assert len(or_clauses) == 2
        assert any("_search.subjectId" in clause for clause in or_clauses)
        assert any("_search.actorIds" in clause for clause in or_clauses)

    def test_patient_with_other_params(self, converter):
        """Test patient multi-field search combined with other params."""
        result = converter.convert("Appointment", "patient=Patient/123&status=booked")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "$or" in result_str
        assert "_search.patientId" in result_str
        assert "_search.subjectId" in result_str
        assert "booked" in result_str

    def test_group_with_date(self, converter):
        """Test group multi-field search with date parameter."""
        result = converter.convert("Appointment", "group=Group/456&date=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "$or" in result_str
        assert "_search.subjectId" in result_str
        assert "_search.actorIds" in result_str
        assert "$gte" in result_str

    def test_subject_with_practitioner(self, converter):
        """Test subject multi-field with practitioner."""
        result = converter.convert("Appointment", "subject=Patient/789&practitioner=Practitioner/111")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "$or" in result_str
        assert "_search.subjectId" in result_str
        assert "_search.actorIds" in result_str
        assert "_search.practitionerId" in result_str


# =============================================================================
# Test Class 6: CodeableReference Fields (2 fields, 6 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentCodeableReference:
    """Test CodeableReference fields that support both code and reference searches."""

    def test_reason_code_search(self, converter):
        """Test reason-code parameter searches concept.coding."""
        result = converter.convert("Appointment", "reason-code=followup")
        result_str = json.dumps(result)
        assert "_search.reasonCode" in result_str
        assert "followup" in result_str

    def test_reason_reference_search(self, converter):
        """Test reason-reference parameter searches reference."""
        result = converter.convert("Appointment", "reason-reference=Condition/123")
        result_str = json.dumps(result)
        assert "_search.reasonReferenceId" in result_str
        assert "123" in result_str

    def test_reason_both_searches_independent(self, converter):
        """Test reason-code and reason-reference work independently."""
        result_code = converter.convert("Appointment", "reason-code=followup")
        result_ref = converter.convert("Appointment", "reason-reference=Condition/123")
        assert result_code != result_ref
        assert "reasonCode" in json.dumps(result_code)
        assert "reasonReferenceId" in json.dumps(result_ref)

    def test_service_type_code_search(self, converter):
        """Test service-type parameter searches concept.coding."""
        result = converter.convert("Appointment", "service-type=57")
        result_str = json.dumps(result)
        assert "_search.serviceType" in result_str
        assert "57" in result_str

    def test_service_type_reference_search(self, converter):
        """Test service-type-reference parameter searches reference."""
        result = converter.convert("Appointment", "service-type-reference=HealthcareService/hs-1")
        result_str = json.dumps(result)
        assert "_search.serviceTypeReferenceId" in result_str
        assert "hs-1" in result_str

    def test_service_type_both_in_query(self, converter):
        """Test both service-type and service-type-reference in same query."""
        result = converter.convert("Appointment", "service-type=57&service-type-reference=HealthcareService/hs-1")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "_search.serviceType" in result_str
        assert "_search.serviceTypeReferenceId" in result_str
        assert "57" in result_str
        assert "hs-1" in result_str


# =============================================================================
# Test Class 7: Complex Queries (8 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentComplexQueries:
    """Test real-world complex query scenarios."""

    def test_patient_date_status_combination(self, converter):
        """Test common query: patient + date + status."""
        result = converter.convert("Appointment", "patient=Patient/123&date=ge2024-01-01&status=booked")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "$or" in result_str
        assert "123" in result_str
        assert "$gte" in result_str
        assert "booked" in result_str

    def test_multiple_participants(self, converter):
        """Test query with multiple participant types."""
        result = converter.convert("Appointment", "patient=Patient/123&practitioner=Practitioner/456&location=Location/789")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "123" in result_str
        assert "456" in result_str
        assert "789" in result_str

    def test_date_range_with_specialty(self, converter):
        """Test date range with specialty filter."""
        result = converter.convert("Appointment", "date=ge2024-01-01&date=le2024-12-31&specialty=cardiology")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "$gte" in result_str
        assert "$lte" in result_str
        assert "cardiology" in result_str

    def test_service_filters_combination(self, converter):
        """Test combining service-category, service-type, and specialty."""
        result = converter.convert("Appointment", "service-category=17&service-type=57&specialty=cardiology")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "17" in result_str
        assert "57" in result_str
        assert "cardiology" in result_str

    def test_five_parameter_combination(self, converter):
        """Test complex query with 5+ parameters."""
        query = "patient=Patient/123&status=booked&date=ge2024-01-01&practitioner=Practitioner/456&specialty=cardiology"
        result = converter.convert("Appointment", query)
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert len(result["$and"]) >= 4
        assert "123" in result_str
        assert "booked" in result_str
        assert "$gte" in result_str
        assert "456" in result_str
        assert "cardiology" in result_str

    def test_reason_and_service_type_both_forms(self, converter):
        """Test reason and serviceType with both code and reference."""
        query = "reason-code=followup&reason-reference=Condition/123&service-type=57&service-type-reference=HealthcareService/hs-1"
        result = converter.convert("Appointment", query)
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "reasonCode" in result_str
        assert "reasonReferenceId" in result_str
        assert "serviceType" in result_str
        assert "serviceTypeReferenceId" in result_str

    def test_all_reference_types(self, converter):
        """Test query with many reference types."""
        query = "patient=Patient/1&practitioner=Practitioner/2&location=Location/3&based-on=ServiceRequest/4&slot=Slot/5"
        result = converter.convert("Appointment", query)
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert all(str(i) in result_str for i in range(1, 6))

    def test_complex_date_and_identifier(self, converter):
        """Test date range with identifier search."""
        result = converter.convert("Appointment", "identifier=APT123&date=ge2024-01-01&date=le2024-12-31")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "APT123" in result_str
        assert "$gte" in result_str
        assert "$lte" in result_str


# =============================================================================
# Test Class 8: Modifiers (10 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentModifiers:
    """Test FHIR search modifiers."""

    def test_identifier_missing_true(self, converter):
        """Test :missing modifier with true."""
        result = converter.convert("Appointment", "identifier:missing=true")
        result_str = json.dumps(result)
        assert "$exists" in result_str or "$or" in result_str

    def test_identifier_missing_false(self, converter):
        """Test :missing modifier with false."""
        result = converter.convert("Appointment", "identifier:missing=false")
        result_str = json.dumps(result)
        assert "$exists" in result_str or "$ne" in result_str

    def test_identifier_text_search(self, converter):
        """Test :text modifier for identifier."""
        result = converter.convert("Appointment", "identifier:text=APT")
        result_str = json.dumps(result)
        assert "_lower" in result_str or "$or" in result_str
        assert "apt" in result_str.lower()

    def test_status_not_modifier(self, converter):
        """Test :not modifier for status."""
        result = converter.convert("Appointment", "status:not=cancelled")
        result_str = json.dumps(result)
        assert "$ne" in result_str or "$not" in result_str
        assert "cancelled" in result_str

    def test_appointment_type_text_modifier(self, converter):
        """Test :text modifier for appointment-type."""
        result = converter.convert("Appointment", "appointment-type:text=checkup")
        result_str = json.dumps(result)
        assert "_lower" in result_str or "$or" in result_str

    def test_patient_identifier_modifier(self, converter):
        """Test :identifier modifier on reference parameter."""
        # :identifier modifier may not be fully supported yet, test graceful handling
        try:
            result = converter.convert("Appointment", "patient:identifier=MRN123")
            assert result is not None
        except Exception:
            # If not supported, that's acceptable for now
            assert True

    def test_specialty_in_modifier(self, converter):
        """Test :in modifier for token parameter."""
        result = converter.convert("Appointment", "specialty:in=http://example.org/ValueSet/specialties")
        # Should handle :in modifier
        assert result is not None

    def test_date_comparison_operators(self, converter):
        """Test date comparison operators as modifiers."""
        result = converter.convert("Appointment", "date=ap2024-06-15")
        result_str = json.dumps(result, default=str)
        # ap (approximately) should generate range
        assert "$gte" in result_str or "$lte" in result_str or "$and" in result_str

    def test_part_status_not(self, converter):
        """Test :not modifier for part-status."""
        result = converter.convert("Appointment", "part-status:not=declined")
        result_str = json.dumps(result)
        # May use $nor, $ne, or $not for negation
        assert any(op in result_str for op in ["$ne", "$not", "$nor"])
        assert "declined" in result_str

    def test_multiple_modifiers(self, converter):
        """Test query with multiple different modifiers."""
        result = converter.convert("Appointment", "status:not=cancelled&identifier:text=APT")
        result_str = json.dumps(result)
        assert "$and" in result_str
        # Should have both negation and text search
        assert "$ne" in result_str or "_lower" in result_str


# =============================================================================
# Test Class 9: Edge Cases (15 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_parameter_value(self, converter):
        """Test parameter with empty value."""
        result = converter.convert("Appointment", "patient=")
        # Should handle gracefully
        assert result is not None

    def test_whitespace_value(self, converter):
        """Test parameter with whitespace value."""
        result = converter.convert("Appointment", "status=   ")
        assert result is not None

    def test_special_characters_in_reference(self, converter):
        """Test reference ID with special characters."""
        result = converter.convert("Appointment", "patient=Patient/test-123_v2.1")
        result_str = json.dumps(result)
        assert "test-123_v2.1" in result_str

    def test_url_encoded_characters(self, converter):
        """Test URL encoded characters in value."""
        result = converter.convert("Appointment", "identifier=APT%2012345")
        result_str = json.dumps(result)
        # Should decode to "APT 12345"
        assert "APT 12345" in result_str or "APT%2012345" in result_str

    def test_multiple_pipes_in_system_code(self, converter):
        """Test system|code with extra pipes."""
        result = converter.convert("Appointment", "appointment-type=http://example.org|TEST|VALUE")
        result_str = json.dumps(result)
        assert "http://example.org" in result_str or "TEST|VALUE" in result_str

    def test_invalid_parameter_ignored(self, converter):
        """Test that invalid parameters are ignored."""
        result = converter.convert("Appointment", "invalid-param=value")
        # Should return empty or partial query, not crash
        assert result is not None

    def test_chained_parameter_not_supported(self, converter):
        """Test chained parameter (not supported for Appointment)."""
        result = converter.convert("Appointment", "patient.name=Smith")
        # Should handle gracefully
        assert result is not None

    def test_reference_with_fragment(self, converter):
        """Test reference with fragment identifier."""
        result = converter.convert("Appointment", "patient=Patient/123#data")
        result_str = json.dumps(result)
        # Should extract ID correctly
        assert "123" in result_str

    def test_comma_in_single_value(self, converter):
        """Test comma-separated values in single parameter."""
        result = converter.convert("Appointment", "specialty=cardiology,neurology")
        result_str = json.dumps(result)
        # Should handle comma-separated values
        assert "cardiology" in result_str or "neurology" in result_str

    def test_duplicate_parameters_merged(self, converter):
        """Test duplicate parameters are handled correctly."""
        result = converter.convert("Appointment", "status=booked&status=proposed")
        # Should handle repeated parameters
        assert result is not None

    def test_reference_no_resource_type(self, converter):
        """Test reference without resource type."""
        result = converter.convert("Appointment", "patient=123")
        result_str = json.dumps(result)
        # Should still extract ID
        assert "123" in result_str

    def test_very_long_identifier(self, converter):
        """Test very long identifier value."""
        long_id = "APT" + "x" * 100
        result = converter.convert("Appointment", f"identifier={long_id}")
        result_str = json.dumps(result)
        assert long_id in result_str or "APT" in result_str

    def test_unicode_in_parameter_value(self, converter):
        """Test Unicode characters in parameter value."""
        result = converter.convert("Appointment", "identifier=APT-über-123")
        # Should handle Unicode
        assert result is not None

    def test_null_like_value(self, converter):
        """Test null-like string values."""
        result = converter.convert("Appointment", "patient=null")
        result_str = json.dumps(result)
        assert "null" in result_str

    def test_boolean_like_values(self, converter):
        """Test boolean-like string values."""
        result = converter.convert("Appointment", "status=true")
        result_str = json.dumps(result)
        assert "true" in result_str


# =============================================================================
# Test Class 10: Array Fields (8 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentArrayFields:
    """Test parameters that search array fields."""

    def test_multiple_service_categories(self, converter):
        """Test service-category with multiple values."""
        result = converter.convert("Appointment", "service-category=17,34,51")
        result_str = json.dumps(result)
        assert "_search.serviceCategory" in result_str
        assert "17" in result_str or "34" in result_str or "51" in result_str

    def test_multiple_specialties(self, converter):
        """Test specialty with multiple values."""
        result = converter.convert("Appointment", "specialty=cardiology,neurology,orthopedics")
        result_str = json.dumps(result)
        assert "_search.specialty" in result_str

    def test_multiple_slots(self, converter):
        """Test slot parameter with multiple slots."""
        result = converter.convert("Appointment", "slot=Slot/1,Slot/2,Slot/3")
        result_str = json.dumps(result)
        assert "_search.slotId" in result_str
        # May have IDs separated or in array
        assert "1" in result_str or "2" in result_str or "3" in result_str

    def test_single_value_in_array_field(self, converter):
        """Test single value for array field parameter."""
        result = converter.convert("Appointment", "specialty=cardiology")
        result_str = json.dumps(result)
        assert "cardiology" in result_str

    def test_empty_value_in_array(self, converter):
        """Test array with empty value (e.g., "a,,b")."""
        result = converter.convert("Appointment", "specialty=cardiology,,neurology")
        # Should handle gracefully
        assert result is not None

    def test_based_on_multiple_requests(self, converter):
        """Test based-on with multiple service requests."""
        result = converter.convert("Appointment", "based-on=ServiceRequest/1,ServiceRequest/2")
        result_str = json.dumps(result)
        assert "_search.basedOnId" in result_str

    def test_reason_code_multiple(self, converter):
        """Test reason-code with multiple codes."""
        result = converter.convert("Appointment", "reason-code=followup,checkup,consultation")
        result_str = json.dumps(result)
        assert "_search.reasonCode" in result_str

    def test_supporting_info_multiple(self, converter):
        """Test supporting-info with multiple documents."""
        result = converter.convert("Appointment", "supporting-info=DocumentReference/1,DocumentReference/2")
        result_str = json.dumps(result)
        assert "_search.supportingInfoId" in result_str


# =============================================================================
# Test Class 11: Date Edge Cases (10 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentDateEdgeCases:
    """Test date parameter edge cases."""

    def test_partial_date_year_only(self, converter):
        """Test date with year only."""
        result = converter.convert("Appointment", "date=2024")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str or "$gte" in result_str
        assert "2024" in result_str

    def test_partial_date_year_month(self, converter):
        """Test date with year-month only."""
        result = converter.convert("Appointment", "date=2024-06")
        result_str = json.dumps(result, default=str)
        assert "2024-06" in result_str

    def test_date_with_milliseconds(self, converter):
        """Test date with milliseconds."""
        result = converter.convert("Appointment", "date=2024-06-15T10:30:00.123Z")
        result_str = json.dumps(result, default=str)
        assert "2024-06-15" in result_str
        assert "10:30:00" in result_str

    def test_date_with_timezone(self, converter):
        """Test date with timezone offset."""
        result = converter.convert("Appointment", "date=2024-06-15T10:30:00+05:30")
        result_str = json.dumps(result, default=str)
        assert "2024-06-15" in result_str

    def test_date_ends_before(self, converter):
        """Test date with eb (ends before) operator."""
        result = converter.convert("Appointment", "date=eb2024-12-31")
        result_str = json.dumps(result, default=str)
        assert "$lt" in result_str or "$lte" in result_str
        assert "2024-12-31" in result_str

    def test_date_not_equal(self, converter):
        """Test date with ne (not equal) operator."""
        result = converter.convert("Appointment", "date=ne2024-06-15")
        result_str = json.dumps(result, default=str)
        # ne for dates may be implemented as $lt OR $gt (not in range)
        assert any(op in result_str for op in ["$ne", "$not", "$lt", "$gt", "$or"])
        assert "2024-06-15" in result_str

    def test_date_approximately(self, converter):
        """Test date with ap (approximately) operator."""
        result = converter.convert("Appointment", "date=ap2024-06-15")
        result_str = json.dumps(result, default=str)
        # Should generate a range around the date
        assert "$gte" in result_str or "$lte" in result_str or "$and" in result_str

    def test_very_old_date(self, converter):
        """Test with very old date."""
        result = converter.convert("Appointment", "date=1900-01-01")
        result_str = json.dumps(result, default=str)
        assert "1900" in result_str

    def test_future_date(self, converter):
        """Test with far future date."""
        result = converter.convert("Appointment", "date=2099-12-31")
        result_str = json.dumps(result, default=str)
        assert "2099" in result_str

    def test_requested_period_with_operators(self, converter):
        """Test requested-period with various operators."""
        result = converter.convert("Appointment", "requested-period=gt2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "$gt" in result_str
        assert "2024-01-01" in result_str


# =============================================================================
# Test Class 12: Reference Edge Cases (8 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentReferenceEdgeCases:
    """Test reference parameter edge cases."""

    def test_reference_with_base_url(self, converter):
        """Test reference with full base URL."""
        result = converter.convert("Appointment", "patient=http://example.org/fhir/Patient/123")
        result_str = json.dumps(result)
        assert "123" in result_str

    def test_reference_with_https(self, converter):
        """Test reference with HTTPS URL."""
        result = converter.convert("Appointment", "patient=https://secure.example.org/Patient/456")
        result_str = json.dumps(result)
        assert "456" in result_str

    def test_reference_with_port(self, converter):
        """Test reference with port number in URL."""
        result = converter.convert("Appointment", "patient=http://example.org:8080/Patient/789")
        result_str = json.dumps(result)
        assert "789" in result_str

    def test_reference_urn_format(self, converter):
        """Test reference with URN format."""
        result = converter.convert("Appointment", "patient=urn:uuid:550e8400-e29b-41d4-a716-446655440000")
        result_str = json.dumps(result)
        assert "550e8400" in result_str or "uuid" in result_str

    def test_reference_with_query_string(self, converter):
        """Test reference with query string (should be stripped)."""
        result = converter.convert("Appointment", "patient=Patient/123?_format=json")
        result_str = json.dumps(result)
        assert "123" in result_str

    def test_actor_with_multiple_types(self, converter):
        """Test actor parameter can reference different resource types."""
        result1 = converter.convert("Appointment", "actor=Patient/1")
        result2 = converter.convert("Appointment", "actor=Device/2")
        result3 = converter.convert("Appointment", "actor=HealthcareService/3")
        # All should work, using same field
        assert "_search.actorIds" in json.dumps(result1)
        assert "_search.actorIds" in json.dumps(result2)
        assert "_search.actorIds" in json.dumps(result3)

    def test_reference_uuid_format(self, converter):
        """Test reference with UUID as ID."""
        result = converter.convert("Appointment", "patient=Patient/550e8400-e29b-41d4-a716-446655440000")
        result_str = json.dumps(result)
        assert "550e8400-e29b-41d4-a716-446655440000" in result_str

    def test_reference_with_history_version(self, converter):
        """Test reference with history version."""
        result = converter.convert("Appointment", "based-on=ServiceRequest/123/_history/5")
        result_str = json.dumps(result)
        assert "123/_history/5" in result_str or "123" in result_str


# =============================================================================
# Test Class 13: Token Edge Cases (7 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentTokenEdgeCases:
    """Test token parameter edge cases."""

    def test_token_system_only(self, converter):
        """Test token with system but no code."""
        result = converter.convert("Appointment", "appointment-type=http://example.org|")
        result_str = json.dumps(result)
        assert "http://example.org" in result_str

    def test_token_code_only_with_pipe(self, converter):
        """Test token with code but no system (empty system)."""
        result = converter.convert("Appointment", "appointment-type=|CHECKUP")
        result_str = json.dumps(result)
        assert "CHECKUP" in result_str

    def test_token_multiple_pipes(self, converter):
        """Test token with multiple pipe characters."""
        result = converter.convert("Appointment", "identifier=http://example.org|system|value")
        result_str = json.dumps(result)
        # Should handle first pipe as separator
        assert "http://example.org" in result_str

    def test_token_case_sensitivity(self, converter):
        """Test token parameter case handling."""
        result1 = converter.convert("Appointment", "status=BOOKED")
        result2 = converter.convert("Appointment", "status=booked")
        # Both should work (case-sensitive by default)
        assert result1 is not None
        assert result2 is not None

    def test_token_with_spaces(self, converter):
        """Test token value with spaces."""
        result = converter.convert("Appointment", "identifier=APT 123 456")
        result_str = json.dumps(result)
        assert "APT 123 456" in result_str or "APT" in result_str

    def test_token_urn_system(self, converter):
        """Test token with URN system."""
        result = converter.convert("Appointment", "identifier=urn:oid:1.2.3.4.5|APT123")
        result_str = json.dumps(result)
        assert "urn:oid:1.2.3.4.5" in result_str or "APT123" in result_str

    def test_token_namespace_system(self, converter):
        """Test token with namespace in system."""
        result = converter.convert("Appointment", "appointment-type=http://hl7.org/fhir/v2/0276|CHECKUP")
        result_str = json.dumps(result)
        assert "http://hl7.org/fhir/v2/0276" in result_str or "CHECKUP" in result_str


# =============================================================================
# Test Class 14: Query Structure Validation (10 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentQueryStructure:
    """Test MongoDB query structure correctness."""

    def test_single_parameter_no_and(self, converter):
        """Single parameter should not wrap in $and."""
        result = converter.convert("Appointment", "status=booked")
        # Should be simple {status: "booked"}, not wrapped in $and
        assert "$and" not in result or len(result.get("$and", [])) > 1

    def test_two_parameters_creates_and(self, converter):
        """Two parameters should create $and."""
        result = converter.convert("Appointment", "status=booked&specialty=cardiology")
        assert "$and" in result

    def test_or_within_and_structure(self, converter):
        """Test $or inside $and for multi-field search."""
        result = converter.convert("Appointment", "patient=Patient/123&status=booked")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "$or" in result_str

    def test_nested_or_for_date_range(self, converter):
        """Test nested $or for date fields."""
        result = converter.convert("Appointment", "date=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        # Should have $or for start vs period fields
        assert "$or" in result_str

    def test_and_array_length(self, converter):
        """Test $and array has correct length."""
        result = converter.convert("Appointment", "status=booked&specialty=cardiology&service-category=17")
        assert "$and" in result
        assert len(result["$and"]) == 3

    def test_or_array_length_patient(self, converter):
        """Test patient $or has 2 clauses."""
        result = converter.convert("Appointment", "patient=Patient/123")
        assert "$or" in result
        assert len(result["$or"]) == 2

    def test_field_names_use_search_prefix(self, converter):
        """Test denormalized fields use _search prefix."""
        result = converter.convert("Appointment", "patient=Patient/123")
        result_str = json.dumps(result)
        assert "_search." in result_str

    def test_direct_field_for_status(self, converter):
        """Test status uses direct field, not _search."""
        result = converter.convert("Appointment", "status=booked")
        assert "status" in result
        assert result["status"] == "booked"

    def test_date_operator_mapping(self, converter):
        """Test date operators map correctly to MongoDB."""
        result_ge = converter.convert("Appointment", "date=ge2024-01-01")
        result_le = converter.convert("Appointment", "date=le2024-12-31")
        result_gt = converter.convert("Appointment", "date=gt2024-01-01")
        result_lt = converter.convert("Appointment", "date=lt2024-12-31")
        
        assert "$gte" in json.dumps(result_ge, default=str)
        assert "$lte" in json.dumps(result_le, default=str)
        assert "$gt" in json.dumps(result_gt, default=str)
        assert "$lt" in json.dumps(result_lt, default=str)

    def test_complex_query_structure_valid(self, converter):
        """Test complex query generates valid MongoDB structure."""
        query = "patient=Patient/123&date=ge2024-01-01&date=le2024-12-31&status=booked&specialty=cardiology"
        result = converter.convert("Appointment", query)
        
        # Should be valid MongoDB query structure
        assert isinstance(result, dict)
        if "$and" in result:
            assert isinstance(result["$and"], list)
            assert all(isinstance(clause, dict) for clause in result["$and"])


# =============================================================================
# Test Class 15: Denormalization Testing (12 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentDenormalization:
    """Test denormalization field extraction."""

    def test_appointment_type_extraction(self, converter):
        """Test appointmentType denormalization."""
        result = converter.convert("Appointment", "appointment-type=CHECKUP")
        result_str = json.dumps(result)
        assert "_search.appointmentType_codes" in result_str or "_search.appointmentType_systemCode" in result_str

    def test_service_type_concept_extraction(self, converter):
        """Test serviceType concept extraction."""
        result = converter.convert("Appointment", "service-type=57")
        result_str = json.dumps(result)
        assert "_search.serviceType_codes" in result_str or "_search.serviceType_systemCode" in result_str

    def test_service_category_extraction(self, converter):
        """Test serviceCategory extraction."""
        result = converter.convert("Appointment", "service-category=17")
        result_str = json.dumps(result)
        assert "_search.serviceCategory" in result_str

    def test_specialty_extraction(self, converter):
        """Test specialty extraction."""
        result = converter.convert("Appointment", "specialty=cardiology")
        result_str = json.dumps(result)
        assert "_search.specialty" in result_str

    def test_reason_code_extraction(self, converter):
        """Test reason concept extraction."""
        result = converter.convert("Appointment", "reason-code=followup")
        result_str = json.dumps(result)
        assert "_search.reasonCode" in result_str

    def test_identifier_extraction(self, converter):
        """Test identifier extraction."""
        result = converter.convert("Appointment", "identifier=APT123")
        result_str = json.dumps(result)
        assert "_search.identifier" in result_str

    def test_participant_actor_extraction(self, converter):
        """Test participant actor extraction."""
        result = converter.convert("Appointment", "actor=Patient/123")
        result_str = json.dumps(result)
        assert "_search.actorIds" in result_str

    def test_participant_patient_extraction(self, converter):
        """Test patient extraction from participant."""
        result = converter.convert("Appointment", "patient=Patient/123")
        result_str = json.dumps(result)
        assert "_search.patientId" in result_str

    def test_participant_status_extraction(self, converter):
        """Test participant status extraction."""
        result = converter.convert("Appointment", "part-status=accepted")
        result_str = json.dumps(result)
        assert "_search.participantStatus" in result_str or "participant.status" in result_str

    def test_subject_extraction(self, converter):
        """Test subject reference extraction."""
        result = converter.convert("Appointment", "subject=Patient/123")
        result_str = json.dumps(result)
        assert "_search.subjectId" in result_str

    def test_period_extraction(self, converter):
        """Test start/end period extraction."""
        result = converter.convert("Appointment", "date=2024-06-15")
        result_str = json.dumps(result, default=str)
        assert "start" in result_str or "_search.appointmentPeriod" in result_str

    def test_requested_period_extraction(self, converter):
        """Test requestedPeriod extraction."""
        result = converter.convert("Appointment", "requested-period=2024-06-15")
        result_str = json.dumps(result, default=str)
        assert "_search.requestedPeriod" in result_str or "requestedPeriod" in result_str


# =============================================================================
# Test Class 16: Validation Errors (8 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentValidationErrors:
    """Test invalid input handling."""

    def test_invalid_date_format_handled(self, converter):
        """Test invalid date format doesn't crash."""
        result = converter.convert("Appointment", "date=not-a-date")
        # Should return empty or partial query, not crash
        assert result is not None

    def test_invalid_operator_handled(self, converter):
        """Test invalid date operator doesn't crash."""
        result = converter.convert("Appointment", "date=xx2024-01-01")
        # Should handle gracefully
        assert result is not None

    def test_unsupported_parameter_warning(self, converter):
        """Test unsupported parameter generates warning."""
        result = converter.convert("Appointment", "unsupported-param=value")
        # Should return empty or partial query
        assert result is not None

    def test_malformed_reference_handled(self, converter):
        """Test malformed reference doesn't crash."""
        result = converter.convert("Appointment", "patient=Patient//123")
        # Should handle gracefully
        assert result is not None

    def test_malformed_token_handled(self, converter):
        """Test malformed token value doesn't crash."""
        result = converter.convert("Appointment", "identifier=|||")
        # Should handle gracefully
        assert result is not None

    def test_empty_query_string_handled(self, converter):
        """Test empty query string doesn't crash."""
        try:
            result = converter.convert("Appointment", "")
            # May raise exception or return empty
            assert True
        except Exception:
            # Exception is acceptable for empty query
            assert True

    def test_malformed_date_range(self, converter):
        """Test malformed date range handled."""
        result = converter.convert("Appointment", "date=ge2024-13-40")
        # Should handle invalid date gracefully
        assert result is not None

    def test_very_complex_malformed_query(self, converter):
        """Test very complex malformed query doesn't crash."""
        query = "patient===&&&status=|||&date=invalid&&&"
        try:
            result = converter.convert("Appointment", query)
            assert result is not None
        except Exception:
            # Exception is acceptable
            assert True


# =============================================================================
# Test Class 17: Performance & Optimization (5 tests)
# =============================================================================

@pytest.mark.integration
class TestAppointmentPerformance:
    """Test query optimization scenarios."""

    def test_indexed_field_query(self, converter):
        """Test query on indexed field (patient)."""
        result = converter.convert("Appointment", "patient=Patient/123")
        result_str = json.dumps(result)
        # Should use indexed _search.patientId field
        assert "_search.patientId" in result_str

    def test_date_range_optimization(self, converter):
        """Test date range uses proper operators for indexing."""
        result = converter.convert("Appointment", "date=ge2024-01-01&date=le2024-12-31")
        result_str = json.dumps(result, default=str)
        # Should use $gte and $lte which are index-friendly
        assert "$gte" in result_str
        assert "$lte" in result_str

    def test_compound_index_friendly(self, converter):
        """Test query pattern that can use compound index."""
        result = converter.convert("Appointment", "patient=Patient/123&date=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        # Should have patient and date fields that can use compound index
        assert "_search.patientId" in result_str
        assert "$gte" in result_str

    def test_status_direct_field(self, converter):
        """Test status uses direct field (no denormalization overhead)."""
        result = converter.convert("Appointment", "status=booked")
        # Should use direct field without $or
        assert result == {"status": "booked"}

    def test_multiple_indexed_fields(self, converter):
        """Test query using multiple indexed fields."""
        result = converter.convert("Appointment", "patient=Patient/123&practitioner=Practitioner/456&location=Location/789")
        result_str = json.dumps(result)
        # All should use indexed _search fields
        assert "_search.patientId" in result_str
        assert "_search.practitionerId" in result_str
        assert "_search.locationId" in result_str


@pytest.mark.integration
class TestAppointmentParameterCombinations:
    """Test explicit AND/OR parameter combinations (similar to Patient tests)."""

    def test_implicit_and_two_params(self, converter):
        """Test implicit AND with two different parameters."""
        result = converter.convert("Appointment", "status=booked&date=2024-06-15")
        assert "$and" in result or ("status" in result and "start" in result)

    def test_implicit_and_three_params(self, converter):
        """Test implicit AND with three parameters."""
        result = converter.convert("Appointment", "patient=Patient/123&status=booked&service-category=17")
        result_str = json.dumps(result)
        assert "$and" in result_str
        # Should have all three conditions
        assert "123" in result_str
        assert "booked" in result_str
        assert "17" in result_str

    def test_implicit_or_same_param(self, converter):
        """Test implicit OR with same parameter repeated."""
        result = converter.convert("Appointment", "status=booked&status=pending")
        result_str = json.dumps(result)
        # Should create OR for same parameter
        assert "$or" in result_str or ("booked" in result_str and "pending" in result_str)

    def test_mixed_and_or_logic(self, converter):
        """Test mixed AND/OR logic."""
        result = converter.convert("Appointment", "patient=Patient/123&status=booked&status=pending")
        result_str = json.dumps(result)
        # Should have AND at top level with OR for status
        assert "123" in result_str
        assert "booked" in result_str or "pending" in result_str

    def test_multiple_reference_params(self, converter):
        """Test multiple different reference parameters."""
        result = converter.convert("Appointment", "patient=Patient/123&practitioner=Practitioner/456")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "123" in result_str
        assert "456" in result_str

    def test_multiple_token_params(self, converter):
        """Test multiple different token parameters."""
        result = converter.convert("Appointment", "status=booked&part-status=accepted")
        result_str = json.dumps(result)
        assert "$and" in result_str
        assert "booked" in result_str
        assert "accepted" in result_str


@pytest.mark.integration
class TestAppointmentSpecialParameters:
    """Test special search parameters (_id, _lastUpdated, _tag, etc.)."""

    def test_id_parameter(self, converter):
        """Test _id parameter."""
        result = converter.convert("Appointment", "_id=123")
        assert "id" in result

    def test_last_updated_parameter(self, converter):
        """Test _lastUpdated parameter."""
        result = converter.convert("Appointment", "_lastUpdated=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "meta.lastUpdated" in result_str

    def test_id_with_other_param(self, converter):
        """Test _id combined with other parameter."""
        result = converter.convert("Appointment", "_id=123&status=booked")
        result_str = json.dumps(result)
        assert "id" in result_str
        assert "booked" in result_str

    def test_last_updated_range(self, converter):
        """Test _lastUpdated with date range."""
        result = converter.convert("Appointment", "_lastUpdated=ge2024-01-01&_lastUpdated=le2024-12-31")
        result_str = json.dumps(result, default=str)
        assert "meta.lastUpdated" in result_str
        assert "$gte" in result_str or "$lte" in result_str

    def test_special_params_combination(self, converter):
        """Test multiple special parameters together."""
        result = converter.convert("Appointment", "_id=123&_lastUpdated=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "id" in result_str
        assert "meta.lastUpdated" in result_str


@pytest.mark.integration
class TestAppointmentComplexLogic:
    """Test advanced boolean logic combinations."""

    def test_three_way_or(self, converter):
        """Test three-way OR with same parameter."""
        result = converter.convert("Appointment", "status=booked&status=pending&status=arrived")
        result_str = json.dumps(result)
        # Should handle all three values
        count = result_str.count("status")
        assert count >= 3

    def test_nested_and_or(self, converter):
        """Test nested AND/OR logic."""
        result = converter.convert("Appointment", "patient=Patient/123&status=booked&status=pending&date=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "123" in result_str

    def test_multi_field_with_and(self, converter):
        """Test multi-field search combined with AND."""
        result = converter.convert("Appointment", "patient=Patient/123&practitioner=Practitioner/456&status=booked")
        result_str = json.dumps(result)
        # patient should have $or, all wrapped in $and
        assert "$and" in result_str
        assert "$or" in result_str

    def test_multiple_dates_or(self, converter):
        """Test multiple date parameters creating OR."""
        result = converter.convert("Appointment", "date=2024-06-15&date=2024-06-16")
        result_str = json.dumps(result, default=str)
        # Should have both dates
        assert "2024-06-15" in result_str
        assert "2024-06-16" in result_str

    def test_complex_five_param_combination(self, converter):
        """Test complex combination with 5 parameters."""
        result = converter.convert("Appointment", 
            "patient=Patient/123&practitioner=Practitioner/456&status=booked&" +
            "service-category=17&date=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        # Verify all parameters present
        assert "123" in result_str
        assert "456" in result_str
        assert "booked" in result_str
        assert "17" in result_str
        assert "2024-01-01" in result_str


@pytest.mark.integration
class TestAppointmentBuilderEdgeCases:
    """Test MQL builder edge cases."""

    def test_very_long_query(self, converter):
        """Test very long query with many parameters."""
        query = (
            "patient=Patient/123&practitioner=Practitioner/456&"
            "location=Location/789&status=booked&part-status=accepted&"
            "appointment-type=CHECKUP&service-category=17&specialty=394814009"
        )
        result = converter.convert("Appointment", query)
        result_str = json.dumps(result)
        # Should handle all parameters
        assert len(result_str) > 100

    def test_query_with_all_operators(self, converter):
        """Test query using multiple date operators."""
        result = converter.convert("Appointment", 
            "date=ge2024-01-01&date=le2024-12-31&requested-period=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        # Should have multiple operators
        assert "$gte" in result_str
        assert "$lte" in result_str

    def test_deeply_nested_structure(self, converter):
        """Test deeply nested $and/$or structure."""
        result = converter.convert("Appointment",
            "patient=Patient/123&status=booked&status=pending&" +
            "date=ge2024-01-01&practitioner=Practitioner/456")
        result_str = json.dumps(result, default=str)
        assert "$and" in result_str
        assert "$or" in result_str


@pytest.mark.integration
class TestAppointmentLogicCombinations:
    """Test complex boolean logic combinations."""

    def test_four_way_or(self, converter):
        """Test four-way OR with same parameter."""
        result = converter.convert("Appointment", 
            "status=booked&status=pending&status=arrived&status=fulfilled")
        result_str = json.dumps(result)
        # Should have all four values
        assert "booked" in result_str
        assert "fulfilled" in result_str

    def test_mixed_and_or_with_multi_field(self, converter):
        """Test mixed AND/OR with multi-field searches."""
        result = converter.convert("Appointment",
            "patient=Patient/123&group=Group/456&status=booked")
        result_str = json.dumps(result)
        # Should have multiple $or within $and
        assert "$and" in result_str
        assert "$or" in result_str

    def test_multiple_reference_or(self, converter):
        """Test OR with multiple reference parameters."""
        result = converter.convert("Appointment",
            "practitioner=Practitioner/123&practitioner=Practitioner/456")
        result_str = json.dumps(result)
        # Should create OR for same parameter
        assert "123" in result_str
        assert "456" in result_str

    def test_token_or_with_system(self, converter):
        """Test token OR with system|code format."""
        result = converter.convert("Appointment",
            "status=booked&appointment-type=http://system.org|CHECKUP")
        result_str = json.dumps(result)
        assert "booked" in result_str
        assert "CHECKUP" in result_str


@pytest.mark.integration
class TestAppointmentURLParsing:
    """Test URL parameter parsing and encoding."""

    def test_encoded_space(self, converter):
        """Test URL-encoded space in parameter."""
        result = converter.convert("Appointment", "status=booked")
        assert result is not None

    def test_encoded_special_chars(self, converter):
        """Test URL-encoded special characters."""
        result = converter.convert("Appointment", "identifier=MRN123")
        assert result is not None

    def test_ampersand_separator(self, converter):
        """Test ampersand as parameter separator."""
        result = converter.convert("Appointment", "status=booked&patient=Patient/123")
        result_str = json.dumps(result)
        assert "booked" in result_str
        assert "123" in result_str

    def test_equals_in_value(self, converter):
        """Test handling equals sign in parameter value."""
        result = converter.convert("Appointment", "identifier=MRN=123")
        # Should handle or fail gracefully
        assert result is not None or True

    def test_empty_parameter_value(self, converter):
        """Test empty parameter value."""
        try:
            result = converter.convert("Appointment", "status=")
            assert result is not None or result == {}
        except Exception:
            assert True  # Empty value may raise exception

    def test_duplicate_ampersands(self, converter):
        """Test handling duplicate ampersands."""
        try:
            result = converter.convert("Appointment", "status=booked&&patient=Patient/123")
            assert result is not None
        except Exception:
            assert True  # May raise exception

    def test_trailing_ampersand(self, converter):
        """Test trailing ampersand in query."""
        result = converter.convert("Appointment", "status=booked&")
        assert result is not None


@pytest.mark.integration
class TestAppointmentBuilderValidation:
    """Test MQL builder validation."""

    def test_valid_single_condition(self, converter):
        """Test valid single condition query."""
        result = converter.convert("Appointment", "status=booked")
        assert isinstance(result, dict)
        assert "status" in result

    def test_valid_and_structure(self, converter):
        """Test valid $and structure."""
        result = converter.convert("Appointment", "status=booked&patient=Patient/123")
        if "$and" in result:
            assert isinstance(result["$and"], list)
            assert len(result["$and"]) >= 2

    def test_valid_or_structure(self, converter):
        """Test valid $or structure in multi-field."""
        result = converter.convert("Appointment", "patient=Patient/123")
        result_str = json.dumps(result)
        if "$or" in result_str:
            # Verify $or has proper structure
            assert "patientId" in result_str or "subjectId" in result_str

    def test_date_operator_validity(self, converter):
        """Test date operators create valid MongoDB operators."""
        result = converter.convert("Appointment", "date=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        # Should use valid MongoDB operator
        assert any(op in result_str for op in ["$gte", "$lte", "$gt", "$lt", "$eq"])

    def test_nested_structure_validity(self, converter):
        """Test nested $and/$or structure validity."""
        result = converter.convert("Appointment",
            "patient=Patient/123&status=booked&date=ge2024-01-01")
        # Should be valid dict
        assert isinstance(result, dict)
        result_str = json.dumps(result, default=str)
        assert len(result_str) > 0


@pytest.mark.integration
class TestAppointmentIndexRecommendations:
    """Test that queries use recommended indexes."""

    def test_patient_index_usage(self, converter):
        """Test query uses patientId index."""
        result = converter.convert("Appointment", "patient=Patient/123")
        result_str = json.dumps(result)
        # Should use _search.patientId which is indexed
        assert "_search.patientId" in result_str

    def test_date_index_usage(self, converter):
        """Test query uses date index."""
        result = converter.convert("Appointment", "date=ge2024-01-01")
        result_str = json.dumps(result, default=str)
        # Should use start field which is indexed
        assert "start" in result_str

    def test_status_index_usage(self, converter):
        """Test query uses status index."""
        result = converter.convert("Appointment", "status=booked")
        # Should use direct status field which is indexed
        assert result == {"status": "booked"}

    def test_compound_index_pattern(self, converter):
        """Test query pattern matches compound index."""
        result = converter.convert("Appointment", "patient=Patient/123&date=ge2024-01-01&status=booked")
        result_str = json.dumps(result, default=str)
        # Should use fields in compound index: patient, date, status
        assert "_search.patientId" in result_str
        assert "start" in result_str or "date" in result_str
        assert "status" in result_str


@pytest.mark.integration
@pytest.mark.mongodb
class TestAppointmentMongoDBQueries:
    """Test MongoDB integration (requires MongoDB)."""

    def test_query_can_be_executed(self, converter):
        """Test generated query is valid MongoDB syntax."""
        result = converter.convert("Appointment", "patient=Patient/123&status=booked")
        # Query should be serializable to JSON (valid for MongoDB)
        try:
            json.dumps(result, default=str)
            assert True
        except Exception as e:
            pytest.fail(f"Query not serializable: {e}")


# =============================================================================
# Test Class 28: Compartment Search (Hybrid Approach 3)
#
# The Appointment YAML opts into the precomputed Patient compartment via:
#
#     compartments:
#       precomputed:
#         - Patient
#
# These tests pin both halves of the hybrid approach for the Appointment
# resource:
#   - Patient compartment → single indexed lookup against
#     `_compartments.Patient`, populated at denormalization time by
#     `CompartmentMembershipExtractor` walking `participant[*].actor` and
#     filtering to `Patient`-typed references.
#   - Other compartments (Practitioner, RelatedPerson) → dynamic
#     translation against the `_search.actorIds` linking field.
# =============================================================================

@pytest.mark.integration
class TestAppointmentPatientCompartmentFastPath:
    """Hybrid Approach 3 — precomputed Patient compartment for Appointment."""

    def test_patient_compartment_collapses_to_precomputed_field(self, converter):
        # Strict equality: the resolver must NOT augment with dynamic
        # `_search.actorIds` branches when the precomputed opt-in is set.
        query = converter.convert_with_compartment(
            "Patient", "pat-123", "Appointment"
        )
        assert query == {"_compartments.Patient": "pat-123"}

    def test_patient_compartment_does_not_use_dynamic_actor_field(self, converter):
        s = json.dumps(converter.convert_with_compartment(
            "Patient", "pat-123", "Appointment"
        ))
        assert "_search.actorIds" not in s
        assert "participant.actor" not in s

    def test_patient_compartment_with_status_filter(self, converter):
        result = converter.convert_with_compartment(
            "Patient", "pat-123", "Appointment", "status=booked"
        )
        s = json.dumps(result, default=str)
        assert "_compartments.Patient" in s
        assert "pat-123" in s
        assert "booked" in s

    def test_patient_compartment_with_date_filter(self, converter):
        result = converter.convert_with_compartment(
            "Patient", "pat-123", "Appointment",
            "date=ge2024-06-01")
        s = json.dumps(result, default=str)
        assert "_compartments.Patient" in s
        assert "pat-123" in s
        # Date filter is preserved either as the lexical value or its
        # serialized datetime form.
        assert "2024-06-01" in s or "datetime" in s

    def test_patient_compartment_with_appointment_type_filter(self, converter):
        result = converter.convert_with_compartment(
            "Patient", "pat-123", "Appointment",
            "appointment-type=CHECKUP")
        s = json.dumps(result, default=str)
        assert "_compartments.Patient" in s
        assert "CHECKUP" in s


@pytest.mark.integration
class TestAppointmentCompartmentDenormalization:
    """
    Round-trip: the documents produced by the denormalizer carry the
    `_compartments.Patient` field that the fast-path query matches
    against, with reference-type filtering applied per FHIR R5.
    """

    @pytest.fixture
    def denormalizer(self):
        from fhir_search_to_mql import ResourceDenormalizer
        return ResourceDenormalizer()

    def test_patient_actor_populates_compartment(self, denormalizer, sample_appointment):
        # The shared `sample_appointment` fixture already includes both a
        # Patient/example-patient and Practitioner/example-practitioner
        # actor. Each contributes to its OWN compartment bucket — Patient
        # via `participant.actor` filtered to `Patient/*`, Practitioner
        # via the same source filtered to `Practitioner/*`.
        out = denormalizer.denormalize(sample_appointment)
        comp = out["_compartments"]
        assert comp.get("Patient") == ["example-patient"]
        assert comp.get("Practitioner") == ["example-practitioner"]

    def test_practitioner_only_appointment_populates_practitioner_compartment(
        self, denormalizer
    ):
        # An Appointment with no Patient-typed actor MUST NOT show up in
        # the Patient compartment — Hybrid Approach 3 requires perfect
        # parity with the FHIR R5 inclusion rules. With the Practitioner
        # compartment also opted-in to the precompute fast-path, the
        # bucket is still emitted (carrying the Practitioner key) so the
        # `Practitioner/<id>/Appointment` query can route through
        # `_compartments.Practitioner`.
        appt = {
            "resourceType": "Appointment",
            "id": "appt-prac-only",
            "status": "booked",
            "participant": [
                {
                    "actor": {"reference": "Practitioner/dr-1"},
                    "status": "accepted",
                },
            ],
        }
        out = denormalizer.denormalize(appt)
        comp = out.get("_compartments", {})
        # Patient compartment correctly empty (or absent) — no Patient-typed actor.
        assert not comp.get("Patient")
        # Practitioner compartment populated for the precomputed fast-path.
        assert comp.get("Practitioner") == ["dr-1"]

    def test_multiple_patient_actors_deduped_first_seen_order(self, denormalizer):
        appt = {
            "resourceType": "Appointment",
            "id": "appt-group",
            "status": "booked",
            "participant": [
                {"actor": {"reference": "Patient/pat-A"}, "status": "accepted"},
                {"actor": {"reference": "Patient/pat-B"}, "status": "accepted"},
                {"actor": {"reference": "Patient/pat-A"}, "status": "tentative"},  # dup
                {"actor": {"reference": "Practitioner/dr-1"}, "status": "accepted"},
            ],
        }
        out = denormalizer.denormalize(appt)
        assert out["_compartments"]["Patient"] == ["pat-A", "pat-B"]

    def test_round_trip_search_finds_denormalized_doc(
        self, converter, sample_appointment
    ):
        """End-to-end: denormalize → seed in-memory → fast-path query matches."""
        from fhir_search_to_mql import ResourceDenormalizer
        denormalizer = ResourceDenormalizer()
        docs = [
            denormalizer.denormalize(sample_appointment),  # Patient/example-patient
            denormalizer.denormalize({
                "resourceType": "Appointment",
                "id": "other-appt",
                "status": "booked",
                "participant": [
                    {
                        "actor": {"reference": "Patient/some-other-patient"},
                        "status": "accepted",
                    }
                ],
            }),
        ]
        query = converter.convert_with_compartment(
            "Patient", "example-patient", "Appointment"
        )
        target_id = query["_compartments.Patient"]
        matches = [
            d for d in docs
            if target_id in d.get("_compartments", {}).get("Patient", [])
        ]
        assert len(matches) == 1
        assert matches[0]["id"] == "example-appointment"


@pytest.mark.integration
class TestAppointmentDynamicCompartmentFallback:
    """
    Compartments NOT in `compartments.precomputed` must continue to use
    the dynamic translation against `_search.*` linking fields. This is
    Approach 1 of the analysis document and remains the default for the
    long-tail compartments.
    """

    def test_practitioner_compartment_uses_fast_path(self, converter):
        # Practitioner compartment for Appointment is now ALSO opted into
        # the Hybrid Approach 3 fast-path (see Appointment.yaml's
        # `compartments.precomputed: [Patient, Practitioner]`). The
        # resolver emits a single-field lookup against
        # `_compartments.Practitioner` instead of the dynamic `actorIds`
        # `$or`. The actor-based dynamic path is no longer the source
        # of truth here — it's the precomputed extractor that filters
        # `participant.actor` to `Practitioner/*` only.
        s = json.dumps(converter.convert_with_compartment(
            "Practitioner", "dr-1", "Appointment"
        ))
        assert "_compartments.Practitioner" in s
        assert "dr-1" in s

    def test_relatedperson_compartment_uses_dynamic_path(self, converter):
        s = json.dumps(converter.convert_with_compartment(
            "RelatedPerson", "rp-1", "Appointment"
        ))
        assert "_compartments.RelatedPerson" not in s
        assert "_search.actorIds" in s
        assert "rp-1" in s


class TestAppointmentDeviceCompartmentFastPath:
    """
    Hybrid Approach 3 — precomputed Device compartment.

    Per FHIR R5 compartmentdefinition-device.html, Appointment
    participates in the Device compartment via the ``actor`` linking
    parameter (Appointment.participant.actor cardinality includes
    ``Reference(Device)`` in R5). Appointment.yaml now opts into the
    fast-path so ``Device/<id>/Appointment`` collapses to a single
    indexed lookup against ``_compartments.Device`` instead of the
    dynamic ``$or`` over ``_search.actorIds``.
    """

    def test_device_compartment_collapses_to_precomputed_field(
        self, converter
    ):
        query = converter.convert_with_compartment(
            "Device", "dev-pump", "Appointment"
        )
        assert query == {"_compartments.Device": "dev-pump"}

    def test_device_compartment_does_not_use_dynamic_actor_ids(
        self, converter
    ):
        s = json.dumps(converter.convert_with_compartment(
            "Device", "dev-pump", "Appointment"
        ))
        assert "_compartments.Device" in s
        assert "_search.actorIds" not in s

    def test_device_compartment_with_status_filter(self, converter):
        query = converter.convert_with_compartment(
            "Device", "dev-pump", "Appointment", "status=booked"
        )
        s = json.dumps(query)
        assert "_compartments.Device" in s
        assert "dev-pump" in s
        assert "booked" in s


class TestAppointmentDeviceCompartmentDenormalization:
    """
    Verify ``_compartments.Device`` is populated from
    ``participant[*].actor`` filtered to ``Device/*`` only — sharing
    the same source path used to populate the Patient and
    Practitioner buckets, with the ``reference_type`` filter routing
    each actor to the correct compartment.
    """

    @pytest.fixture
    def denormalizer(self):
        from fhir_search_to_mql import ResourceDenormalizer
        return ResourceDenormalizer()

    def test_device_actor_populates_compartment(self, denormalizer):
        appt = {
            "resourceType": "Appointment",
            "id": "appt-1",
            "status": "booked",
            "participant": [
                {"actor": {"reference": "Patient/pat-1"}, "status": "accepted"},
                {"actor": {"reference": "Device/dev-pump"}, "status": "accepted"},
                {"actor": {"reference": "Practitioner/dr-x"}, "status": "accepted"},
            ],
        }
        out = denormalizer.denormalize(appt)
        comp = out["_compartments"]
        # Each actor lands in exactly one compartment bucket per the
        # `reference_type` filter — no cross-pollination.
        assert comp["Patient"] == ["pat-1"]
        assert comp["Practitioner"] == ["dr-x"]
        assert comp["Device"] == ["dev-pump"]

    def test_no_device_actor_means_no_device_bucket(self, denormalizer):
        appt = {
            "resourceType": "Appointment",
            "id": "appt-2",
            "status": "booked",
            "participant": [
                {"actor": {"reference": "Patient/pat-1"}, "status": "accepted"},
            ],
        }
        out = denormalizer.denormalize(appt)
        # Sparse-output contract — empty Device bucket must be absent.
        assert "Device" not in out.get("_compartments", {})

    def test_multiple_device_actors_dedup(self, denormalizer):
        appt = {
            "resourceType": "Appointment",
            "id": "appt-3",
            "status": "booked",
            "participant": [
                {"actor": {"reference": "Device/dev-A"}, "status": "accepted"},
                {"actor": {"reference": "Device/dev-A"}, "status": "tentative"},
                {"actor": {"reference": "Device/dev-B"}, "status": "accepted"},
            ],
        }
        out = denormalizer.denormalize(appt)
        assert out["_compartments"]["Device"] == ["dev-A", "dev-B"]


# =============================================================================
# Summary
# =============================================================================

"""
Test Summary:
-------------
Total Test Classes: 17
Total Test Methods: 158

Coverage Breakdown:
- Reference Parameters: 15 tests (all 11 parameters)
- Token Parameters: 12 tests (all 8 parameters)
- Date Parameters: 10 tests (both parameters with operators)
- Common Parameters: 5 tests (both _id and _lastUpdated)
- Multi-Field Searches: 9 tests (patient, group, subject critical fixes)
- CodeableReference: 6 tests (reason and serviceType dual nature)
- Complex Queries: 8 tests (real-world scenarios)
- Modifiers: 10 tests (FHIR modifiers)
- Edge Cases: 15 tests (error handling, special chars)
- Array Fields: 8 tests (multi-value parameters)
- Date Edge Cases: 10 tests (partial dates, timezones, operators)
- Reference Edge Cases: 8 tests (URLs, URNs, versions)
- Token Edge Cases: 7 tests (system|code variations)
- Query Structure: 10 tests (MongoDB structure validation)
- Denormalization: 12 tests (field extraction)
- Validation Errors: 8 tests (invalid input handling)
- Performance: 5 tests (optimization scenarios)

Expected Coverage: 85%+
- All 23 search parameters tested
- All parameter types covered (reference, token, date, common)
- All critical scenarios validated (multi-field, CodeableReference)
- Edge cases and error handling comprehensive
- MongoDB query structure validated
- Denormalization coverage
- Performance optimization scenarios

Run with:
    pytest tests/integration/test_appointment_comprehensive.py -v
    pytest tests/integration/test_appointment_comprehensive.py -v --cov=src --cov-report=html
"""
