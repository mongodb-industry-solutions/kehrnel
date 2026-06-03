# Appointment Configuration Update Summary

**Date**: May 22, 2026  
**Resource**: Appointment (FHIR R5)  
**Reference**: 
- https://hl7.org/fhir/R5/appointment-search.html
- https://hl7.org/fhir/R5/appointment-definitions.html

---

## Overview

Successfully updated the Appointment.yaml configuration to be comprehensive and fully aligned with FHIR R5 specification, following the same pattern established for Patient configuration.

### Update Results
- ✅ **Search Parameters**: 12 → **23 parameters** (added 11 new)
- ✅ **Denormalization Rules**: 6 → **15 rules** (added 9 new)
- ✅ **MongoDB Indexes**: 10 → **21 indexes** (added 11 new)
- ✅ **extractType Support**: Added to all reference parameters
- ✅ **Comprehensive Comments**: Replaced "transformation" and "description" with clear "comment" fields

---

## Search Parameters (23 Total)

### Reference Parameters (11)
| Parameter | Status | Description |
|-----------|--------|-------------|
| actor | ✅ Enhanced | Any participant in appointment (added actorTypes field) |
| patient | ✅ Exists | Patient participant |
| practitioner | ✅ Exists | Practitioner participant |
| location | ✅ Exists | Location participant |
| based-on | ✅ **NEW** | Service request appointment is allocated to assess |
| reason-reference | ✅ **NEW** | Reference to reason (Condition, Procedure, etc.) |
| service-type-reference | ✅ **NEW** | HealthcareService reference |
| slot | ✅ **NEW** | Slots filled by appointment |
| subject | ✅ **NEW** | Patient or Group associated with appointment |
| group | ✅ **NEW** | Group subject (alias for subject) |
| supporting-info | ✅ **NEW** | Additional supporting information |

### Token Parameters (8)
| Parameter | Status | Description |
|-----------|--------|-------------|
| status | ✅ Exists | Overall appointment status |
| appointment-type | ✅ Exists | Style of appointment booked |
| service-type | ✅ Enhanced | Specific service to be performed (updated for CodeableReference) |
| service-category | ✅ **NEW** | Broad service categorization |
| specialty | ✅ Exists | Practitioner specialty required |
| identifier | ✅ Exists | Appointment identifier |
| part-status | ✅ **NEW** | Participant status (accepted, declined, tentative, needs-action) |
| reason-code | ✅ **NEW** | Coded reason for appointment |

### Date Parameters (2)
| Parameter | Status | Description |
|-----------|--------|-------------|
| date | ✅ Exists | Appointment date/time |
| requested-period | ✅ **NEW** | Requested period for appointment |

### Common Parameters (2)
| Parameter | Status | Description |
|-----------|--------|-------------|
| _id | ✅ Exists | Logical resource ID |
| _lastUpdated | ✅ Exists | Last update timestamp |

---

## Denormalization Configuration (15 Rules)

### CodeableConcept Extractors (5)
1. **appointmentType** - Extract appointment type codes and system|code pairs
2. **serviceType** - Extract service type from CodeableReference
3. **serviceCategory** ✅ **NEW** - Extract service category codes
4. **specialty** - Extract specialty codes
5. **reasonCode** ✅ **NEW** - Extract reason codes from CodeableReference

### Identifier Extractor (1)
6. **identifier** - Extract identifier values and system|value pairs

### Reference Extractors (8)
7. **participant** - Extract actor IDs, types, and specific participant types (Patient, Practitioner, Location) with participation status
8. **basedOn** ✅ **NEW** - Extract IDs and types from basedOn references
9. **reasonReference** ✅ **NEW** - Extract IDs and types from reason references
10. **serviceTypeReference** ✅ **NEW** - Extract IDs from HealthcareService references
11. **slot** ✅ **NEW** - Extract slot IDs
12. **subject** ✅ **NEW** - Extract subject ID and type (Patient/Group)
13. **supportingInformation** ✅ **NEW** - Extract supporting info IDs

### Period Extractors (2)
14. **period** - Extract appointment start/end period
15. **requestedPeriod** ✅ **NEW** - Extract requested period array

---

## Key Enhancements

### 1. extractType Support for References
All reference denormalization rules now use the `extractType` field (matching Patient config pattern):
```yaml
- source_path: participant[*].actor.reference
  target_field: actorIds
  datatype: array[string]
  extractType: id  # ← NEW: Explicitly specifies ID extraction
  comment: "IDs extracted from all participant actor references"

- source_path: participant[*].actor.reference
  target_field: actorTypes
  datatype: array[string]
  extractType: type  # ← NEW: Explicitly specifies type extraction
  comment: "Resource types from all participant actor references"
```

### 2. Updated Field Mapping Documentation
Replaced verbose "transformation" and "description" fields with concise "comment" fields:

**Before:**
```yaml
transformation: Extract IDs from all participant actor references
description: Array of all participant actor IDs
```

**After:**
```yaml
comment: "IDs extracted from all participant actor references"
```

### 3. CodeableReference Support
Updated serviceType and reason extractors to handle FHIR R5 CodeableReference datatype:
```yaml
- source_path: serviceType[*].concept.coding[*].code
  target_field: serviceType_codes
  datatype: array[string]
  comment: "Array of service type codes from CodeableReference"
```

### 4. Comprehensive MongoDB Indexes

#### High-Priority Single Field Indexes (7)
- `_search.patientId` (sparse)
- `_search.practitionerId` (sparse)
- `start` (date)
- `status` (code)
- `_search.locationId` (sparse)
- `_search.actorIds` (sparse)
- `_search.identifier_systemCode` (sparse)

#### Compound Indexes for Common Queries (4)
- `patientId + start` - Patient appointment timeline
- `practitionerId + start` - Practitioner schedule
- `locationId + start` - Location schedule
- `status + start` - Status-based date queries

#### Token Field Indexes (4)
- `_search.appointmentType_codes` (sparse)
- `_search.serviceType_codes` (sparse)
- `_search.specialty_codes` (sparse)
- `_search.participantStatus` (sparse)

#### Date Range Indexes (1)
- `start + end` - Period overlap queries

#### Reference Lookup Indexes (3)
- `_search.basedOnId` (sparse)
- `_search.subjectId` (sparse)
- `_search.slotId` (sparse)

#### Metadata Indexes (2)
- `meta.lastUpdated` - Change tracking
- `id` - Primary key (unique)

---

## Alignment with Patient Config

The updated Appointment config now follows the same patterns as Patient config:

| Feature | Patient Config | Appointment Config | Status |
|---------|---------------|-------------------|--------|
| extractType for references | ✅ | ✅ | Aligned |
| Comment-based documentation | ✅ | ✅ | Aligned |
| Comprehensive search parameters | 22 params | 23 params | Aligned |
| Sparse indexes | ✅ | ✅ | Aligned |
| CodeableConcept extraction | ✅ | ✅ | Aligned |
| Identifier extraction | ✅ | ✅ | Aligned |
| Period extraction | ✅ | ✅ | Aligned |

---

## FHIR R5 Compliance

### Coverage by Parameter Type

| Type | FHIR R5 Spec | Configured | Coverage |
|------|-------------|-----------|----------|
| Reference | 11 | 11 | 100% ✅ |
| Token | 8 | 8 | 100% ✅ |
| Date | 2 | 2 | 100% ✅ |
| Common | 2 | 2 | 100% ✅ |
| **Total** | **23** | **23** | **100%** ✅ |

### Search Parameter Validation
All 23 FHIR R5 Appointment search parameters from the specification are now configured:
- ✅ All reference parameters with proper extractType
- ✅ All token parameters with systemCode and code fields
- ✅ All date parameters with proper field mappings
- ✅ Common parameters (_id, _lastUpdated)

---

## Testing Recommendations

### 1. Reference Parameter Tests
```python
# Test actor search (any participant)
query = converter.convert('Appointment', 'actor=Practitioner/123')

# Test patient search
query = converter.convert('Appointment', 'patient=Patient/456')

# Test based-on search (NEW)
query = converter.convert('Appointment', 'based-on=ServiceRequest/789')
```

### 2. Token Parameter Tests
```python
# Test part-status search (NEW)
query = converter.convert('Appointment', 'part-status=accepted')

# Test service-category search (NEW)
query = converter.convert('Appointment', 'service-category=http://example.org|cardiology')

# Test reason-code search (NEW)
query = converter.convert('Appointment', 'reason-code=http://snomed.info/sct|182836005')
```

### 3. Date Parameter Tests
```python
# Test date search with period
query = converter.convert('Appointment', 'date=ge2026-05-01&date=le2026-05-31')

# Test requested-period search (NEW)
query = converter.convert('Appointment', 'requested-period=2026-06')
```

### 4. Denormalization Tests
Test that denormalization creates proper _search fields for:
- basedOn references → _search.basedOnId, _search.basedOnType
- subject reference → _search.subjectId, _search.subjectType
- reason codes → _search.reasonCode_codes, _search.reasonCode_systemCode
- participation status → _search.participantStatus
- requested periods → _search.requestedPeriod

---

## Next Steps

### Immediate
1. ✅ **DONE**: Update Appointment.yaml with all FHIR R5 parameters
2. ✅ **DONE**: Add extractType to all reference denormalization
3. ✅ **DONE**: Add comprehensive MongoDB indexes

### Follow-Up
1. **Create Integration Tests**: Similar to test_patient_comprehensive.py
   - Test all 23 search parameters
   - Test all FHIR modifiers
   - Test denormalization for all extractors
   - Target: 197 tests (same as Patient)

2. **Validate with Real Data**: 
   - Test with sample Appointment resources
   - Verify CodeableReference handling for serviceType and reason
   - Verify participant extraction with multiple actor types

3. **Performance Testing**:
   - Benchmark appointment queries with date ranges
   - Test compound indexes for patient/practitioner schedules
   - Validate sparse index usage

4. **Documentation**:
   - Add Appointment search examples to README
   - Document CodeableReference handling
   - Create appointment scheduling query patterns guide

---

## Summary

The Appointment configuration has been **comprehensively updated** to:
- ✅ Match FHIR R5 specification 100% (23/23 parameters)
- ✅ Follow Patient config patterns (extractType, comments, indexes)
- ✅ Support all reference types with proper extraction
- ✅ Include 21 optimized MongoDB indexes
- ✅ Handle FHIR R5 CodeableReference datatype
- ✅ Support appointment scheduling workflows

**Status**: ✅ **READY FOR TESTING**
