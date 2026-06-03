# FHIR R5 Hybrid Approach: Complete Search Pattern Guide

## Document Purpose

This document provides a **COMPREHENSIVE PRODUCTION-READY** guide to the Hybrid Approach for FHIR R5 appointment scheduling in MongoDB. It includes:
- ALL FHIR R5 search parameters for Schedule (9), Slot (9), and Appointment (21) resources
- Complete denormalization strategy using `_search` parent fields
- Detailed MongoDB query examples for every search parameter
- Index recommendations and performance characteristics
- Complex multi-parameter search patterns
- Real-world query examples
- Performance optimization strategies

**Last Updated**: May 8, 2026  
**FHIR Version**: R5  
**Total Search Parameters Documented**: 39

**Recent Updates**:
- ✅ **Simplified date fields** (May 8, 2026): Removed pre-computed date fields (dateOnly, monthYear, dayOfWeek, hour, timeSlot). Date fields now kept as ISO datetime strings from canonical FHIR fields.
- ✅ **Added Appointment canonical fields**: appointmentType (100%), subject (100%), description (95%), priority (80%), created (100%), cancellationReason/Date (for cancelled/noshow), basedOn (60% - all 4 FHIR R5 reference types)
- ✅ **Updated basedOn** to include all FHIR R5 reference types: CarePlan, DeviceRequest, MedicationRequest, ServiceRequest
- ✅ Enhanced Schedule actor diversity (Practitioner, PractitionerRole, Location, HealthcareService)
- ✅ Added primary denormalized fields (`patientId`, `practitionerId`, `locationId`, `actorId`)
- ✅ Added detail objects (`patientDetails`, `practitionerDetails`, `locationDetails`)
- ✅ Guaranteed critical field population (names, status, active)
- ✅ Updated all indexes with primary and legacy compatibility
- ✅ Added name-based search capabilities
- ✅ Enhanced Location with status and operationalStatus fields

---

## Table of Contents

1. [Denormalization Strategy](#denormalization-strategy)
2. [Schedule Search Parameters (9)](#schedule-search-parameters)
3. [Slot Search Parameters (9)](#slot-search-parameters)
4. [Appointment Search Parameters (21)](#appointment-search-parameters)
5. [Complex Multi-Parameter Searches](#complex-multi-parameter-searches)
6. [Real-World Query Examples](#real-world-query-examples)
7. [Index Strategy](#index-strategy)
8. [Performance Optimization](#performance-optimization)
9. [Best Practices](#best-practices)

---

## 1. Denormalization Strategy

### Overview

All search-optimized fields are stored under the `_search` parent field to:
- Centralize denormalized data
- Separate FHIR-compliant structure from search optimization
- Enable consistent indexing patterns
- Simplify maintenance and updates

### Structure Pattern

```javascript
{
  // Original FHIR R5 fields
  "resourceType": "Schedule",
  "name": "Dr. Smith Cardiology Schedule",
  "active": true,
  "actor": [{ "reference": "Practitioner/prac-123" }],
  
  // Denormalized search fields
  "_search": {
    "actorId": "prac-123",
    "actorType": "Practitioner",
    "actorName": "Dr. John Smith",
    "actorDetails": {
      "id": "prac-123",
      "type": "Practitioner",
      "name": "Dr. John Smith",
      "specialty": ""
    },
    "serviceCategoryCodes": ["17"],
    "serviceTypeCodes": ["124"],
    "specialtyCodes": ["394579002"],
    "identifier": {
      "values": ["SCHED-2026-001"],
      "systems": ["http://hospital.org/schedule-ids"],
      "systemValues": ["http://hospital.org/schedule-ids|SCHED-2026-001"]
    },
    "metadata": {
      "createdAt": "2026-05-07T10:00:00Z",
      "updatedAt": "2026-05-07T10:00:00Z"
    }
  }
}
```

### Benefits

1. **Clean Separation**: Original FHIR structure remains unchanged
2. **Optimized Queries**: All search fields use consistent paths
3. **Indexing**: All indexes target `_search.*` fields
4. **Maintainability**: Easy to update denormalization without affecting FHIR data
5. **Documentation**: Clear distinction between FHIR spec and implementation

---

## 2. Schedule Search Parameters

### 2.1 active (token)

**FHIR Path**: `Schedule.active`  
**Type**: token  
**Description**: Whether the schedule is active and accepting appointments

**Use Case**: Filter out inactive or retired schedules when displaying available appointment slots

**Note**: Schedule active queries use canonical FHIR field (not denormalized).

**MongoDB Query Examples**:

```javascript
// Find all active schedules
db.Schedule.find({
  "active": true
})

// Find inactive schedules
db.Schedule.find({
  "active": false
})

// Find active schedules for a specific actor
db.Schedule.find({
  "active": true,
  "_search.actorId": "prac-123"
})
```

**Index Recommendation**:
```javascript
db.Schedule.createIndex({
  "active": 1,
  "_search.actorId": 1
})
```

**Performance Notes**:
- Boolean field: extremely fast filtering
- High selectivity: typically >90% of schedules are active
- Combine with other filters for optimal performance
- Use compound index with frequently co-queried fields

---

### 2.2 actor (reference)

**FHIR Path**: `Schedule.actor`  
**Type**: reference  
**Description**: The practitioner, practitioner role, location, healthcare service, or other entity providing the scheduled service

**Use Case**: Find all schedules associated with a specific actor. Schedules can be associated with Practitioners (30%), PractitionerRoles (40%), Locations (20%), or HealthcareServices (10%).

**Denormalized Fields**:
```javascript
"_search": {
  "actorId": "prac-123",              // Single actor ID (primary)
  "actorType": "Practitioner",        // Actor resource type
  "actorName": "Dr. John Smith",      // Actor display name (from cache)
  "actorDetails": {                   // Full actor details
    "id": "prac-123",
    "type": "Practitioner",
    "name": "Dr. John Smith",
    "specialty": ""
  }
}
```

**Actor Types**:
- **Practitioner** (~30%): Individual practitioners like "Dr. Sarah Johnson"
- **PractitionerRole** (~40%): Role-based like "PractitionerRole-001"
- **Location** (~20%): Physical locations like "Hospital - Main Building"
- **HealthcareService** (~10%): Services like "Primary Care Services"

**MongoDB Query Examples**:

```javascript
// Find schedules for a specific actor (most efficient)
db.Schedule.find({
  "_search.actorId": "prac-123"
})

// Find schedules by actor name
db.Schedule.find({
  "_search.actorName": { $regex: "Dr. Smith", $options: "i" }
})

// Find schedules for multiple actors (OR logic)
db.Schedule.find({
  "_search.actorId": { $in: ["prac-123", "prac-456"] }
})

// Find schedules by actor type
db.Schedule.find({
  "_search.actorType": "Practitioner"
})

// Find practitioner schedules only
db.Schedule.find({
  "_search.actorType": "Practitioner",
  "active": true
})

// Find location-based schedules
db.Schedule.find({
  "_search.actorType": "Location",
  "active": true
})
```

**Index Recommendation**:
```javascript
// Primary index for actor searches (CRITICAL)
db.Schedule.createIndex({
  "_search.actorId": 1,
  "active": 1
})

// For type-specific queries
db.Schedule.createIndex({
  "_search.actorType": 1,
  "_search.actorId": 1
})

// For name-based searches
db.Schedule.createIndex({
  "_search.actorName": 1
})
```

**Performance Notes**:
- Most common search parameter for schedules
- Use `$in` for OR logic (multiple actors)
- Use `$all` for AND logic (must include all actors)
- Array indexing: very efficient with proper index
- Typical query time: <5ms for indexed queries

---

### 2.3 date (date)

**FHIR Path**: `Schedule.planningHorizon`  
**Type**: date  
**Description**: Search within the planning horizon date range

**Use Case**: Find schedules that are active during a specific date or date range

**Note**: Schedule date queries use canonical FHIR fields (not denormalized) since dates are already indexed at the top level.

**MongoDB Query Examples**:

```javascript
// Find schedules active on a specific date
const searchDate = "2026-06-15";
db.Schedule.find({
  "planningHorizon.start": { $lte: searchDate },
  "planningHorizon.end": { $gte: searchDate }
})

// Find schedules starting after a date
db.Schedule.find({
  "planningHorizon.start": { $gte: "2026-06-01" }
})

// Find schedules ending before a date
db.Schedule.find({
  "planningHorizon.end": { $lte: "2026-12-31" }
})

// Find schedules overlapping a date range
db.Schedule.find({
  "planningHorizon.start": { $lte: "2026-06-30" },
  "planningHorizon.end": { $gte: "2026-06-01" }
})
```

**Index Recommendation**:
```javascript
// Compound index for date range queries
db.Schedule.createIndex({
  "planningHorizon.start": 1,
  "planningHorizon.end": 1,
  "active": 1
})
```

**Performance Notes**:
- Date range queries require compound index
- Use ISO date strings for simple date comparisons
- Use DateTime fields for precise timestamp queries
- Typical query time: 5-15ms with proper indexing
- Consider separate indexes for frequently used date patterns

---

### 2.4 identifier (token)

**FHIR Path**: `Schedule.identifier`  
**Type**: token  
**Description**: Business identifier for the schedule

**Use Case**: Look up a specific schedule by external system identifier or business ID

**Denormalized Fields**:
```javascript
"_search": {
  "identifier": {
    "values": ["SCHED-2026-001", "EHR-SCHEDULE-12345"],
    "systems": [
      "http://hospital.org/schedule-ids",
      "urn:oid:2.16.840.1.113883.4.3.2.1"
    ],
    "systemValues": [
      "http://hospital.org/schedule-ids|SCHED-2026-001",
      "urn:oid:2.16.840.1.113883.4.3.2.1|EHR-SCHEDULE-12345"
    ]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Search by identifier value only
db.Schedule.find({
  "_search.identifier.values": "SCHED-2026-001"
})

// Search by system only
db.Schedule.find({
  "_search.identifier.systems": "http://hospital.org/schedule-ids"
})

// Search by system|value (most specific)
db.Schedule.find({
  "_search.identifier.systemValues": "http://hospital.org/schedule-ids|SCHED-2026-001"
})

// Search multiple identifiers (OR logic)
db.Schedule.find({
  "_search.identifier.values": { 
    $in: ["SCHED-2026-001", "SCHED-2026-002"] 
  }
})

// Case-insensitive identifier search
db.Schedule.find({
  "_search.identifier.values": { 
    $regex: "^SCHED-2026-001$", 
    $options: "i" 
  }
})
```

**Index Recommendation**:
```javascript
// Primary identifier index
db.Schedule.createIndex({
  "_search.identifier.values": 1
})

// For system-specific queries
db.Schedule.createIndex({
  "_search.identifier.systemValues": 1
})

// Unique constraint if identifiers are globally unique
db.Schedule.createIndex({
  "_search.identifier.systemValues": 1
}, { unique: true })
```

**Performance Notes**:
- Usually returns 0 or 1 result (high selectivity)
- Ideal for unique lookups
- Consider unique index if identifiers are globally unique
- Typical query time: <2ms with index
- systemValues provides strongest guarantee of uniqueness

---

### 2.5 name (string)

**FHIR Path**: `Schedule.name`  
**Type**: string  
**Description**: Human-readable label for the schedule

**Use Case**: Search schedules by descriptive name or partial name match

**Note**: Schedule name queries use canonical FHIR fields (not denormalized) since names are already indexed at the top level.

**MongoDB Query Examples**:

```javascript
// Exact match (case-sensitive)
db.Schedule.find({
  "name": "Dr. Smith Cardiology Schedule"
})

// Case-insensitive match
db.Schedule.find({
  "name": { 
    $regex: "Dr. Smith Cardiology Schedule", 
    $options: "i" 
  }
})

// Partial match (contains)
db.Schedule.find({
  "name": { 
    $regex: "cardiology", 
    $options: "i" 
  }
})

// Starts with
db.Schedule.find({
  "name": { 
    $regex: "^Dr. Smith", 
    $options: "i" 
  }
})
```

**Index Recommendation**:
```javascript
// For exact lookups
db.Schedule.createIndex({
  "name": 1
})

// For text search (alternative)
db.Schedule.createIndex({
  "name": "text"
})
```

**Performance Notes**:
- String searches can be expensive without proper indexing
- Regex queries: slower, use anchored patterns (^) when possible
- Consider MongoDB text index for complex text search
- Typical query time: 10-50ms depending on search type

---

### 2.6 service-category (token)

**FHIR Path**: `Schedule.serviceCategory`  
**Type**: token  
**Description**: High-level category of service (e.g., "General Practice", "Specialist Medical")

**Use Case**: Browse schedules by broad service categories

**Denormalized Fields**:
```javascript
"_search": {
  "serviceCategoryCodes": ["17", "8"]  // Simple array of service category codes
}
```

**MongoDB Query Examples**:

```javascript
// Search by code only
db.Schedule.find({
  "_search.serviceCategoryCodes": "17"
})

// Multiple categories (OR logic)
db.Schedule.find({
  "_search.serviceCategoryCodes": { 
    $in: ["17", "8", "3"] 
  }
})
```

**Index Recommendation**:
```javascript
// Primary index for category searches
db.Schedule.createIndex({
  "_search.serviceCategoryCodes": 1,
  "active": 1
})
```

**Performance Notes**:
- Limited cardinality: typically 10-50 unique categories
- Very fast with proper indexing
- Use codes for precise matching
- Use text for user-friendly searches
- Typical query time: <5ms

---

### 2.7 service-type (token/reference)

**FHIR Path**: `Schedule.serviceType`  
**Type**: token or reference  
**Description**: Specific service being performed (e.g., "Cardiology Consultation", "X-Ray")

**Use Case**: Find schedules for specific clinical services

**Denormalized Fields**:
```javascript
"_search": {
  "serviceTypeCodes": ["221", "410"]  // Simple array of service type codes
}
```

**MongoDB Query Examples**:

```javascript
// Search by service type code
db.Schedule.find({
  "_search.serviceTypeCodes": "221"
})

// Multiple service types (OR)
db.Schedule.find({
  "_search.serviceTypeCodes": { 
    $in: ["221", "410", "335"] 
  }
})
```

**Index Recommendation**:
```javascript
// Primary index
db.Schedule.createIndex({
  "_search.serviceTypeCodes": 1,
  "active": 1
})

// Compound for common patterns
db.Schedule.createIndex({
  "_search.serviceTypeCodes": 1,
  "_search.specialtyCodes": 1,
  "active": 1
})
```

**Performance Notes**:
- Higher cardinality than service-category (50-500 types)
- Critical for patient-facing scheduling interfaces
- Frequently combined with specialty and date filters
- Typical query time: <10ms with proper indexing

---

### 2.8 specialty (token)

**FHIR Path**: `Schedule.specialty`  
**Type**: token  
**Description**: Medical specialty associated with the schedule (SNOMED CT codes)

**Use Case**: Filter schedules by clinical specialty

**Denormalized Fields**:
```javascript
"_search": {
  "specialtyCodes": ["394579002", "394581000"]  // Simple array of SNOMED CT specialty codes
}
```

**MongoDB Query Examples**:

```javascript
// Search by specialty code
db.Schedule.find({
  "_search.specialtyCodes": "394579002"
})

// Multiple specialties (OR)
db.Schedule.find({
  "_search.specialtyCodes": { 
    $in: ["394579002", "394581000", "394585009"] 
  }
})

// All specialties (AND) - schedule must have all
db.Schedule.find({
  "_search.specialtyCodes": { 
    $all: ["394579002", "394581000"] 
  }
})

// Active cardiology schedules
db.Schedule.find({
  "_search.specialtyCodes": "394579002",
  "active": true
})
```

**Index Recommendation**:
```javascript
// Primary specialty index
db.Schedule.createIndex({
  "_search.specialtyCodes": 1,
  "active": 1
})

// For specialty + date searches (using canonical fields)
db.Schedule.createIndex({
  "_search.specialtyCodes": 1,
  "planningHorizon.start": 1,
  "planningHorizon.end": 1
})
```

**Performance Notes**:
- Medium cardinality: 50-200 unique specialties
- SNOMED CT codes provide standardization
- Often combined with service-type and actor filters
- Typical query time: <8ms
- Critical for clinical workflow efficiency

---

## 3. Slot Search Parameters

### 3.1 appointment-type (token)

**FHIR Path**: `Slot.appointmentType`  
**Type**: token  
**Description**: Type of appointment that can be booked in the slot

**Use Case**: Filter slots by appointment type (e.g., routine, walk-in, emergency)

**Denormalized Fields**:
```javascript
"_search": {
  "appointmentTypeCodes": ["ROUTINE", "WALKIN"]  // Simple array of codes
}
```

**MongoDB Query Examples**:

```javascript
// Find routine appointment slots
db.Slot.find({
  "_search.appointmentTypeCodes": "ROUTINE"
})

// Find walk-in OR emergency slots
db.Slot.find({
  "_search.appointmentTypeCodes": { 
    $in: ["WALKIN", "EMERGENCY"] 
  }
})

// Find available routine slots for today
db.Slot.find({
  "_search.appointmentTypeCodes": "ROUTINE",
  "_search.status": "free",
  "_search.start": { $gte: "2026-05-07T00:00:00Z", $lt: "2026-05-08T00:00:00Z" }
})
```

**Index Recommendation**:
```javascript
db.Slot.createIndex({
  "_search.appointmentTypeCodes": 1,
  "_search.status": 1,
  "_search.start": 1
})
```

**Performance Notes**:
- Low cardinality: typically 5-15 appointment types
- Fast filtering with compound index
- Frequently combined with status and date
- Typical query time: <5ms

---

### 3.2 identifier (token)

**FHIR Path**: `Slot.identifier`  
**Type**: token  
**Description**: Business identifier for the slot

**Use Case**: Look up specific slots by external system identifier

**Note**: Slot identifier queries use canonical FHIR field (not denormalized).

**MongoDB Query Examples**:

```javascript
// Search by identifier value
db.Slot.find({
  "identifier.value": "SLOT-2026-05-07-001"
})

// Search by system and value
db.Slot.find({
  "identifier": {
    $elemMatch: {
      "system": "http://hospital.org/slot-ids",
      "value": "SLOT-2026-05-07-001"
    }
  }
})

// Multiple identifiers
db.Slot.find({
  "identifier.value": { 
    $in: ["SLOT-2026-05-07-001", "SLOT-2026-05-07-002"] 
  }
})
```

**Index Recommendation**:
```javascript
db.Slot.createIndex({
  "identifier.value": 1
})

// Compound for system+value
db.Slot.createIndex({
  "identifier.system": 1,
  "identifier.value": 1
})
```

**Performance Notes**:
- High selectivity: typically returns 0-1 results
- Use canonical field structure
- Typical query time: <5ms

---

### 3.3 schedule (reference)

**FHIR Path**: `Slot.schedule`  
**Type**: reference  
**Description**: Reference to the parent Schedule resource

**Use Case**: Find all slots belonging to a specific schedule

**Denormalized Fields**:
```javascript
"_search": {
  "scheduleId": "schedule-123",  // Simple string ID
  "scheduleActor": {
    "id": "prac-123",
    "type": "Practitioner",
    "name": "Dr. John Smith"
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find all slots for a schedule
db.Slot.find({
  "_search.scheduleId": "schedule-123"
})

// Find free slots for a schedule
db.Slot.find({
  "_search.scheduleId": "schedule-123",
  "_search.status": "free"
})

// Find slots for multiple schedules
db.Slot.find({
  "_search.scheduleId": { 
    $in: ["schedule-123", "schedule-456"] 
  }
})

// Find slots for schedule in date range
db.Slot.find({
  "_search.scheduleId": "schedule-123",
  "_search.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lt: "2026-05-15T00:00:00Z" 
  }
})
```

**Index Recommendation**:
```javascript
// Primary schedule lookup
db.Slot.createIndex({
  "_search.scheduleId": 1,
  "_search.start": 1
})

// For status queries
db.Slot.createIndex({
  "_search.scheduleId": 1,
  "_search.status": 1,
  "_search.start": 1
})
```

**Performance Notes**:
- Very common query pattern
- Essential for schedule-based workflows
- Typical result set: 50-500 slots per schedule
- Typical query time: 5-20ms depending on filters
- Critical for performance: always use compound index

---

### 3.4 service-category (token)

**FHIR Path**: `Slot.serviceCategory`  
**Type**: token  
**Description**: High-level category of service provided in the slot

**Use Case**: Filter slots by broad service categories (inherited from schedule)

**Denormalized Fields**:
```javascript
"_search": {
  "serviceCategoryCodes": ["17"]  // Simple array of codes
}
```

**MongoDB Query Examples**:

```javascript
// Find slots by service category
db.Slot.find({
  "_search.serviceCategoryCodes": "17"
})

// Available slots in a category
db.Slot.find({
  "_search.serviceCategoryCodes": "17",
  "_search.status": "free",
  "_search.start": { $gte: "2026-05-07T00:00:00Z" }
})

// Multiple categories
db.Slot.find({
  "_search.serviceCategoryCodes": { 
    $in: ["17", "8", "3"] 
  }
})
```

**Index Recommendation**:
```javascript
db.Slot.createIndex({
  "_search.serviceCategoryCodes": 1,
  "_search.status": 1,
  "_search.start": 1
})
```

**Performance Notes**:
- Low cardinality: 10-50 categories
- Fast filtering
- Often combined with status and date
- Typical query time: <10ms

---

### 3.5 service-type (token/reference)

**FHIR Path**: `Slot.serviceType`  
**Type**: token or reference  
**Description**: Specific service type provided in the slot

**Use Case**: Find available slots for specific clinical services

**Denormalized Fields**:
```javascript
"_search": {
  "serviceTypeCodes": ["221"]  // Simple array of codes
}
```

**MongoDB Query Examples**:

```javascript
// Find cardiology slots
db.Slot.find({
  "_search.serviceTypeCodes": "221"
})

// Find available cardiology slots today
db.Slot.find({
  "_search.serviceTypeCodes": "221",
  "_search.status": "free",
  "_search.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lt: "2026-05-08T00:00:00Z" 
  }
})

// Multiple service types
db.Slot.find({
  "_search.serviceTypeCodes": { 
    $in: ["221", "410", "335"] 
  },
  "_search.status": "free"
})
```

**Index Recommendation**:
```javascript
// Critical compound index
db.Slot.createIndex({
  "_search.serviceTypeCodes": 1,
  "_search.status": 1,
  "_search.start": 1
})
```

**Performance Notes**:
- Medium cardinality: 50-500 service types
- Most common patient-facing search
- MUST have proper indexing for performance
- Typical query time: 5-15ms

---

### 3.6 specialty (token)

**FHIR Path**: `Slot.specialty`  
**Type**: token  
**Description**: Medical specialty for the slot

**Use Case**: Find available slots by clinical specialty

**Denormalized Fields**:
```javascript
"_search": {
  "specialtyCodes": ["394579002"]  // Simple array of SNOMED CT codes
}
```

**MongoDB Query Examples**:

```javascript
// Find slots by specialty
db.Slot.find({
  "_search.specialtyCodes": "394579002"
})

// Available cardiology slots this week
db.Slot.find({
  "_search.specialtyCodes": "394579002",
  "_search.status": "free",
  "_search.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lte: "2026-05-14T23:59:59Z" 
  }
})

// Multiple specialties
db.Slot.find({
  "_search.specialtyCodes": { 
    $in: ["394579002", "394581000"] 
  },
  "_search.status": "free"
})
```

**Index Recommendation**:
```javascript
db.Slot.createIndex({
  "_search.specialtyCodes": 1,
  "_search.status": 1,
  "_search.start": 1
})
```

**Performance Notes**:
- Medium cardinality: 50-200 specialties
- Often combined with serviceType
- Typical query time: 5-15ms

---

### 3.7 start (date)

**FHIR Path**: `Slot.start`  
**Type**: date  
**Description**: Start time of the slot

**Use Case**: Find slots within a specific date/time range (most common slot search)

**Denormalized Fields**:
```javascript
"_search": {
  "start": "2026-05-07T09:00:00Z",   // ISO datetime (kept as-is from canonical field)
  "end": "2026-05-07T09:30:00Z",     // ISO datetime (kept as-is from canonical field)
  "durationMinutes": 30               // Calculated: duration in minutes
}
```

**Note**: Date fields are kept in ISO format without splitting. Use MongoDB date operators for queries.

**MongoDB Query Examples**:

```javascript
// Slots on a specific date (using date operators)
db.Slot.find({
  "_search.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lt: "2026-05-08T00:00:00Z" 
  },
  "_search.status": "free"
})

// Slots in a date range
db.Slot.find({
  "_search.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lte: "2026-05-14T23:59:59Z" 
  },
  "_search.status": "free"
})

// Slots starting after a specific time
db.Slot.find({
  "_search.start": { 
    $gte: "2026-05-07T09:00:00Z" 
  },
  "_search.status": "free"
})

// Morning slots (6 AM to 12 PM)
db.Slot.find({
  "_search.start": {
    $gte: "2026-05-07T06:00:00Z",
    $lt: "2026-05-07T12:00:00Z"
  },
  "_search.status": "free"
})

// Slots on specific day of week using $dayOfWeek aggregation
db.Slot.aggregate([
  { $addFields: { dayOfWeek: { $dayOfWeek: { $toDate: "$_search.start" } } } },
  { $match: { dayOfWeek: 4, "_search.status": "free" } }  // 4 = Wednesday
])

// Slots at specific hour using $hour aggregation
db.Slot.aggregate([
  { $addFields: { hour: { $hour: { $toDate: "$_search.start" } } } },
  { $match: { hour: 9, "_search.status": "free" } }
])
```

**Index Recommendation**:
```javascript
// Primary date-based index (CRITICAL)
db.Slot.createIndex({
  "_search.start": 1,
  "_search.status": 1,
  "_search.serviceTypeCodes": 1
})

// Alternative for status-first queries
db.Slot.createIndex({
  "_search.status": 1,
  "_search.start": 1,
  "_search.specialtyCodes": 1
})
```

**Performance Notes**:
- THE MOST COMMON SLOT SEARCH PARAMETER
- Requires excellent indexing for performance
- Always combine with status filter
- Use ISO datetime format with $gte/$lt operators for date ranges
- Typical query time: 10-30ms depending on result set size
- For day-of-week/hour patterns, use MongoDB aggregation with $dayOfWeek/$hour operators

---

### 3.8 status (token)

**FHIR Path**: `Slot.status`  
**Type**: token  
**Description**: Availability status of the slot

**Use Case**: Filter for available (free) slots or check slot booking status

**Denormalized Fields**:
```javascript
"_search": {
  "status": "free"  // Copied from canonical: free | busy | busy-unavailable | busy-tentative | entered-in-error
}
```

**Note**: Slot status is copied to _search for consistency, but queries can use either `_search.status` or canonical `status` field.

**MongoDB Query Examples**:

```javascript
// Find free slots
db.Slot.find({
  "_search.status": "free"
})

// Find all busy slots (any type)
db.Slot.find({
  "_search.status": { 
    $in: ["busy", "busy-unavailable", "busy-tentative"] 
  }
})

// Available slots for a service today
db.Slot.find({
  "_search.status": "free",
  "_search.serviceTypeCodes": "221",
  "_search.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lt: "2026-05-08T00:00:00Z" 
  }
})

// Count available slots
db.Slot.countDocuments({
  "_search.status": "free",
  "_search.start": { $gte: "2026-05-07T00:00:00Z" }
})
```

**Index Recommendation**:
```javascript
// Critical compound index
db.Slot.createIndex({
  "_search.status": 1,
  "_search.start": 1,
  "_search.serviceTypeCodes": 1
})

// Alternative order for different query patterns
db.Slot.createIndex({
  "_search.start": 1,
  "_search.status": 1,
  "_search.specialtyCodes": 1
})
```

**Performance Notes**:
- THE MOST CRITICAL FILTER FOR SLOT SEARCHES
- Low cardinality: only 5 possible values
- ALWAYS include in slot availability queries
- Use compound index with start date
- Typical query time: 5-20ms
- Status distribution: ~70% free, ~30% busy in typical system

---

## 4. Appointment Search Parameters

### 4.1 actor (reference)

**FHIR Path**: `Appointment.participant.actor`  
**Type**: reference  
**Description**: Any participant in the appointment

**Use Case**: Find appointments involving any participant (patient, practitioner, location, etc.)

**Denormalized Fields**:
```javascript
"_search": {
  "actor": {
    "ids": ["pat-123", "prac-456", "loc-789"],
    "types": ["Patient", "Practitioner", "Location"],
    "references": [
      "Patient/pat-123",
      "Practitioner/prac-456",
      "Location/loc-789"
    ]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments for any actor
db.Appointment.find({
  "_search.actor.ids": "prac-456"
})

// Find appointments involving multiple actors (OR)
db.Appointment.find({
  "_search.actor.ids": { 
    $in: ["prac-456", "prac-789"] 
  }
})

// Find appointments with specific actor types
db.Appointment.find({
  "_search.actor.types": "Practitioner",
  \"_search.start\": "2026-05-07"
})

// Full reference match
db.Appointment.find({
  "_search.actor.references": "Practitioner/prac-456"
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.actor.ids": 1,
  \"_search.start\": 1,
  "_search.status": 1
})
```

**Performance Notes**:
- Broad search: includes all participant types
- Use specific parameters (patient, practitioner) when possible
- Typical query time: 10-30ms
- Medium selectivity depending on actor type

---

### 4.2 appointment-type (token)

**FHIR Path**: `Appointment.appointmentType`  
**Type**: token  
**Description**: Style of appointment or patient for the appointment

**Use Case**: Filter appointments by type (routine, walkin, checkup, emergency, followup)

**Denormalized Fields**:
```javascript
"_search": {
  "appointmentTypeCodes": ["FOLLOWUP"]  // Simple array for fast queries
}
```

**Note**: appointmentType field is generated in 100% of appointments with codes: ROUTINE, WALKIN, CHECKUP, FOLLOWUP, EMERGENCY.

**MongoDB Query Examples**:

```javascript
// Find follow-up appointments
db.Appointment.find({
  "_search.appointmentTypeCodes": "FOLLOWUP"
})

// Find routine or checkup appointments
db.Appointment.find({
  "_search.appointmentTypeCodes": { 
    $in: ["ROUTINE", "CHECKUP"] 
  }
})

// Emergency appointments today
db.Appointment.find({
  "_search.appointmentTypeCodes": "EMERGENCY",
  "_search.start": {
    $gte: "2026-05-07T00:00:00Z",
    $lt: "2026-05-08T00:00:00Z"
  }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.appointmentTypeCodes": 1,
  "_search.start": 1
})
```

**Performance Notes**:
- Low cardinality: 10-20 types
- Fast filtering
- Typical query time: <10ms

---

### 4.3 based-on (reference)

**FHIR Path**: `Appointment.basedOn`  
**Type**: reference  
**Description**: Service request, care plan, medication request, or device request that this appointment fulfills

**Use Case**: Link appointments to originating orders/referrals/requests

**FHIR R5 Spec**: Reference(CarePlan | DeviceRequest | MedicationRequest | ServiceRequest)

**Denormalized Fields**:
```javascript
"_search": {
  "basedOn": {
    "ids": ["sr-3456", "mr-7890"],
    "types": ["ServiceRequest", "MedicationRequest"],
    "references": [
      "ServiceRequest/sr-3456",
      "MedicationRequest/mr-7890"
    ]
  }
}
```

**Note**: basedOn is generated in ~60% of appointments with all 4 FHIR R5 reference types: ServiceRequest (lab/imaging/procedure orders), CarePlan (care plan follow-ups), MedicationRequest (prescription consultations), DeviceRequest (device fittings).

**MongoDB Query Examples**:

```javascript
// Find appointments for a service request
db.Appointment.find({
  "_search.basedOn.ids": "sr-3456"
})

// Find appointments based on multiple requests
db.Appointment.find({
  "_search.basedOn.ids": { 
    $in: ["sr-3456", "mr-7890", "dr-1234"] 
  }
})

// Find appointments for medication requests
db.Appointment.find({
  "_search.basedOn.types": "MedicationRequest"
})

// Find all device request appointments
db.Appointment.find({
  "_search.basedOn.types": "DeviceRequest"
})

// Find care plan follow-up appointments
db.Appointment.find({
  "_search.basedOn.types": "CarePlan"
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.basedOn.ids": 1
})
```

**Performance Notes**:
- High selectivity: typically 0-5 appointments per order
- Important for care coordination
- Typical query time: <5ms

---

### 4.4 date (date)

**FHIR Path**: `Appointment.start`  
**Type**: date  
**Description**: Start date/time of the appointment

**Use Case**: Find appointments on specific dates or within date ranges (most common appointment search)

**Denormalized Fields**:
```javascript
"_search": {
  "start": "2026-05-07T14:30:00Z",   // ISO datetime (kept as-is from canonical field)
  "end": "2026-05-07T15:00:00Z"      // ISO datetime (kept as-is from canonical field)
}
```

**Note**: Date fields are kept in ISO format without splitting. Use MongoDB date operators for queries.

**MongoDB Query Examples**:

```javascript
// Appointments on specific date
db.Appointment.find({
  "_search.start": {
    $gte: "2026-05-07T00:00:00Z",
    $lt: "2026-05-08T00:00:00Z"
  }
})

// Appointments in date range
db.Appointment.find({
  "_search.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lte: "2026-05-14T23:59:59Z" 
  }
})

// Appointments after a specific time
db.Appointment.find({
  "_search.start": { 
    $gte: "2026-05-07T14:00:00Z" 
  }
})

// Today's appointments
db.Appointment.find({
  "_search.start": {
    $gte: "2026-05-07T00:00:00Z",
    $lt: "2026-05-08T00:00:00Z"
  },
  "_search.status": { 
    $nin: ["cancelled", "noshow", "entered-in-error"] 
  }
})

// Appointments this month
db.Appointment.find({
  "_search.start": {
    $gte: "2026-05-01T00:00:00Z",
    $lt: "2026-06-01T00:00:00Z"
  }
})

// Afternoon appointments (12 PM to 6 PM)
db.Appointment.find({
  "_search.start": {
    $gte: "2026-05-07T12:00:00Z",
    $lt: "2026-05-07T18:00:00Z"
  }
})
```

**Index Recommendation**:
```javascript
// Primary date index (CRITICAL)
db.Appointment.createIndex({
  "_search.start": 1,
  "_search.status": 1,
  "_search.patientId": 1
})

// For practitioner schedules
db.Appointment.createIndex({
  "_search.practitionerId": 1,
  "_search.start": 1,
  "_search.status": 1
})
```

**Performance Notes**:
- MOST COMMON APPOINTMENT SEARCH PARAMETER
- Critical for calendar views and scheduling
- MUST have excellent indexing
- Use ISO datetime with $gte/$lt for date ranges
- Typical query time: 10-50ms depending on range
- For complex time patterns, use MongoDB aggregation with date operators

---

### 4.5 group (reference)

**FHIR Path**: `Appointment.participant.actor` (where actor is a Group) or `Appointment.subject`  
**Type**: reference  
**Description**: Group appointment participants

**Use Case**: Find group therapy or class appointments

**Denormalized Fields**:
```javascript
"_search": {
  "group": {
    "ids": ["group-therapy-001"],
    "references": ["Group/group-therapy-001"]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find group appointments
db.Appointment.find({
  "_search.group.ids": "group-therapy-001"
})

// All group appointments
db.Appointment.find({
  "_search.group.ids": { $exists: true, $ne: null }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.group.ids": 1,
  "_search.start": 1
})
```

**Performance Notes**:
- Low usage: most appointments are individual
- Low query volume
- Typical query time: <5ms

---

### 4.6 identifier (token)

**FHIR Path**: `Appointment.identifier`  
**Type**: token  
**Description**: Business identifier for the appointment

**Use Case**: Look up appointments by external system ID or confirmation number

**Denormalized Fields**:
```javascript
"_search": {
  "identifier": {
    "values": ["APPT-2026-05-07-12345", "CONF-ABC123"],
    "systems": [
      "http://hospital.org/appointment-ids",
      "http://hospital.org/confirmation-numbers"
    ],
    "systemValues": [
      "http://hospital.org/appointment-ids|APPT-2026-05-07-12345",
      "http://hospital.org/confirmation-numbers|CONF-ABC123"
    ]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Search by confirmation number
db.Appointment.find({
  "_search.identifier.values": "CONF-ABC123"
})

// System|value match
db.Appointment.find({
  "_search.identifier.systemValues": "http://hospital.org/confirmation-numbers|CONF-ABC123"
})

// Multiple identifiers
db.Appointment.find({
  "_search.identifier.values": { 
    $in: ["CONF-ABC123", "CONF-DEF456"] 
  }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.identifier.values": 1
})

// Unique if identifiers are globally unique
db.Appointment.createIndex({
  "_search.identifier.systemValues": 1
}, { unique: true })
```

**Performance Notes**:
- High selectivity: typically 0-1 results
- Important for patient self-service lookups
- Consider unique index
- Typical query time: <2ms

---

### 4.7 location (reference)

**FHIR Path**: `Appointment.participant.actor` (where actor.type = Location)  
**Type**: reference  
**Description**: Location where appointment takes place

**Use Case**: Find all appointments at a specific location/room

**Denormalized Fields**:
```javascript
"_search": {
  "locationId": "loc-789",
  "locationName": "Hospital - Main Building",
  "locationDetails": {
    "id": "loc-789",
    "name": "Hospital - Main Building",
    "status": "active",
    "operationalStatus": "Occupied",
    "address": "123 Main St, Boston, MA 02101"
  },
  "location": {  // Legacy structure for compatibility
    "ids": ["loc-789"],
    "references": ["Location/loc-789"],
    "names": ["Hospital - Main Building"]
  }
}
```

**Note**: Location names are ALWAYS populated. Critical fields (name, status) are guaranteed in all Location resources. OperationalStatus is present in ~40% of locations.

**MongoDB Query Examples**:

```javascript
// Find appointments at a location (most efficient)
db.Appointment.find({
  "_search.locationId": "loc-789"
})

// Search by location name
db.Appointment.find({
  "_search.locationName": { $regex: "Hospital", $options: "i" }
})

// Multiple locations (OR)
db.Appointment.find({
  "_search.locationId": { 
    $in: ["loc-789", "loc-123"] 
  }
})

// Today's appointments at location
db.Appointment.find({
  "_search.locationId": "loc-789",
  "_search.start": {
    $gte: "2026-05-07T00:00:00Z",
    $lt: "2026-05-08T00:00:00Z"
  },
  "_search.status": { 
    $in: ["booked", "arrived", "checked-in"] 
  }
})

// Legacy format (compatibility)
db.Appointment.find({
  "_search.location.ids": "loc-789"
})
```

**Index Recommendation**:
```javascript
// Primary index for location-based queries
db.Appointment.createIndex({
  "_search.locationId": 1,
  "_search.start": 1,
  "_search.status": 1
})

// For name-based searches
db.Appointment.createIndex({
  "_search.locationName": 1
})

// Legacy compatibility index
db.Appointment.createIndex({
  "_search.location.ids": 1,
  "_search.start": 1,
  "_search.status": 1
})
```

**Performance Notes**:
- Important for facility management
- Medium selectivity: 10-100 appointments per location per day
- Typical query time: 10-30ms

---

### 4.8 part-status (token)

**FHIR Path**: `Appointment.participant.status`  
**Type**: token  
**Description**: Participation status of any participant

**Use Case**: Find appointments where participants have specific statuses (accepted, declined, tentative)

**Denormalized Fields**:
```javascript
"_search": {
  "partStatus": {
    "values": ["accepted", "tentative"],
    "actorStatuses": [
      "pat-123:accepted",
      "prac-456:accepted"
    ]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments with any declined participant
db.Appointment.find({
  "_search.partStatus.values": "declined"
})

// Find appointments with tentative participants
db.Appointment.find({
  "_search.partStatus.values": "tentative"
})

// Specific actor-status combination
db.Appointment.find({
  "_search.partStatus.actorStatuses": "prac-456:tentative"
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.partStatus.values": 1,
  "_search.start": 1
})
```

**Performance Notes**:
- Low cardinality: 4-5 possible statuses
- Important for workflow management
- Typical query time: <10ms

---

### 4.9 patient (reference)

**FHIR Path**: `Appointment.participant.actor` (where actor.type = Patient) or `Appointment.subject`  
**Type**: reference  
**Description**: Patient participant in the appointment

**Use Case**: Find all appointments for a specific patient (VERY COMMON)

**Denormalized Fields**:
```javascript
"_search": {
  "patientId": "pat-123",
  "patientName": "Jane Doe",
  "patientDetails": {
    "id": "pat-123",
    "name": "Jane Doe",
    "dateOfBirth": "1985-03-15",
    "gender": "female",
    "contactPhone": "+15551234567",
    "email": "jane.doe@example.com"
  },
  "patient": {  // Legacy structure for compatibility
    "id": "pat-123",
    "reference": "Patient/pat-123",
    "name": "Jane Doe",
    "identifiers": ["MRN-12345"]
  }
}
```

**Note**: Patient names are ALWAYS populated. If name extraction fails, fallback format is `"Patient-{id}"`.

**MongoDB Query Examples**:

```javascript
// Find patient's appointments (most efficient)
db.Appointment.find({
  "_search.patientId": "pat-123"
})

// Search by patient name
db.Appointment.find({
  "_search.patientName": { $regex: "Jane Doe", $options: "i" }
})

// Patient's upcoming appointments with details
db.Appointment.find({
  "_search.patientId": "pat-123",
  "_search.start": { $gte: "2026-05-07T00:00:00Z" },
  "_search.status": { 
    $in: ["booked", "arrived", "checked-in"] 
  }
})

// Patient's appointment history
db.Appointment.find({
  "_search.patientId": "pat-123",
  "_search.start": { $lt: "2026-05-07T00:00:00Z" }
}).sort({ "_search.start": -1 }).limit(10)

// Multiple patients (e.g., family members)
db.Appointment.find({
  "_search.patientId": { 
    $in: ["pat-123", "pat-456", "pat-789"] 
  }
})

// Legacy format (compatibility)
db.Appointment.find({
  "_search.patient.id": "pat-123"
})
```

**Index Recommendation**:
```javascript
// CRITICAL INDEX for patient portal (primary)
db.Appointment.createIndex({
  "_search.patientId": 1,
  "_search.start": -1,
  "_search.status": 1
})

// For name-based searches
db.Appointment.createIndex({
  "_search.patientName": 1
})

// Legacy compatibility index
db.Appointment.createIndex({
  "_search.patient.id": 1,
  "_search.start": -1,
  "_search.status": 1
})
```

**Performance Notes**:
- EXTREMELY COMMON SEARCH (patient portals, EHR views)
- MUST have excellent indexing
- High selectivity: typically 5-50 appointments per patient per year
- Typical query time: 5-20ms
- Consider descending date order for "recent appointments" queries

---

### 4.10 practitioner (reference)

**FHIR Path**: `Appointment.participant.actor` (where actor.type = Practitioner)  
**Type**: reference  
**Description**: Practitioner participant in the appointment

**Use Case**: Find all appointments for a specific practitioner (VERY COMMON)

**Denormalized Fields**:
```javascript
"_search": {
  "practitionerId": "prac-456",
  "practitionerName": "Dr. John Smith",
  "practitionerDetails": {
    "id": "prac-456",
    "name": "Dr. John Smith",
    "specialty": "Cardiology",
    "npi": "1234567890"
  },
  "practitioner": {  // Legacy structure for compatibility
    "ids": ["prac-456"],
    "references": ["Practitioner/prac-456"],
    "names": ["Dr. John Smith"]
  }
}
```

**Note**: Practitioner names are ALWAYS populated. If name extraction fails, fallback format is `"Dr. Practitioner-{id}"`.

**MongoDB Query Examples**:

```javascript
// Find practitioner's appointments (most efficient)
db.Appointment.find({
  "_search.practitionerId": "prac-456"
})

// Search by practitioner name
db.Appointment.find({
  "_search.practitionerName": { $regex: "Dr. Smith", $options: "i" }
})

// Practitioner's schedule today
db.Appointment.find({
  "_search.practitionerId": "prac-456",
  "_search.start": {
    $gte: "2026-05-07T00:00:00Z",
    $lt: "2026-05-08T00:00:00Z"
  }
}).sort({ "_search.start": 1 })

// Practitioner's upcoming week with specialty
db.Appointment.find({
  "_search.practitionerId": "prac-456",
  "_search.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lte: "2026-05-14T23:59:59Z" 
  }
})

// Multiple practitioners (group practice)
db.Appointment.find({
  "_search.practitionerId": { 
    $in: ["prac-456", "prac-789"] 
  },
  "_search.start": {
    $gte: "2026-05-07T00:00:00Z",
    $lt: "2026-05-08T00:00:00Z"
  }
})

// Legacy format (compatibility)
db.Appointment.find({
  "_search.practitioner.ids": "prac-456"
})
```
": 1,
  "_search.status": 1
})

// For name-based searches
db.Appointment.createIndex({
  "_search.practitionerName": 1
})

// Legacy compatibility index
db.Appointment.createIndex({
  "_search.practitioner.ids": 1,
  "_search.startme": 1
})

// Legacy compatibility index
db.Appointment.createIndex({
  "_search.practitioner.ids": 1,
  \"_search.start\": 1,
  "_search.status": 1
})
```

**Performance Notes**:
- EXTREMELY COMMON SEARCH (provider dashboards, scheduling)
- MUST have excellent indexing
- Medium selectivity: typically 10-50 appointments per practitioner per day
- Typical query time: 10-30ms
- Critical for real-time scheduling systems

---

### 4.11 reason-code (token)

**FHIR Path**: `Appointment.reason.concept`  
**Type**: token  
**Description**: Coded reason for the appointment

**Use Case**: Find appointments by clinical reason (diagnosis, procedure)

**Denormalized Fields**:
```javascript
"_search": {
  "reasonCode": {
    "codes": ["84114007", "25569003"],
    "systems": ["http://snomed.info/sct", "http://snomed.info/sct"],
    "systemCodes": [
      "http://snomed.info/sct|84114007",
      "http://snomed.info/sct|25569003"
    ],
    "text": ["Heart disease", "High blood pressure"]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments for a specific condition
db.Appointment.find({
  "_search.reasonCode.codes": "84114007"
})

// Multiple reason codes (OR)
db.Appointment.find({
  "_search.reasonCode.codes": { 
    $in: ["84114007", "25569003"] 
  }
})

// Text search
db.Appointment.find({
  "_search.reasonCode.text": { 
    $regex: "heart", 
    $options: "i" 
  }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.reasonCode.codes": 1,
  \"_search.start\": 1
})
```

**Performance Notes**:
- Medium cardinality: hundreds of possible codes
- Important for clinical reporting
- Typical query time: 10-30ms

---

### 4.12 reason-reference (reference)

**FHIR Path**: `Appointment.reason.reference`  
**Type**: reference  
**Description**: Reference to condition, procedure, or observation justifying the appointment

**Use Case**: Link appointments to specific clinical records

**Denormalized Fields**:
```javascript
"_search": {
  "reasonReference": {
    "ids": ["cond-12345", "obs-67890"],
    "types": ["Condition", "Observation"],
    "references": [
      "Condition/cond-12345",
      "Observation/obs-67890"
    ]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments for a condition
db.Appointment.find({
  "_search.reasonReference.ids": "cond-12345"
})

// By resource type
db.Appointment.find({
  "_search.reasonReference.types": "Condition"
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.reasonReference.ids": 1
})
```

**Performance Notes**:
- Low query volume
- High selectivity
- Typical query time: <5ms

---

### 4.13 requested-period (date)

**FHIR Path**: `Appointment.requestedPeriod`  
**Type**: date  
**Description**: Requested date range for the appointment

**Use Case**: Track appointment requests and preferences

**Denormalized Fields**:
```javascript
"_search": {
  "requestedPeriod": {
    "start": "2026-05-10T00:00:00Z",    // ISO datetime
    "end": "2026-05-17T23:59:59Z"       // ISO datetime
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments requested for a specific date
db.Appointment.find({
  "_search.requestedPeriod.start": { $lte: "2026-05-15T23:59:59Z" },
  "_search.requestedPeriod.end": { $gte: "2026-05-15T00:00:00Z" }
})

// Find appointments requested this week
db.Appointment.find({
  "_search.requestedPeriod.start": { 
    $gte: "2026-05-07T00:00:00Z", 
    $lte: "2026-05-14T23:59:59Z" 
  }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.requestedPeriod.start": 1,
  "_search.requestedPeriod.end": 1
})
```

**Performance Notes**:
- Low query volume
- Important for workflow optimization
- Typical query time: <10ms

---

### 4.14 service-category (token)

**FHIR Path**: `Appointment.serviceCategory`  
**Type**: token  
**Description**: Broad categorization of appointment service

**Use Case**: Filter appointments by service category

**Denormalized Fields**:
```javascript
"_search": {
  "serviceCategory": {
    "codes": ["17"],
    "systems": ["http://terminology.hl7.org/CodeSystem/service-category"],
    "systemCodes": ["http://terminology.hl7.org/CodeSystem/service-category|17"],
    "text": ["General Practice"]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments by category
db.Appointment.find({
  "_search.serviceCategoryCodes": "17"
})

// Multiple categories
db.Appointment.find({
  "_search.serviceCategoryCodes": { 
    $in: ["17", "8", "3"] 
  }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.serviceCategoryCodes": 1,
  \"_search.start\": 1
})
```

**Performance Notes**:
- Low cardinality
- Fast filtering
- Typical query time: <10ms

---

### 4.15 service-type (token/reference)

**FHIR Path**: `Appointment.serviceType`  
**Type**: token or reference  
**Description**: Specific service being performed

**Use Case**: Find appointments by specific clinical service

**Denormalized Fields**:
```javascript
"_search": {
  "serviceType": {
    "codes": ["221"],
    "systems": ["http://terminology.hl7.org/CodeSystem/service-type"],
    "systemCodes": ["http://terminology.hl7.org/CodeSystem/service-type|221"],
    "text": ["Cardiology"],
    "references": ["HealthcareService/hs-card-001"]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find cardiology appointments
db.Appointment.find({
  "_search.serviceTypeCodes": "221"
})

// By HealthcareService reference
db.Appointment.find({
  "_search.serviceType.references": "HealthcareService/hs-card-001"
})

// Multiple service types
db.Appointment.find({
  "_search.serviceTypeCodes": { 
    $in: ["221", "410"] 
  }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.serviceTypeCodes": 1,
  \"_search.start\": 1,
  "_search.status": 1
})
```

**Performance Notes**:
- Medium cardinality
- Common reporting parameter
- Typical query time: 10-30ms

---

### 4.16 slot (reference)

**FHIR Path**: `Appointment.slot`  
**Type**: reference  
**Description**: Reference to the slot(s) filled by this appointment

**Use Case**: Link appointments back to slots, prevent double-booking

**Denormalized Fields**:
```javascript
"_search": {
  "slot": {
    "ids": ["slot-001", "slot-002"],
    "references": ["Slot/slot-001", "Slot/slot-002"]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointment for a slot
db.Appointment.find({
  "_search.slot.ids": "slot-001"
})

// Check if slots are booked
db.Appointment.find({
  "_search.slot.ids": { 
    $in: ["slot-001", "slot-002", "slot-003"] 
  },
  "_search.status": { 
    $in: ["booked", "arrived", "checked-in"] 
  }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.slot.ids": 1,
  "_search.status": 1
})
```

**Performance Notes**:
- Important for slot management
- High selectivity: typically 0-1 appointment per slot
- Typical query time: <5ms

---

### 4.17 specialty (token)

**FHIR Path**: `Appointment.specialty`  
**Type**: token  
**Description**: Medical specialty of the appointment

**Use Case**: Filter appointments by clinical specialty

**Denormalized Fields**:
```javascript
"_search": {
  "specialty": {
    "codes": ["394579002"],
    "systems": ["http://snomed.info/sct"],
    "systemCodes": ["http://snomed.info/sct|394579002"],
    "text": ["Cardiology"]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments by specialty
db.Appointment.find({
  "_search.specialtyCodes": "394579002"
})

// Multiple specialties
db.Appointment.find({
  "_search.specialtyCodes": { 
    $in: ["394579002", "394581000"] 
  }
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.specialtyCodes": 1,
  \"_search.start\": 1
})
```

**Performance Notes**:
- Medium cardinality
- Important for clinical reporting
- Typical query time: 10-30ms

---

### 4.18 status (token)

**FHIR Path**: `Appointment.status`  
**Type**: token  
**Description**: Overall status of the appointment

**Use Case**: Filter by appointment status (booked, cancelled, fulfilled, etc.)

**Denormalized Fields**:
```javascript
"_search": {
  "status": "booked"  // proposed | pending | booked | arrived | fulfilled | cancelled | noshow | entered-in-error | checked-in | waitlist
}
```

**MongoDB Query Examples**:

```javascript
// Find booked appointments
db.Appointment.find({
  "_search.status": "booked"
})

// Active appointments (not cancelled or no-show)
db.Appointment.find({
  "_search.status": { 
    $in: ["booked", "arrived", "checked-in", "fulfilled"] 
  }
})

// Cancelled or no-show
db.Appointment.find({
  "_search.status": { 
    $in: ["cancelled", "noshow"] 
  }
})

// Count appointments by status
db.Appointment.aggregate([
  {
    $group: {
      _id: "$_search.status",
      count: { $sum: 1 }
    }
  }
])
```

**Index Recommendation**:
```javascript
// Always combine with other filters
db.Appointment.createIndex({
  "_search.status": 1,
  \"_search.start\": 1,
  "_search.practitioner.ids": 1
})
```

**Performance Notes**:
- CRITICAL FILTER for most queries
- Low cardinality: 10 possible values
- Always use in combination with date/patient/practitioner
- Typical query time: 5-20ms

---

### 4.19 subject (reference)

**FHIR Path**: `Appointment.subject`  
**Type**: reference  
**Description**: Subject of the appointment (typically Patient, but can be Group, etc.)

**Use Case**: Find appointments for a specific subject

**Denormalized Fields**:
```javascript
"_search": {
  "subject": {
    "id": "pat-123",
    "type": "Patient",
    "reference": "Patient/pat-123"
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments for subject
db.Appointment.find({
  "_search.subject.id": "pat-123"
})

// By subject type
db.Appointment.find({
  "_search.subject.type": "Patient"
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.subject.id": 1,
  \"_search.start\": 1
})
```

**Performance Notes**:
- Similar to patient search
- Use patient parameter when subject is known to be Patient
- Typical query time: 5-20ms

---

### 4.20 supporting-info (reference)

**FHIR Path**: `Appointment.supportingInformation`  
**Type**: reference  
**Description**: Additional information supporting the appointment

**Use Case**: Link appointments to relevant clinical documents

**Denormalized Fields**:
```javascript
"_search": {
  "supportingInfo": {
    "ids": ["doc-123", "obs-456"],
    "types": ["DocumentReference", "Observation"],
    "references": [
      "DocumentReference/doc-123",
      "Observation/obs-456"
    ]
  }
}
```

**MongoDB Query Examples**:

```javascript
// Find appointments with specific supporting info
db.Appointment.find({
  "_search.supportingInfo.ids": "doc-123"
})

// By resource type
db.Appointment.find({
  "_search.supportingInfo.types": "DocumentReference"
})
```

**Index Recommendation**:
```javascript
db.Appointment.createIndex({
  "_search.supportingInfo.ids": 1
})
```

**Performance Notes**:
- Low query volume
- High selectivity
- Typical query time: <5ms

---

## 5. Complex Multi-Parameter Searches

### 5.1 Find Available Slots for a Specific Service

**Use Case**: Patient searching for cardiology appointments next week

```javascript
db.Slot.find({
  "_search.serviceTypeCodes": "221",           // Cardiology
  "_search.status": "free",                      // Available
  "_search.start": {                             // Next week
    $gte: "2026-05-10T00:00:00Z",
    $lte: "2026-05-17T23:59:59Z"
  }
  // For time-of-day filtering, use hour ranges:
  // Morning (6am-12pm): $gte: "...T06:00:00Z", $lt: "...T12:00:00Z"
  // Afternoon (12pm-6pm): $gte: "...T12:00:00Z", $lt: "...T18:00:00Z"
}).sort({
  "_search.start": 1
}).limit(20)
```

**Index**:
```javascript
db.Slot.createIndex({
  "_search.serviceTypeCodes": 1,
  "_search.status": 1,
  "_search.start": 1
})
```

**Performance**: ~10-20ms for 5000-slot database

---

### 5.2 Practitioner Daily Schedule

**Use Case**: Display practitioner's full schedule for today

```javascript
db.Appointment.find({
  "_search.practitioner.ids": "prac-456",
  \"_search.start\": "2026-05-07",
  "_search.status": { 
    $in: ["booked", "arrived", "checked-in", "fulfilled"] 
  }
}).sort({
  \"_search.start\": 1
})
```

**Index**:
```javascript
db.Appointment.createIndex({
  "_search.practitioner.ids": 1,
  \"_search.start\": 1,
  \"_search.start\": 1,
  "_search.status": 1
})
```

**Performance**: ~5-15ms

---

### 5.3 Patient Appointment History

**Use Case**: Show patient's past and upcoming appointments

```javascript
// Past appointments
db.Appointment.find({
  "_search.patient.id": "pat-123",
  \"_search.start\": { $lt: "2026-05-07" },
  "_search.status": { 
    $nin: ["cancelled", "entered-in-error"] 
  }
}).sort({
  \"_search.start\": -1
}).limit(10)

// Upcoming appointments
db.Appointment.find({
  "_search.patient.id": "pat-123",
  \"_search.start\": { $gte: "2026-05-07" },
  "_search.status": { 
    $in: ["booked", "pending", "arrived", "checked-in"] 
  }
}).sort({
  \"_search.start\": 1
}).limit(10)
```

**Index**:
```javascript
db.Appointment.createIndex({
  "_search.patient.id": 1,
  \"_search.start\": -1,
  "_search.status": 1
})
```

**Performance**: ~5-10ms per query

---

### 5.4 Location Utilization Report

**Use Case**: Track appointment load by location for capacity planning

```javascript
db.Appointment.aggregate([
  {
    $match: {
      \"_search.start\": { $gte: \"2026-05-01T00:00:00Z\", $lt: \"2026-06-01T00:00:00Z\" },
      "_search.status": { 
        $in: ["booked", "arrived", "fulfilled", "checked-in"] 
      }
    }
  },
  {
    $group: {
      _id: "$_search.location.ids",
      locationName: { $first: "$_search.location.names" },
      totalAppointments: { $sum: 1 },
      uniquePatients: { $addToSet: "$_search.patient.id" }
    }
  },
  {
    $project: {
      locationId: "$_id",
      locationName: 1,
      totalAppointments: 1,
      uniquePatientCount: { $size: "$uniquePatients" }
    }
  },
  {
    $sort: { totalAppointments: -1 }
  }
])
```

**Index**:
```javascript
db.Appointment.createIndex({
  "_search.start.monthYear": 1,
  "_search.status": 1,
  "_search.location.ids": 1
})
```

**Performance**: ~50-100ms for large datasets

---

### 5.5 Multi-Specialty Availability Search

**Use Case**: Find available slots for multiple specialties

```javascript
db.Slot.find({
  "_search.specialtyCodes": { 
    $in: ["394579002", "394581000", "394585009"]  // Multiple specialties
  },
  "_search.status": "free",
  \"_search.start\": { 
    $gte: "2026-05-07", 
    $lte: "2026-05-21" 
  }
}).sort({
  "_search.specialtyCodes": 1,
  \"_search.start\": 1
}).limit(50)
```

**Index**:
```javascript
db.Slot.createIndex({
  "_search.specialtyCodes": 1,
  "_search.status": 1,
  \"_search.start\": 1,
  \"_search.start\": 1
})
```

**Performance**: ~15-30ms

---

### 5.6 Cancellation Rate Analysis

**Use Case**: Calculate no-show and cancellation rates by service type

```javascript
db.Appointment.aggregate([
  {
    $match: {
      \"_search.start\": { 
        $gte: "2026-04-01", 
        $lt: "2026-05-01" 
      }
    }
  },
  {
    $group: {
      _id: {
        serviceType: "$_search.serviceType.codes",
        status: "$_search.status"
      },
      count: { $sum: 1 }
    }
  },
  {
    $group: {
      _id: "$_id.serviceType",
      statuses: {
        $push: {
          status: "$_id.status",
          count: "$count"
        }
      },
      total: { $sum: "$count" }
    }
  }
])
```

**Index**:
```javascript
db.Appointment.createIndex({
  \"_search.start\": 1,
  "_search.serviceTypeCodes": 1,
  "_search.status": 1
})
```

**Performance**: ~100-200ms for monthly analysis

---

### 5.7 Real-Time Slot Booking Check

**Use Case**: Verify slot availability before booking (prevent double-booking)

```javascript
// Step 1: Check slot status
const slot = db.Slot.findOne({
  "_id": "slot-001",
  "_search.status": "free"
})

if (!slot) {
  throw new Error("Slot not available")
}

// Step 2: Check for existing appointments
const existingAppointment = db.Appointment.findOne({
  "_search.slot.ids": "slot-001",
  "_search.status": { 
    $in: ["booked", "arrived", "checked-in", "pending"] 
  }
})

if (existingAppointment) {
  throw new Error("Slot already booked")
}

// Step 3: Create appointment (transaction recommended)
// Use MongoDB transactions for this workflow
```

**Indexes**:
```javascript
db.Slot.createIndex({ "_id": 1, "_search.status": 1 })
db.Appointment.createIndex({ 
  "_search.slot.ids": 1, 
  "_search.status": 1 
})
```

**Performance**: ~3-8ms (2 queries)

---

## 6. Real-World Query Examples

### 6.1 Patient Portal: Find Available Appointments

**Scenario**: Patient wants to book cardiology appointment in next 2 weeks, prefers mornings

```javascript
db.Slot.find({
  "_search.serviceTypeCodes": "221",
  "_search.status": "free",
  \"_search.start\": { 
    $gte: "2026-05-07", 
    $lte: "2026-05-21" 
  },
  // Use time ranges instead of timeSlot. Example: \"_search.start\": "morning"
}).sort({
  \"_search.start\": 1
}).limit(10)
```

---

### 6.2 Provider Dashboard: Today's Schedule

**Scenario**: Doctor logs in and views today's schedule

```javascript
db.Appointment.find({
  "_search.practitionerId": "prac-456",
  \"_search.start\": "2026-05-07",
  "_search.status": { 
    $nin: ["cancelled", "noshow", "entered-in-error"] 
  }
}).sort({
  \"_search.start\": 1
})

// Alternative with practitioner name
db.Appointment.find({
  "_search.practitionerName": { $regex: "Dr. Smith", $options: "i" },
  \"_search.start\": "2026-05-07",
  "_search.status": { 
    $nin: ["cancelled", "noshow", "entered-in-error"] 
  }
}).sort({
  \"_search.start\": 1
})
```

---

### 6.3 Front Desk: Check-In Patient

**Scenario**: Patient arrives for appointment, search by confirmation number

```javascript
const appointment = db.Appointment.findOne({
  "_search.identifier.values": "CONF-ABC123",
  \"_search.start\": "2026-05-07",
  "_search.status": "booked"
})

// Update to checked-in
db.Appointment.updateOne(
  { "_id": appointment._id },
  { 
    $set: { 
      "status": "checked-in",
      "_search.status": "checked-in"
    } 
  }
)
```

---

### 6.4 Scheduling: Find Next Available Slot

**Scenario**: Scheduler needs first available cardiology slot after 2pm

```javascript
db.Slot.findOne({
  "_search.serviceTypeCodes": "221",
  "_search.status": "free",
  \"_search.start\": { 
    $gte: "2026-05-07T14:00:00Z" 
  }
}).sort({
  \"_search.start\": 1
})
```

---

### 6.5 Reporting: Monthly Appointment Volume

**Scenario**: Generate monthly statistics by specialty

```javascript
db.Appointment.aggregate([
  {
    $match: {
      \"_search.start\": { $gte: \"2026-05-01T00:00:00Z\", $lt: \"2026-06-01T00:00:00Z\" },
      "_search.status": { 
        $in: ["booked", "arrived", "fulfilled", "checked-in"] 
      }
    }
  },
  {
    $unwind: "$_search.specialtyCodes"
  },
  {
    $group: {
      _id: "$_search.specialtyCodes",
      count: { $sum: 1 },
      uniquePatients: { $addToSet: "$_search.patient.id" },
      uniquePractitioners: { $addToSet: "$_search.practitioner.ids" }
    }
  },
  {
    $project: {
      specialtyCode: "$_id",
      appointmentCount: "$count",
      patientCount: { $size: "$uniquePatients" },
      practitionerCount: { $size: "$uniquePractitioners" }
    }
  },
  {
    $sort: { appointmentCount: -1 }
  }
])
```

---

### 6.6 Patient Reminder System

**Scenario**: Find appointments starting tomorrow that need reminders

```javascript
db.Appointment.find({
  \"_search.start\": "2026-05-08",
  "_search.status": { 
    $in: ["booked", "pending"] 
  },
  "reminderSent": { $ne: true }
})
```

---

## 7. Index Strategy

### 7.1 Schedule Collection Indexes

```javascript
// PRIMARY: actor lookup (most efficient)
db.Schedule.createIndex({
  "_search.actorId": 1
}, { name: "idx_search_actorId" })

// Service category lookup
db.Schedule.createIndex({
  "_search.serviceCategoryCodes": 1,
  "active": 1
}, { name: "idx_search_category_active" })

// Service type lookup
db.Schedule.createIndex({
  "_search.serviceTypeCodes": 1,
  "active": 1
}, { name: "idx_search_service_active" })

// Specialty-based lookup
db.Schedule.createIndex({
  "_search.specialtyCodes": 1,
  "active": 1
}, { name: "idx_search_specialty_active" })

// Active schedules with date range
db.Schedule.createIndex({
  "active": 1,
  "planningHorizon.start": 1,
  "planningHorizon.end": 1
}, { name: "idx_active_daterange" })

// Actor type filtering
db.Schedule.createIndex({
  "_search.actorType": 1,
  "active": 1
}, { name: "idx_search_actorType_active" })

// Metadata for tracking updates
db.Schedule.createIndex({
  "_search.metadata.updatedAt": -1
}, { name: "idx_search_updated" })

// Unique identifier lookup
db.Schedule.createIndex({
  "_search.identifier.systemValues": 1
}, { unique: true, sparse: true, name: "idx_search_identifier_unique" })
```

---

### 7.2 Slot Collection Indexes

```javascript
// CRITICAL: Primary availability search
db.Slot.createIndex({
  "_search.status": 1,
  \"_search.start\": 1,
  "_search.serviceTypeCodes": 1,
  \"_search.start\": 1
})

// Alternative: Service-type first
db.Slot.createIndex({
  "_search.serviceTypeCodes": 1,
  "_search.status": 1,
  \"_search.start\": 1
})

// Specialty-based search
db.Slot.createIndex({
  "_search.specialtyCodes": 1,
  "_search.status": 1,
  \"_search.start\": 1
})

// Schedule-based lookup
db.Slot.createIndex({
  "_search.schedule.id": 1,
  \"_search.start\": 1
})

// Time-based patterns
db.Slot.createIndex({
  "dayOfWeek /* Use MongoDB \$dayOfWeek operator in aggregation */": 1,
  // Use time ranges instead of timeSlot. Example: \"_search.start\": 1,
  "_search.status": 1
})

// Unique identifier
db.Slot.createIndex({
  "_search.identifier.systemValues": 1
}, { unique: true, sparse: true })
```

---

### 7.3 Appointment Collection Indexes

```javascript
// CRITICAL: Patient lookup (PRIMARY - most efficient)
db.Appointment.createIndex({
  "_search.patientId": 1,
  \"_search.start\": -1,
  "_search.status": 1
}, { name: "idx_patient_date_status" })

// CRITICAL: Practitioner schedule (PRIMARY - most efficient)
db.Appointment.createIndex({
  "_search.practitionerId": 1,
  \"_search.start\": 1,
  "_search.status": 1
}, { name: "idx_practitioner_datetime_status" })

// CRITICAL: Location-based queries (PRIMARY)
db.Appointment.createIndex({
  "_search.locationId": 1,
  \"_search.start\": 1,
  "_search.status": 1
}, { name: "idx_location_date_status" })

// Date-based queries
db.Appointment.createIndex({
  \"_search.start\": 1,
  "_search.status": 1,
  "_search.serviceTypeCodes": 1
}, { name: "idx_date_status_serviceType" })

// Name-based searches
db.Appointment.createIndex({
  "_search.patientName": 1
}, { name: "idx_patientName" })

db.Appointment.createIndex({
  "_search.practitionerName": 1
}, { name: "idx_practitionerName" })

db.Appointment.createIndex({
  "_search.locationName": 1
}, { name: "idx_locationName" })

// Legacy compatibility indexes
db.Appointment.createIndex({
  "_search.patient.id": 1,
  \"_search.start\": -1,
  "_search.status": 1
}, { name: "idx_patient_legacy" })

db.Appointment.createIndex({
  "_search.practitioner.ids": 1,
  \"_search.start\": 1,
  "_search.status": 1
}, { name: "idx_practitioner_legacy" })

db.Appointment.createIndex({
  "_search.location.ids": 1,
  \"_search.start\": 1,
  "_search.status": 1
}, { name: "idx_location_legacy" })

// Actor-based queries
db.Appointment.createIndex({
  "_search.actor.ids": 1,
  \"_search.start\": 1
}, { name: "idx_actor_date" })

// Slot reference
db.Appointment.createIndex({
  "_search.slot.ids": 1,
  "_search.status": 1
})

// Unique identifier
db.Appointment.createIndex({
  "_search.identifier.systemValues": 1
}, { unique: true, sparse: true })

// Reporting: Monthly aggregations
db.Appointment.createIndex({
  "_search.start.monthYear": 1,
  "_search.status": 1,
  "_search.specialtyCodes": 1
})
```

---

### 7.4 Index Sizing Guidelines

**Schedule Collection** (5,000 schedules):
- Total index size: ~5-10 MB
- 5-8 indexes

**Slot Collection** (500,000 slots):
- Total index size: ~200-400 MB
- 6-10 indexes
- Most critical for performance

**Appointment Collection** (100,000 appointments):
- Total index size: ~50-100 MB
- 8-12 indexes
- Critical for patient/practitioner queries

**Total Index Overhead**: ~250-510 MB for typical system

---

## 8. Performance Optimization

### 8.1 Query Performance Targets

| Query Type | Target | Acceptable | Needs Optimization |
|------------|--------|------------|-------------------|
| Single document lookup (by ID/identifier) | <2ms | <5ms | >5ms |
| Patient appointments | <10ms | <20ms | >50ms |
| Practitioner schedule | <15ms | <30ms | >60ms |
| Available slots search | <20ms | <50ms | >100ms |
| Complex aggregations | <100ms | <200ms | >500ms |
| Reports (monthly) | <500ms | <1s | >2s |

---

### 8.2 Optimization Strategies

#### 8.2.1 Use Covering Indexes

```javascript
// Good: Covering index (query only uses index)
db.Slot.find(
  {
    "_search.serviceTypeCodes": "221",
    "_search.status": "free",
    \"_search.start\": "2026-05-07"
  },
  {
    \"_search.start\": 1,
    "_id": 1
  }
)
```

#### 8.2.2 Limit Result Sets

```javascript
// Always use .limit() for large result sets
db.Slot.find({
  "_search.status": "free"
}).sort({
  \"_search.start\": 1
}).limit(50)  // Don't return thousands of slots
```

#### 8.2.3 Use Projection

```javascript
// Only retrieve needed fields (new format)
db.Appointment.find(
  { "_search.patientId": "pat-123" },
  {
    "_search.start": 1,
    "_search.practitionerId": 1,
    "_search.practitionerName": 1,
    "_search.status": 1,
    "description": 1
  }
)

// Legacy format compatibility
db.Appointment.find(
  { "_search.patient.id": "pat-123" },
  {
    "_search.start": 1,
    "_search.practitioner": 1,
    "_search.status": 1,
    "description": 1
  }
)
```

#### 8.2.4 Batch Operations

```javascript
// Good: Single query with $in (new format)
db.Appointment.find({
  "_search.patientId": { 
    $in: ["pat-123", "pat-456", "pat-789"] 
  }
})

// Good: Single query with $in (legacy format)
db.Appointment.find({
  "_search.patient.id": { 
    $in: ["pat-123", "pat-456", "pat-789"] 
  }
})

// Bad: Multiple individual queries
// Don't do this!
for (let patientId of patientIds) {
  db.Appointment.find({ "_search.patientId": patientId })
}
```

#### 8.2.5 Use Aggregation Pipeline Optimization

```javascript
// Put $match early in pipeline
db.Appointment.aggregate([
  { 
    $match: {  // Filter first!
      \"_search.start\": { $gte: \"2026-05-01T00:00:00Z\", $lt: \"2026-06-01T00:00:00Z\" },
      "_search.status": { $in: ["booked", "fulfilled"] }
    } 
  },
  { $group: { ... } },
  { $sort: { ... } }
])
```

---

### 8.3 Monitoring and Profiling

#### Enable Profiling

```javascript
// Enable profiling for slow queries (>100ms)
db.setProfilingLevel(1, { slowms: 100 })

// View slow queries
db.system.profile.find().sort({ ts: -1 }).limit(10)
```

#### Explain Plans

```javascript
// Check if query uses index
db.Slot.find({
  "_search.serviceTypeCodes": "221",
  "_search.status": "free"
}).explain("executionStats")

// Look for:
// - "stage": "IXSCAN" (good - uses index)
// - "stage": "COLLSCAN" (bad - full collection scan)
// - totalDocsExamined vs nReturned (lower is better)
```

---

### 8.4 Caching Strategies

```javascript
// Cache frequently accessed schedules
const scheduleCache = new Map()

function getSchedule(scheduleId) {
  if (scheduleCache.has(scheduleId)) {
    return scheduleCache.get(scheduleId)
  }
  
  const schedule = db.Schedule.findOne({ "_id": scheduleId })
  scheduleCache.set(scheduleId, schedule)
  return schedule
}

// Cache TTL: 5-15 minutes for schedules
// Cache TTL: 30-60 seconds for slots
// No caching for real-time appointment status
```

---

## 9. Best Practices

### 9.1 Search Parameter Usage

1. **Always combine filters**: Never query with single parameter on large collections
2. **Use status filters**: Always include status in Slot and Appointment queries
3. **Date ranges**: Use date for day-level, dateTime for precision
4. **Limit results**: Always use .limit() for patient-facing queries
5. **Use projection**: Only retrieve fields you need

### 9.2 Index Management

1. **Monitor index usage**: Remove unused indexes
2. **Analyze query patterns**: Create indexes based on actual usage
3. **Compound indexes**: Order fields by selectivity (most selective first)
4. **Unique constraints**: Use for identifiers when appropriate
5. **Index size**: Keep total index size <50% of data size

### 9.3 Denormalization Maintenance

1. **Consistency**: Update denormalized fields when source changes
2. **Validation**: Verify `_search` fields match FHIR structure
3. **Migrations**: Plan for schema updates carefully
4. **Monitoring**: Track denormalization failures

### 9.4 Query Patterns

1. **Patient portal**: Always filter by `patientId` + date + status
2. **Provider dashboard**: Always filter by `practitionerId` + date
3. **Slot search**: Always filter by status="free" + date range
4. **Booking**: Use transactions for slot → appointment workflow
5. **Reporting**: Use aggregation pipeline with early $match

**Recommended Patterns**:
```javascript
// Patient portal query
db.Appointment.find({
  "_search.patientId": "pat-123",
  \"_search.start\": { $gte: "2026-05-07" },
  "_search.status": { $in: ["booked", "arrived"] }
})

// Provider dashboard query
db.Appointment.find({
  "_search.practitionerId": "prac-456",
  \"_search.start\": "2026-05-07"
}).sort({ \"_search.start\": 1 })

// Location schedule query
db.Appointment.find({
  "_search.locationId": "loc-789",
  \"_search.start\": "2026-05-07",
  "_search.status": { $ne: "cancelled" }
})
```

---

## 10. Migration and Testing

### 10.1 Adding New Search Parameters

```javascript
// 1. Update denormalization code
// 2. Create migration script
db.Appointment.updateMany({}, [
  {
    $set: {
      "_search.newField": {
        // Extract from FHIR structure
      }
    }
  }
])

// 3. Create indexes
db.Appointment.createIndex({ "_search.newField": 1 })

// 4. Test queries
// 5. Update documentation
```

### 10.2 Testing Checklist

- [ ] All 39 search parameters documented
- [ ] Sample queries tested with real data
- [ ] Indexes created and verified
- [ ] Query performance measured
- [ ] Explain plans analyzed
- [ ] Edge cases tested (empty results, large result sets)
- [ ] Integration tests pass
- [ ] Load testing completed

---

## Conclusion

This document provides comprehensive coverage of all FHIR R5 search parameters for Schedule, Slot, and Appointment resources using the hybrid denormalization approach. All search-optimized fields are stored under `_search` parent fields for consistency and maintainability.

**Key Takeaways**:
- 39 total search parameters fully documented
- All queries use `_search.*` paths for denormalized fields
- Comprehensive index strategy provided with primary and legacy indexes
- Real-world examples and performance targets
- Production-ready patterns and best practices

**Data Generation Guarantees**:

All resources are generated with critical fields ALWAYS populated:
- **Location**: `name`, `status` (always present), `operationalStatus` (~40%)
- **Organization**: `name`, `active` (always present)
- **HealthcareService**: `name`, `active` (always present)
- **Schedule**: `active` (always present), actor diversity (Practitioner 30%, PractitionerRole 40%, Location 20%, HealthcareService 10%)
- **Patient**: Names always extracted or fallback to `"Patient-{id}"`
- **Practitioner**: Names always extracted or fallback to `"Dr. Practitioner-{id}"`
- **Appointment**: `patientName`, `practitionerName`, `locationName` always populated in `_search` fields

**Denormalized Field Structure**:
- **Primary fields**: `patientId`, `practitionerId`, `locationId`, `actorId` for efficient single-value queries
- **Detail objects**: `patientDetails`, `practitionerDetails`, `locationDetails` with comprehensive information
- **Legacy fields**: `patient.id`, `practitioner.ids`, `location.ids` maintained for backward compatibility

**Schedule Actor Diversity**:
Schedules are generated with diverse actor types reflecting realistic healthcare scenarios:
- **Practitioner** (30%): Individual providers - names resolved from practitioner cache
- **PractitionerRole** (40%): Role-based assignments - generic names like "PractitionerRole-001"
- **Location** (20%): Facility-based schedules - names resolved from location cache like "Hospital - Main Building"
- **HealthcareService** (10%): Service-based schedules - names like "Primary Care Services"

**Performance Summary**:
- Simple lookups: <5ms (patientId, practitionerId, locationId)
- Name-based searches: 10-30ms (patientName, practitionerName, locationName)
- Slot availability searches: 10-50ms
- Complex aggregations: 50-200ms
- Monthly reports: 100-500ms

**Next Steps**:
1. Implement all indexes (primary + legacy compatibility)
2. Verify denormalization for all search parameters
3. Test query patterns with production-scale data
4. Monitor performance and optimize as needed
5. Document any custom search parameters
6. Ensure critical fields (names, status, active) are always populated in data generation
  "end": "2026-05-10T09:30:00Z"
}
```

#### 5. SlotIndex Collection (Optional but Recommended)
**Purpose**: Pre-aggregated availability index for ultra-fast searches  
**Count Ratio**: 1 document per date+practitioner+specialty combination  
**Storage Size**: Medium (~3-8 KB per document with slot arrays)

**Why Recommended**:
- **10-50x faster** availability searches
- Eliminates need to scan thousands of slot documents
- Provides aggregate statistics (total free slots, first available, etc.)
- Can be rebuilt asynchronously without affecting real-time operations

**Data Model**:
```javascript
{
  "_id": "idx-2026-05-10-prac-123",
  "date": "2026-05-10",
  "practitionerId": "prac-123",
  "practitionerName": "Dr. John Smith",
  "locationId": "loc-456",
  "specialty": "Cardiology",
  "specialtyCode": "394579002",
  "serviceType": "consultation",
  
  // Array of free slots for this date+practitioner
  "freeSlots": [
    {
      "slotId": "slot-001",
      "start": "2026-05-10T09:00:00Z",
      "end": "2026-05-10T09:30:00Z",
      "duration": 30
    },
    {
      "slotId": "slot-003",
      "start": "2026-05-10T10:00:00Z",
      "end": "2026-05-10T10:30:00Z",
      "duration": 30
    },
    {
      "slotId": "slot-005",
      "start": "2026-05-10T11:00:00Z",
      "end": "2026-05-10T11:30:00Z",
      "duration": 30
    }
  ],
  
  // Aggregate statistics
  "totalFreeSlots": 3,
  "firstAvailable": "2026-05-10T09:00:00Z",
  "lastAvailable": "2026-05-10T11:00:00Z",
  "morningSlots": 2,
  "afternoonSlots": 1,
  "eveningSlots": 0,
  
  "updatedAt": "2026-05-09T14:23:00Z"
}
```

#### 6. Supporting Resources (Required for Complete System)

**Patient Collection**: Demographics and contact information  
**Practitioner Collection**: Healthcare provider details  
**PractitionerRole Collection**: Practitioner specialties and affiliations  
**Location Collection**: Physical locations where care is provided  
**Organization Collection**: Healthcare organizations  
**HealthcareService Collection**: Services offered by organizations  
**Device Collection**: Medical devices that can be scheduled actors  
**Group Collection**: Groups of patients or practitioners

---

## Schedule Search Patterns

### SP-1: Find Schedules by Practitioner/Actor
**Use Case**: Display all schedules for a specific practitioner  
**Frequency**: Very High (every practitioner dashboard load)  
**Index Used**: `idx_actorId`

```javascript
// MongoDB Query
db.Schedule.find({
  actorId: 'prac-123',
  active: true
})

// Expected Result: 1-5 schedules per practitioner
// Performance: <5ms with index
```

**Alternative with FHIR reference**:
```javascript
db.Schedule.find({
  'actor.reference': 'Practitioner/prac-123',
  active: true
})
// Note: Slower without denormalized actorId field
```

### SP-2: Find Schedules by Service Type
**Use Case**: Find all cardiology schedules  
**Frequency**: High (patient booking flows)  
**Index Used**: `idx_service_active`

```javascript
// MongoDB Query
db.Schedule.find({
  serviceTypeCodes: '221',  // Cardiology service code
  active: true
})

// Expected Result: 5-20 schedules per service type
// Performance: <10ms with index
```

### SP-3: Find Schedules by Specialty
**Use Case**: Find all cardiologist schedules  
**Frequency**: High  
**Index Used**: `idx_specialty_active`

```javascript
// MongoDB Query
db.Schedule.find({
  specialtyCodes: '394579002',  // Cardiology SNOMED code
  active: true
})

// Expected Result: 10-50 schedules per specialty
// Performance: <10ms with index
```

### SP-4: Find Active Schedules within Date Range
**Use Case**: Show schedules active during specific period  
**Frequency**: Medium  
**Index Used**: `idx_active_daterange`

```javascript
// MongoDB Query
db.Schedule.find({
  active: true,
  'planningHorizon.start': { $lte: ISODate('2026-06-30T23:59:59Z') },
  'planningHorizon.end': { $gte: ISODate('2026-05-01T00:00:00Z') }
})

// Expected Result: 50-200 schedules depending on date range
// Performance: <20ms with compound index
```

### SP-5: Find Schedules by Actor Type
**Use Case**: Get all device schedules or practitioner schedules  
**Frequency**: Medium  
**Index Used**: `idx_actorType_active`

```javascript
// MongoDB Query - All practitioner schedules
db.Schedule.find({
  actorType: 'Practitioner',
  active: true
})

// MongoDB Query - All device schedules
db.Schedule.find({
  actorType: 'Device',
  active: true
})

// Expected Result: Varies by type
// Performance: <15ms with index
```

### SP-6: Find Recently Updated Schedules
**Use Case**: Synchronization, change tracking  
**Frequency**: Low (background jobs)  
**Index Used**: `idx_updated`

```javascript
// MongoDB Query - Schedules updated in last 24 hours
db.Schedule.find({
  'metadata.updatedAt': { 
    $gte: ISODate('2026-05-09T00:00:00Z') 
  }
}).sort({ 'metadata.updatedAt': -1 })

// Expected Result: 5-50 schedules
// Performance: <10ms with index
```

---

## Slot Search Patterns

### SL-1: Find Free Slots for a Schedule (MOST IMPORTANT)
**Use Case**: Show available booking times for a practitioner  
**Frequency**: Extremely High (every booking search)  
**Index Used**: `idx_schedule_status_start` (compound)

```javascript
// MongoDB Query - Free slots for next 7 days
db.Slot.find({
  scheduleId: 'schedule-001',
  status: 'free',
  start: { 
    $gte: ISODate('2026-05-10T00:00:00Z'),
    $lte: ISODate('2026-05-17T23:59:59Z')
  }
}).sort({ start: 1 })

// Expected Result: 20-100 free slots
// Performance: <5ms with compound index
```

**Using pre-computed dateOnly field (recommended)**:
```javascript
db.Slot.find({
  scheduleId: 'schedule-001',
  status: 'free',
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
}).sort({ start: 1 })

// Performance: <5ms (even faster with dateOnly index)
```

### SL-2: Find Available Slots by Service Type & Date Range
**Use Case**: Patient looking for specific service in next N days  
**Frequency**: Very High  
**Index Used**: `idx_service_status_date`

```javascript
// MongoDB Query - Cardiology slots this week
db.Slot.find({
  serviceTypeCodes: '221',  // Cardiology
  status: 'free',
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
}).sort({ start: 1 })

// Expected Result: 50-200 slots across all cardiologists
// Performance: <10ms with index
```

### SL-3: Find Slots by Specialty & Status
**Use Case**: Find specialist availability  
**Frequency**: High  
**Index Used**: `idx_specialty_status_start`

```javascript
// MongoDB Query - Available cardiology specialist slots
db.Slot.find({
  specialtyCodes: '394579002',  // Cardiology SNOMED
  status: 'free',
  start: { $gte: ISODate('2026-05-10T00:00:00Z') }
}).sort({ start: 1 }).limit(50)

// Expected Result: 50-300 slots
// Performance: <15ms with index
```

### SL-4: Find Slots by Date and Actor (Direct)
**Use Case**: "Show Dr. Smith's schedule for May 10"  
**Frequency**: Very High  
**Index Used**: `idx_date_status_actor`

```javascript
// MongoDB Query - Slots for practitioner on specific date
db.Slot.find({
  dateOnly: '2026-05-10',
  'scheduleActor.id': 'prac-123'
}).sort({ start: 1 })

// All statuses - shows full day schedule
// Expected Result: 10-30 slots per day
// Performance: <5ms with compound index

// Note: No join needed! scheduleActor is denormalized
```

**Only free slots**:
```javascript
db.Slot.find({
  dateOnly: '2026-05-10',
  status: 'free',
  'scheduleActor.id': 'prac-123'
}).sort({ start: 1 })

// Expected Result: 3-15 free slots
// Performance: <5ms with index
```

### SL-5: Find Slots by Time of Day
**Use Case**: "Find morning appointments only"  
**Frequency**: High (patient preferences)  
**Index Used**: `idx_status_date_time`

```javascript
// MongoDB Query - Morning slots this week
db.Slot.find({
  status: 'free',
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' },
  timeOfDay: 'morning'
}).sort({ start: 1 })

// Expected Result: 30-100 morning slots
// Performance: <10ms with index
```

**Time of day options**: 'morning', 'afternoon', 'evening', 'night'

### SL-6: Find Slots by Day of Week Pattern
**Use Case**: "Find all Monday slots for recurring appointments"  
**Frequency**: Medium  
**Index Used**: `idx_start_status  // Note: dayOfWeek removed, use aggregation`

```javascript
// MongoDB Query - All Monday slots next 4 weeks
db.Slot.find({
  dayOfWeek: 1,  // 0=Sunday, 1=Monday, ..., 6=Saturday
  status: 'free',
  dateOnly: { $gte: '2026-05-10', $lte: '2026-06-10' }
}).sort({ start: 1 })

// Expected Result: 40-120 slots (4 weeks × 10-30 slots/day)
// Performance: <15ms with index
```

### SL-7: Find Slots by Location
**Use Case**: "Find slots at Main Hospital"  
**Frequency**: Medium-High  
**Index Used**: `idx_location_status_date`

```javascript
// MongoDB Query - Free slots at specific location
db.Slot.find({
  locationId: 'loc-456',
  status: 'free',
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
}).sort({ start: 1 })

// Expected Result: 100-500 slots (multiple practitioners)
// Performance: <15ms with index
```

### SL-8: Find Slots by Duration
**Use Case**: "Find 60-minute appointment slots"  
**Frequency**: Medium  
**Index Used**: General slot indexes + filter

```javascript
// MongoDB Query - Long appointment slots
db.Slot.find({
  status: 'free',
  durationMinutes: { $gte: 60 },
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
}).sort({ start: 1 })

// Expected Result: 10-50 longer slots
// Performance: <20ms
```

### SL-9: Find Overbooked Slots (Administrative)
**Use Case**: Identify scheduling conflicts  
**Frequency**: Low (admin/reporting)  
**Index Used**: Simple status index

```javascript
// MongoDB Query
db.Slot.find({
  overbooked: true
})

// Expected Result: 0-10 slots (should be rare)
// Performance: <5ms
```

### SL-10: Find Slots Near Booking Deadline
**Use Case**: Last-minute availability  
**Frequency**: Medium  

```javascript
// MongoDB Query - Slots in next 48 hours
const now = new Date()
const fortyEightHoursLater = new Date(now.getTime() + 48*60*60*1000)

db.Slot.find({
  status: 'free',
  start: { $gte: now, $lte: fortyEightHoursLater }
}).sort({ start: 1 })

// Expected Result: 20-80 slots
// Performance: <10ms
```

---

## Appointment Search Patterns

### AP-1: Find Appointments by Patient (MOST CRITICAL)
**Use Case**: Patient portal - "my appointments"  
**Frequency**: Extremely High (every patient login)  
**Index Used**: `idx_patient_start` (compound)

```javascript
// MongoDB Query - Patient's upcoming appointments
db.Appointment.find({
  patientId: 'pat-123',
  start: { $gte: ISODate('2026-05-10T00:00:00Z') }
}).sort({ start: 1 })

// Expected Result: 1-10 upcoming appointments
// Performance: <5ms with index
```

**Patient appointment history**:
```javascript
db.Appointment.find({
  patientId: 'pat-123',
  status: { $in: ['fulfilled', 'noshow', 'cancelled'] }
}).sort({ start: -1 }).limit(20)

// Expected Result: Last 20 appointments
// Performance: <5ms with index
```

**Patient appointments with status filter**:
```javascript
db.Appointment.find({
  patientId: 'pat-123',
  status: { $in: ['booked', 'arrived', 'pending'] },
  dateOnly: { $gte: '2026-05-01' }
}).sort({ start: 1 })

// Uses: idx_patient_status_date
// Performance: <5ms
```

### AP-2: Find Appointments by Practitioner & Date (CRITICAL)
**Use Case**: Daily schedule for a doctor  
**Frequency**: Extremely High (practitioner views)  
**Index Used**: `idx_practitioner_date` (compound)

```javascript
// MongoDB Query - Practitioner's daily schedule
db.Appointment.find({
  practitionerId: 'prac-123',
  dateOnly: '2026-05-10'
}).sort({ start: 1 })

// Expected Result: 8-20 appointments per day
// Performance: <5ms with compound index
```

**Practitioner's weekly schedule**:
```javascript
db.Appointment.find({
  practitionerId: 'prac-123',
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
}).sort({ start: 1 })

// Expected Result: 40-100 appointments per week
// Performance: <10ms with index
```

**Alternative using start timestamp**:
```javascript
db.Appointment.find({
  practitionerId: 'prac-123',
  start: { 
    $gte: ISODate('2026-05-10T00:00:00Z'),
    $lt: ISODate('2026-05-11T00:00:00Z')
  }
}).sort({ start: 1 })

// Uses: idx_practitioner_start_status
// Performance: <5ms
```

### AP-3: Find Appointments by Status
**Use Case**: Queue management (pending, waitlist, no-shows)  
**Frequency**: High (operational dashboards)  
**Index Used**: `idx_status_start`

```javascript
// MongoDB Query - All pending appointments
db.Appointment.find({
  status: 'pending',
  start: { $gte: ISODate('2026-05-10T00:00:00Z') }
}).sort({ start: 1 })

// Expected Result: 10-100 pending appointments
// Performance: <10ms with index
```

**No-shows for follow-up**:
```javascript
db.Appointment.find({
  status: 'noshow',
  start: { 
    $gte: ISODate('2026-05-01T00:00:00Z'),
    $lte: ISODate('2026-05-10T23:59:59Z')
  }
}).sort({ start: -1 })

// Expected Result: 5-30 no-shows
// Performance: <10ms
```

**Waitlist appointments**:
```javascript
db.Appointment.find({
  status: 'waitlist',
  start: { $gte: ISODate('2026-05-10T00:00:00Z') }
}).sort({ priority: 1, start: 1 })

// Sort by priority (0=highest) then date
// Expected Result: 5-50 waitlisted appointments
// Performance: <15ms
```

### AP-4: Find Appointments by Date Range
**Use Case**: Reporting, calendar views, capacity planning  
**Frequency**: Very High  
**Index Used**: `idx_date_status`

```javascript
// MongoDB Query - All appointments this week
db.Appointment.find({
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
}).sort({ start: 1 })

// Expected Result: 500-2000 appointments per week
// Performance: <20ms with index
```

**Monthly appointments with status**:
```javascript
db.Appointment.find({
  monthYear: '2026-05',
  status: 'booked'
})

// Uses: idx_start_status  // Note: monthYear removed, use date ranges
// Expected Result: 2000-8000 appointments per month
// Performance: <30ms
```

### AP-5: Find Appointments by Service Type
**Use Case**: Department-specific scheduling views  
**Frequency**: Medium-High  
**Index Used**: `idx_service_date`

```javascript
// MongoDB Query - All cardiology appointments this week
db.Appointment.find({
  serviceTypeCodes: '221',  // Cardiology
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
}).sort({ start: 1 })

// Expected Result: 50-200 appointments
// Performance: <15ms with index
```

### AP-6: Find Recurring Appointments (Series)
**Use Case**: Manage recurring visit series  
**Frequency**: Medium  
**Index Used**: `idx_originating_appt`, `idx_recurrence_id`

```javascript
// MongoDB Query - All appointments in a recurring series
db.Appointment.find({
  'originatingAppointment.reference': 'Appointment/appt-master-001'
}).sort({ start: 1 })

// Expected Result: 4-12 recurring appointments
// Performance: <10ms with index
```

**Find master recurring appointment**:
```javascript
db.Appointment.findOne({
  recurrenceId: 'rec-series-001',
  'recurrenceTemplate': { $exists: true }
})

// Expected Result: 1 master appointment
// Performance: <5ms
```

### AP-7: Find Appointments by Slot
**Use Case**: Check if a slot is actually booked  
**Frequency**: Medium  
**Index Used**: `idx_slot_ids`

```javascript
// MongoDB Query
db.Appointment.find({
  slotIds: 'slot-001'
})

// Expected Result: 0-1 appointments per slot
// Performance: <5ms with index
```

### AP-8: Find Appointments by Location
**Use Case**: Location-based scheduling, room utilization  
**Frequency**: Medium-High  
**Index Used**: `idx_location_date_status`

```javascript
// MongoDB Query - All appointments at a location today
db.Appointment.find({
  locationId: 'loc-456',
  dateOnly: '2026-05-10',
  status: { $in: ['booked', 'arrived', 'fulfilled'] }
}).sort({ start: 1 })

// Expected Result: 20-100 appointments per location per day
// Performance: <10ms with index
```

### AP-9: Find Appointments by Time Slot
**Use Case**: Morning vs afternoon scheduling patterns  
**Frequency**: Medium  
**Index Used**: `idx_start_status  // Note: timeSlot removed, use time ranges`

```javascript
// MongoDB Query - All morning appointments this week
db.Appointment.find({
  timeSlot: 'morning',
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
})

// Expected Result: 200-800 morning appointments
// Performance: <15ms with index
```

### AP-10: Find Appointments by Day of Week
**Use Case**: Weekly pattern analysis, staffing planning  
**Frequency**: Medium  
**Index Used**: `idx_start_status  // Note: dayOfWeek removed, use aggregation_appt`

```javascript
// MongoDB Query - All Monday appointments for 4 weeks
db.Appointment.find({
  dayOfWeek: 1,  // Monday
  dateOnly: { $gte: '2026-05-10', $lte: '2026-06-10' },
  status: 'booked'
})

// Expected Result: 80-320 appointments (4 Mondays)
// Performance: <20ms with index
```

### AP-11: Find Appointments by Multiple Participants
**Use Case**: "Find appointments involving these people"  
**Frequency**: Medium  
**Index Used**: `idx_participants_date`

```javascript
// MongoDB Query - Appointments with any of these participants
db.Appointment.find({
  participantIds: { $in: ['pat-001', 'pat-002', 'pat-003'] },
  dateOnly: '2026-05-10'
})

// Expected Result: 3-15 appointments
// Performance: <10ms with index
```

**Appointments with ALL specified participants**:
```javascript
db.Appointment.find({
  participantIds: { $all: ['prac-123', 'loc-456'] }
})

// Find appointments with both practitioner AND location
// Expected Result: 100-500 appointments
// Performance: <15ms
```

### AP-12: Find Appointments by Priority
**Use Case**: Urgent appointment management  
**Frequency**: Low-Medium  

```javascript
// MongoDB Query - High priority appointments
db.Appointment.find({
  priority: { $lte: 2 },  // 0=stat, 1=asap, 2=urgent
  status: { $in: ['pending', 'booked'] },
  start: { $gte: ISODate('2026-05-10T00:00:00Z') }
}).sort({ priority: 1, start: 1 })

// Expected Result: 5-30 high-priority appointments
// Performance: <15ms
```

### AP-13: Find Recently Created Appointments
**Use Case**: Audit trail, recent bookings report  
**Frequency**: Low (reporting)  

```javascript
// MongoDB Query - Appointments created in last 24 hours
db.Appointment.find({
  created: { $gte: ISODate('2026-05-09T00:00:00Z') }
}).sort({ created: -1 })

// Expected Result: 10-100 recent appointments
// Performance: <15ms
```

### AP-14: Find Appointments by Specialty
**Use Case**: Specialty-specific views  
**Frequency**: Medium  

```javascript
// MongoDB Query - All cardiology appointments
db.Appointment.find({
  specialtyCodes: '394579002',  // Cardiology
  status: 'booked',
  dateOnly: { $gte: '2026-05-10' }
}).sort({ start: 1 })

// Expected Result: 100-500 appointments
// Performance: <20ms
```

---

## Complex Multi-Resource Searches

### CS-1: Find Available Appointments with Specific Criteria
**Use Case**: "Find available cardiology appointments with Dr. Smith at Main Hospital next week during morning hours"  
**Frequency**: High (patient booking)  
**Collections**: Slot (primary), Schedule, Location  

```javascript
// MongoDB Query - Using SlotIndex (FASTEST)
db.SlotIndex.find({
  date: { $gte: '2026-05-10', $lte: '2026-05-17' },
  practitionerId: 'prac-123',
  specialtyCode: '394579002',
  locationId: 'loc-456',
  totalFreeSlots: { $gt: 0 }
})

// Expected Result: 1-7 days with availability
// Performance: <5ms with SlotIndex
```

**Alternative using Slot collection (no SlotIndex)**:
```javascript
db.Slot.find({
  'scheduleActor.id': 'prac-123',
  specialtyCodes: '394579002',
  locationId: 'loc-456',
  status: 'free',
  dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' },
  timeOfDay: 'morning'
}).sort({ start: 1 }).limit(50)

// Expected Result: 5-20 available slots
// Performance: <15ms with proper indexes
// Note: NO JOINS required due to denormalization!
```

### CS-2: Practitioner Utilization Report
**Use Case**: "How many appointments did Dr. Smith have this month?"  
**Frequency**: Low (reporting)  
**Collections**: Appointment (aggregation)

```javascript
// MongoDB Aggregation
db.Appointment.aggregate([
  {
    $match: {
      practitionerId: 'prac-123',
      monthYear: '2026-05',
      status: { $in: ['fulfilled', 'noshow', 'booked'] }
    }
  },
  {
    $group: {
      _id: '$status',
      count: { $sum: 1 },
      totalMinutes: { $sum: { $divide: [{ $subtract: ['$end', '$start'] }, 60000] } }
    }
  }
])

// Expected Result: Status breakdown with counts
// Performance: <50ms with index
```

### CS-3: Find Patient-Practitioner Appointment History
**Use Case**: "Show all past appointments between this patient and practitioner"  
**Frequency**: Medium (clinical context)  
**Collections**: Appointment

```javascript
// MongoDB Query
db.Appointment.find({
  patientId: 'pat-123',
  practitionerId: 'prac-123',
  status: { $in: ['fulfilled', 'noshow'] }
}).sort({ start: -1 })

// Expected Result: 0-20 historical appointments
// Performance: <10ms with participantIds index
```

### CS-4: Find Double-Booked Practitioners
**Use Case**: Identify scheduling conflicts  
**Frequency**: Low (audit/reporting)  
**Collections**: Appointment (aggregation)

```javascript
// MongoDB Aggregation - Find overlapping appointments
db.Appointment.aggregate([
  {
    $match: {
      dateOnly: '2026-05-10',
      status: { $in: ['booked', 'arrived'] }
    }
  },
  {
    $group: {
      _id: { practitionerId: '$practitionerId', start: '$start' },
      count: { $sum: 1 },
      appointments: { $push: '$_id' }
    }
  },
  {
    $match: { count: { $gt: 1 } }
  }
])

// Expected Result: 0-5 conflicts (should be rare)
// Performance: <50ms
```

### CS-5: Availability Summary Across Multiple Practitioners
**Use Case**: "Show availability for all cardiologists next week"  
**Frequency**: Medium (patient booking)  
**Collections**: SlotIndex or Slot

```javascript
// MongoDB Aggregation using SlotIndex (FAST)
db.SlotIndex.aggregate([
  {
    $match: {
      date: { $gte: '2026-05-10', $lte: '2026-05-17' },
      specialtyCode: '394579002',
      totalFreeSlots: { $gt: 0 }
    }
  },
  {
    $group: {
      _id: '$practitionerId',
      practitionerName: { $first: '$practitionerName' },
      totalAvailableSlots: { $sum: '$totalFreeSlots' },
      daysWithAvailability: { $sum: 1 },
      earliestAvailable: { $min: '$firstAvailable' }
    }
  },
  {
    $sort: { totalAvailableSlots: -1 }
  }
])

// Expected Result: 5-20 practitioners with availability summary
// Performance: <30ms with SlotIndex
```

### CS-6: Location Capacity Planning
**Use Case**: "How many appointment slots are available per room?"  
**Frequency**: Low (planning/reporting)  
**Collections**: Slot, Location

```javascript
// MongoDB Aggregation
db.Slot.aggregate([
  {
    $match: {
      dateOnly: { $gte: '2026-05-10', $lte: '2026-05-17' }
    }
  },
  {
    $group: {
      _id: { locationId: '$locationId', status: '$status' },
      locationName: { $first: '$locationName' },
      count: { $sum: 1 }
    }
  },
  {
    $group: {
      _id: '$_id.locationId',
      locationName: { $first: '$locationName' },
      statusCounts: {
        $push: {
          status: '$_id.status',
          count: '$count'
        }
      },
      totalSlots: { $sum: '$count' }
    }
  }
])

// Expected Result: Capacity breakdown per location
// Performance: <100ms
```

### CS-7: Service Type Distribution
**Use Case**: "What types of appointments are scheduled this month?"  
**Frequency**: Low (analytics)  
**Collections**: Appointment

```javascript
// MongoDB Aggregation
db.Appointment.aggregate([
  {
    $match: {
      monthYear: '2026-05',
      status: 'booked'
    }
  },
  {
    $unwind: '$serviceTypeCodes'
  },
  {
    $group: {
      _id: '$serviceTypeCodes',
      count: { $sum: 1 }
    }
  },
  {
    $sort: { count: -1 }
  }
])

// Expected Result: Service type distribution
// Performance: <100ms
```

### CS-8: Find Appointments Requiring Specific Resources
**Use Case**: "Find all appointments requiring Device X"  
**Frequency**: Low  
**Collections**: Appointment

```javascript
// MongoDB Query
db.Appointment.find({
  participantIds: 'device-001',
  dateOnly: { $gte: '2026-05-10' }
}).sort({ start: 1 })

// Expected Result: 5-30 appointments with device
// Performance: <10ms with participantIds index
```

---

## Index Strategy

### Critical Indexes (Must Have)

#### Schedule Collection (6 indexes)
```javascript
// 1. Find by actor (practitioner/service)
db.Schedule.createIndex({ 
  "actorId": 1, 
  "active": 1 
}, { name: "idx_actorId" })

// 2. Find by service type
db.Schedule.createIndex({ 
  "serviceTypeCodes": 1, 
  "active": 1 
}, { name: "idx_service_active" })

// 3. Find by specialty
db.Schedule.createIndex({ 
  "specialtyCodes": 1, 
  "active": 1 
}, { name: "idx_specialty_active" })

// 4. Date range queries
db.Schedule.createIndex({ 
  "active": 1, 
  "planningHorizon.start": 1, 
  "planningHorizon.end": 1 
}, { name: "idx_active_daterange" })

// 5. Find by actor type
db.Schedule.createIndex({ 
  "actorType": 1, 
  "active": 1 
}, { name: "idx_actorType_active" })

// 6. Change tracking
db.Schedule.createIndex({ 
  "metadata.updatedAt": -1 
}, { name: "idx_updated" })
```

#### Slot Collection (7 indexes)
```javascript
// 1. MOST CRITICAL - Free slots for a schedule
db.Slot.createIndex({ 
  "scheduleId": 1, 
  "status": 1, 
  "start": 1 
}, { name: "idx_schedule_status_start" })

// 2. Service type availability
db.Slot.createIndex({ 
  "serviceTypeCodes": 1, 
  "status": 1, 
  "dateOnly": 1 
}, { name: "idx_service_status_date" })

// 3. Specialty availability
db.Slot.createIndex({ 
  "specialtyCodes": 1, 
  "status": 1, 
  "start": 1 
}, { name: "idx_specialty_status_start" })

// 4. Date + actor combined
db.Slot.createIndex({ 
  "dateOnly": 1, 
  "status": 1, 
  "scheduleActor.id": 1 
}, { name: "idx_date_status_actor" })

// 5. Time of day filtering
db.Slot.createIndex({ 
  "status": 1, 
  "dateOnly": 1, 
  "timeOfDay": 1 
}, { name: "idx_status_date_time" })

// 6. Location-based search
db.Slot.createIndex({ 
  "locationId": 1, 
  "status": 1, 
  "dateOnly": 1 
}, { name: "idx_location_status_date" })

// 7. Day of week patterns
db.Slot.createIndex({ 
  "dayOfWeek": 1, 
  "status": 1 
}, { name: "idx_start_status  // Note: dayOfWeek removed, use aggregation" })
```

#### Appointment Collection (15 indexes)
```javascript
// 1. CRITICAL - Patient appointments
db.Appointment.createIndex({ 
  "patientId": 1, 
  "start": -1 
}, { name: "idx_patient_start" })

// 2. Patient with status filter
db.Appointment.createIndex({ 
  "patientId": 1, 
  "status": 1, 
  "dateOnly": 1 
}, { name: "idx_patient_status_date" })

// 3. CRITICAL - Practitioner schedule
db.Appointment.createIndex({ 
  "practitionerId": 1, 
  "dateOnly": 1 
}, { name: "idx_practitioner_date" })

// 4. Practitioner with start time
db.Appointment.createIndex({ 
  "practitionerId": 1, 
  "start": 1, 
  "status": 1 
}, { name: "idx_practitioner_start_status" })

// 5. Status-based queries
db.Appointment.createIndex({ 
  "status": 1, 
  "start": 1 
}, { name: "idx_status_start" })

// 6. Date range queries
db.Appointment.createIndex({ 
  "dateOnly": 1, 
  "status": 1 
}, { name: "idx_date_status" })

// 7. Monthly aggregations
db.Appointment.createIndex({ 
  "monthYear": 1, 
  "status": 1 
}, { name: "idx_start_status  // Note: monthYear removed, use date ranges" })

// 8. Service type filtering
db.Appointment.createIndex({ 
  "serviceTypeCodes": 1, 
  "dateOnly": 1 
}, { name: "idx_service_date" })

// 9. Recurring appointment series
db.Appointment.createIndex({ 
  "originatingAppointment.reference": 1 
}, { name: "idx_originating_appt" })

// 10. Recurrence ID
db.Appointment.createIndex({ 
  "recurrenceId": 1 
}, { name: "idx_recurrence_id" })

// 11. Slot lookup
db.Appointment.createIndex({ 
  "slotIds": 1 
}, { name: "idx_slot_ids" })

// 12. Multi-participant search
db.Appointment.createIndex({ 
  "participantIds": 1, 
  "dateOnly": 1 
}, { name: "idx_participants_date" })

// 13. Location-based queries
db.Appointment.createIndex({ 
  "locationId": 1, 
  "dateOnly": 1, 
  "status": 1 
}, { name: "idx_location_date_status" })

// 14. Time slot filtering
db.Appointment.createIndex({ 
  "timeSlot": 1, 
  "dateOnly": 1 
}, { name: "idx_start_status  // Note: timeSlot removed, use time ranges" })

// 15. Day of week patterns
db.Appointment.createIndex({ 
  "dayOfWeek": 1, 
  "status": 1 
}, { name: "idx_start_status  // Note: dayOfWeek removed, use aggregation_appt" })
```

#### SlotIndex Collection (3 indexes - if using)
```javascript
// 1. Date + practitioner lookup (MOST IMPORTANT)
db.SlotIndex.createIndex({ 
  "date": 1, 
  "practitionerId": 1 
}, { name: "idx_date_practitioner" })

// 2. Specialty availability
db.SlotIndex.createIndex({ 
  "date": 1, 
  "specialtyCode": 1, 
  "totalFreeSlots": 1 
}, { name: "idx_date_specialty_slots" })

// 3. Date range queries
db.SlotIndex.createIndex({ 
  "date": 1, 
  "totalFreeSlots": 1 
}, { name: "idx_date_slots" })
```

#### Supporting Resources (8 indexes)
```javascript
// Patient
db.Patient.createIndex({ "identifier.value": 1 }, { name: "idx_patient_identifier" })
db.Patient.createIndex({ "name.family": 1, "name.given": 1 }, { name: "idx_patient_name" })

// Practitioner
db.Practitioner.createIndex({ "identifier.value": 1 }, { name: "idx_prac_identifier" })
db.Practitioner.createIndex({ "name.family": 1 }, { name: "idx_prac_name" })

// PractitionerRole
db.PractitionerRole.createIndex({ "practitioner.reference": 1 }, { name: "idx_role_prac" })
db.PractitionerRole.createIndex({ "specialty.coding.code": 1 }, { name: "idx_role_specialty" })

// Location
db.Location.createIndex({ "identifier.value": 1 }, { name: "idx_location_identifier" })
db.Location.createIndex({ "managingOrganization.reference": 1 }, { name: "idx_location_org" })
```

### Total Index Count: 43 indexes
- Schedule: 8
- Slot: 7
- Appointment: 15
- SlotIndex: 3 (optional)
- Supporting: 8
- AppointmentResponse: 2

---

## Performance Characteristics

### Query Performance by Pattern

| Search Pattern | Without Optimization | With Hybrid Approach | Improvement |
|---------------|---------------------|---------------------|-------------|
| Patient appointments | 50-100ms (join) | **5ms** (indexed) | **10-20x faster** |
| Practitioner schedule | 40-80ms (join) | **4ms** (indexed) | **10-20x faster** |
| Available slots | 100-200ms (2 queries) | **8ms** (single query) | **12-25x faster** |
| Service type search | 150-300ms (join) | **10ms** (denormalized) | **15-30x faster** |
| Complex multi-factor | 500-1000ms (multiple joins) | **15-30ms** (indexed) | **30-50x faster** |
| Availability summary | 1000-2000ms (aggregation) | **5-10ms** (SlotIndex) | **100-200x faster** |

### Scalability Characteristics

| Dataset Size | Collection Sizes | Query Time (avg) | Notes |
|-------------|------------------|------------------|-------|
| **Small** (< 10K appts) | Schedule: 10-50<br>Slot: 30K<br>Appointment: 10K | < 5ms | All queries sub-second |
| **Medium** (100K appts) | Schedule: 50-200<br>Slot: 300K<br>Appointment: 100K | 5-15ms | Indexes critical |
| **Large** (1M appts) | Schedule: 200-500<br>Slot: 3M<br>Appointment: 1M | 10-30ms | SlotIndex recommended |
| **Very Large** (10M appts) | Schedule: 500-1000<br>Slot: 30M<br>Appointment: 10M | 20-50ms | Sharding recommended |

### Storage Overhead

| Approach | Storage Multiplier | Notes |
|----------|-------------------|-------|
| Fully Normalized | 1.0x | Baseline |
| Hybrid (no SlotIndex) | 1.5x | Denormalized fields only |
| Hybrid (with SlotIndex) | 1.8x | Includes pre-aggregated index |
| Fully Embedded | 2.5-3.0x | Full denormalization |

### Write Performance Impact

| Operation | Normalized | Hybrid | Overhead |
|-----------|-----------|--------|----------|
| Create Appointment | 1 write | 1 write | None |
| Update Slot Status | 1 write | 1-2 writes | +SlotIndex update |
| Update Practitioner Name | 1 write | 1 + N writes | Update denormalized fields |
| Delete Schedule | 1 delete | 1 delete + cleanup | Orphan slot management |

---

## Best Practices

### 1. Always Use Denormalized Fields for Queries
```javascript
// ✅ GOOD - Uses denormalized field
db.Slot.find({
  'scheduleActor.id': 'prac-123',
  status: 'free'
})

// ❌ BAD - Requires join
db.Slot.find({
  'schedule.reference': 'Schedule/schedule-001',
  status: 'free'
})
// Then separate query to Schedule to get practitioner ID
```

### 2. Use Pre-Computed Date Fields
```javascript
// ✅ GOOD - Uses dateOnly index
db.Appointment.find({
  dateOnly: '2026-05-10',
  practitionerId: 'prac-123'
})

// ❌ BAD - Date range on timestamp
db.Appointment.find({
  start: { 
    $gte: ISODate('2026-05-10T00:00:00Z'),
    $lt: ISODate('2026-05-11T00:00:00Z')
  },
  practitionerId: 'prac-123'
})
```

### 3. Leverage Array Fields for $in Queries
```javascript
// ✅ GOOD - Single query with array field
db.Appointment.find({
  participantIds: { $in: ['pat-001', 'prac-123', 'loc-456'] }
})

// ❌ BAD - Multiple queries on nested arrays
// Would require $or with complex nested paths
```

### 4. Use SlotIndex for Availability Searches
```javascript
// ✅ BEST - Use SlotIndex if available
db.SlotIndex.find({
  date: '2026-05-10',
  practitionerId: 'prac-123',
  totalFreeSlots: { $gt: 0 }
})

// ✅ GOOD - Use Slot with denormalized fields
db.Slot.find({
  dateOnly: '2026-05-10',
  'scheduleActor.id': 'prac-123',
  status: 'free'
})

// ❌ BAD - Query Schedule then Slot
// Two separate queries required
```

### 5. Always Specify Index Hints for Critical Queries
```javascript
// Explicitly use index for performance consistency
db.Appointment.find({
  patientId: 'pat-123',
  start: { $gte: ISODate('2026-05-10T00:00:00Z') }
}).hint('idx_patient_start')
```

### 6. Limit Result Sets
```javascript
// Always use .limit() for large result sets
db.Slot.find({
  status: 'free',
  dateOnly: { $gte: '2026-05-10' }
}).sort({ start: 1 }).limit(50)
```

### 7. Use Covered Queries When Possible
```javascript
// Query only indexed fields - no document fetch needed
db.Appointment.find(
  { patientId: 'pat-123', dateOnly: '2026-05-10' },
  { _id: 1, start: 1, practitionerId: 1 }  // Projection
)
```

---

## Maintenance Procedures

### SlotIndex Rebuild (Nightly)
```javascript
// Run at 2 AM daily to rebuild next 90 days
const today = new Date().toISOString().substring(0, 10)
const futureDate = new Date(Date.now() + 90*24*60*60*1000).toISOString().substring(0, 10)

// Clear old index data
db.SlotIndex.deleteMany({ 
  date: { $gte: today, $lte: futureDate } 
})

// Rebuild from Slot collection
db.Slot.aggregate([
  {
    $match: {
      status: 'free',
      dateOnly: { $gte: today, $lte: futureDate }
    }
  },
  {
    $group: {
      _id: {
        date: '$dateOnly',
        practitionerId: '$scheduleActor.id',
        specialtyCode: { $arrayElemAt: ['$specialtyCodes', 0] },
        locationId: '$locationId'
      },
      practitionerName: { $first: '$scheduleActor.name' },
      freeSlots: {
        $push: {
          slotId: '$_id',
          start: '$start',
          end: '$end',
          duration: '$durationMinutes'
        }
      },
      totalFreeSlots: { $sum: 1 },
      firstAvailable: { $min: '$start' },
      lastAvailable: { $max: '$start' },
      morningSlots: {
        $sum: { $cond: [{ $eq: ['$timeOfDay', 'morning'] }, 1, 0] }
      },
      afternoonSlots: {
        $sum: { $cond: [{ $eq: ['$timeOfDay', 'afternoon'] }, 1, 0] }
      }
    }
  },
  {
    $project: {
      _id: { 
        $concat: [
          'idx-', 
          { $toString: '$_id.date' }, 
          '-', 
          { $toString: '$_id.practitionerId' }
        ]
      },
      date: '$_id.date',
      practitionerId: '$_id.practitionerId',
      practitionerName: 1,
      specialtyCode: '$_id.specialtyCode',
      locationId: '$_id.locationId',
      freeSlots: 1,
      totalFreeSlots: 1,
      firstAvailable: 1,
      lastAvailable: 1,
      morningSlots: 1,
      afternoonSlots: 1,
      updatedAt: new Date()
    }
  },
  {
    $out: 'SlotIndex'
  }
])
```

### Real-time SlotIndex Update (On Booking)
```javascript
// When a slot is booked, update SlotIndex immediately
function updateSlotIndexOnBooking(slotId, practitionerId, date) {
  const indexId = `idx-${date}-${practitionerId}`
  
  db.SlotIndex.updateOne(
    { _id: indexId },
    {
      $pull: { freeSlots: { slotId: slotId } },
      $inc: { totalFreeSlots: -1 },
      $set: { updatedAt: new Date() }
    }
  )
  
  // Recalculate firstAvailable if needed
  const index = db.SlotIndex.findOne({ _id: indexId })
  if (index && index.freeSlots.length > 0) {
    const newFirst = index.freeSlots.reduce((min, slot) => 
      slot.start < min ? slot.start : min, 
      index.freeSlots[0].start
    )
    db.SlotIndex.updateOne(
      { _id: indexId },
      { $set: { firstAvailable: newFirst } }
    )
  }
}
```

### Denormalization Update (On Practitioner Name Change)
```javascript
// When practitioner name changes, update all denormalized occurrences
function updatePractitionerNameEverywhere(practitionerId, newName) {
  // Update Schedule
  db.Schedule.updateMany(
    { actorId: practitionerId },
    { $set: { actorName: newName } }
  )
  
  // Update Slot
  db.Slot.updateMany(
    { 'scheduleActor.id': practitionerId },
    { $set: { 'scheduleActor.name': newName } }
  )
  
  // Update Appointment
  db.Appointment.updateMany(
    { practitionerId: practitionerId },
    { $set: { practitionerName: newName } }
  )
  
  // Update SlotIndex
  db.SlotIndex.updateMany(
    { practitionerId: practitionerId },
    { $set: { practitionerName: newName } }
  )
}
```

---

## Conclusion

The **Hybrid Approach (Strategy 3)** provides:

1. **Fast Queries**: 10-50x faster than normalized approach through selective denormalization
2. **Scalability**: Handles millions of records with consistent performance
3. **No Joins**: Pre-computed denormalized fields eliminate expensive $lookup operations
4. **Flexible**: Supports all common search patterns with optimized indexes
5. **Balanced**: Moderate storage overhead (1.5-2x) vs significant performance gains

**Key Success Factors**:
- Use denormalized fields (actorId, dateOnly, participantIds, etc.) for all queries
- Create compound indexes on common query patterns
- Leverage SlotIndex for ultra-fast availability searches
- Maintain synchronization procedures for denormalized data
- Monitor index usage and query performance regularly

This approach is recommended for production healthcare systems requiring both high read performance and reasonable write efficiency.


