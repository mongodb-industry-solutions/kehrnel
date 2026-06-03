# FHIR Search Query to MongoDB Query Language (MQL) Conversion Guide

## Table of Contents

1. [Introduction](#introduction)
2. [FHIR Search Parameters Reference](#fhir-search-parameters-reference)
3. [The `_search` Field Pattern (CRITICAL)](#the-_search-field-pattern-critical)
4. [Mapping Configuration Approach](#mapping-configuration-approach)
5. [Architecture Overview](#architecture-overview)
6. [FHIR to MQL Mapping Reference](#fhir-to-mql-mapping-reference)
7. [Implementation Strategy](#implementation-strategy)
8. [Code Library Structure](#code-library-structure)
9. [Detailed Conversion Rules](#detailed-conversion-rules)
10. [Implementation Examples](#implementation-examples)
11. [Testing Strategy](#testing-strategy)
12. [Performance Optimization](#performance-optimization)
13. [Advanced Features](#advanced-features)

---

## Introduction

### Purpose

This document provides a comprehensive guide for building an integrated code library that converts FHIR REST search queries into MongoDB Query Language (MQL). The library enables FHIR servers backed by MongoDB to efficiently translate standard FHIR search parameters into native MongoDB queries.

### Key Challenges

1. **Parameter Type Diversity**: FHIR defines 9+ search parameter types, each with unique matching semantics
2. **Modifiers**: FHIR supports 15+ modifiers that change search behavior
3. **Prefixes**: Date, number, and quantity parameters support 9 comparison prefixes
4. **Chaining**: FHIR allows cross-resource reference searches
5. **Complex Logic**: AND/OR combinations, reverse chaining, composite parameters
6. **Performance**: MongoDB queries must be optimized with proper indexes

### Solution Approach

Build a modular, extensible library that:
- Parses FHIR search URLs
- Maps each parameter type to MQL operators using the `_search` field pattern
- Handles modifiers and prefixes systematically
- Generates **simple, performance-optimized** MongoDB queries
- Avoids complex `$elemMatch` and deep nesting
- Leverages denormalized `_search` fields for fast querying
- Supports query validation and testing

### Critical Design Decision: The `_search` Field Pattern

**THIS IMPLEMENTATION USES A HYBRID APPROACH**: All search-optimized fields are stored under the `_search` parent field. This enables:

1. **Simple MQL Queries**: Use direct field access with `$eq`, `$in`, `$gte`, `$lte` instead of complex `$elemMatch`
2. **Performance**: 5-10x faster queries by avoiding array traversal and nested object matching
3. **Clean Separation**: FHIR canonical structure preserved, search optimization separate
4. **Consistent Indexing**: All search indexes use `_search.*` paths
5. **Maintainability**: Clear distinction between spec compliance and performance optimization

**Example: Token Search Without vs With `_search`**

```javascript
// ❌ COMPLEX: Without _search (slow, requires $elemMatch)
{
  "code.coding": {
    "$elemMatch": {
      "system": "http://loinc.org",
      "code": "8480-6"
    }
  }
}

// ✅ SIMPLE: With _search (fast, direct array access)
{
  "_search.codeCodes": "8480-6"
}
// or for system|code precision:
{
  "_search.code.systemValues": "http://loinc.org|8480-6"
}
```

---

## FHIR Search Parameters Reference

This section provides a comprehensive reference of all FHIR search parameter types, modifiers, and prefixes supported by this library.

### Search Parameter Types

FHIR R5 defines **9 primary search parameter types**, each with distinct matching semantics:

| Parameter Type | Description | Example | Common Use Cases |
|----------------|-------------|---------|------------------|
| **string** | Text-based search with partial matching | `name=Smith` | Names, addresses, descriptions |
| **token** | Exact matching of codes, identifiers, booleans | `code=8480-6` | Codes, identifiers, enumerations |
| **reference** | References to other resources | `subject=Patient/123` | Resource relationships |
| **date** | Date/time values with precision and ranges | `birthdate=ge1980-01-01` | Dates, periods, instants |
| **number** | Numeric values with implicit ranges | `value=5.4` | Counts, scores, measurements |
| **quantity** | Numbers with units | `value-quantity=5.4\|\|mg` | Lab values, dosages, dimensions |
| **uri** | Uniform Resource Identifiers | `url=http://example.com` | Canonical URLs, references |
| **composite** | Combination of multiple parameters | `code-value-quantity=...` | Complex multi-component searches |
| **special** | Custom search logic | `_text`, `_filter`, `_has` | Full-text search, advanced queries |

**Important Note:** There is **NO "resource" parameter type** in FHIR R5. If you've seen references to "resource" searches, they likely refer to:
- **Reference chaining**: Using the `reference` parameter type with chaining syntax (e.g., `subject:Patient.name=Smith`)
- **Contained resource searches**: Implementation-specific functionality for searching within contained resources
- **The `_type` special parameter**: For searching across multiple resource types in system-level searches
- **Embedded resource navigation**: Achieved through chaining on `reference` parameters, not a separate type

### Search Parameter Modifiers

**Modifiers** change how a search parameter is interpreted. Different parameter types support different modifiers.

#### Universal Modifiers (All Types)

| Modifier | Syntax | Description | Example |
|----------|--------|-------------|---------|
| **:missing** | `param:missing=true/false` | Search for missing or present values | `gender:missing=false` |

#### String Modifiers

| Modifier | Syntax | Description | Example |
|----------|--------|-------------|---------|
| **:exact** | `param:exact=value` | Case-sensitive exact match | `family:exact=Smith` |
| **:contains** | `param:contains=value` | Case-insensitive substring match | `name:contains=mit` |

#### Token Modifiers

| Modifier | Syntax | Description | Example |
|----------|--------|-------------|---------|
| **:not** | `code:not=value` | Negation - resources that do NOT match | `status:not=cancelled` |
| **:text** | `code:text=display` | Search by display text instead of code (case-insensitive, starts-with) | `code:text=blood pressure` |
| **:in** | `code:in=valueset-url` | Code exists in specified ValueSet | `code:in=http://...` |
| **:not-in** | `code:not-in=valueset-url` | Code NOT in specified ValueSet | `code:not-in=http://...` |
| **:above** | `code:above=parent-code` | Code is subsumed by parent (hierarchy) | `code:above=123456` |
| **:below** | `code:below=parent-code` | Code subsumes child codes | `code:below=123456` |
| **:of-type** | `identifier:of-type=system\|code\|value` | Identifier with specific type | `identifier:of-type=...` |

#### Reference Modifiers

| Modifier | Syntax | Description | Example |
|----------|--------|-------------|---------|
| **:[type]** | `subject:Patient=123` | Limit reference to specific resource type | `subject:Patient=pat-123` |
| **:identifier** | `subject:identifier=system\|value` | Search by referenced resource identifier | `subject:identifier=urn:oid:...\|12345` |
| **:text** | `subject:text=display` | Search by reference display text (case-insensitive, starts-with) | `subject:text=John Smith` |

#### Date/Number/Quantity Modifiers

*These types primarily use **prefixes** rather than modifiers (see next section)*

### Search Prefixes

**Prefixes** are used with `date`, `number`, and `quantity` parameters to specify comparison operators.

| Prefix | Operator | Description | Example |
|--------|----------|-------------|---------|
| **eq** | `=` (equal) | Equal (default if no prefix specified) | `birthdate=eq1980-01-01` or `birthdate=1980-01-01` |
| **ne** | `≠` (not equal) | Not equal to | `value-quantity=ne5.4\|\|mg` |
| **gt** | `>` (greater than) | Greater than (exclusive) | `date=gt2024-01-01` |
| **lt** | `<` (less than) | Less than (exclusive) | `date=lt2024-12-31` |
| **ge** | `≥` (greater or equal) | Greater than or equal to (inclusive) | `birthdate=ge1980-01-01` |
| **le** | `≤` (less or equal) | Less than or equal to (inclusive) | `birthdate=le2000-12-31` |
| **sa** | starts after | Value starts after specified value | `period=sa2024-01-01` |
| **eb** | ends before | Value ends before specified value | `period=eb2024-12-31` |
| **ap** | approximately | Value is approximately equal (±10%) | `value=ap5.4` |

**Prefix Behavior by Type:**

- **date**: All prefixes supported (`eq`, `ne`, `gt`, `lt`, `ge`, `le`, `sa`, `eb`, `ap`)
- **number**: All prefixes supported (`eq`, `ne`, `gt`, `lt`, `ge`, `le`, `ap`)
- **quantity**: All prefixes supported (`eq`, `ne`, `gt`, `lt`, `ge`, `le`, `ap`)

### Common Search Patterns

#### Basic Search (AND Logic)
```
GET /Patient?name=Smith&gender=male&birthdate=ge1980-01-01
```
Multiple parameters are combined with AND logic (all must match).

#### OR Logic (Same Parameter)
```
GET /Patient?name=Smith,Johnson
```
Multiple values for the same parameter use OR logic (any must match).

#### Composite Logic (Advanced)
```
GET /Patient?name=Smith&name=Johnson
```
Repeated parameter with different values creates OR logic.

#### Chaining
```
GET /Observation?subject:Patient.name=Smith
```
Search across resource references.

#### Reverse Chaining
```
GET /Patient?_has:Observation:subject:code=8480-6
```
Find resources referenced by others.

#### Modifiers + Prefixes
```
GET /Patient?birthdate=ge1980-01-01&status:not=deceased
```
Combine prefixes and modifiers for complex queries.

### Special Search Parameters

FHIR defines several special parameters that apply to all resources:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| **_id** | token | Logical resource ID | `_id=patient-123` |
| **_lastUpdated** | date | Last modification date | `_lastUpdated=ge2024-01-01` |
| **_type** | token | Filter by resource type (system-level searches) | `_type=Patient` or `_type=Patient,Observation` |
| **_profile** | uri | Profile resource conforms to | `_profile=http://...` |
| **_security** | token | Security labels | `_security=http://...\|restricted` |
| **_tag** | token | Tags applied to resource | `_tag=http://...\|test` |
| **_source** | uri | Source of resource | `_source=http://...` |
| **_text** | special | Full-text search across narrative | `_text=diabetes` |
| **_content** | special | Full-text search across entire resource | `_content=diabetes` |
| **_filter** | special | Advanced query language | `_filter=status eq 'active'` |
| **_has** | special | Reverse chaining | `_has:Observation:subject:code=8480-6` |

### Result Parameters

These parameters control result formatting and paging:

| Parameter | Description | Example |
|-----------|-------------|---------|
| **_count** | Number of results per page | `_count=50` |
| **_sort** | Sort order (ascending/descending) | `_sort=-date` (descending) |
| **_include** | Include referenced resources | `_include=Observation:subject` |
| **_include:iterate** | Apply inclusion recursively to included resources | `_include:iterate=Observation:subject` |
| **_revinclude** | Include resources that reference this | `_revinclude=Observation:subject` |
| **_revinclude:iterate** | Apply reverse inclusion recursively | `_revinclude:iterate=Provenance:target` |
| **_summary** | Return summary instead of full resource | `_summary=true` |
| **_elements** | Return only specified elements | `_elements=id,name,birthDate` |
| **_contained** | How to handle contained resources | `_contained=true` |
| **_containedType** | How to present contained resources | `_containedType=container` |

### Quick Reference: Modifier Support by Type

| Parameter Type | :missing | :exact | :contains | :not | :text | :[type] | :identifier | Others |
|----------------|----------|--------|-----------|------|-------|---------|-------------|--------|
| **string** | ✓ | ✓ | ✓ | - | - | - | - | - |
| **token** | ✓ | - | - | ✓ | ✓ | - | - | :in, :not-in, :above, :below, :of-type |
| **reference** | ✓ | - | - | - | ✓ | ✓ | ✓ | - |
| **date** | ✓ | - | - | - | - | - | - | *Uses prefixes* |
| **number** | ✓ | - | - | - | - | - | - | *Uses prefixes* |
| **quantity** | ✓ | - | - | - | - | - | - | *Uses prefixes* |
| **uri** | ✓ | - | - | - | - | - | - | :below, :above |
| **composite** | ✓ | - | - | - | - | - | - | *Component-specific* |
| **special** | - | - | - | - | - | - | - | *Custom logic* |
| **_include/_revinclude** | - | - | - | - | - | - | - | :iterate |

**Notes:**
- The **:text** modifier performs case-insensitive, starts-with style matching on textual display fields:
  - For **token** parameters: searches the `display` field of CodeableConcept/Coding
  - For **reference** parameters: searches the `display` field of the Reference
- The **:iterate** modifier applies inclusion directives recursively to already-included resources

### Resources

- **FHIR R5 Search Specification**: [http://hl7.org/fhir/search.html](http://hl7.org/fhir/search.html)
- **Search Parameter Registry**: [http://hl7.org/fhir/searchparameter-registry.html](http://hl7.org/fhir/searchparameter-registry.html)
- **FHIR R5 Search Parameter Types**: [http://hl7.org/fhir/valueset-search-param-type.html](http://hl7.org/fhir/valueset-search-param-type.html)

---

## The `_search` Field Pattern (CRITICAL)

### Overview

**FOUNDATIONAL DESIGN PRINCIPLE**: This implementation uses a **hybrid denormalization approach** where all search-optimized fields are stored under the `_search` parent field. This architectural decision is THE KEY to achieving simple, fast MQL queries.

### Why `_search`? The Performance Problem

FHIR resources contain deeply nested, complex structures that are difficult to query efficiently in MongoDB:

```javascript
// FHIR Canonical Structure (hard to query)
{
  "code": {
    "coding": [
      {
        "system": "http://loinc.org",
        "code": "8480-6",
        "display": "Systolic blood pressure"
      },
      {
        "system": "http://snomed.info/sct",
        "code": "271649006",
        "display": "Systolic pressure"
      }
    ],
    "text": "Blood pressure systolic"
  }
}

// Requires complex $elemMatch queries (SLOW):
db.Observation.find({
  "code.coding": {
    "$elemMatch": {
      "system": "http://loinc.org",
      "code": "8480-6"
    }
  }
})
```

**Problems:**
- Requires `$elemMatch` for array element matching (slow)
- Cannot use simple indexes effectively
- Complex to write and maintain
- Poor query performance at scale

### The `_search` Solution

Denormalize complex structures into simple, flat fields under `_search`:

```javascript
// WITH _search: Simple, Fast Structure
{
  // Original FHIR structure (preserved for compliance)
  "code": {
    "coding": [
      {"system": "http://loinc.org", "code": "8480-6", "display": "Systolic blood pressure"},
      {"system": "http://snomed.info/sct", "code": "271649006", "display": "Systolic pressure"}
    ]
  },
  
  // Denormalized search fields
  "_search": {
    "codeCodes": ["8480-6", "271649006"],                    // Simple array of codes
    "codeSystems": ["http://loinc.org", "http://snomed.info/sct"],
    "codeSystemValues": [                                     // System|code pairs
      "http://loinc.org|8480-6",
      "http://snomed.info/sct|271649006"
    ]
  }
}

// SIMPLE query (FAST):
db.Observation.find({
  "_search.codeCodes": "8480-6"  // Direct array membership, uses index
})

// Or precise system|code query:
db.Observation.find({
  "_search.codeSystemValues": "http://loinc.org|8480-6"
})
```

**Benefits:**
- Simple `$eq` and `$in` queries (5-10x faster)
- Effective index utilization
- Easy to write and understand
- Excellent query performance at scale

### When to Use Canonical vs `_search` Fields

| Field Type | Strategy | Example |
|------------|----------|---------|
| **Simple booleans** | Use canonical field | `"active": true` |
| **Simple strings** | Use canonical field | `"name": "Dr. Smith"` |
| **Simple dates (at root)** | Use canonical field or copy to `_search` | `"birthDate"` or `"_search.birthDate"` |
| **CodeableConcept arrays** | Denormalize to `_search` | `"_search.serviceTypeCodes": ["221"]` |
| **Reference arrays** | Extract IDs to `_search` | `"_search.patientId": "pat-123"` |
| **Identifier arrays** | Multiple formats in `_search` | `"_search.identifier.values": ["MRN-123"]` |
| **Nested dates (Period/Timing)** | Extract to `_search` | `"_search.start": "2026-05-15T14:30:00Z"` |

### Standard `_search` Field Patterns

#### Pattern 1: CodeableConcept → Simple Code Arrays

```javascript
// FHIR Canonical
{
  "serviceCategory": [
    {
      "coding": [
        {"system": "http://terminology.hl7.org/CodeSystem/service-category", "code": "17"}
      ]
    }
  ]
}

// _search Denormalization
{
  "_search": {
    "serviceCategoryCodes": ["17"]  // Flat array of all codes
  }
}

// Simple MQL Query
{
  "_search.serviceCategoryCodes": "17"
}
```

#### Pattern 2: Reference → Extracted IDs

```javascript
// FHIR Canonical
{
  "subject": {
    "reference": "Patient/pat-123",
    "display": "John Smith"
  }
}

// _search Denormalization (Primary Pattern)
{
  "_search": {
    "patientId": "pat-123",          // Extracted ID (primary, most efficient)
    "patientName": "John Smith"      // Cached display name
  }
}

// Simple MQL Query
{
  "_search.patientId": "pat-123"
}
```

#### Pattern 3: Identifier → Multiple Query Formats

```javascript
// FHIR Canonical
{
  "identifier": [
    {
      "system": "http://hospital.org/mrn",
      "value": "MRN-12345"
    },
    {
      "system": "http://hl7.org/fhir/sid/us-ssn",
      "value": "123-45-6789"
    }
  ]
}

// _search Denormalization
{
  "_search": {
    "identifier": {
      "values": ["MRN-12345", "123-45-6789"],                    // Value-only search
      "systems": [                                                // System-only search
        "http://hospital.org/mrn",
        "http://hl7.org/fhir/sid/us-ssn"
      ],
      "systemValues": [                                          // Precise system|value search
        "http://hospital.org/mrn|MRN-12345",
        "http://hl7.org/fhir/sid/us-ssn|123-45-6789"
      ]
    }
  }
}

// MQL Queries (choose based on search precision)
{"_search.identifier.values": "MRN-12345"}                              // Value only
{"_search.identifier.systems": "http://hospital.org/mrn"}               // System only
{"_search.identifier.systemValues": "http://hospital.org/mrn|MRN-12345"} // Most precise
```

#### Pattern 4: Participant Arrays → Multiple Extracted Fields

```javascript
// FHIR Canonical (Appointment example)
{
  "participant": [
    {
      "actor": {"reference": "Patient/pat-123", "display": "John Smith"},
      "status": "accepted"
    },
    {
      "actor": {"reference": "Practitioner/prac-456", "display": "Dr. Johnson"},
      "status": "accepted"
    },
    {
      "actor": {"reference": "Location/loc-789", "display": "Room 101"},
      "status": "accepted"
    }
  ]
}

// _search Denormalization (Multiple Levels)
{
  "_search": {
    // Primary IDs (most efficient, single value)
    "patientId": "pat-123",
    "patientName": "John Smith",
    "practitionerId": "prac-456",
    "practitionerName": "Dr. Johnson",
    "locationId": "loc-789",
    "locationName": "Room 101",
    
    // All actors (for broad searches)
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
}

// MQL Queries
{"_search.patientId": "pat-123"}                    // Find patient's appointments (fastest)
{"_search.practitionerId": "prac-456"}              // Find practitioner's appointments
{"_search.actor.ids": "loc-789"}                    // Find any actor
{"_search.actor.types": "Practitioner"}             // Find by actor type
```

### Complete Resource Example: Appointment

```javascript
{
  // FHIR R5 Canonical Fields (preserve for interoperability)
  "resourceType": "Appointment",
  "id": "appt-001",
  "status": "booked",
  "appointmentType": {
    "coding": [{"code": "FOLLOWUP", "display": "Follow-up"}]
  },
  "description": "Cardiology follow-up",
  "start": "2026-05-15T14:30:00Z",
  "end": "2026-05-15T15:00:00Z",
  "created": "2026-05-01T10:00:00Z",
  "subject": {
    "reference": "Patient/pat-123",
    "display": "John Smith"
  },
  "participant": [
    {
      "actor": {"reference": "Patient/pat-123", "display": "John Smith"},
      "required": true,
      "status": "accepted"
    },
    {
      "actor": {"reference": "Practitioner/prac-456", "display": "Dr. Sarah Johnson"},
      "required": true,
      "status": "accepted"
    }
  ],
  "basedOn": [
    {"reference": "ServiceRequest/sr-789"}
  ],
  
  // Search-Optimized Denormalized Fields
  "_search": {
    // Copy simple fields for compound index performance
    "status": "booked",
    "start": "2026-05-15T14:30:00Z",
    "end": "2026-05-15T15:00:00Z",
    "created": "2026-05-01T10:00:00Z",
    "description": "Cardiology follow-up",
    
    // Flattened CodeableConcept
    "appointmentTypeCodes": ["FOLLOWUP"],
    
    // Extracted primary participants (single values, most efficient)
    "patientId": "pat-123",
    "patientName": "John Smith",
    "practitionerId": "prac-456",
    "practitionerName": "Dr. Sarah Johnson",
    
    // Detailed participant info (for advanced queries)
    "patientDetails": {
      "id": "pat-123",
      "name": "John Smith",
      "dateOfBirth": "1985-03-15",
      "gender": "male"
    },
    
    // All actors (for broad "any participant" searches)
    "actor": {
      "ids": ["pat-123", "prac-456"],
      "types": ["Patient", "Practitioner"],
      "references": ["Patient/pat-123", "Practitioner/prac-456"]
    },
    
    // Extracted basedOn references
    "basedOn": {
      "ids": ["sr-789"],
      "types": ["ServiceRequest"],
      "references": ["ServiceRequest/sr-789"]
    },
    
    // Metadata
    "metadata": {
      "createdAt": "2026-05-01T10:00:00Z",
      "updatedAt": "2026-05-01T10:00:00Z"
    }
  }
}
```

### MQL Query Examples Using `_search`

```javascript
// 1. Find patient's appointments (MOST COMMON, FASTEST)
db.Appointment.find({
  "_search.patientId": "pat-123",
  "_search.status": "booked"
})

// 2. Find appointments by practitioner
db.Appointment.find({
  "_search.practitionerId": "prac-456",
  "_search.start": { $gte: "2026-05-15T00:00:00Z" }
})

// 3. Find appointments by type
db.Appointment.find({
  "_search.appointmentTypeCodes": "FOLLOWUP",
  "_search.start": {
    $gte: "2026-05-15T00:00:00Z",
    $lt: "2026-05-16T00:00:00Z"
  }
})

// 4. Find appointments for any actor
db.Appointment.find({
  "_search.actor.ids": "prac-456"
})

// 5. Find appointments based on service request
db.Appointment.find({
  "_search.basedOn.ids": "sr-789"
})

// 6. Complex multi-parameter search (still simple!)
db.Appointment.find({
  "_search.patientId": "pat-123",
  "_search.status": { $in: ["booked", "arrived"] },
  "_search.start": {
    $gte: "2026-05-01T00:00:00Z",
    $lte: "2026-05-31T23:59:59Z"
  },
  "_search.appointmentTypeCodes": { $in: ["FOLLOWUP", "ROUTINE"] }
})
```

### Performance Impact

**Query Performance Comparison** (1 million appointments):

| Query Type | Without `_search` | With `_search` | Speedup |
|------------|-------------------|----------------|---------|
| Patient appointments | 250ms ($elemMatch) | 15ms (direct) | **17x faster** |
| Code search | 180ms (nested path) | 8ms (array member) | **23x faster** |
| Multi-parameter | 400ms (multiple $elemMatch) | 35ms (compound index) | **11x faster** |
| Reference lookup | 120ms (regex split) | 5ms (direct ID) | **24x faster** |

### Implementation Guidelines

1. **Always denormalize to `_search`** for:
   - CodeableConcept arrays
   - Reference arrays (extract IDs)
   - Identifier arrays
   - Complex nested structures

2. **Use canonical fields directly** for:
   - Simple booleans (`active`)
   - Top-level strings (`name`)
   - Top-level dates (optional - can copy to `_search` for consistency)

3. **Index all `_search` fields** that will be queried

4. **Maintain both structures**:
   - Canonical FHIR: For FHIR API responses, interoperability
   - `_search`: For query performance

5. **Update both on write**:
   - When inserting/updating, populate both canonical and `_search` fields
   - Keep them synchronized

### Key Takeaway

**The `_search` field pattern is NOT optional** - it is the FOUNDATION of this implementation's performance and simplicity. All MQL query generation MUST target `_search` fields for denormalized data.

---

## Mapping Configuration Approach

### Overview

**SECOND CRITICAL DESIGN DECISION**: This library uses **explicit mapping configuration files** to define how FHIR search parameters map to MongoDB fields. This solves the "nested field problem" and provides:

1. **Clarity**: Explicitly defines which MongoDB fields to search for each FHIR parameter
2. **Flexibility**: Supports multi-field searches (e.g., `name` searches family, given, and full name)
3. **Maintainability**: Add new resources by creating a configuration file
4. **Validation**: Catch configuration errors at startup, not query time
5. **Documentation**: Config files serve as living documentation

### The Nested Field Problem

**Problem Statement:**

FHIR search parameters often correspond to multiple MongoDB fields. Without explicit configuration, the library cannot know which fields to search.

**Example: The `name` Parameter**

```
GET /Patient?name=Smith
```

**Question:** Which MongoDB fields should be searched?

**Canonical FHIR Structure:**
```javascript
{
  "name": [
    {
      "use": "official",
      "family": "Smith",
      "given": ["John", "Michael"],
      "prefix": ["Dr."],
      "text": "Dr. John Michael Smith"
    }
  ]
}
```

**Denormalized `_search` Structure:**
```javascript
{
  "_search": {
    "familyName": "Smith",
    "givenNames": ["John", "Michael"],
    "fullName": "Dr. John Michael Smith"
  }
}
```

**The Challenge:**
- Should we search `_search.familyName` only?
- Should we search `_search.givenNames` too?
- Should we search `_search.fullName`?
- What about the canonical `name[].family` and `name[].given`?

**Solution: Mapping Configuration**

Define explicitly in a configuration file:

```yaml
# config/mappings/Patient.yaml
parameters:
  name:
    type: string
    description: "Search by patient name"
    fields:
      - field: _search.familyName
      - field: _search.givenNames
      - field: _search.fullName
    operator: OR  # Combine fields with OR logic
```

Now the library knows:
- `name` parameter searches 3 fields
- Use OR logic to combine them
- All fields are in `_search` for performance

### Configuration File Format

#### Basic Structure

```yaml
# config/mappings/{ResourceType}.yaml

resource: ResourceTypeName
version: 1.0

# Define search parameter mappings
parameters:
  param-name:
    type: string | token | reference | date | number | quantity | uri | composite
    description: "Human-readable description"
    fields:
      - field: mongodb.field.path
        weight: 1.0  # Optional: for ranking
    operator: OR | AND  # How to combine multiple fields
    modifiers: [exact, contains, not]  # Allowed modifiers
    examples:
      - "param-name=value"
      - "param-name:modifier=value"

# Global settings
settings:
  defaultParameterOperator: AND
  optimize: true
  indexHints:
    - parameters: [param1, param2]
      index: index_name
```

#### Example: Patient Resource

```yaml
# config/mappings/Patient.yaml
resource: Patient
version: 1.0

parameters:
  
  # Multi-field string search
  name:
    type: string
    description: "Search by patient name (family, given, or full name)"
    fields:
      - field: _search.familyName
        weight: 1.0
      - field: _search.givenNames
        weight: 0.8
      - field: _search.fullName
        weight: 0.6
    operator: OR
    modifiers: [exact, contains]
    examples:
      - "name=Smith"
      - "name:exact=Smith"
  
  # Single-field string search
  family:
    type: string
    description: "Search by family name only"
    fields:
      - field: _search.familyName
    modifiers: [exact, contains]
  
  # Simple token (use canonical field)
  gender:
    type: token
    description: "Search by gender"
    fields:
      - field: gender
    tokenType: simple
  
  # Simple date (use canonical field)
  birthdate:
    type: date
    description: "Search by birth date"
    fields:
      - field: birthDate
    prefixes: [eq, ne, gt, lt, ge, le]
  
  # Complex token (CodeableConcept with _search)
  marital-status:
    type: token
    description: "Search by marital status"
    fields:
      - field: _search.maritalStatusCodes
        tokenType: code
      - field: _search.maritalStatusSystemValues
        tokenType: systemCode
    operator: OR
  
  # Identifier with multiple search formats
  identifier:
    type: token
    description: "Search by identifier"
    fields:
      - field: _search.identifier.values
        tokenType: value
      - field: _search.identifier.systemValues
        tokenType: systemValue
    modifiers: [not]
  
  # Reference with extracted ID
  general-practitioner:
    type: reference
    description: "Search by general practitioner"
    fields:
      - field: _search.generalPractitionerId
    referenceTypes: [Practitioner, Organization, PractitionerRole]
    modifiers: [identifier]

settings:
  defaultParameterOperator: AND
  optimize: true
  indexHints:
    - parameters: [name, birthdate]
      index: name_birthdate_idx
    - parameters: [identifier]
      index: identifier_values_idx
```

#### Example: Observation Resource

```yaml
# config/mappings/Observation.yaml
resource: Observation
version: 1.0

parameters:
  
  # Reference to patient (most common)
  subject:
    type: reference
    description: "The subject of the observation"
    fields:
      - field: _search.patientId  # Primary: most observations are for patients
        primary: true
      - field: _search.subjectId  # Fallback: for non-patient subjects
    operator: OR
    referenceTypes: [Patient, Group, Device, Location]
    modifiers: [Patient, identifier]
  
  # CodeableConcept token
  code:
    type: token
    description: "The code of the observation"
    fields:
      - field: _search.codeCodes  # For simple code searches
        tokenType: code
        primary: true
      - field: _search.codeSystemValues  # For system|code searches
        tokenType: systemCode
    operator: OR
    modifiers: [text, not]
  
  # Date field (extracted to _search for consistency)
  date:
    type: date
    description: "Search by observation date"
    fields:
      - field: _search.start
    prefixes: [eq, ne, gt, lt, ge, le, sa, eb]
  
  # Simple status token
  status:
    type: token
    description: "The status of the observation"
    fields:
      - field: status
    tokenType: simple
    allowedValues: [registered, preliminary, final, amended, corrected, cancelled, entered-in-error, unknown]

settings:
  defaultParameterOperator: AND
  indexHints:
    - parameters: [subject, code, date]
      index: patient_code_date_idx
    - parameters: [subject, date]
      index: patient_date_idx
```

#### Example: Appointment Resource

```yaml
# config/mappings/Appointment.yaml
resource: Appointment
version: 1.0

parameters:
  
  # Patient participant (extracted ID)
  patient:
    type: reference
    description: "Search by patient participant"
    fields:
      - field: _search.patientId
    referenceTypes: [Patient]
    modifiers: [identifier]
  
  # Practitioner participant (extracted ID)
  practitioner:
    type: reference
    description: "Search by practitioner participant"
    fields:
      - field: _search.practitionerId
    referenceTypes: [Practitioner, PractitionerRole]
    modifiers: [identifier]
  
  # Location (extracted ID)
  location:
    type: reference
    description: "Search by location"
    fields:
      - field: _search.locationId
    referenceTypes: [Location]
  
  # Any participant (array of all IDs)
  actor:
    type: reference
    description: "Search by any participant actor"
    fields:
      - field: _search.actor.ids
    referenceTypes: [Patient, Practitioner, PractitionerRole, RelatedPerson, Device, HealthcareService, Location]
  
  # Date
  date:
    type: date
    description: "Appointment date"
    fields:
      - field: _search.start
    prefixes: [eq, ne, gt, lt, ge, le]
  
  # Status (simple token)
  status:
    type: token
    description: "Appointment status"
    fields:
      - field: _search.status
    tokenType: simple
    allowedValues: [proposed, pending, booked, arrived, fulfilled, cancelled, noshow, entered-in-error, checked-in, waitlist]
  
  # Appointment type (CodeableConcept)
  appointment-type:
    type: token
    description: "Type of appointment"
    fields:
      - field: _search.appointmentTypeCodes
        tokenType: code
  
  # Service type (CodeableConcept)
  service-type:
    type: token
    description: "Service type"
    fields:
      - field: _search.serviceTypeCodes
        tokenType: code

settings:
  defaultParameterOperator: AND
  indexHints:
    - parameters: [patient, date, status]
      index: patient_date_status_idx
    - parameters: [practitioner, date]
      index: practitioner_date_idx
```

### How Mapping Configuration Works

#### 1. Configuration Loading

```python
from fhir_query_mql import FHIRToMQLConverter

# Initialize converter (loads all mapping configs)
converter = FHIRToMQLConverter()

# Or specify custom config path
converter = FHIRToMQLConverter(config_path='/path/to/config')
```

On initialization:
1. Loads all YAML files from `config/mappings/` directory
2. Validates each configuration against schema
3. Caches configurations in memory
4. Reports any validation errors

#### 2. Query Conversion Flow

```python
# Convert FHIR search query
mql = converter.convert('Patient', 'name=Smith&gender=male')
```

Conversion process:
1. **Parse URL**: Extract parameters, values, modifiers, prefixes
2. **Load Mapping**: Get mapping config for 'Patient' resource
3. **Resolve Fields**: For each parameter, look up which MongoDB fields to search
4. **Select Converter**: Based on parameter type (string, token, etc.)
5. **Generate MQL**: Convert each parameter to MQL using field paths from config
6. **Combine**: Merge MQL fragments with AND/OR logic from config
7. **Optimize**: Simplify and optimize final query
8. **Return**: Complete MQL query ready for MongoDB

#### 3. Field Resolution Example

```
FHIR Query: name=Smith
          ↓
Mapping Config Lookup:
  parameters.name.fields = [
    {field: "_search.familyName"},
    {field: "_search.givenNames"},
    {field: "_search.fullName"}
  ]
  parameters.name.operator = "OR"
          ↓
MQL Generation:
  {
    "$or": [
      {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
      {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}},
      {"_search.fullName": {"$regex": "^Smith", "$options": "i"}}
    ]
  }
```

### Benefits of Mapping Configuration

1. **Explicit Control**: You decide which fields are searched
2. **Multi-Field Search**: One parameter can search multiple fields (e.g., name → family, given, fullName)
3. **Type-Specific**: Different configs per resource type
4. **Validated**: Errors caught at startup, not runtime
5. **Documented**: Configs serve as documentation
6. **Testable**: Configs can be unit tested independently
7. **Flexible**: Easy to add new parameters or change field mappings
8. **Performance**: Ensures `_search` fields are used for fast queries

### Creating Mapping Configurations

#### Step-by-Step Guide

**1. Create new YAML file:**
```bash
touch config/mappings/Condition.yaml
```

**2. Define resource and parameters:**
```yaml
resource: Condition
version: 1.0

parameters:
  # Start with most common search parameters
  patient:
    type: reference
    description: "Patient with the condition"
    fields:
      - field: _search.patientId
    referenceTypes: [Patient]
  
  code:
    type: token
    description: "Condition code"
    fields:
      - field: _search.codeCodes
        tokenType: code
      - field: _search.codeSystemValues
        tokenType: systemCode
    operator: OR
  
  clinical-status:
    type: token
    description: "Clinical status"
    fields:
      - field: _search.clinicalStatusCodes
        tokenType: code
    
  onset-date:
    type: date
    description: "Date when condition started"
    fields:
      - field: _search.onsetDate
    prefixes: [eq, ne, gt, lt, ge, le]

settings:
  defaultParameterOperator: AND
  indexHints:
    - parameters: [patient, code]
      index: patient_code_idx
```

**3. Ensure `_search` fields exist in data:**

Your data ingestion process must populate these fields:
```javascript
{
  "resourceType": "Condition",
  "id": "cond-001",
  "subject": {"reference": "Patient/pat-123"},
  "code": {
    "coding": [
      {"system": "http://snomed.info/sct", "code": "38341003"}
    ]
  },
  // ... canonical FHIR fields ...
  
  "_search": {
    "patientId": "pat-123",
    "codeCodes": ["38341003"],
    "codeSystemValues": ["http://snomed.info/sct|38341003"],
    "clinicalStatusCodes": ["active"],
    "onsetDate": "2024-03-15"
  }
}
```

**4. Validate configuration:**
```python
from fhir_query_mql.utils.validation import validate_mapping_file

validate_mapping_file('config/mappings/Condition.yaml')
# Checks: required fields, valid types, field paths exist, etc.
```

**5. Test the mapping:**
```python
converter = FHIRToMQLConverter()

# Test each parameter
mql = converter.convert('Condition', 'patient=pat-123')
print(mql)  # Should target _search.patientId

mql = converter.convert('Condition', 'code=38341003')
print(mql)  # Should target _search.codeCodes
```

### Advanced Configuration Features

#### 1. Field Weights (for relevance scoring)

```yaml
name:
  type: string
  fields:
    - field: _search.familyName
      weight: 1.0  # Highest priority
    - field: _search.givenNames
      weight: 0.8  # Medium priority
    - field: _search.fullName
      weight: 0.6  # Lowest priority
```

Use weights for relevance scoring in search results.

#### 2. Primary Field Flag

```yaml
code:
  type: token
  fields:
    - field: _search.codeCodes
      tokenType: code
      primary: true  # Use this for simple code searches
    - field: _search.codeSystemValues
      tokenType: systemCode
      primary: false  # Use this only for system|code format
```

Primary field is used for unmodified searches.

#### 3. Allowed Values (validation)

```yaml
status:
  type: token
  fields:
    - field: status
  allowedValues: [active, inactive, resolved]
```

Validates parameter values at query time.

#### 4. Index Hints

```yaml
settings:
  indexHints:
    - parameters: [patient, date, status]
      index: patient_date_status_idx
    - parameters: [code]
      index: code_idx
```

Provides hints for MongoDB query planner.

### Configuration Validation

The library validates configurations at startup:

```python
# Validation checks:
# 1. Required fields present (resource, parameters)
# 2. Parameter types valid (string, token, date, etc.)
# 3. Field paths follow MongoDB naming conventions
# 4. Operators valid (AND, OR)
# 5. Token types valid (code, systemCode, value, etc.)
# 6. Reference types are valid FHIR resources
# 7. Modifiers are supported for parameter type
# 8. Prefixes are supported for parameter type
# 9. No duplicate parameter names
# 10. Index hints reference valid parameters

# Example validation error:
ValidationError: Invalid parameter type 'invalid_type' for parameter 'name' in Patient.yaml.
                Valid types: string, token, reference, date, number, quantity, uri, composite, special
```

### See Also

- **FHIR_TO_MQL_APPROACHES.md**: Detailed comparison of 5 different approaches, including why this approach was chosen
- **config/mappings/**: Example mapping configurations for Patient, Observation, Appointment, Schedule, Slot
- **docs/configuration_guide.md**: Complete reference for all configuration options

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    FHIR Search Request                          │
│         GET /Patient?name=Smith&birthdate=gt1980-01-01          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Query Parser Module                            │
│  • Parse URL parameters                                          │
│  • Extract resource type, parameters, modifiers, prefixes        │
│  • Validate parameter syntax                                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│            Mapping Configuration Loader (CRITICAL)               │
│  • Load config/mappings/{ResourceType}.yaml                      │
│  • Cache configuration in memory                                 │
│  • Validate configuration schema                                 │
│  • Provide field path mappings for each search parameter         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              Field Path Resolver Module                          │
│  • For each parameter, look up MongoDB field paths from config   │
│  • Resolve parameter type (string, token, date, etc.)            │
│  • Get operator (OR/AND for multi-field searches)                │
│  • Retrieve allowed modifiers and prefixes                       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                MQL Converter Module                              │
│  ┌──────────────┬──────────────┬──────────────┬───────────────┐ │
│  │ String Conv  │  Token Conv  │  Date Conv   │  Number Conv  │ │
│  ├──────────────┼──────────────┼──────────────┼───────────────┤ │
│  │Reference Conv│Quantity Conv │  URI Conv    │Composite Conv │ │
│  └──────────────┴──────────────┴──────────────┴───────────────┘ │
│  • Use field paths from configuration                            │
│  • Apply modifiers (e.g., :exact, :contains, :missing)           │
│  • Apply prefixes (e.g., gt, lt, ge, le)                         │
│  • Target _search fields for performance                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                Query Builder & Optimizer                         │
│  • Combine multi-field searches with $or/$and                    │
│  • Combine multiple parameters (AND/OR from config)              │
│  • Handle chaining and reverse chaining                          │
│  • Optimize query structure                                      │
│  • Add index hints from configuration                            │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                 MongoDB Query (MQL)                              │
│  {                                                               │
│    "$and": [                                                     │
│      {"$or": [                                                   │
│        {"_search.familyName": {"$regex": "^Smith", ...}},        │
│        {"_search.givenNames": {"$regex": "^Smith", ...}}         │
│      ]},                                                         │
│      {"birthDate": {"$gt": "1980-01-01"}}                       │
│    ]                                                             │
│  }                                                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              MongoDB Database Execution                          │
│  • Execute query against collection                              │
│  • Apply result modifiers (_count, _sort, _include)              │
│  • Return FHIR Bundle                                            │
└─────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Input | Output |
|-----------|---------------|-------|--------|
| **Query Parser** | Parse URL, extract parameters | FHIR search URL | Parameter dictionary |
| **Mapping Loader** | Load configuration files | Resource type | Mapping configuration |
| **Field Resolver** | Resolve MongoDB field paths | Parameter name + config | Field paths, type, operators |
| **MQL Converter** | Convert to MongoDB syntax | Field paths + values | MongoDB query fragments |
| **Query Builder** | Combine and optimize | Query fragments + config | Final MQL query |
| **Result Processor** | Format results | MongoDB documents | FHIR Bundle |

### Data Flow Example

```
FHIR Search Query:
  GET /Patient?name=Smith&gender=male

         ↓ [Query Parser]

Parsed Parameters:
  {
    "name": ["Smith"],
    "gender": ["male"]
  }

         ↓ [Mapping Loader]

Loaded Config (Patient.yaml):
  parameters:
    name:
      type: string
      fields: [_search.familyName, _search.givenNames, _search.fullName]
      operator: OR
    gender:
      type: token
      fields: [gender]

         ↓ [Field Resolver]

Resolved Field Info:
  name → {
    type: "string",
    fields: ["_search.familyName", "_search.givenNames", "_search.fullName"],
    operator: "OR"
  }
  gender → {
    type: "token",
    fields: ["gender"],
    operator: "AND"
  }

         ↓ [MQL Converter]

MQL Fragments:
  name → {
    "$or": [
      {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
      {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}},
      {"_search.fullName": {"$regex": "^Smith", "$options": "i"}}
    ]
  }
  gender → {"gender": "male"}

         ↓ [Query Builder]

Final MQL:
  {
    "$and": [
      {"$or": [
        {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
        {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}},
        {"_search.fullName": {"$regex": "^Smith", "$options": "i"}}
      ]},
      {"gender": "male"}
    ]
  }

         ↓ [MongoDB Execution]

Results: [Patient documents matching both conditions]
```

---

## FHIR to MQL Mapping Reference

### Quick Reference Table

| FHIR Parameter Type | `_search` Field Pattern | MQL Query Pattern | Performance Notes |
|---------------------|------------------------|-------------------|-------------------|
| **string** | Use canonical or `_search.familyName` | `{"field": {"$regex": "^value", "$options": "i"}}` | Anchor regex with `^` |
| **token** (simple) | Use canonical field | `{"gender": "male"}` | Fast equality check |
| **token** (CodeableConcept) | `_search.codeCodes: ["8480-6"]` | `{"_search.codeCodes": "8480-6"}` | **5-10x faster** than $elemMatch |
| **reference** | `_search.patientId: "pat-123"` | `{"_search.patientId": "pat-123"}` | **10-20x faster** than reference string matching |
| **date** | Use canonical or `_search.start` | `{"_search.start": {"$gte": "2026-05-15"}}` | ISO datetime, compound indexes |
| **number** | `_search.value` with range | `{"$and": [{"field": {"$gte": 5.35}}, {"field": {"$lt": 5.45}}]}` | Precision-based ranges |
| **quantity** | Complex - multiple `_search` fields | `{"$and": [value range, system, code]}` | Denormalize unit info |
| **uri** | Use canonical field | `{"field": "http://example.com"}` | Direct equality |
| **composite** | Denormalized component `_search` fields | `{"$and": [{"_search.codeCodes": "X"}, {"_search.value": {"$gt": 140}}]}` | **NO $elemMatch** - use independent fields or composite keys (10-60x faster) |
| **special** | Custom `_search` logic | Varies by parameter | Implementation-specific |

### Key Principles

1. **Always use `_search` for complex structures**: CodeableConcept, References, Identifiers, Composite parameters
2. **Use canonical fields for simple types**: Booleans, simple strings, root-level dates
3. **Avoid `$elemMatch`**: Denormalize to flat arrays in `_search` - this applies to composite parameters too
4. **Extract reference IDs**: Store `patientId` not `subject.reference`
5. **Composite parameters use denormalization**: Use independent `_search` fields or composite keys, NOT `$elemMatch`
6. **Index all `_search` fields**: Critical for performance

### Recommended Indexes

#### Patient Resource
```javascript
db.Patient.createIndex({"active": 1});
db.Patient.createIndex({"_search.familyName": 1});
db.Patient.createIndex({"birthDate": 1});
db.Patient.createIndex({"_search.identifier.values": 1});
db.Patient.createIndex({"_search.identifier.systemValues": 1});
```

#### Observation Resource
```javascript
db.Observation.createIndex({"_search.patientId": 1, "_search.start": -1});
db.Observation.createIndex({"_search.codeCodes": 1, "_search.start": -1});
db.Observation.createIndex({"_search.codeSystemValues": 1});
db.Observation.createIndex({"status": 1});
```

#### Appointment Resource  
```javascript
db.Appointment.createIndex({"_search.patientId": 1, "_search.start": -1, "_search.status": 1});
db.Appointment.createIndex({"_search.practitionerId": 1, "_search.start": -1});
db.Appointment.createIndex({"_search.locationId": 1, "_search.start": 1});
db.Appointment.createIndex({"_search.appointmentTypeCodes": 1, "_search.start": 1});
db.Appointment.createIndex({"_search.actor.ids": 1, "_search.start": 1});
```

#### Schedule Resource
```javascript
db.Schedule.createIndex({"active": 1, "_search.actorId": 1});
db.Schedule.createIndex({"_search.serviceTypeCodes": 1, "active": 1});
db.Schedule.createIndex({"_search.specialtyCodes": 1, "active": 1});
db.Schedule.createIndex({"planningHorizon.start": 1, "planningHorizon.end": 1});
```

#### Slot Resource
```javascript
db.Slot.createIndex({"_search.scheduleId": 1, "_search.start": 1});
db.Slot.createIndex({"_search.status": 1, "_search.start": 1});
db.Slot.createIndex({"_search.serviceTypeCodes": 1, "_search.status": 1, "_search.start": 1});
db.Slot.createIndex({"_search.specialtyCodes": 1, "_search.status": 1, "_search.start": 1});
```

---

## Implementation Strategy

### Phase 1: Core Infrastructure (Week 1-2)

**Objectives:**
- Set up project structure
- Implement URL parser
- Create parameter type resolver
- Define converter interfaces

**Deliverables:**
1. Project skeleton with proper module structure
2. URL parsing with parameter extraction
3. SearchParameter definition loader
4. Unit tests for parsing

### Phase 2: Basic Parameter Converters (Week 3-4)

**Objectives:**
- Implement converters for common types
- Handle basic modifiers
- Support simple queries

**Parameter Types to Implement:**
1. String (with :exact, :contains modifiers)
2. Token (basic system|code)
3. Reference (id format)
4. Date (with prefixes)
5. Number (with prefixes)

**Deliverables:**
1. Working converters for 5 core types
2. Integration tests
3. Example queries

### Phase 3: Advanced Features (Week 5-6)

**Objectives:**
- Implement remaining parameter types
- Add complex modifiers
- Support chaining

**Features:**
1. Quantity parameters
2. Composite parameters
3. URI parameters
4. Chaining (forward and reverse)
5. Advanced modifiers (:in, :not-in, :above, :below)

### Phase 4: Optimization & Special Cases (Week 7-8)

**Objectives:**
- Query optimization
- Performance tuning
- Handle edge cases

**Features:**
1. Index utilization
2. Query plan analysis
3. Result modifiers (_sort, _count, _include)
4. Special parameters (_filter, _has, _text)

### Phase 5: Testing & Documentation (Week 9-10)

**Objectives:**
- Comprehensive testing
- Performance benchmarks
- Documentation

**Deliverables:**
1. Test suite with 200+ test cases
2. Performance benchmark report
3. API documentation
4. Usage examples

---

## Code Library Structure

### Directory Layout

```
fhir_query_mql/
│
├── README.md                          # Library overview
├── setup.py                           # Installation configuration
├── requirements.txt                   # Dependencies (PyYAML, pymongo, etc.)
│
├── config/                            # Configuration files (CRITICAL)
│   ├── __init__.py
│   ├── mappings/                      # Resource-specific mapping configs
│   │   ├── Patient.yaml               # Patient search parameter mappings
│   │   ├── Observation.yaml           # Observation mappings
│   │   ├── Appointment.yaml           # Appointment mappings
│   │   ├── Schedule.yaml              # Schedule mappings
│   │   ├── Slot.yaml                  # Slot mappings
│   │   ├── Condition.yaml             # Condition mappings
│   │   ├── Procedure.yaml             # Procedure mappings
│   │   ├── MedicationRequest.yaml     # MedicationRequest mappings
│   │   └── ...                        # Additional resources
│   ├── settings.yaml                  # Global library settings
│   └── schema/                        # Configuration validation schemas
│       └── mapping_schema.json        # JSON schema for mapping configs
│
├── fhir_to_mql/                       # Main package
│   ├── __init__.py                    # Package initialization
│   │
│   ├── core/                          # Core conversion logic
│   │   ├── __init__.py
│   │   ├── converter.py               # Main FHIRToMQLConverter class
│   │   ├── mapping_loader.py          # Load & cache mapping configs
│   │   ├── field_resolver.py          # Resolve MongoDB field paths
│   │   ├── query_builder.py           # Build final MQL query
│   │   └── optimizer.py               # Query optimization
│   │
│   ├── parsers/                       # Query parsing
│   │   ├── __init__.py
│   │   ├── url_parser.py              # Parse FHIR search URLs
│   │   ├── parameter_parser.py        # Parse individual parameters
│   │   └── validator.py               # Validate syntax
│   │
│   ├── converters/                    # Type-specific converters
│   │   ├── __init__.py
│   │   ├── base.py                    # BaseConverter abstract class
│   │   ├── string_converter.py        # String parameter converter
│   │   ├── token_converter.py         # Token parameter converter
│   │   ├── reference_converter.py     # Reference parameter converter
│   │   ├── date_converter.py          # Date parameter converter
│   │   ├── number_converter.py        # Number parameter converter
│   │   ├── quantity_converter.py      # Quantity parameter converter
│   │   ├── uri_converter.py           # URI parameter converter
│   │   ├── composite_converter.py     # Composite parameter converter
│   │   └── special_converter.py       # Special parameter converter
│   │
│   ├── modifiers/                     # Modifier handlers
│   │   ├── __init__.py
│   │   ├── string_modifiers.py        # String modifiers (:exact, :contains)
│   │   ├── token_modifiers.py         # Token modifiers (:not, :text, :in)
│   │   ├── reference_modifiers.py     # Reference modifiers (:identifier, :[type])
│   │   └── common_modifiers.py        # Common modifiers (:missing, etc.)
│   │
│   ├── utils/                         # Utilities
│   │   ├── __init__.py
│   │   ├── validation.py              # Validate configurations
│   │   ├── cache.py                   # Caching utilities
│   │   ├── date_utils.py              # Date/time utilities
│   │   ├── regex_builder.py           # Regex pattern builder
│   │   └── helpers.py                 # Helper functions
│   │
│   ├── exceptions.py                  # Custom exceptions
│   └── types.py                       # Type definitions
│
├── tests/                             # Test suite
│   ├── __init__.py
│   ├── test_converters/
│   │   ├── test_string_converter.py
│   │   ├── test_token_converter.py
│   │   ├── test_reference_converter.py
│   │   └── test_date_converter.py
│   ├── test_mappings/
│   │   ├── test_patient_mapping.py
│   │   ├── test_observation_mapping.py
│   │   └── test_config_validation.py
│   ├── test_integration/
│   │   ├── test_patient_queries.py
│   │   ├── test_observation_queries.py
│   │   └── test_performance.py
│   └── fixtures/
│       ├── sample_configs/
│       │   └── TestResource.yaml
│       └── test_cases.json
│
├── examples/                          # Usage examples
│   ├── basic_usage.py
│   ├── custom_mapping.py
│   ├── advanced_queries.py
│   └── performance_test.py
│
└── docs/                              # Documentation
    ├── configuration_guide.md         # Complete config reference
    ├── adding_new_resource.md         # Guide for adding resources
    ├── api_reference.md               # API documentation
    ├── performance_guide.md           # Performance optimization
    └── troubleshooting.md             # Common issues & solutions
```

### Core Classes

#### 1. `FHIRToMQLConverter` (Main Entry Point)

```python
# fhir_to_mql/core/converter.py

from typing import Dict, List, Optional
from .mapping_loader import MappingLoader
from .field_resolver import FieldResolver
from .query_builder import QueryBuilder
from ..parsers.url_parser import URLParser
from ..converters import get_converter

class FHIRToMQLConverter:
    """
    Main converter class that orchestrates the conversion process.
    
    This class coordinates:
    1. Parsing FHIR search queries
    2. Loading mapping configurations
    3. Resolving MongoDB field paths
    4. Converting parameters to MQL
    5. Building and optimizing final query
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize converter with configuration.
        
        Args:
            config_path: Path to configuration directory.
                        If None, uses default config/ directory.
        """
        self.mapping_loader = MappingLoader(config_path)
        self.field_resolver = FieldResolver(self.mapping_loader)
        self.query_builder = QueryBuilder()
        self.url_parser = URLParser()
    
    def convert(self, resource_type: str, search_url: str) -> Dict:
        """
        Convert FHIR search URL to MongoDB query.
        
        Args:
            resource_type: FHIR resource type (e.g., 'Patient', 'Observation')
            search_url: FHIR search query string or full URL
            
        Returns:
            MongoDB query dictionary
            
        Example:
            >>> converter = FHIRToMQLConverter()
            >>> mql = converter.convert('Patient', 'name=Smith&gender=male')
            >>> print(mql)
            {
                "$and": [
                    {"$or": [
                        {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
                        {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}}
                    ]},
                    {"gender": "male"}
                ]
            }
        """
        # 1. Parse search URL
        parsed_query = self.url_parser.parse(search_url)
        
        # 2. Load mapping configuration for resource type
        mapping_config = self.mapping_loader.load(resource_type)
        
        # 3. Convert each parameter
        mql_fragments = []
        
        for param_name, param_values in parsed_query.parameters.items():
            # Resolve field paths and metadata from mapping config
            field_info = self.field_resolver.resolve(
                resource_type, 
                param_name, 
                mapping_config
            )
            
            # Get appropriate converter for parameter type
            converter = get_converter(field_info['type'])
            
            # Convert parameter values to MQL
            for param_value in param_values:
                mql_fragment = converter.convert(
                    field_info=field_info,
                    value=param_value,
                    modifier=parsed_query.get_modifier(param_name),
                    prefix=parsed_query.get_prefix(param_value)
                )
                mql_fragments.append(mql_fragment)
        
        # 4. Build final query
        final_query = self.query_builder.build(
            mql_fragments,
            operator=mapping_config.get('settings', {}).get(
                'defaultParameterOperator', 'AND'
            )
        )
        
        return final_query
    
    def convert_to_aggregation(self, resource_type: str, 
                               search_url: str) -> List[Dict]:
        """
        Convert FHIR search URL to MongoDB aggregation pipeline.
        
        Use this for complex queries requiring aggregation operations.
        
        Args:
            resource_type: FHIR resource type
            search_url: FHIR search query string
            
        Returns:
            MongoDB aggregation pipeline (list of stages)
        """
        # Convert to basic query first
        mql_query = self.convert(resource_type, search_url)
        
        # Build aggregation pipeline
        pipeline = [
            {"$match": mql_query},
            # Additional stages can be added based on modifiers
            # e.g., _sort, _count, _include
        ]
        
        return pipeline
    
    def validate_config(self, resource_type: str) -> List[str]:
        """
        Validate mapping configuration for a resource type.
        
        Args:
            resource_type: FHIR resource type to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        from ..utils.validation import validate_mapping_config
        
        mapping_config = self.mapping_loader.load(resource_type)
        errors = validate_mapping_config(mapping_config, resource_type)
        
        return errors
```

#### 2. `MappingLoader` (Configuration Management)

```python
# fhir_to_mql/core/mapping_loader.py

import os
import yaml
from typing import Dict, Optional
from ..utils.cache import Cache
from ..utils.validation import validate_mapping_config
from ..exceptions import ConfigurationError

class MappingLoader:
    """
    Load and cache mapping configurations.
    
    Responsibilities:
    - Load YAML configuration files from disk
    - Validate configuration schemas
    - Cache configurations in memory for performance
    - Provide configuration reload capability
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize mapping loader.
        
        Args:
            config_path: Path to config directory. If None, uses default.
        """
        if config_path is None:
            # Default to config/ directory relative to package root
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'config'
            )
        
        self.config_path = config_path
        self.mappings_path = os.path.join(config_path, 'mappings')
        self.cache = Cache()
        
        # Validate config directory exists
        if not os.path.exists(self.mappings_path):
            raise ConfigurationError(
                f"Mapping configuration directory not found: {self.mappings_path}"
            )
    
    def load(self, resource_type: str) -> Dict:
        """
        Load mapping configuration for a resource type.
        
        Args:
            resource_type: FHIR resource type (e.g., 'Patient')
            
        Returns:
            Mapping configuration dictionary
            
        Raises:
            ConfigurationError: If mapping file doesn't exist or is invalid
        """
        # Check cache first
        cache_key = f"mapping:{resource_type}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached
        
        # Load from file
        mapping_file = os.path.join(self.mappings_path, f"{resource_type}.yaml")
        
        if not os.path.exists(mapping_file):
            raise ConfigurationError(
                f"No mapping configuration found for resource type: {resource_type}. "
                f"Expected file: {mapping_file}"
            )
        
        try:
            with open(mapping_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Invalid YAML in mapping file {mapping_file}: {e}"
            )
        
        # Validate configuration
        errors = validate_mapping_config(config, resource_type)
        if errors:
            raise ConfigurationError(
                f"Invalid mapping configuration for {resource_type}:\n" +
                "\n".join(f"  - {error}" for error in errors)
            )
        
        # Cache and return
        self.cache.set(cache_key, config, ttl=3600)  # Cache for 1 hour
        return config
    
    def reload(self, resource_type: str) -> Dict:
        """
        Force reload of mapping configuration (bypass cache).
        
        Use this during development or when configuration changes.
        """
        cache_key = f"mapping:{resource_type}"
        self.cache.delete(cache_key)
        return self.load(resource_type)
    
    def list_available_resources(self) -> List[str]:
        """
        List all available resource types with mapping configurations.
        
        Returns:
            List of resource type names
        """
        yaml_files = [
            f for f in os.listdir(self.mappings_path)
            if f.endswith('.yaml') or f.endswith('.yml')
        ]
        
        return [os.path.splitext(f)[0] for f in yaml_files]
```

#### 3. `FieldResolver` (Field Path Resolution)

```python
# fhir_to_mql/core/field_resolver.py

from typing import Dict, List, Optional
from ..exceptions import ParameterNotFoundError

class FieldResolver:
    """
    Resolve MongoDB field paths from mapping configuration.
    
    Responsibilities:
    - Look up parameter definitions in mapping config
    - Extract field paths for a given search parameter
    - Provide parameter type and metadata
    - Handle multi-field searches
    """
    
    def __init__(self, mapping_loader):
        self.mapping_loader = mapping_loader
    
    def resolve(self, resource_type: str, param_name: str, 
                mapping_config: Dict) -> Dict:
        """
        Resolve field information for a search parameter.
        
        Args:
            resource_type: FHIR resource type
            param_name: Search parameter name
            mapping_config: Loaded mapping configuration
            
        Returns:
            Dictionary with field information:
            {
                'type': 'string',
                'fields': [
                    {'field': '_search.familyName', 'weight': 1.0},
                    {'field': '_search.givenNames', 'weight': 0.8}
                ],
                'operator': 'OR',
                'tokenType': 'code',  # For token parameters
                'referenceTypes': ['Patient'],  # For reference parameters
                'prefixes': ['gt', 'lt', 'ge', 'le'],  # For date/number
                'modifiers': ['exact', 'contains']  # Allowed modifiers
            }
            
        Raises:
            ParameterNotFoundError: If parameter not found in configuration
        """
        parameters = mapping_config.get('parameters', {})
        
        if param_name not in parameters:
            available = list(parameters.keys())
            raise ParameterNotFoundError(
                f"Unknown search parameter '{param_name}' for resource "
                f"type '{resource_type}'. Available parameters: {available}"
            )
        
        param_config = parameters[param_name]
        
        # Build field information
        field_info = {
            'type': param_config['type'],
            'fields': self._parse_fields(param_config['fields']),
            'operator': param_config.get('operator', 'OR'),
            'modifiers': param_config.get('modifiers', []),
        }
        
        # Add type-specific metadata
        if param_config['type'] == 'token':
            field_info['tokenType'] = param_config.get('tokenType', 'code')
            field_info['allowedValues'] = param_config.get('allowedValues')
        elif param_config['type'] == 'reference':
            field_info['referenceTypes'] = param_config.get('referenceTypes', [])
        elif param_config['type'] in ['date', 'number']:
            field_info['prefixes'] = param_config.get('prefixes', [])
        
        return field_info
    
    def _parse_fields(self, fields_config: List) -> List[Dict]:
        """
        Parse fields configuration.
        
        Handles both simple format:
            fields: [field1, field2]
        
        And complex format:
            fields:
              - field: field1
                weight: 1.0
              - field: field2
                weight: 0.8
        """
        parsed_fields = []
        
        for field in fields_config:
            if isinstance(field, str):
                # Simple format
                parsed_fields.append({
                    'field': field,
                    'weight': 1.0,
                    'primary': True
                })
            elif isinstance(field, dict):
                # Complex format
                parsed_fields.append({
                    'field': field['field'],
                    'weight': field.get('weight', 1.0),
                    'tokenType': field.get('tokenType'),
                    'primary': field.get('primary', True)
                })
        
        return parsed_fields
```

#### 4. `QueryBuilder` (Query Construction)
        Convert to MongoDB aggregation pipeline.
        
        Useful for complex queries with _include, _revinclude, sorting.
        """
        pass
```

#### 2. `URLParser`

```python
class URLParser:
    """Parse FHIR search URLs and extract parameters."""
    
    def parse(self, url: str) -> ParsedQuery:
        """
        Parse FHIR search URL.
        
        Returns:
            ParsedQuery object containing:
            - resource_type
            - parameters (dict)
            - modifiers (dict)
            - result_parameters (dict)
        """
        pass
```

#### 3. `BaseConverter` (Abstract Base Class)

```python
class BaseConverter(ABC):
    """Base class for all parameter type converters."""
    
    @abstractmethod
    def convert(self, field_path: str, value: str, 
                modifier: str = None, prefix: str = None) -> Dict:
        """
        Convert parameter to MongoDB query fragment.
        
        Args:
            field_path: MongoDB field path (e.g., 'name.family')
            value: Parameter value
            modifier: Optional modifier (e.g., 'exact', 'contains')
            prefix: Optional prefix (e.g., 'gt', 'le')
            
        Returns:
            MongoDB query fragment
        """
        pass
```

---

## Detailed Conversion Rules

### 1. String Parameter Type

**FHIR Behavior:**
- Case-insensitive
- Accent-insensitive
- Default: Starts with match
- Supports: :exact, :contains modifiers

**`_search` Denormalization:**

For simple string fields at root level, use canonical field directly:
```javascript
{
  "name": "Dr. Smith",  // Canonical field
  // No _search needed for top-level simple strings
}
```

For extracted/flattened strings, use `_search`:
```javascript
{
  "_search": {
    "familyName": "Smith",            // Extracted from name[].family
    "givenNames": ["John", "Michael"], // Extracted from name[].given
    "fullName": "John Michael Smith"   // Concatenated for full-text search
  }
}
```

**MQL Conversion:**

#### Default String Search (Starts With)

**FHIR:**
```
GET /Patient?name=Smith
```

**MQL (Simple - use canonical field when possible):**
```javascript
{
  "name": { "$regex": "^Smith", "$options": "i" }
}
```

**MQL (With _search - when searching extracted fields):**
```javascript
{
  "$or": [
    { "_search.familyName": { "$regex": "^Smith", "$options": "i" } },
    { "_search.givenNames": { "$regex": "^Smith", "$options": "i" } }
  ]
}
```

**Implementation:**
```python
class StringConverter(BaseConverter):
    def convert(self, search_param: str, value: str, 
                modifier: str = None) -> Dict:
        """
        Convert string parameter to MQL.
        
        Strategy:
        1. Check if field is simple (use canonical) or complex (use _search)
        2. Apply modifier (exact, contains, or default starts-with)
        3. Use regex only when necessary
        """
        # Get field path (canonical or _search)
        field_path = self._resolve_field_path(search_param)
        
        if modifier == "exact":
            return self._exact_match(field_path, value)
        elif modifier == "contains":
            return self._contains_match(field_path, value)
        else:
            return self._starts_with_match(field_path, value)
    
    def _starts_with_match(self, field_path: str, value: str) -> Dict:
        """Case-insensitive starts-with match."""
        escaped_value = re.escape(value)
        return {
            field_path: {
                "$regex": f"^{escaped_value}",
                "$options": "i"
            }
        }
    
    def _exact_match(self, field_path: str, value: str) -> Dict:
        """Exact match (case-insensitive in MongoDB by default)."""
        return {field_path: value}
    
    def _contains_match(self, field_path: str, value: str) -> Dict:
        """Case-insensitive substring match."""
        escaped_value = re.escape(value)
        return {
            field_path: {
                "$regex": escaped_value,
                "$options": "i"
            }
        }
```

#### String with :exact Modifier

**FHIR:**
```
GET /Patient?family:exact=Smith
```

**MQL:**
```javascript
{
  "_search.familyName": "Smith"
}
```

#### String with :contains Modifier

**FHIR:**
```
GET /Patient?family:contains=mit
```

**MQL:**
```javascript
{
  "_search.familyName": { "$regex": "mit", "$options": "i" }
}
```

**Performance Notes:**
- Use canonical fields when possible (avoid unnecessary denormalization)
- Anchor regex with `^` for starts-with (much faster)
- Avoid unanchored regex on large collections
- Consider MongoDB text indexes for complex text search
- Typical query time: 5-20ms depending on regex complexity

---

### 2. Token Parameter Type

**FHIR Behavior:**
- Matches code, system|code, or |code
- Case-sensitive for codes
- Supports: :text, :not, :in, :not-in, :above, :below

**`_search` Denormalization (CRITICAL FOR PERFORMANCE):**

**ALWAYS denormalize CodeableConcept to simple arrays:**

```javascript
{
  // Canonical FHIR
  "code": {
    "coding": [
      {"system": "http://loinc.org", "code": "8480-6", "display": "Systolic BP"},
      {"system": "http://snomed.info/sct", "code": "271649006", "display": "Systolic"}
    ]
  },
  
  // _search Denormalization
  "_search": {
    "codeCodes": ["8480-6", "271649006"],                    // Code-only array (FAST)
    "codeSystems": ["http://loinc.org", "http://snomed.info/sct"],
    "codeSystemValues": [                                     // System|code pairs (PRECISE)
      "http://loinc.org|8480-6",
      "http://snomed.info/sct|271649006"
    ]
  }
}
```

**MQL Conversion:**

#### Simple Token (Code Only) - FAST

**FHIR:**
```
GET /Patient?gender=male
```

**MQL (Simple field):**
```javascript
{
  "gender": "male"
}
```

**FHIR:**
```
GET /Observation?code=8480-6
```

**MQL (Denormalized - use _search):**
```javascript
{
  "_search.codeCodes": "8480-6"  // Simple array membership - FAST!
}
```

**❌ DO NOT USE** (complex, slow):
```javascript
{
  "code.coding.code": "8480-6"  // Array field, less efficient
}
```

**❌ DEFINITELY DO NOT USE** (very slow):
```javascript
{
  "code.coding": {
    "$elemMatch": {"code": "8480-6"}  // Slow $elemMatch
  }
}
```

#### Token with System (system|code) - PRECISE

**FHIR:**
```
GET /Observation?code=http://loinc.org|8480-6
```

**MQL (Use systemValues for precision):**
```javascript
{
  "_search.codeSystemValues": "http://loinc.org|8480-6"
}
```

**Alternative (less precise):**
```javascript
{
  "$and": [
    { "_search.codeCodes": "8480-6" },
    { "_search.codeSystems": "http://loinc.org" }
  ]
}
```

**Implementation:**
```python
class TokenConverter(BaseConverter):
    def convert(self, search_param: str, value: str, 
                modifier: str = None) -> Dict:
        """
        Convert token parameter to MQL using _search fields.
        
        Strategy:
        1. For simple tokens (gender, status): use canonical field
        2. For CodeableConcept: ALWAYS use _search denormalized arrays
        3. Parse system|code format
        4. Choose appropriate _search field (codes, systems, or systemValues)
        """
        field_info = self._get_field_info(search_param)
        
        if field_info['is_simple']:
            # Simple token field (gender, status, etc.)
            return self._simple_token_match(field_info['canonical_path'], value, modifier)
        else:
            # Complex CodeableConcept - use _search
            return self._codeable_concept_match(field_info['search_path'], value, modifier)
    
    def _codeable_concept_match(self, base_path: str, value: str, modifier: str = None) -> Dict:
        """Match CodeableConcept using _search arrays."""
        
        if modifier == "not":
            base_query = self._codeable_concept_match(base_path, value, modifier=None)
            return {"$nor": [base_query]}
        
        if "|" in value:
            return self._parse_system_code(base_path, value)
        else:
            # Code only - use codes array
            return {
                f"{base_path}Codes": value  # e.g., "_search.codeCodes"
            }
    
    def _parse_system_code(self, base_path: str, value: str) -> Dict:
        """Parse system|code format."""
        parts = value.split("|", 1)
        system = parts[0]
        code = parts[1] if len(parts) > 1 else ""
        
        if not system and code:  # |code format (any system)
            return {f"{base_path}Codes": code}
        elif system and not code:  # system| format (any code in system)
            return {f"{base_path}Systems": system}
        else:  # system|code format (most precise)
            return {
                f"{base_path}SystemValues": f"{system}|{code}"
            }
    
    def _simple_token_match(self, field_path: str, value: str, modifier: str = None) -> Dict:
        """Match simple token fields."""
        if modifier == "not":
            return {"$nor": [{field_path: value}]}
        else:
            return {field_path: value}
```

#### Token with :not Modifier

**FHIR:**
```
GET /Patient?gender:not=male
```

**MQL:**
```javascript
{
  "$nor": [
    { "gender": "male" }
  ]
}
```

#### Multiple Token Values (OR)

**FHIR:**
```
GET /Observation?code=8480-6,8462-4
```

**MQL:**
```javascript
{
  "_search.codeCodes": { 
    "$in": ["8480-6", "8462-4"] 
  }
}
```

**Performance Notes:**
- `_search` arrays with simple equality: **5-10x faster** than `$elemMatch`
- Use `codeCodes` for code-only search (fastest)
- Use `codeSystemValues` for system|code precision
- Index all three arrays: `codeCodes`, `codeSystems`, `codeSystemValues`
- Typical query time: <5ms with proper indexing

---

### 3. Reference Parameter Type

**FHIR Behavior:**
- Matches: id, Type/id, or full URL
- Supports: :identifier, :[type] modifiers
- Chaining allowed

**`_search` Denormalization (CRITICAL):**

**ALWAYS extract reference IDs to `_search` for performance:**

```javascript
{
  // Canonical FHIR
  "subject": {
    "reference": "Patient/pat-123",
    "display": "John Smith"
  },
  
  // _search Denormalization
  "_search": {
    "patientId": "pat-123",          // Extracted ID (PRIMARY, FASTEST)
    "patientName": "John Smith"      // Cached display name
  }
}
```

**For multiple participants:**
```javascript
{
  // Canonical FHIR
  "participant": [
    {"actor": {"reference": "Patient/pat-123"}},
    {"actor": {"reference": "Practitioner/prac-456"}},
    {"actor": {"reference": "Location/loc-789"}}
  ],
  
  // _search Denormalization
  "_search": {
    // Primary IDs (single values, most efficient)
    "patientId": "pat-123",
    "practitionerId": "prac-456",
    "locationId": "loc-789",
    
    // All actors (for broad searches)
    "actor": {
      "ids": ["pat-123", "prac-456", "loc-789"],
      "types": ["Patient", "Practitioner", "Location"],
      "references": ["Patient/pat-123", "Practitioner/prac-456", "Location/loc-789"]
    }
  }
}
```

**MQL Conversion:**

#### Simple Reference (ID) - FASTEST

**FHIR:**
```
GET /Observation?subject=Patient/pat-123
```
or
```
GET /Observation?subject=pat-123
```

**MQL (Use extracted ID - SIMPLE and FAST):**
```javascript
{
  "_search.patientId": "pat-123"  // Direct ID match - FASTEST!
}
```

**❌ DO NOT USE** (slower):
```javascript
{
  "subject.reference": "Patient/pat-123"  // String match, needs regex for variations
}
```

**❌ DEFINITELY DO NOT USE** (much slower):
```javascript
{
  "$or": [
    { "subject.reference": "Patient/pat-123" },
    { "subject.reference": "pat-123" },
    { "subject.reference": { "$regex": "/pat-123$" } }
  ]
}
```

#### Reference with Type Modifier

**FHIR:**
```
GET /Observation?subject:Patient=pat-123
```

**MQL:**
```javascript
{
  "_search.patientId": "pat-123"  // Type already implicit in field name
}
```

#### Reference with :identifier Modifier

**FHIR:**
```
GET /Observation?subject:identifier=http://hospital.org/mrn|12345
```

**MQL (Use subject's identifier search - requires join or denormalization):**

Option 1: If subject identifiers are denormalized in Observation:
```javascript
{
  "_search.patientIdentifier.systemValues": "http://hospital.org/mrn|12345"
}
```

Option 2: Two-stage query (find patient first, then observations):
```javascript
// Stage 1: Find patient
db.Patient.find({
  "_search.identifier.systemValues": "http://hospital.org/mrn|12345"
}, {_id: 1})

// Stage 2: Find observations
db.Observation.find({
  "_search.patientId": { $in: ["pat-123", ...] }
})
```

**Implementation:**
```python
class ReferenceConverter(BaseConverter):
    def convert(self, search_param: str, value: str, 
                modifier: str = None) -> Dict:
        """
        Convert reference parameter to MQL using _search extracted IDs.
        
        Strategy:
        1. ALWAYS use _search extracted ID fields (e.g., patientId, practitionerId)
        2. This avoids string parsing and regex
        3. Much faster than matching reference strings
        """
        field_info = self._get_field_info(search_param)
        
        if modifier == "identifier":
            return self._identifier_match(field_info, value)
        elif modifier and modifier[0].isupper():  # Type modifier
            return self._type_restricted_match(field_info, value, modifier)
        else:
            return self._standard_match(field_info, value)
    
    def _standard_match(self, field_info: Dict, value: str) -> Dict:
        """
        Standard reference match using _search extracted ID.
        
        Example: subject=Patient/pat-123 or subject=pat-123
        Both resolve to: {"_search.patientId": "pat-123"}
        """
        # Extract ID from value
        if "/" in value and not value.startswith("http"):
            # Type/id format: "Patient/pat-123" -> "pat-123"
            resource_type, resource_id = value.split("/", 1)
        elif value.startswith("http"):
            # Full URL: extract last part
            resource_id = value.split("/")[-1]
        else:
            # Just ID: "pat-123"
            resource_id = value
        
        # Use extracted ID field from _search
        id_field = field_info['search_id_field']  # e.g., "_search.patientId"
        
        return {id_field: resource_id}
    
    def _type_restricted_match(self, field_info: Dict, 
                               value: str, resource_type: str) -> Dict:
        """Match with specific resource type."""
        # Type is already implicit in the _search field name
        # e.g., subject:Patient -> use _search.patientId
        return self._standard_match(field_info, value)
```

**Performance Notes:**
- Using `_search` extracted IDs: **10-20x faster** than reference string matching
- No regex needed, no string parsing
- Direct index lookup
- Typical query time: <5ms

---

### 4. Date Parameter Type

**FHIR Behavior:**
- Implicit precision-based ranges
- Prefixes: eq, ne, gt, lt, ge, le, sa, eb, ap
- Handles: date, dateTime, instant, Period, Timing

**`_search` Denormalization:**

For simple date fields at root, use canonical OR copy to `_search` for consistency:

```javascript
{
  "birthDate": "1985-03-15",  // Canonical
  "_search": {
    "birthDate": "1985-03-15"  // Optional copy for compound indexes
  }
}
```

For Period/Timing fields, ALWAYS extract to `_search`:

```javascript
{
  // Canonical FHIR
  "start": "2026-05-15T14:30:00Z",
  "end": "2026-05-15T15:00:00Z",
  
  // _search Denormalization
  "_search": {
    "start": "2026-05-15T14:30:00Z",   // ISO datetime (for indexed queries)
    "end": "2026-05-15T15:00:00Z",
    "durationMinutes": 30               // Optional: pre-calculated duration
  }
}
```

**MQL Conversion:**

#### Date with Prefix

**FHIR:**
```
GET /Patient?birthdate=gt1980-01-01
```

**MQL:**
```javascript
{
  "birthDate": { "$gt": "1980-01-01" }
}
```

**FHIR:**
```
GET /Appointment?date=ge2026-05-15
```

**MQL:**
```javascript
{
  "_search.start": { "$gte": "2026-05-15T00:00:00Z" }
}
```

**Implementation:**
```python
class DateConverter(BaseConverter):
    def convert(self, search_param: str, value: str, 
                modifier: str = None, prefix: str = None) -> Dict:
        """
        Convert date parameter to MQL.
        
        Strategy:
        1. Use canonical field for simple root-level dates (birthDate)
        2. Use _search for extracted dates (start, end from Period)
        3. Handle precision (year, month, day, datetime)
        4. Apply prefix operators (gt, lt, ge, le, etc.)
        """
        field_info = self._get_field_info(search_param)
        field_path = field_info['field_path']  # Canonical or _search path
        
        # Parse date and determine precision
        date_obj, precision = self._parse_date(value)
        
        # Calculate implicit range based on precision
        start_date, end_date = self._calculate_range(date_obj, precision)
        
        if prefix is None or prefix == "eq":
            return self._equal_match(field_path, start_date, end_date)
        elif prefix == "ne":
            return self._not_equal_match(field_path, start_date, end_date)
        elif prefix == "gt":
            return {field_path: {"$gt": end_date}}
        elif prefix == "ge":
            return {field_path: {"$gte": start_date}}
        elif prefix == "lt":
            return {field_path: {"$lt": start_date}}
        elif prefix == "le":
            return {field_path: {"$lte": end_date}}
        elif prefix == "sa":  # starts after
            return {field_path: {"$gte": end_date}}
        elif prefix == "eb":  # ends before
            return {field_path: {"$lt": start_date}}
        elif prefix == "ap":  # approximately
            return self._approximate_match(field_path, date_obj)
    
    def _parse_date(self, value: str) -> Tuple[datetime, str]:
        """Parse date and determine precision."""
        if len(value) == 4:  # YYYY
            return datetime.strptime(value, "%Y"), "year"
        elif len(value) == 7:  # YYYY-MM
            return datetime.strptime(value, "%Y-%m"), "month"
        elif len(value) == 10:  # YYYY-MM-DD
            return datetime.strptime(value, "%Y-%m-%d"), "day"
        else:  # Full datetime
            return datetime.fromisoformat(value.replace('Z', '+00:00')), "instant"
    
    def _calculate_range(self, date_obj: datetime, 
                        precision: str) -> Tuple[str, str]:
        """Calculate implicit range based on precision."""
        # Implementation details for year, month, day precision...
        pass
    
    def _equal_match(self, field_path: str, 
                    start_date: str, end_date: str) -> Dict:
        """Equal match using range."""
        if start_date == end_date:
            # Exact datetime
            return {field_path: start_date}
        else:
            # Range for precision (year, month, day)
            return {
                "$and": [
                    {field_path: {"$gte": start_date}},
                    {field_path: {"$lte": end_date}}
                ]
            }
```

#### Date Range Query

**FHIR:**
```
GET /Observation?date=ge2024-01-01&date=le2024-12-31
```

**MQL:**
```javascript
{
  "$and": [
    { "_search.start": { "$gte": "2024-01-01T00:00:00Z" } },
    { "_search.start": { "$lte": "2024-12-31T23:59:59Z" } }
  ]
}
```

**Performance Notes:**
- ISO datetime strings: MongoDB handles efficiently
- Use compound indexes with date + other fields
- Date range queries: 5-15ms with proper indexing
- Always use ISO format for date strings

---

### 5. Number Parameter Type

**FHIR Behavior:**
- Significant figures matter
- Implicit ranges based on precision
- Prefixes: eq, ne, gt, lt, ge, le

**MQL Conversion:**

#### Number with Significant Figures

**FHIR:**
```
GET /RiskAssessment?probability=0.5
```

**MQL (with implicit range for 1 significant figure):**
```javascript
{
  "$and": [
    { "prediction.probability": { "$gte": 0.45 } },
    { "prediction.probability": { "$lt": 0.55 } }
  ]
}
```

**Implementation:**
```python
class NumberConverter(BaseConverter):
    def convert(self, field_path: str, value: str, 
                modifier: str = None, prefix: str = None) -> Dict:
        
        # Parse number and calculate precision
        number, precision = self._parse_number(value)
        
        if prefix in ["gt", "lt", "ge", "le", "ne"]:
            # Use exact number for comparison operators
            return self._comparison_match(field_path, number, prefix)
        else:
            # Use range for equality
            lower, upper = self._calculate_range(number, precision)
            return {
                "$and": [
                    {field_path: {"$gte": lower}},
                    {field_path: {"$lt": upper}}
                ]
            }
    
    def _parse_number(self, value: str) -> Tuple[float, float]:
        """Parse number and determine precision."""
        if 'e' in value.lower():
            # Exponential notation
            number = float(value)
            # Count significant figures
            mantissa = value.lower().split('e')[0].replace('.', '').replace('-', '')
            sig_figs = len(mantissa.lstrip('0'))
            precision = 10 ** (len(str(int(number))) - sig_figs)
        else:
            number = float(value)
            # Count decimal places
            if '.' in value:
                decimal_places = len(value.split('.')[1])
                precision = 0.5 * (10 ** -decimal_places)
            else:
                precision = 0.5
        
        return number, precision
    
    def _calculate_range(self, number: float, 
                        precision: float) -> Tuple[float, float]:
        """Calculate implicit range based on precision."""
        lower = number - precision
        upper = number + precision
        return lower, upper
```

---

### 6. Quantity Parameter Type

**FHIR Behavior:**
- Matches value, system, and code/unit
- Three formats: [number], [number]|[system]|[code], [number]||[code]
- Prefixes apply to value

**MQL Conversion:**

#### Quantity with System and Code

**FHIR:**
```
GET /Observation?value-quantity=5.4|http://unitsofmeasure.org|mg
```

**MQL:**
```javascript
{
  "$or": [
    {
      "$and": [
        { "valueQuantity.value": { "$gte": 5.35, "$lt": 5.45 } },
        { "valueQuantity.system": "http://unitsofmeasure.org" },
        { "valueQuantity.code": "mg" }
      ]
    }
  ]
}
```

**Implementation:**
```python
class QuantityConverter(BaseConverter):
    def convert(self, field_path: str, value: str, 
                modifier: str = None, prefix: str = None) -> Dict:
        
        # Parse quantity value
        parts = value.split("|")
        
        if len(parts) == 1:
            # Value only
            number_value = parts[0]
            return self._value_only_match(field_path, number_value, prefix)
        elif len(parts) == 3:
            number_value, system, code = parts
            return self._full_match(field_path, number_value, system, code, prefix)
    
    def _full_match(self, field_path: str, number_value: str,
                   system: str, code: str, prefix: str = None) -> Dict:
        """Full quantity match with system and code."""
        # Parse number with precision
        number, precision = self._parse_number(number_value)
        
        conditions = []
        
        # Value condition (with prefix if provided)
        if prefix == "gt":
            conditions.append({f"{field_path}.value": {"$gt": number}})
        elif prefix == "ge":
            conditions.append({f"{field_path}.value": {"$gte": number}})
        elif prefix == "lt":
            conditions.append({f"{field_path}.value": {"$lt": number}})
        elif prefix == "le":
            conditions.append({f"{field_path}.value": {"$lte": number}})
        else:
            # Default: range based on precision
            lower, upper = self._calculate_range(number, precision)
            conditions.append({f"{field_path}.value": {"$gte": lower, "$lt": upper}})
        
        # System condition
        if system:
            conditions.append({f"{field_path}.system": system})
        
        # Code condition
        if code:
            conditions.append({f"{field_path}.code": code})
        
        return {"$and": conditions}
```

---

### 7. Composite Parameter Type

**FHIR Behavior:**
- Combines multiple parameters with `$` separator
- All components must match together
- Example: Search for observations with specific code AND specific value range

**The Composite Parameter Challenge:**

Composite parameters combine multiple simple parameters that must all match **on the same instance** of a repeating element. The naive approach might use `$elemMatch`, but this is **slow and should be avoided**.

**❌ ANTI-PATTERN: Using $elemMatch (DON'T DO THIS)**

```javascript
// SLOW - Avoid this!
{
  "$and": [
    {
      "component": {
        "$elemMatch": {
          "code.coding.code": "8480-6",
          "valueQuantity.value": { "$gt": 140 }
        }
      }
    }
  ]
}
```

**✅ RECOMMENDED: Use `_search` Field Denormalization**

For composite parameters, denormalize the combined criteria into `_search` fields during data ingestion.

---

#### Approach 1: Simple Composite (Independent Components)

For many composite searches, the components are actually **independent** (don't need to match on same nested object), so we can use simple `$and` with `_search` fields.

**Example: Observation code-value-quantity**

**FHIR Query:**
```
GET /Observation?code-value-quantity=http://loinc.org|8480-6$gt140|http://unitsofmeasure.org|mm[Hg]
```

This searches for:
- Code = LOINC 8480-6 (Systolic BP)
- Value > 140 mm[Hg]

**Mapping Configuration:**
```yaml
# config/mappings/Observation.yaml
parameters:
  code-value-quantity:
    type: composite
    description: "Search by code and value range"
    components:
      - name: code
        type: token
        fields:
          - field: _search.codeCodes
          - field: _search.codeSystemValues
      - name: value
        type: quantity
        fields:
          - field: _search.value
          - field: _search.valueUnit
          - field: _search.valueSystem
    operator: AND  # All components must match
```

**Denormalized Data Structure:**
```javascript
{
  "resourceType": "Observation",
  "id": "obs-001",
  "code": {
    "coding": [
      {
        "system": "http://loinc.org",
        "code": "8480-6",
        "display": "Systolic blood pressure"
      }
    ]
  },
  "valueQuantity": {
    "value": 145,
    "unit": "mm[Hg]",
    "system": "http://unitsofmeasure.org",
    "code": "mm[Hg]"
  },
  
  // Denormalized _search fields (populated at ingestion)
  "_search": {
    "codeCodes": ["8480-6"],
    "codeSystemValues": ["http://loinc.org|8480-6"],
    "value": 145,
    "valueUnit": "mm[Hg]",
    "valueSystem": "http://unitsofmeasure.org",
    "valueCode": "mm[Hg]"
  }
}
```

**Generated MQL (Simple, Fast):**
```javascript
{
  "$and": [
    // Code component
    {
      "$or": [
        {"_search.codeCodes": "8480-6"},
        {"_search.codeSystemValues": "http://loinc.org|8480-6"}
      ]
    },
    // Quantity component
    {
      "$and": [
        {"_search.value": {"$gt": 140}},
        {"_search.valueSystem": "http://unitsofmeasure.org"},
        {"_search.valueCode": "mm[Hg]"}
      ]
    }
  ]
}
```

**Performance:**
- ✅ No `$elemMatch` - uses simple indexed fields
- ✅ <15ms typical query time (vs 150ms+ with $elemMatch)
- ✅ Indexes: `(_search.codeCodes, _search.value)` compound index

---

#### Approach 2: True Composite (Same Element Match Required)

When components **must match on the same nested element** (rare), denormalize the combination.

**Example: Observation.component search**

Some observations have multiple components (e.g., Blood Pressure has systolic and diastolic):

```javascript
{
  "resourceType": "Observation",
  "code": {"coding": [{"code": "85354-9", "display": "Blood pressure"}]},
  "component": [
    {
      "code": {"coding": [{"code": "8480-6", "display": "Systolic"}]},
      "valueQuantity": {"value": 145, "unit": "mm[Hg]"}
    },
    {
      "code": {"coding": [{"code": "8462-4", "display": "Diastolic"}]},
      "valueQuantity": {"value": 90, "unit": "mm[Hg]"}
    }
  ]
}
```

**Challenge:** Search for systolic BP component > 140

**Denormalization Strategy:**

Create component-specific `_search` fields:

```javascript
{
  "_search": {
    // Component-specific denormalization
    "components": [
      {
        "code": "8480-6",
        "systemCode": "http://loinc.org|8480-6",
        "value": 145,
        "unit": "mm[Hg]"
      },
      {
        "code": "8462-4",
        "systemCode": "http://loinc.org|8462-4",
        "value": 90,
        "unit": "mm[Hg]"
      }
    ],
    
    // OR: Create flattened composite keys (BEST for performance)
    "component_8480-6_value": 145,      // Systolic value
    "component_8480-6_unit": "mm[Hg]",
    "component_8462-4_value": 90,        // Diastolic value
    "component_8462-4_unit": "mm[Hg]"
  }
}
```

**Option A: Use `$elemMatch` on denormalized array (Better than canonical)**

```javascript
{
  "_search.components": {
    "$elemMatch": {
      "code": "8480-6",
      "value": {"$gt": 140},
      "unit": "mm[Hg]"
    }
  }
}
```

This is better than canonical `$elemMatch` because:
- Smaller, flatter structure
- Fewer fields to check
- Still indexed on `_search.components`

**Option B: Use composite field keys (BEST - No $elemMatch)**

```javascript
{
  "$and": [
    {"_search.component_8480-6_value": {"$gt": 140}},
    {"_search.component_8480-6_unit": "mm[Hg]"}
  ]
}
```

This is fastest because:
- ✅ No `$elemMatch` at all
- ✅ Direct field access
- ✅ Can create specific indexes: `_search.component_8480-6_value`
- ✅ <5ms query time

**Mapping Configuration:**
```yaml
# config/mappings/Observation.yaml
parameters:
  component-code-value-quantity:
    type: composite
    description: "Search by component code and value"
    denormalizationStrategy: compositeKeys
    components:
      - name: code
        type: token
        keyPattern: "component_{code}_"  # Creates component_8480-6_*
      - name: value
        type: quantity
        keyPattern: "component_{code}_value"
        unitKeyPattern: "component_{code}_unit"
    operator: AND
```

**Data Ingestion Logic:**
```python
def denormalize_observation_components(observation: Dict) -> Dict:
    """Create composite key fields for components."""
    search_fields = {}
    
    for component in observation.get('component', []):
        code = component['code']['coding'][0]['code']
        
        if 'valueQuantity' in component:
            value = component['valueQuantity']['value']
            unit = component['valueQuantity'].get('unit', '')
            
            # Create composite keys
            search_fields[f'component_{code}_value'] = value
            search_fields[f'component_{code}_unit'] = unit
    
    return search_fields
```

---

#### Approach 3: Pre-computed Composite Strings

For common composite searches, pre-compute the entire composite as a string.

**Example: code-value-string composite**

```javascript
{
  "_search": {
    // Individual components
    "codeCodes": ["8480-6"],
    "value": 145,
    
    // Pre-computed composite (for exact match)
    "codeValueComposite": "8480-6:145:mm[Hg]",
    
    // Multiple composites for common searches
    "codeValueSystemComposites": [
      "http://loinc.org|8480-6:145:mm[Hg]",
      "8480-6:145:mm[Hg]"
    ]
  }
}
```

**Query:**
```javascript
{
  "_search.codeValueComposite": {
    "$regex": "^8480-6:1[4-5][0-9]:"  // Code 8480-6, value 140-159
  }
}
```

---

### Composite Parameter Implementation

**Implementation:**
```python
# converters/composite_converter.py

class CompositeConverter(BaseConverter):
    """
    Convert composite parameters using _search field patterns.
    """
    
    def __init__(self):
        self.component_converters = {
            'token': TokenConverter(),
            'quantity': QuantityConverter(),
            'string': StringConverter(),
            'date': DateConverter(),
            'number': NumberConverter()
        }
    
    def convert(self, field_info: Dict, value: str,
                modifier: str = None, prefix: str = None) -> Dict:
        """
        Convert composite parameter.
        
        Args:
            field_info: Field information from mapping config including:
                - components: List of component definitions
                - denormalizationStrategy: 'independent', 'compositeKeys', 'precomputed'
            value: Composite value with $ separators (e.g., "8480-6$gt140$mm[Hg]")
        
        Returns:
            MongoDB query combining all component conditions
        """
        # Split by $ separator
        component_values = value.split("$")
        components = field_info['components']
        
        if len(component_values) != len(components):
            raise ValueError(
                f"Expected {len(components)} components, got {len(component_values)}"
            )
        
        strategy = field_info.get('denormalizationStrategy', 'independent')
        
        if strategy == 'compositeKeys':
            return self._convert_composite_keys(field_info, component_values)
        elif strategy == 'precomputed':
            return self._convert_precomputed(field_info, value)
        else:
            return self._convert_independent(field_info, component_values)
    
    def _convert_independent(self, field_info: Dict, component_values: List[str]) -> Dict:
        """
        Convert composite with independent components.
        Each component searches its own _search fields.
        """
        conditions = []
        
        for component_def, component_value in zip(
            field_info['components'], component_values
        ):
            # Get converter for component type
            converter = self.component_converters[component_def['type']]
            
            # Create field_info for this component
            component_field_info = {
                'type': component_def['type'],
                'fields': component_def['fields'],
                'operator': component_def.get('operator', 'OR')
            }
            
            # Convert component
            component_query = converter.convert(
                field_info=component_field_info,
                value=component_value,
                modifier=component_def.get('modifier'),
                prefix=self._extract_prefix(component_value)
            )
            
            conditions.append(component_query)
        
        # Combine all component conditions with AND
        if len(conditions) == 1:
            return conditions[0]
        return {"$and": conditions}
    
    def _convert_composite_keys(self, field_info: Dict, 
                                 component_values: List[str]) -> Dict:
        """
        Convert composite using composite key pattern.
        Example: component_8480-6_value, component_8480-6_unit
        """
        components = field_info['components']
        
        # First component is typically the discriminator (e.g., code)
        code_value = component_values[0].split('|')[-1]  # Get code from system|code
        
        conditions = []
        
        for i, (component_def, component_value) in enumerate(
            zip(components, component_values)
        ):
            if i == 0:
                # Skip code component (used as key prefix)
                continue
            
            # Build field path with code as prefix
            key_pattern = component_def.get('keyPattern', '{field}')
            field_path = key_pattern.replace('{code}', code_value)
            
            # Get converter and convert value
            converter = self.component_converters[component_def['type']]
            prefix = self._extract_prefix(component_value)
            clean_value = component_value.lstrip('0123456789<>!=')
            
            # Build condition for this composite key
            condition = converter.convert_value(
                field_path=field_path,
                value=clean_value,
                prefix=prefix
            )
            
            conditions.append(condition)
        
        return {"$and": conditions} if len(conditions) > 1 else conditions[0]
    
    def _convert_precomputed(self, field_info: Dict, composite_value: str) -> Dict:
        """
        Convert using pre-computed composite string field.
        """
        composite_field = field_info.get('compositeField', '_search.composite')
        
        # Build regex or exact match
        return {
            composite_field: composite_value  # or {"$regex": pattern}
        }
    
    def _extract_prefix(self, value: str) -> Optional[str]:
        """Extract prefix from value (gt, lt, ge, le, etc.)."""
        for prefix in ['ge', 'le', 'gt', 'lt', 'ne', 'eq', 'sa', 'eb', 'ap']:
            if value.startswith(prefix):
                return prefix
        return None
```

---

### Key Principles for Composite Parameters

1. **Avoid `$elemMatch` whenever possible**
   - Denormalize components into flat `_search` fields
   - Use independent field searches with `$and`

2. **Choose denormalization strategy based on use case:**
   - **Independent components**: Most common, simplest (e.g., code + value)
   - **Composite keys**: When need same-element matching (e.g., component[].code + component[].value)
   - **Pre-computed strings**: For exact composite matches

3. **Configure in mapping files:**
   ```yaml
   parameters:
     composite-param:
       type: composite
       denormalizationStrategy: independent | compositeKeys | precomputed
       components: [...]
   ```

4. **Performance comparison:**
   - ❌ Canonical `$elemMatch`: 150-300ms
   - ✅ Denormalized `$elemMatch`: 50-100ms (3-6x faster)
   - ✅ Composite keys (no `$elemMatch`): 5-15ms (10-60x faster)

5. **Always index composite fields:**
   ```javascript
   // Independent components
   db.Observation.createIndex({"_search.codeCodes": 1, "_search.value": 1});
   
   // Composite keys
   db.Observation.createIndex({"_search.component_8480-6_value": 1});
   db.Observation.createIndex({"_search.component_8462-4_value": 1});
   ```

**Summary:** Composite parameters should use `_search` field denormalization, NOT `$elemMatch` on canonical structures. This maintains the 10-20x performance advantage we achieve with other parameter types.

---

### 8. Chaining

**FHIR Behavior:**
- Follow references across resources
- Syntax: `[param].[chained-param]=[value]`
- Example: `Observation?subject:Patient.name=Smith` (find observations for patients named Smith)

**Challenge:**
Chaining requires querying multiple resources, which MongoDB doesn't support natively in a single query.

**MQL Conversion Strategy with `_search` Fields:**

#### Recommended: Two-Stage Query (Best Performance)

Since we use `_search` fields with extracted IDs, chaining becomes much simpler and faster.

**FHIR:**
```
GET /Observation?subject:Patient.name=Smith
```

**MQL with `_search` (Two queries):**
```javascript
// Step 1: Find patients named Smith (using _search fields)
const patient_ids = db.Patient.distinct("id", {
  "$or": [
    {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
    {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}}
  ]
});
// Returns: ["pat-123", "pat-456", ...]

// Step 2: Find observations for those patients (using _search.patientId)
db.Observation.find({
  "_search.patientId": {"$in": patient_ids}
});
```

**Performance Benefits:**
- ✅ **10-20x faster** than using `subject.reference` string matching
- ✅ Uses indexed `_search.patientId` field (5ms lookup vs 100ms+ string regex)
- ✅ Simple, maintainable queries
- ✅ No complex aggregation pipeline needed

**Key Insight:** Because we extract reference IDs into `_search` fields (e.g., `_search.patientId`), the second query is extremely fast - just an indexed array membership check.

---

#### Alternative: Aggregation Pipeline with $lookup (When Needed)

Use this approach only when you need to return data from both resources or perform complex joins.

**FHIR:**
```
GET /Observation?subject:Patient.name=Smith
```

**MQL (Aggregation with `_search`):**
```javascript
db.Observation.aggregate([
  {
    // Use $lookup to join Patient collection
    "$lookup": {
      "from": "Patient",
      "localField": "_search.patientId",  // Using extracted ID
      "foreignField": "id",
      "as": "patient_data",
      "pipeline": [
        {
          "$match": {
            "$or": [
              {"_search.familyName": {"$regex": "^Smith", "$options": "i"}},
              {"_search.givenNames": {"$regex": "^Smith", "$options": "i"}}
            ]
          }
        }
      ]
    }
  },
  {
    // Keep only observations where patient matched
    "$match": {
      "patient_data": {"$ne": []}
    }
  },
  {
    // Remove joined patient data (we only needed it for filtering)
    "$project": {
      "patient_data": 0
    }
  }
]);
```

**Note:** Aggregation is slower (50-100ms+) but necessary if you need patient data in results or complex multi-hop chains.

---

#### Implementation

```python
# converters/chaining_handler.py

from typing import Dict, List, Tuple
from pymongo.database import Database

class ChainingHandler:
    """
    Handle chained search parameters using _search fields.
    """
    
    def __init__(self, db: Database, converter):
        self.db = db
        self.converter = converter
    
    def handle_chained_query(self, resource_type: str, 
                            chained_param: str, 
                            value: str) -> Dict:
        """
        Handle chained parameter using two-stage query.
        
        Args:
            resource_type: Base resource type (e.g., 'Observation')
            chained_param: Chained parameter (e.g., 'subject:Patient.name')
            value: Search value (e.g., 'Smith')
        
        Returns:
            MongoDB query for base resource
            
        Example:
            >>> handler.handle_chained_query(
            ...     'Observation', 
            ...     'subject:Patient.name', 
            ...     'Smith'
            ... )
            {"_search.patientId": {"$in": ["pat-123", "pat-456"]}}
        """
        # Parse chain: 'subject:Patient.name' -> ('subject', 'Patient', 'name')
        ref_param, target_type, target_param = self._parse_chain(chained_param)
        
        # Step 1: Query target resource using _search fields
        target_query = self.converter.convert(target_type, f"{target_param}={value}")
        
        # Get distinct IDs from target resource
        target_ids = list(
            self.db[target_type].distinct("id", target_query)
        )
        
        if not target_ids:
            # No matching target resources - return impossible query
            return {"_id": {"$in": []}}
        
        # Step 2: Map reference parameter to _search field
        # Examples:
        #   - subject -> _search.patientId (for Patient references)
        #   - performer -> _search.practitionerId (for Practitioner references)
        #   - location -> _search.locationId (for Location references)
        
        search_field = self._get_search_field_for_reference(
            resource_type, 
            ref_param, 
            target_type
        )
        
        # Return query for base resource using extracted ID field
        return {
            search_field: {"$in": target_ids}
        }
    
    def handle_chained_query_with_aggregation(
        self, 
        resource_type: str,
        chained_param: str,
        value: str
    ) -> List[Dict]:
        """
        Handle chained parameter using aggregation pipeline.
        
        Use when you need data from both resources or complex joins.
        
        Returns:
            MongoDB aggregation pipeline
        """
        ref_param, target_type, target_param = self._parse_chain(chained_param)
        
        # Build target resource query
        target_query = self.converter.convert(target_type, f"{target_param}={value}")
        
        # Get _search field for this reference
        search_field = self._get_search_field_for_reference(
            resource_type,
            ref_param,
            target_type
        )
        
        # Build aggregation pipeline
        pipeline = [
            {
                "$lookup": {
                    "from": target_type,
                    "localField": search_field,  # e.g., "_search.patientId"
                    "foreignField": "id",
                    "as": "joined_data",
                    "pipeline": [{"$match": target_query}]
                }
            },
            {
                "$match": {
                    "joined_data": {"$ne": []}
                }
            },
            {
                "$project": {
                    "joined_data": 0
                }
            }
        ]
        
        return pipeline
    
    def _parse_chain(self, chained_param: str) -> Tuple[str, str, str]:
        """
        Parse chained parameter into components.
        
        Args:
            chained_param: e.g., 'subject:Patient.name' or 'subject.name'
        
        Returns:
            (ref_param, target_type, target_param)
            e.g., ('subject', 'Patient', 'name')
        """
        parts = chained_param.split('.')
        ref_part = parts[0]
        
        if ':' in ref_part:
            # Explicit type: 'subject:Patient'
            ref_param, target_type = ref_part.split(':', 1)
        else:
            # Inferred type: 'subject' -> infer from context
            ref_param = ref_part
            target_type = self._infer_target_type(ref_param)
        
        target_param = '.'.join(parts[1:]) if len(parts) > 1 else None
        
        return ref_param, target_type, target_param
    
    def _get_search_field_for_reference(
        self, 
        resource_type: str,
        ref_param: str,
        target_type: str
    ) -> str:
        """
        Get the _search field name for a reference parameter.
        
        Maps reference parameters to their denormalized _search fields.
        
        Args:
            resource_type: Base resource (e.g., 'Observation')
            ref_param: Reference parameter (e.g., 'subject')
            target_type: Target resource type (e.g., 'Patient')
        
        Returns:
            _search field path (e.g., '_search.patientId')
            
        Examples:
            - Observation.subject -> Patient: '_search.patientId'
            - Observation.performer -> Practitioner: '_search.performerId'
            - Appointment.participant.actor -> Patient: '_search.patientId'
            - Appointment.participant.actor -> Practitioner: '_search.practitionerId'
        """
        # Common reference mappings
        mappings = {
            'Observation': {
                'subject': {
                    'Patient': '_search.patientId',
                    'Group': '_search.subjectId',
                    'Device': '_search.subjectId',
                    'Location': '_search.subjectId'
                },
                'performer': {
                    'Practitioner': '_search.performerId',
                    'Organization': '_search.performerId',
                    'Patient': '_search.performerId'
                }
            },
            'Appointment': {
                'patient': {
                    'Patient': '_search.patientId'
                },
                'practitioner': {
                    'Practitioner': '_search.practitionerId',
                    'PractitionerRole': '_search.practitionerId'
                },
                'location': {
                    'Location': '_search.locationId'
                },
                'actor': {
                    'Patient': '_search.patientId',
                    'Practitioner': '_search.practitionerId',
                    'Location': '_search.locationId',
                    # Fallback for generic actor
                    '_default': '_search.actor.ids'
                }
            },
            'DiagnosticReport': {
                'subject': {
                    'Patient': '_search.patientId'
                },
                'performer': {
                    'Practitioner': '_search.performerId',
                    'Organization': '_search.performerId'
                }
            },
            'MedicationRequest': {
                'subject': {
                    'Patient': '_search.patientId'
                },
                'requester': {
                    'Practitioner': '_search.requesterId',
                    'Organization': '_search.requesterId'
                }
            }
        }
        
        # Look up mapping
        if resource_type in mappings:
            if ref_param in mappings[resource_type]:
                if target_type in mappings[resource_type][ref_param]:
                    return mappings[resource_type][ref_param][target_type]
                elif '_default' in mappings[resource_type][ref_param]:
                    return mappings[resource_type][ref_param]['_default']
        
        # Fallback: construct from parameter name + type
        # subject + Patient -> _search.subjectId or _search.patientId
        if target_type == 'Patient':
            return '_search.patientId'
        elif target_type == 'Practitioner':
            return '_search.practitionerId'
        elif target_type == 'Location':
            return '_search.locationId'
        else:
            # Generic fallback
            return f"_search.{ref_param}Id"
    
    def _infer_target_type(self, ref_param: str) -> str:
        """
        Infer target resource type from reference parameter name.
        
        Common patterns:
        - patient -> Patient
        - subject -> Patient (most common)
        - practitioner -> Practitioner
        - location -> Location
        """
        inference_map = {
            'patient': 'Patient',
            'subject': 'Patient',  # Most common
            'performer': 'Practitioner',
            'practitioner': 'Practitioner',
            'location': 'Location',
            'organization': 'Organization',
            'encounter': 'Encounter'
        }
        
        return inference_map.get(ref_param.lower(), 'Patient')
```

---

### Chaining Performance Comparison

| Approach | Query Time (1M records) | Notes |
|----------|------------------------|-------|
| **Two-stage with `_search`** | **10-25ms** | ✅ RECOMMENDED - Fast, indexed, simple |
| Two-stage with canonical | 100-200ms | ❌ Slow - regex on reference strings |
| Aggregation with `_search` | 50-100ms | Use when need joined data |
| Aggregation with canonical | 200-500ms+ | ❌ Very slow - avoid |

**Key Takeaway:** Using `_search` fields with extracted IDs makes chaining 10-20x faster than canonical reference matching.

---

### Configuration for Chaining

Add reference field mappings to your resource configurations:

```yaml
# config/mappings/Observation.yaml
parameters:
  subject:
    type: reference
    description: "The subject of the observation"
    fields:
      - field: _search.patientId
        targetType: Patient
        primary: true
      - field: _search.subjectId
        targetType: [Group, Device, Location]
    referenceTypes: [Patient, Group, Device, Location]
    
    # Chaining configuration
    supportsChaining: true
    chainableParameters: [name, identifier, birthdate]  # Patient parameters that can be chained
```

---

### 9. Reverse Chaining (_has)

**FHIR Behavior:**
- Find resources that are referenced by other resources
- Syntax: `_has:[resource]:[reference-param]:[search-param]=[value]`
- Example: `Patient?_has:Observation:patient:code=8480-6` (find patients who have observations with code 8480-6)

**MQL Conversion with `_search` Fields:**

Since we use `_search` fields with extracted IDs, reverse chaining is also much simpler and faster.

**FHIR:**
```
GET /Patient?_has:Observation:patient:code=8480-6
```

This means: Find Patients who have Observations (via `patient` reference) where the observation code is 8480-6.

**MQL with `_search` (Two queries):**
```javascript
// Step 1: Find observations with code 8480-6 (using _search fields)
const patient_ids = db.Observation.distinct("_search.patientId", {
  "$or": [
    {"_search.codeCodes": "8480-6"},
    {"_search.codeSystemValues": "http://loinc.org|8480-6"}
  ]
});
// Returns: ["pat-123", "pat-456", ...]

// Step 2: Find those patients
db.Patient.find({
  "id": {"$in": patient_ids}
});
```

**Performance Benefits:**
- ✅ **20-30x faster** than canonical approach
- ✅ Uses indexed `_search.patientId` field for extraction
- ✅ Uses indexed `_search.codeCodes` for filtering
- ✅ Simple distinct() operation (5-10ms vs 200ms+ with canonical)

---

#### Complex Reverse Chaining Example

**FHIR:**
```
GET /Patient?_has:Observation:patient:code=8480-6&_has:Observation:patient:value-quantity=gt140
```

Find patients with observations where code=8480-6 AND value>140.

**MQL with `_search`:**
```javascript
// Step 1: Find observations matching both conditions
const patient_ids = db.Observation.distinct("_search.patientId", {
  "$and": [
    {
      "$or": [
        {"_search.codeCodes": "8480-6"},
        {"_search.codeSystemValues": "http://loinc.org|8480-6"}
      ]
    },
    {
      "_search.value": {"$gt": 140}
    }
  ]
});

// Step 2: Find those patients
db.Patient.find({
  "id": {"$in": patient_ids}
});
```

---

#### Implementation

```python
# converters/reverse_chaining_handler.py

from typing import Dict, List
from pymongo.database import Database

class ReverseChainingHandler:
    """
    Handle _has (reverse chaining) parameters using _search fields.
    """
    
    def __init__(self, db: Database, converter):
        self.db = db
        self.converter = converter
    
    def handle_reverse_chain(self, base_resource: str,
                            has_param: str) -> Dict:
        """
        Handle _has parameter.
        
        Args:
            base_resource: Resource type being searched (e.g., 'Patient')
            has_param: _has parameter value (e.g., 'Observation:patient:code=8480-6')
        
        Returns:
            MongoDB query for base resource
            
        Example:
            >>> handler.handle_reverse_chain(
            ...     'Patient',
            ...     'Observation:patient:code=8480-6'
            ... )
            {"id": {"$in": ["pat-123", "pat-456"]}}
        """
        # Parse: Observation:patient:code=8480-6
        parts = has_param.split(':', 2)
        if len(parts) < 3:
            raise ValueError(f"Invalid _has format: {has_param}")
        
        source_resource = parts[0]   # Observation
        reference_param = parts[1]   # patient
        search_spec = parts[2]       # code=8480-6
        
        # Parse search parameter and value
        if '=' not in search_spec:
            raise ValueError(f"No search value in _has: {has_param}")
        
        search_param, search_value = search_spec.split('=', 1)
        
        # Step 1: Build query for source resource (using _search fields)
        source_query = self.converter.convert(
            source_resource, 
            f"{search_param}={search_value}"
        )
        
        # Step 2: Get _search field that contains extracted IDs
        # For reference_param 'patient' in Observation -> use '_search.patientId'
        search_field = self._get_extracted_id_field(
            source_resource,
            reference_param,
            base_resource
        )
        
        # Step 3: Get distinct IDs from source resource
        referenced_ids = list(
            self.db[source_resource].distinct(search_field, source_query)
        )
        
        if not referenced_ids:
            # No matching source resources - return impossible query
            return {"id": {"$in": []}}
        
        # Step 4: Build query for base resource
        return {
            "id": {"$in": referenced_ids}
        }
    
    def handle_multiple_reverse_chains(
        self, 
        base_resource: str,
        has_params: List[str],
        operator: str = 'AND'
    ) -> Dict:
        """
        Handle multiple _has parameters.
        
        Args:
            base_resource: Resource type being searched
            has_params: List of _has parameter values
            operator: 'AND' or 'OR' to combine conditions
            
        Returns:
            Combined MongoDB query
            
        Example:
            Handle: _has:Observation:patient:code=8480-6&_has:Observation:patient:value-quantity=gt140
            
            This can be optimized if both _has reference the same source resource.
        """
        # Check if all _has parameters reference the same source resource
        source_resources = [p.split(':', 1)[0] for p in has_params]
        
        if len(set(source_resources)) == 1 and operator == 'AND':
            # Optimization: combine into single query
            return self._handle_combined_reverse_chains(base_resource, has_params)
        else:
            # Separate queries, then combine results
            conditions = [
                self.handle_reverse_chain(base_resource, has_param)
                for has_param in has_params
            ]
            
            if operator == 'AND':
                # Intersect ID sets
                id_sets = [
                    set(cond['id']['$in']) 
                    for cond in conditions
                ]
                combined_ids = list(set.intersection(*id_sets))
                return {"id": {"$in": combined_ids}}
            else:  # OR
                # Union ID sets
                all_ids = []
                for cond in conditions:
                    all_ids.extend(cond['id']['$in'])
                return {"id": {"$in": list(set(all_ids))}}
    
    def _handle_combined_reverse_chains(
        self, 
        base_resource: str,
        has_params: List[str]
    ) -> Dict:
        """
        Optimize multiple _has parameters on same source resource.
        
        Instead of multiple queries, combine conditions into one query on source.
        """
        # All params reference same source, parse them
        source_resource = has_params[0].split(':', 1)[0]
        reference_param = None
        search_conditions = []
        
        for has_param in has_params:
            parts = has_param.split(':', 2)
            ref_param = parts[1]
            search_spec = parts[2]
            
            if reference_param is None:
                reference_param = ref_param
            elif reference_param != ref_param:
                # Different reference params, can't combine
                raise ValueError("Cannot combine _has with different reference parameters")
            
            # Parse search
            search_param, search_value = search_spec.split('=', 1)
            
            # Build condition
            condition = self.converter.convert(
                source_resource,
                f"{search_param}={search_value}"
            )
            search_conditions.append(condition)
        
        # Combine all conditions with AND
        combined_query = {"$and": search_conditions} if len(search_conditions) > 1 else search_conditions[0]
        
        # Get extracted ID field
        search_field = self._get_extracted_id_field(
            source_resource,
            reference_param,
            base_resource
        )
        
        # Get distinct IDs
        referenced_ids = list(
            self.db[source_resource].distinct(search_field, combined_query)
        )
        
        return {"id": {"$in": referenced_ids}}
    
    def _get_extracted_id_field(
        self,
        source_resource: str,
        reference_param: str,
        target_resource: str
    ) -> str:
        """
        Get the _search field that contains extracted reference IDs.
        
        Args:
            source_resource: Source resource type (e.g., 'Observation')
            reference_param: Reference parameter name (e.g., 'patient', 'subject')
            target_resource: Target resource type (e.g., 'Patient')
        
        Returns:
            Field path in source resource (e.g., '_search.patientId')
            
        Examples:
            - Observation, patient, Patient -> '_search.patientId'
            - Observation, subject, Patient -> '_search.patientId'
            - DiagnosticReport, subject, Patient -> '_search.patientId'
            - MedicationRequest, subject, Patient -> '_search.patientId'
        """
        # Mapping of common reference parameters to _search fields
        mappings = {
            'Observation': {
                'patient': '_search.patientId',
                'subject': '_search.patientId',  # Most observations are for patients
                'performer': '_search.performerId'
            },
            'DiagnosticReport': {
                'patient': '_search.patientId',
                'subject': '_search.patientId',
                'performer': '_search.performerId'
            },
            'Condition': {
                'patient': '_search.patientId',
                'subject': '_search.patientId'
            },
            'Procedure': {
                'patient': '_search.patientId',
                'subject': '_search.patientId',
                'performer': '_search.performerId'
            },
            'MedicationRequest': {
                'patient': '_search.patientId',
                'subject': '_search.patientId',
                'requester': '_search.requesterId'
            },
            'Encounter': {
                'patient': '_search.patientId',
                'subject': '_search.patientId',
                'participant': '_search.participantId'
            }
        }
        
        # Look up mapping
        if source_resource in mappings:
            if reference_param in mappings[source_resource]:
                return mappings[source_resource][reference_param]
        
        # Fallback: construct from target resource type
        if target_resource == 'Patient':
            return '_search.patientId'
        elif target_resource == 'Practitioner':
            return '_search.practitionerId'
        elif target_resource == 'Organization':
            return '_search.organizationId'
        elif target_resource == 'Location':
            return '_search.locationId'
        else:
            # Generic fallback
            return f"_search.{reference_param}Id"
```

---

### Reverse Chaining Performance Comparison

| Approach | Query Time (1M records) | Notes |
|----------|------------------------|-------|
| **Two-stage with `_search`** | **10-20ms** | ✅ RECOMMENDED - Fast distinct() on indexed field |
| Two-stage with canonical | 200-400ms | ❌ Slow - regex on reference strings |
| Aggregation with `$lookup` | 300-600ms+ | ❌ Very slow - avoid for _has |

**Key Takeaway:** Using `_search` fields with extracted IDs makes reverse chaining 20-30x faster.

---

### Configuration for Reverse Chaining

Document which resources reference your base resource:

```yaml
# config/mappings/Patient.yaml
resource: Patient

# Document reverse references (resources that reference Patient)
reverseReferences:
  - sourceResource: Observation
    referenceParam: patient
    extractedIdField: _search.patientId
    searchableParameters: [code, date, category, status, value-quantity]
  
  - sourceResource: Observation
    referenceParam: subject
    extractedIdField: _search.patientId  # Same field (subject is usually Patient)
    searchableParameters: [code, date, category]
  
  - sourceResource: Condition
    referenceParam: patient
    extractedIdField: _search.patientId
    searchableParameters: [code, clinical-status, onset-date]
  
  - sourceResource: MedicationRequest
    referenceParam: patient
    extractedIdField: _search.patientId
    searchableParameters: [medication, status, intent]
```

This configuration helps validate _has queries and provides documentation.

---

### 10. Combining Parameters (AND/OR Logic)

**FHIR Behavior:**
- Multiple parameters = AND
- Comma-separated values = OR

**MQL Conversion:**

#### Multiple Parameters (AND)

**FHIR:**
```
GET /Patient?family=Smith&gender=male
```

**MQL:**
```javascript
{
  "$and": [
    { "name.family": { "$regex": "^Smith", "$options": "i" } },
    { "gender": "male" }
  ]
}
```

#### Comma-Separated Values (OR)

**FHIR:**
```
GET /Patient?family=Smith,Jones
```

**MQL:**
```javascript
{
  "$or": [
    { "name.family": { "$regex": "^Smith", "$options": "i" } },
    { "name.family": { "$regex": "^Jones", "$options": "i" } }
  ]
}
```

#### Complex AND/OR Logic

**FHIR:**
```
GET /Patient?family=Smith,Jones&gender=male,female
```

**MQL:**
```javascript
{
  "$and": [
    {
      "$or": [
        { "name.family": { "$regex": "^Smith", "$options": "i" } },
        { "name.family": { "$regex": "^Jones", "$options": "i" } }
      ]
    },
    {
      "$or": [
        { "gender": "male" },
        { "gender": "female" }
      ]
    }
  ]
}
```

**Implementation:**
```python
class QueryCombiner:
    def combine_parameters(self, param_queries: List[Dict]) -> Dict:
        """
        Combine multiple parameter queries with AND logic.
        """
        if not param_queries:
            return {}
        elif len(param_queries) == 1:
            return param_queries[0]
        else:
            return {"$and": param_queries}
    
    def handle_or_values(self, field_path: str, 
                        values: List[str],
                        converter: BaseConverter) -> Dict:
        """
        Handle comma-separated values (OR logic).
        """
        if len(values) == 1:
            return converter.convert(field_path, values[0])
        else:
            or_queries = [
                converter.convert(field_path, value)
                for value in values
            ]
            return {"$or": or_queries}
```

---

## Implementation Examples

### Example 1: Simple Patient Search (Using `_search`)

**FHIR Query:**
```
GET /Patient?name=John&birthdate=gt1980-01-01&gender=male
```

**Python Code:**
```python
from fhir_to_mql import FHIRToMQLConverter

converter = FHIRToMQLConverter()

mql_query = converter.convert(
    resource_type="Patient",
    search_url="name=John&birthdate=gt1980-01-01&gender=male"
)

print(json.dumps(mql_query, indent=2))
```

**Generated MQL (Simple, Fast):**
```json
{
  "$and": [
    {
      "$or": [
        {
          "_search.familyName": {
            "$regex": "^John",
            "$options": "i"
          }
        },
        {
          "_search.givenNames": {
            "$regex": "^John",
            "$options": "i"
          }
        }
      ]
    },
    {
      "birthDate": {
        "$gt": "1980-01-01"
      }
    },
    {
      "gender": "male"
    }
  ]
}
```

**Explanation:**
- Name search uses `_search.familyName` and `_search.givenNames` (extracted from name array)
- birthDate uses canonical field (simple root-level field)
- gender uses canonical field (simple token)

---

### Example 2: Observation Search with Token (Using `_search`)

**FHIR Query:**
```
GET /Observation?subject=Patient/pat-123&code=http://loinc.org|8480-6&date=ge2024-01-01
```

**Python Code:**
```python
converter = FHIRToMQLConverter()

mql_query = converter.convert(
    resource_type="Observation",
    search_url="subject=Patient/pat-123&code=http://loinc.org|8480-6&date=ge2024-01-01"
)

print(json.dumps(mql_query, indent=2))
```

**Generated MQL (Simple, Fast):**
```json
{
  "$and": [
    {
      "_search.patientId": "pat-123"
    },
    {
      "_search.codeSystemValues": "http://loinc.org|8480-6"
    },
    {
      "_search.start": {
        "$gte": "2024-01-01T00:00:00Z"
      }
    }
  ]
}
```

**Performance Comparison:**

❌ **Old Approach (Slow)**:
```json
{
  "$and": [
    {
      "subject.reference": {
        "$regex": "Patient/pat-123"
      }
    },
    {
      "code.coding": {
        "$elemMatch": {
          "system": "http://loinc.org",
          "code": "8480-6"
        }
      }
    },
    {
      "effectiveDateTime": {
        "$gte": "2024-01-01T00:00:00Z"
      }
    }
  ]
}
```
Query Time: ~180ms (1M records)

✅ **New Approach with `_search` (Fast)**:
```json
{
  "$and": [
    {"_search.patientId": "pat-123"},
    {"_search.codeSystemValues": "http://loinc.org|8480-6"},
    {"_search.start": {"$gte": "2024-01-01T00:00:00Z"}}
  ]
}
```
Query Time: ~8ms (1M records) - **23x faster!**

---

### Example 3: Appointment Search (Complex Multi-Participant)

**FHIR Query:**
```
GET /Appointment?patient=pat-123&practitioner=prac-456&date=2026-05-15&status=booked
```

**Python Code:**
```python
converter = FHIRToMQLConverter()

mql_query = converter.convert(
    resource_type="Appointment",
    search_url="patient=pat-123&practitioner=prac-456&date=2026-05-15&status=booked"
)
```

**Generated MQL:**
```json
{
  "$and": [
    {
      "_search.patientId": "pat-123"
    },
    {
      "_search.practitionerId": "prac-456"
    },
    {
      "$and": [
        {
          "_search.start": {
            "$gte": "2026-05-15T00:00:00Z"
          }
        },
        {
          "_search.start": {
            "$lt": "2026-05-16T00:00:00Z"
          }
        }
      ]
    },
    {
      "_search.status": "booked"
    }
  ]
}
```

**Why This is Fast:**
- Uses extracted IDs: `_search.patientId`, `_search.practitionerId`
- No array traversal through participants
- Compound index on `(_search.patientId, _search.start, _search.status)` makes this lightning fast
- Typical query time: **5-10ms** vs 100+ms with canonical participant array

---

### Example 4: Token Search with Multiple Values (OR)

**FHIR Query:**
```
GET /Observation?code=8480-6,8462-4,8310-5&status=final,amended
```

**Generated MQL:**
```json
{
  "$and": [
    {
      "_search.codeCodes": {
        "$in": ["8480-6", "8462-4", "8310-5"]
      }
    },
    {
      "status": {
        "$in": ["final", "amended"]
      }
    }
  ]
}
```

**Explanation:**
- Comma-separated values become `$in` arrays
- `_search.codeCodes` is a flat array - no need for `$elemMatch`
- MongoDB can use index efficiently with `$in`

---

### Example 5: Identifier Search (Multiple Formats)

**FHIR Query:**
```
GET /Patient?identifier=http://hospital.org/mrn|MRN-12345
```

**Generated MQL:**
```json
{
  "_search.identifier.systemValues": "http://hospital.org/mrn|MRN-12345"
}
```

**Alternative: Value-Only Search**

**FHIR Query:**
```
GET /Patient?identifier=MRN-12345
```

**Generated MQL:**
```json
{
  "_search.identifier.values": "MRN-12345"
}
```

**Data Structure:**
```json
{
  "_search": {
    "identifier": {
      "values": ["MRN-12345", "SSN-123-45-6789"],
      "systems": ["http://hospital.org/mrn", "http://hl7.org/fhir/sid/us-ssn"],
      "systemValues": [
        "http://hospital.org/mrn|MRN-12345",
        "http://hl7.org/fhir/sid/us-ssn|SSN-123-45-6789"
      ]
    }
  }
}
```

---

### Example 6: Complex Search with Modifiers

**FHIR Query:**
```
GET /Patient?family:exact=Smith&active:not=false&birthdate=gt1990-01-01
```

**Generated MQL:**
```json
{
  "$and": [
    {
      "_search.familyName": "Smith"
    },
    {
      "$nor": [
        {"active": false}
      ]
    },
    {
      "birthDate": {
        "$gt": "1990-01-01"
      }
    }
  ]
}
```

---

### Example 7: Slot Availability Search (Real-World Use Case)

**FHIR Query:**
```
GET /Slot?schedule=sched-123&service-type=221&status=free&start=ge2026-05-15T08:00:00Z&start=lt2026-05-15T17:00:00Z
```

**Generated MQL:**
```json
{
  "$and": [
    {
      "_search.scheduleId": "sched-123"
    },
    {
      "_search.serviceTypeCodes": "221"
    },
    {
      "_search.status": "free"
    },
    {
      "_search.start": {
        "$gte": "2026-05-15T08:00:00Z"
      }
    },
    {
      "_search.start": {
        "$lt": "2026-05-15T17:00:00Z"
      }
    }
  ]
}
```

**Compound Index:**
```javascript
db.Slot.createIndex({
  "_search.scheduleId": 1,
  "_search.serviceTypeCodes": 1,
  "_search.status": 1,
  "_search.start": 1
})
```

**Query Performance:** <5ms for 100K+ slots

---

### Example 8: Appointment History with Pagination

**FHIR Query:**
```
GET /Appointment?patient=pat-123&_sort=-date&_count=20
```

**Generated MQL Query:**
```json
{
  "_search.patientId": "pat-123"
}
```

**With Sort and Limit:**
```python
cursor = db.Appointment.find(
    {"_search.patientId": "pat-123"}
).sort("_search.start", -1).limit(20)
```

**Index for This Query:**
```javascript
db.Appointment.createIndex({
  "_search.patientId": 1,
  "_search.start": -1
})
```

This index supports:
- Fast patient lookup
- Descending date sort without in-memory sort
- **Critical for patient portal performance**

---

## Testing Strategy

### Test Categories

#### 1. Unit Tests (Per Converter)

Test each converter in isolation:

```python
class TestStringConverter(unittest.TestCase):
    def setUp(self):
        self.converter = StringConverter()
    
    def test_starts_with_default(self):
        result = self.converter.convert("name.family", "Smith")
        expected = {
            "name.family": {
                "$regex": "^Smith",
                "$options": "i"
            }
        }
        self.assertEqual(result, expected)
    
    def test_exact_modifier(self):
        result = self.converter.convert(
            "name.family", "Smith", modifier="exact"
        )
        expected = {"name.family": "Smith"}
        self.assertEqual(result, expected)
    
    def test_contains_modifier(self):
        result = self.converter.convert(
            "name.family", "mit", modifier="contains"
        )
        expected = {
            "name.family": {
                "$regex": "mit",
                "$options": "i"
            }
        }
        self.assertEqual(result, expected)
```

#### 2. Integration Tests

Test full conversion pipeline:

```python
class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.converter = FHIRToMQLConverter()
    
    def test_patient_search(self):
        mql = self.converter.convert(
            "Patient",
            "name=Smith&birthdate=gt1980-01-01"
        )
        
        # Verify query structure
        self.assertIn("$and", mql)
        self.assertEqual(len(mql["$and"]), 2)
        
        # Verify can execute against MongoDB
        # (requires test database)
```

#### 3. FHIR Conformance Tests

Test against official FHIR test cases:

```python
class TestFHIRConformance(unittest.TestCase):
    """Test against FHIR specification examples."""
    
    def load_test_cases(self):
        # Load from tests/fixtures/fhir_search_tests.json
        pass
    
    def test_all_fhir_examples(self):
        test_cases = self.load_test_cases()
        
        for test_case in test_cases:
            with self.subTest(test=test_case['name']):
                mql = self.converter.convert(
                    test_case['resource_type'],
                    test_case['search_url']
                )
                # Validate query
                self.validate_mql(mql)
```

#### 4. Performance Tests

Test query performance:

```python
class TestPerformance(unittest.TestCase):
    def setUp(self):
        # Setup MongoDB with test data
        self.setup_test_data()
    
    def test_query_performance(self):
        """Test that generated queries execute efficiently."""
        mql = self.converter.convert("Patient", "name=Smith")
        
        start_time = time.time()
        results = self.db.Patient.find(mql).limit(100)
        list(results)  # Force execution
        duration = time.time() - start_time
        
        # Should complete in under 100ms
        self.assertLess(duration, 0.1)
```

---

## Performance Optimization

### 1. Index Strategy

**Required Indexes:**

```javascript
// Patient indexes
db.Patient.createIndex({ "name.family": 1 })
db.Patient.createIndex({ "name.given": 1 })
db.Patient.createIndex({ "birthDate": 1 })
db.Patient.createIndex({ "gender": 1 })
db.Patient.createIndex({ "identifier.system": 1, "identifier.value": 1 })

// Observation indexes
db.Observation.createIndex({ "subject.reference": 1 })
db.Observation.createIndex({ "code.coding.system": 1, "code.coding.code": 1 })
db.Observation.createIndex({ "effectiveDateTime": 1 })
db.Observation.createIndex({ "status": 1 })

// Compound indexes for common searches
db.Observation.createIndex({
  "subject.reference": 1,
  "code.coding.code": 1,
  "effectiveDateTime": -1
})
```

**Index Optimizer:**

```python
class IndexOptimizer:
    def suggest_indexes(self, mql_query: Dict) -> List[str]:
        """
        Analyze query and suggest optimal indexes.
        """
        indexes = []
        
        # Extract fields used in query
        fields = self._extract_query_fields(mql_query)
        
        for field in fields:
            indexes.append(f"CREATE INDEX ON {field}")
        
        return indexes
    
    def add_index_hint(self, mql_query: Dict, 
                      index_name: str) -> Dict:
        """
        Add index hint to query.
        """
        return {
            **mql_query,
            "hint": index_name
        }
```

### 2. Query Optimization

**Optimization Rules:**

1. **Put most selective filters first**
2. **Use covered queries when possible**
3. **Avoid $regex on large collections**
4. **Use $in with caution (limit to 100 values)**
5. **Prefer exact matches over ranges**

**Query Rewriter:**

```python
class QueryOptimizer:
    def optimize(self, mql_query: Dict) -> Dict:
        """
        Optimize MongoDB query for performance.
        """
        # Flatten unnecessary $and with single element
        mql_query = self._flatten_and(mql_query)
        
        # Reorder conditions (most selective first)
        mql_query = self._reorder_conditions(mql_query)
        
        # Convert regex to exact match where possible
        mql_query = self._optimize_regex(mql_query)
        
        return mql_query
    
    def _flatten_and(self, query: Dict) -> Dict:
        """Remove unnecessary $and operators."""
        if "$and" in query and len(query["$and"]) == 1:
            return query["$and"][0]
        return query
    
    def _reorder_conditions(self, query: Dict) -> Dict:
        """Reorder conditions by selectivity."""
        if "$and" not in query:
            return query
        
        conditions = query["$and"]
        
        # Estimate selectivity (simple heuristic)
        def selectivity_score(condition):
            if any(k in condition for k in ["_id", "identifier"]):
                return 1  # Most selective
            elif any(k.startswith("$") for k in condition.keys()):
                return 5
            else:
                return 10
        
        sorted_conditions = sorted(conditions, key=selectivity_score)
        
        return {"$and": sorted_conditions}
```

### 3. Result Caching

**Cache Strategy:**

```python
from functools import lru_cache
import hashlib

class CachedConverter:
    def __init__(self):
        self.converter = FHIRToMQLConverter()
        self.cache = {}
    
    def convert_with_cache(self, resource_type: str, 
                          search_url: str) -> Dict:
        """
        Convert with caching for identical queries.
        """
        # Create cache key
        cache_key = hashlib.md5(
            f"{resource_type}:{search_url}".encode()
        ).hexdigest()
        
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # Convert and cache
        mql = self.converter.convert(resource_type, search_url)
        self.cache[cache_key] = mql
        
        return mql
```

---

## Advanced Features

### 1. Aggregation Pipeline Generation

For complex queries with _include, _revinclude, or sorting:

```python
class AggregationPipelineBuilder:
    def build_pipeline(self, resource_type: str,
                      mql_query: Dict,
                      result_params: Dict) -> List[Dict]:
        """
        Build MongoDB aggregation pipeline.
        """
        pipeline = []
        
        # Stage 1: Match
        pipeline.append({"$match": mql_query})
        
        # Stage 2: Lookup (for _include)
        if "_include" in result_params:
            for include_spec in result_params["_include"]:
                pipeline.extend(
                    self._build_lookup_stage(include_spec)
                )
        
        # Stage 3: Sort
        if "_sort" in result_params:
            sort_spec = self._parse_sort(result_params["_sort"])
            pipeline.append({"$sort": sort_spec})
        
        # Stage 4: Limit/Skip
        if "_count" in result_params:
            count = int(result_params["_count"])
            pipeline.append({"$limit": count})
        
        return pipeline
    
    def _build_lookup_stage(self, include_spec: str) -> List[Dict]:
        """
        Build $lookup stage for _include.
        
        Example: 'Observation:patient' -> lookup Patient
        """
        source_resource, ref_param = include_spec.split(":")
        
        # Parse reference to determine target
        # This is simplified - real implementation more complex
        
        return [
            {
                "$lookup": {
                    "from": "Patient",
                    "localField": "subject.reference",
                    "foreignField": "_id",
                    "as": "included_patient"
                }
            }
        ]
```

### 2. _filter Parameter Support

Advanced filtering with custom grammar:

```python
class FilterParser:
    """
    Parse FHIR _filter parameter.
    
    Grammar:
    filter := expression
    expression := term (("and" | "or") term)*
    term := parameter operator value | "(" expression ")"
    operator := "eq" | "ne" | "gt" | "lt" | "ge" | "le" | "co" | "in"
    """
    
    def parse(self, filter_string: str) -> Dict:
        """Parse _filter parameter to MQL."""
        # Tokenize
        tokens = self._tokenize(filter_string)
        
        # Parse to AST
        ast = self._parse_expression(tokens)
        
        # Convert AST to MQL
        mql = self._ast_to_mql(ast)
        
        return mql
    
    def _tokenize(self, filter_string: str) -> List[str]:
        """Tokenize filter string."""
        # Implementation here
        pass
    
    def _parse_expression(self, tokens: List[str]):
        """Parse tokens to AST."""
        # Implementation here
        pass
    
    def _ast_to_mql(self, ast) -> Dict:
        """Convert AST to MongoDB query."""
        # Implementation here
        pass
```

### 3. Resource-Specific Optimizations

Handle denormalized fields for better performance:

```python
class ResourceOptimizer:
    """
    Apply resource-specific optimizations.
    """
    
    def optimize_for_resource(self, resource_type: str,
                             mql_query: Dict) -> Dict:
        """
        Apply resource-specific optimizations.
        """
        if resource_type == "Observation":
            return self._optimize_observation(mql_query)
        elif resource_type == "Patient":
            return self._optimize_patient(mql_query)
        else:
            return mql_query
    
    def _optimize_observation(self, query: Dict) -> Dict:
        """
        Optimize Observation queries.
        
        Use denormalized fields if available:
        - _subject_id (instead of subject.reference)
        - _code (flattened codes)
        - _date (normalized date)
        """
        optimized = query.copy()
        
        # Replace subject.reference with _subject_id
        if "subject.reference" in optimized:
            ref_value = optimized["subject.reference"]
            if isinstance(ref_value, str) and "/" in ref_value:
                _, subject_id = ref_value.split("/")
                optimized["_subject_id"] = subject_id
                del optimized["subject.reference"]
        
        return optimized
```

---

## Complete Usage Example

### Full Integration Example

```python
#!/usr/bin/env python3
"""
Complete example of FHIR to MQL conversion and query execution.
"""

from fhir_to_mql import FHIRToMQLConverter
from pymongo import MongoClient
import json


def main():
    # Initialize converter
    converter = FHIRToMQLConverter(
        search_parameters_path="config/search_parameters.json"
    )
    
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["fhir_db"]
    
    # Example 1: Simple search
    print("Example 1: Simple Patient Search")
    print("-" * 50)
    
    fhir_query = "name=Smith&birthdate=gt1980-01-01&gender=male"
    mql_query = converter.convert("Patient", fhir_query)
    
    print(f"FHIR Query: /Patient?{fhir_query}")
    print(f"MQL Query: {json.dumps(mql_query, indent=2)}")
    
    # Execute query
    results = list(db.Patient.find(mql_query).limit(10))
    print(f"Results: {len(results)} patients found\n")
    
    # Example 2: Complex search with chaining
    print("Example 2: Observation with Patient Chaining")
    print("-" * 50)
    
    fhir_query = "subject:Patient.name=Smith&code=8480-6&date=ge2024-01-01"
    
    # Convert with chaining support
    queries = converter.convert_with_chaining("Observation", fhir_query)
    
    print(f"FHIR Query: /Observation?{fhir_query}")
    print(f"Stage 1 (Patient): {json.dumps(queries['stage1'], indent=2)}")
    print(f"Stage 2 (Observation): {json.dumps(queries['stage2'], indent=2)}")
    
    # Execute chained query
    # Step 1: Find matching patients
    patient_ids = [
        doc['_id'] 
        for doc in db.Patient.find(queries['stage1'], {'_id': 1})
    ]
    
    # Step 2: Find observations
    results = list(db.Observation.find(queries['stage2']).limit(10))
    print(f"Results: {len(results)} observations found\n")
    
    # Example 3: Aggregation pipeline
    print("Example 3: Aggregation with _include")
    print("-" * 50)
    
    fhir_query = "code=8480-6&_include=Observation:patient&_sort=-date"
    
    pipeline = converter.convert_to_aggregation("Observation", fhir_query)
    
    print(f"FHIR Query: /Observation?{fhir_query}")
    print(f"Aggregation Pipeline: {json.dumps(pipeline, indent=2)}")
    
    # Execute aggregation
    results = list(db.Observation.aggregate(pipeline))
    print(f"Results: {len(results)} observations with patients\n")
    
    # Cleanup
    client.close()


if __name__ == "__main__":
    main()
```

---

## Next Steps

### Immediate Actions

1. **Set up development environment**
   - Python 3.8+
   - MongoDB 4.4+
   - PyMongo, pytest, black, mypy

2. **Create project structure**
   - Follow directory layout above
   - Initialize git repository
   - Set up virtual environment

3. **Implement Phase 1 (Core Infrastructure)**
   - URL parser
   - Parameter extractor
   - SearchParameter loader
   - Basic tests

4. **Start with simple converters**
   - String converter
   - Token converter
   - Basic integration tests

### Long-Term Roadmap

**Month 1-2: Core Implementation**
- All basic parameter types
- Common modifiers
- Integration with MongoDB

**Month 3-4: Advanced Features**
- Chaining support
- Aggregation pipelines
- Performance optimization

**Month 5-6: Production Readiness**
- Comprehensive testing
- Documentation
- Performance tuning
- Security review

---

## References

1. **FHIR Search Specification**: https://www.hl7.org/fhir/search.html
2. **MongoDB Query Operators**: https://docs.mongodb.com/manual/reference/operator/query/
3. **PyMongo Documentation**: https://pymongo.readthedocs.io/
4. **FHIR SearchParameter Registry**: https://www.hl7.org/fhir/searchparameter-registry.html
5. **SEARCH_HYBRID_APPROACH.md**: See `schedule_appointment_generator/SEARCH_HYBRID_APPROACH.md` for detailed examples of the `_search` field pattern in production use

---

## Summary: Key Principles for FHIR to MQL Conversion

### 1. The `_search` Field is Foundational

**THIS IS NOT OPTIONAL.** The `_search` field pattern is the core architectural decision that enables:
- Simple MQL queries (5-20x faster)
- Effective index utilization
- Maintainable code
- Scalable performance

### 2. Denormalization Rules

**ALWAYS Denormalize:**
- CodeableConcept → `_search.{param}Codes: ["code1", "code2"]`
- References → `_search.patientId: "pat-123"`
- Identifiers → `_search.identifier.{values, systems, systemValues}`
- Complex nested arrays → Flat arrays in `_search`
- **Composite parameters** → Denormalized component fields or composite keys in `_search`

**Use Canonical Fields:**
- Simple booleans: `active: true`
- Simple strings (optional): `name: "Dr. Smith"`
- Root-level dates (optional): `birthDate: "1985-03-15"`

### 3. Query Patterns to AVOID

❌ **Never use `$elemMatch` when `_search` can be used:**
```javascript
// ❌ WRONG - Slow
{"code.coding": {"$elemMatch": {"system": "...", "code": "..."}}}

// ✅ RIGHT - Fast
{"_search.codeSystemValues": "system|code"}
```

❌ **Never traverse arrays for references:**
```javascript
// ❌ WRONG - Slow, regex
{"subject.reference": {"$regex": "Patient/pat-123"}}

// ✅ RIGHT - Fast, direct
{"_search.patientId": "pat-123"}
```

❌ **Never use deep nested paths:**
```javascript
// ❌ WRONG - Complex, slow
{"participant.actor.reference": "Patient/pat-123"}

// ✅ RIGHT - Simple, fast
{"_search.patientId": "pat-123"}
```

❌ **Never use `$elemMatch` for composite parameters:**
```javascript
// ❌ WRONG - Slow composite search
{
  "component": {
    "$elemMatch": {
      "code.coding.code": "8480-6",
      "valueQuantity.value": {"$gt": 140}
    }
  }
}

// ✅ RIGHT - Use composite keys in _search
{
  "$and": [
    {"_search.component_8480-6_value": {"$gt": 140}},
    {"_search.component_8480-6_unit": "mm[Hg]"}
  ]
}
```

### 4. Index Strategy

**Index ALL `_search` fields that will be queried:**

```javascript
// Primary participant lookups
db.Appointment.createIndex({"_search.patientId": 1, "_search.start": -1, "_search.status": 1})
db.Appointment.createIndex({"_search.practitionerId": 1, "_search.start": -1})

// Code lookups
db.Observation.createIndex({"_search.codeCodes": 1, "_search.start": -1})
db.Observation.createIndex({"_search.codeSystemValues": 1})

// Identifier lookups
db.Patient.createIndex({"_search.identifier.systemValues": 1})
db.Patient.createIndex({"_search.identifier.values": 1})
```

**Compound indexes should match common query patterns** (see SEARCH_HYBRID_APPROACH.md for comprehensive examples).

### 5. Performance Targets

With proper `_search` denormalization and indexing:

| Query Type | Target Performance (1M records) |
|------------|-------------------------------|
| Patient ID lookup | <5ms |
| Code search | <10ms |
| Date range | <15ms |
| Multi-parameter (2-3 fields) | <20ms |
| Complex (4+ fields) | <50ms |
| With sorting | <30ms |

### 6. Implementation Checklist

When implementing FHIR to MQL conversion:

- [ ] **Understand the `_search` pattern** - Read "The `_search` Field Pattern" section
- [ ] **Design `_search` structure** for each resource type
- [ ] **Implement denormalization** logic (populate `_search` on insert/update)
- [ ] **Generate simple MQL** using `_search` fields (avoid `$elemMatch`)
- [ ] **Create indexes** on all `_search` fields
- [ ] **Test performance** with realistic data volumes
- [ ] **Validate correctness** against FHIR test cases
- [ ] **Monitor query plans** to ensure index usage

### 7. Additional Resources

For detailed, production-ready examples of the `_search` pattern:

**See `schedule_appointment_generator/SEARCH_HYBRID_APPROACH.md`** which contains:
- Complete `_search` structure for Schedule, Slot, Appointment resources
- 39 documented search parameters with MQL examples
- Index recommendations
- Performance benchmarks
- Real-world query patterns
- Multi-parameter search examples

### 8. Quick Migration Guide

If you have an existing FHIR MongoDB implementation without `_search`:

**Step 1: Add `_search` to New Documents**
- Update insert/update logic to populate `_search` alongside canonical fields
- Start with high-priority search parameters (patient, date, status, code)

**Step 2: Backfill Existing Documents**
- Write migration script to add `_search` to existing documents
- Can be done incrementally (by resource type or date range)

**Step 3: Update Query Generation**
- Modify converter to target `_search` fields
- Keep old query logic temporarily for fallback

**Step 4: Create Indexes**
- Add indexes on `_search` fields
- Monitor query performance improvement

**Step 5: Remove Old Query Logic**
- Once validated, remove old complex queries
- Document the `_search` pattern for team

**Estimated Migration Time:** 2-4 weeks depending on data volume and resource types.

---

**Document Version:** 2.0 (Updated with `_search` Field Pattern)  
**Last Updated:** May 13, 2026  
**Author:** FHIR-MongoDB Integration Team  
**Key Change:** Incorporated hybrid `_search` denormalization approach for performance-optimized queries
