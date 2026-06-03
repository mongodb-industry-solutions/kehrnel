# FHIR Compartment-Based Queries for MongoDB

## Table of Contents

1. [Introduction to FHIR Compartments](#introduction-to-fhir-compartments)
2. [Understanding Compartments](#understanding-compartments)
3. [Compartment Types](#compartment-types)
4. [How Compartments Work](#how-compartments-work)
5. [Compartments vs Regular Search](#compartments-vs-regular-search)
6. [MongoDB Implementation Strategy](#mongodb-implementation-strategy)
7. [Integration with FHIR_TO_MQL](#integration-with-fhir_to_mql)
8. [Implementation Examples](#implementation-examples)
9. [Performance Optimization](#performance-optimization)
10. [Configuration Approach](#configuration-approach)

---

## Introduction to FHIR Compartments

### What Are FHIR Compartments?

**FHIR Compartments** are a logical grouping mechanism that defines a set of resources that are "related to" or "about" a specific entity (like a Patient, Encounter, Practitioner, etc.). They provide a way to:

1. **Scope searches** to resources relevant to a specific context
2. **Implement security** by restricting access to related resources
3. **Optimize queries** by pre-defining common relationships
4. **Simplify client logic** with implicit filtering

### Official FHIR Specification

- **FHIR R5 CompartmentDefinition**: https://www.hl7.org/fhir/compartmentdefinition.html
- **Compartments Overview**: https://www.hl7.org/fhir/compartments.html

### Key Concept

> A compartment is a way to say: "Give me all [ResourceType] resources that are related to this [CompartmentType]/[id]"

**Example:**
```
GET /Patient/123/Observation
```
Means: "Give me all Observations that are related to Patient/123"

This is functionally equivalent to:
```
GET /Observation?subject=Patient/123
```
But compartments can define MORE complex relationships beyond a single parameter.

---

## Understanding Compartments

### The Problem Compartments Solve

**Scenario:** A doctor wants to see "everything about Patient/123"

**Without Compartments:**
```
GET /Observation?subject=Patient/123
GET /Condition?subject=Patient/123
GET /MedicationRequest?subject=Patient/123
GET /Procedure?subject=Patient/123
GET /Encounter?subject=Patient/123
GET /DiagnosticReport?subject=Patient/123
GET /AllergyIntolerance?patient=Patient/123
GET /CarePlan?subject=Patient/123
... (many more queries)
```

**With Compartments:**
```
GET /Patient/123/*
```
Or individual resource types:
```
GET /Patient/123/Observation
GET /Patient/123/Condition
GET /Patient/123/MedicationRequest
```

### Core Components

1. **Compartment Type**: The type of entity that defines the compartment (Patient, Encounter, Practitioner, etc.)
2. **Compartment ID**: The specific instance of that entity (e.g., Patient/123)
3. **Resource Inclusion Rules**: Which search parameters link a resource to the compartment
4. **Resource Type Scope**: Which resource types can be in each compartment

---

## Compartment Types

FHIR R5 defines **5 standard compartment types**:

### 1. Patient Compartment

**Purpose:** All resources about/for a specific patient

**URL:** `[base]/Patient/[id]/[ResourceType]`

**Common Resource Types in Patient Compartment:**
- Observation (subject=Patient/[id])
- Condition (subject=Patient/[id])
- MedicationRequest (subject=Patient/[id])
- Procedure (subject=Patient/[id])
- Encounter (subject=Patient/[id])
- AllergyIntolerance (patient=Patient/[id])
- DiagnosticReport (subject=Patient/[id])
- CarePlan (subject=Patient/[id])
- Appointment (actor=Patient/[id])
- DocumentReference (subject=Patient/[id])

**Example:**
```
GET /Patient/pat-123/Observation
→ Returns all Observations where subject=Patient/pat-123
```

### 2. Encounter Compartment

**Purpose:** All resources related to a specific encounter/visit

**URL:** `[base]/Encounter/[id]/[ResourceType]`

**Common Resource Types:**
- Observation (encounter=Encounter/[id])
- Condition (encounter=Encounter/[id])
- Procedure (encounter=Encounter/[id])
- MedicationRequest (encounter=Encounter/[id])
- DiagnosticReport (encounter=Encounter/[id])
- ServiceRequest (encounter=Encounter/[id])

**Example:**
```
GET /Encounter/enc-456/Observation
→ Returns all Observations during encounter enc-456
```

### 3. Practitioner Compartment

**Purpose:** All resources related to a specific practitioner

**URL:** `[base]/Practitioner/[id]/[ResourceType]`

**Common Resource Types:**
- Observation (performer=Practitioner/[id])
- Procedure (performer=Practitioner/[id])
- Encounter (practitioner=Practitioner/[id])
- Appointment (actor=Practitioner/[id])
- Schedule (actor=Practitioner/[id])
- DiagnosticReport (performer=Practitioner/[id])

**Example:**
```
GET /Practitioner/prac-789/Appointment
→ Returns appointments for practitioner prac-789
```

### 4. Device Compartment

**Purpose:** All resources related to a specific device

**URL:** `[base]/Device/[id]/[ResourceType]`

**Common Resource Types:**
- Observation (device=Device/[id])
- DiagnosticReport (subject=Device/[id])
- Procedure (used-reference=Device/[id])

**Example:**
```
GET /Device/device-001/Observation
→ Returns observations from device-001
```

### 5. RelatedPerson Compartment

**Purpose:** All resources related to a related person (family member, guardian)

**URL:** `[base]/RelatedPerson/[id]/[ResourceType]`

**Common Resource Types:**
- Patient (link=RelatedPerson/[id])
- Appointment (actor=RelatedPerson/[id])
- DocumentReference (author=RelatedPerson/[id])

---

## How Compartments Work

### CompartmentDefinition Structure

Each compartment type is defined by a **CompartmentDefinition** resource that specifies:

```json
{
  "resourceType": "CompartmentDefinition",
  "id": "patient",
  "url": "http://hl7.org/fhir/CompartmentDefinition/patient",
  "name": "Patient",
  "status": "active",
  "code": "Patient",
  "search": true,
  "resource": [
    {
      "code": "Observation",
      "param": ["subject", "performer"]
    },
    {
      "code": "Condition",
      "param": ["subject"]
    },
    {
      "code": "MedicationRequest",
      "param": ["subject"]
    },
    {
      "code": "Encounter",
      "param": ["subject"]
    },
    {
      "code": "Appointment",
      "param": ["actor"]
    }
  ]
}
```

### Inclusion Rules

For each resource type, the CompartmentDefinition specifies which search parameters create the relationship:

**Example for Observation in Patient Compartment:**
- `subject` parameter: `Observation?subject=Patient/123`
- `performer` parameter: `Observation?performer=Patient/123`

**Meaning:** An Observation is in Patient/123's compartment if:
- The patient is the subject of the observation, OR
- The patient is the performer of the observation

### OR Logic Between Parameters

**IMPORTANT:** Multiple parameters for the same resource type use **OR logic**:

```
GET /Patient/123/Observation
```

Translates to:
```
GET /Observation?subject=Patient/123
  OR
GET /Observation?performer=Patient/123
```

MongoDB equivalent:
```javascript
{
  "$or": [
    {"_search.patientId": "123"},
    {"_search.performerId": "123"}
  ]
}
```

---

## Compartments vs Regular Search

### Key Differences

| Aspect | Regular Search | Compartment Search |
|--------|---------------|-------------------|
| **URL Pattern** | `/Observation?subject=Patient/123` | `/Patient/123/Observation` |
| **Parameter Count** | Single parameter | Multiple parameters (OR logic) |
| **Definition** | Ad-hoc by client | Pre-defined by server |
| **Security** | Explicit filtering | Implicit scope |
| **Use Case** | Flexible querying | Context-based access |

### When to Use Compartments

**Use Compartments When:**
1. Implementing patient portals (patient-scoped access)
2. Enforcing security boundaries (user can only see their compartment)
3. Simplifying client applications (don't need to know all relationships)
4. Performance optimization (pre-indexed compartment memberships)

**Use Regular Search When:**
1. Need complex multi-parameter queries
2. Cross-compartment searches
3. Advanced filtering beyond compartment scope
4. Administrative/system-level queries

### Combining Compartments with Search Parameters

You CAN combine compartment URLs with search parameters:

```
GET /Patient/123/Observation?code=8480-6&date=ge2024-01-01
```

This means:
- Scope: Observations in Patient/123's compartment
- Filter: code=8480-6 AND date>=2024-01-01

---

## MongoDB Implementation Strategy

### Approach 1: Dynamic Query Translation (Recommended)

**Concept:** Translate compartment URL to regular search query with OR conditions

**Compartment Request:**
```
GET /Patient/pat-123/Observation
```

**Translation Process:**
1. Load CompartmentDefinition for "Patient"
2. Find "Observation" resource entry
3. Get parameters: ["subject", "performer"]
4. Generate MQL query with OR logic

**Resulting MQL:**
```javascript
{
  "$or": [
    {"_search.patientId": "pat-123"},      // subject parameter
    {"_search.performerId": "pat-123"}     // performer parameter
  ]
}
```

**Advantages:**
- No data duplication
- Uses existing `_search` indexes
- Flexible and maintainable
- Works with existing FHIR_TO_MQL infrastructure

### Approach 2: Pre-Computed Compartment Membership (High Performance)

**Concept:** Store compartment membership directly in each resource

**Data Structure:**
```javascript
{
  "resourceType": "Observation",
  "id": "obs-001",
  "subject": {"reference": "Patient/pat-123"},
  "performer": [
    {"reference": "Practitioner/prac-456"}
  ],
  
  // Standard _search fields
  "_search": {
    "patientId": "pat-123",
    "performerId": "prac-456",
    // ... other fields
  },
  
  // Pre-computed compartment memberships
  "_compartments": {
    "Patient": ["pat-123"],           // This obs is in Patient/pat-123 compartment
    "Practitioner": ["prac-456"],     // And in Practitioner/prac-456 compartment
    "Encounter": ["enc-789"]          // And in Encounter/enc-789 compartment
  }
}
```

**MQL Query:**
```javascript
// Ultra-fast compartment query
{
  "_compartments.Patient": "pat-123"
}
```

**Advantages:**
- **Extremely fast** - single field lookup
- Simple query structure
- Easy to secure (restrict by compartment)

**Disadvantages:**
- Data duplication
- Must update on every reference change
- Additional storage overhead
- Complexity in data management

**When to Use:**
- High-volume patient portal applications
- Security-critical environments
- Read-heavy workloads
- When performance is paramount

### Approach 3: Hybrid (Recommended for Production)

**Concept:** Combine both approaches based on usage patterns

**Strategy:**
1. Use **pre-computed `_compartments`** for Patient compartment (most common)
2. Use **dynamic translation** for other compartments
3. Cache CompartmentDefinitions in memory

**Example:**
```javascript
{
  "_search": {
    "patientId": "pat-123",
    "encounterId": "enc-789",
    "practitionerId": "prac-456"
  },
  
  // Only pre-compute Patient compartment (most queried)
  "_compartments": {
    "Patient": ["pat-123"]
  }
}
```

**Query Logic:**
```python
if compartment_type == "Patient":
    # Fast path: use pre-computed
    query = {"_compartments.Patient": patient_id}
else:
    # Dynamic path: translate to _search fields
    query = translate_compartment_to_search(compartment_type, id, resource_type)
```

---

## Integration with FHIR_TO_MQL

### Current FHIR_TO_MQL Coverage

**The FHIR_TO_MQL.md guide currently focuses on:**
- ✅ Regular search parameter conversion
- ✅ The `_search` field pattern for performance
- ✅ Mapping configuration for field resolution
- ✅ Modifiers, prefixes, chaining

**What's NOT explicitly covered:**
- ❌ Compartment-based URL patterns
- ❌ CompartmentDefinition loading and parsing
- ❌ Multi-parameter OR logic for compartments
- ❌ Compartment + search parameter combination

### Extending FHIR_TO_MQL for Compartments

#### 1. Add Compartment Parser Module

```python
# fhir_to_mql/parsers/compartment_parser.py

from typing import Dict, List, Optional
from ..exceptions import CompartmentNotFoundError

class CompartmentParser:
    """Parse compartment-based URLs and resolve resource inclusion rules."""
    
    def __init__(self, compartment_definitions_path: str):
        """
        Initialize with path to CompartmentDefinition resources.
        
        Args:
            compartment_definitions_path: Path to JSON files with CompartmentDefinitions
        """
        self.compartments = self._load_compartment_definitions(compartment_definitions_path)
    
    def parse_compartment_url(self, url: str) -> Dict:
        """
        Parse a compartment URL into components.
        
        Args:
            url: Compartment URL (e.g., "/Patient/123/Observation")
            
        Returns:
            {
                "compartment_type": "Patient",
                "compartment_id": "123",
                "resource_type": "Observation",
                "search_params": {}  # Additional query params if any
            }
        
        Example:
            >>> parser.parse_compartment_url("/Patient/123/Observation?code=8480-6")
            {
                "compartment_type": "Patient",
                "compartment_id": "123",
                "resource_type": "Observation",
                "search_params": {"code": ["8480-6"]}
            }
        """
        # Implementation here
        pass
    
    def get_compartment_parameters(self, compartment_type: str, 
                                   resource_type: str) -> List[str]:
        """
        Get search parameters that define compartment membership.
        
        Args:
            compartment_type: Type of compartment (Patient, Encounter, etc.)
            resource_type: Type of resource to query (Observation, Condition, etc.)
            
        Returns:
            List of search parameter names that create compartment relationship
            
        Example:
            >>> parser.get_compartment_parameters("Patient", "Observation")
            ["subject", "performer"]
        """
        compartment_def = self.compartments.get(compartment_type)
        if not compartment_def:
            raise CompartmentNotFoundError(f"Compartment {compartment_type} not defined")
        
        for resource_entry in compartment_def.get("resource", []):
            if resource_entry["code"] == resource_type:
                return resource_entry.get("param", [])
        
        # Resource type not in this compartment
        return []
    
    def is_resource_in_compartment(self, compartment_type: str, 
                                    resource_type: str) -> bool:
        """
        Check if a resource type can be in a compartment.
        
        Example:
            >>> parser.is_resource_in_compartment("Patient", "Observation")
            True
            >>> parser.is_resource_in_compartment("Patient", "StructureDefinition")
            False
        """
        params = self.get_compartment_parameters(compartment_type, resource_type)
        return len(params) > 0
```

#### 2. Extend FHIRToMQLConverter

```python
# fhir_to_mql/core/converter.py (additions)

class FHIRToMQLConverter:
    """Main converter - now with compartment support."""
    
    def __init__(self, config_path: Optional[str] = None,
                 compartment_defs_path: Optional[str] = None):
        # Existing initialization...
        self.mapping_loader = MappingLoader(config_path)
        self.field_resolver = FieldResolver(self.mapping_loader)
        self.query_builder = QueryBuilder()
        self.url_parser = URLParser()
        
        # NEW: Add compartment support
        if compartment_defs_path:
            self.compartment_parser = CompartmentParser(compartment_defs_path)
        else:
            self.compartment_parser = None
    
    def convert_compartment_query(self, compartment_type: str, 
                                   compartment_id: str,
                                   resource_type: str,
                                   search_params: str = "") -> Dict:
        """
        Convert compartment-based query to MongoDB query.
        
        Args:
            compartment_type: Type of compartment (e.g., "Patient")
            compartment_id: ID of compartment entity (e.g., "123")
            resource_type: Type of resource to query (e.g., "Observation")
            search_params: Additional search parameters (optional)
            
        Returns:
            MongoDB query dictionary
            
        Example:
            >>> converter.convert_compartment_query(
            ...     "Patient", "pat-123", "Observation", "code=8480-6"
            ... )
            {
                "$and": [
                    {"$or": [
                        {"_search.patientId": "pat-123"},
                        {"_search.performerId": "pat-123"}
                    ]},
                    {"_search.codeCodes": "8480-6"}
                ]
            }
        """
        if not self.compartment_parser:
            raise ConfigurationError("Compartment definitions not loaded")
        
        # Check if resource type can be in this compartment
        if not self.compartment_parser.is_resource_in_compartment(
            compartment_type, resource_type
        ):
            raise CompartmentError(
                f"{resource_type} is not included in {compartment_type} compartment"
            )
        
        # Get parameters that define compartment membership
        compartment_params = self.compartment_parser.get_compartment_parameters(
            compartment_type, resource_type
        )
        
        # Load mapping config for the resource type
        mapping_config = self.mapping_loader.load(resource_type)
        
        # Build OR query for compartment parameters
        compartment_conditions = []
        
        for param_name in compartment_params:
            # Resolve field path for this parameter
            field_info = self.field_resolver.resolve(
                resource_type, param_name, mapping_config
            )
            
            # Get the primary field that stores the ID
            # For reference parameters, this should be the extracted ID field
            # e.g., "_search.patientId" for subject parameter
            primary_field = field_info['fields'][0]['field']
            
            # Add condition for this parameter
            compartment_conditions.append({
                primary_field: compartment_id
            })
        
        # Build compartment query (OR of all parameters)
        if len(compartment_conditions) == 1:
            compartment_query = compartment_conditions[0]
        else:
            compartment_query = {"$or": compartment_conditions}
        
        # If there are additional search parameters, combine them with AND
        if search_params:
            additional_query = self.convert(resource_type, search_params)
            
            final_query = {
                "$and": [
                    compartment_query,
                    additional_query
                ]
            }
        else:
            final_query = compartment_query
        
        return final_query
```

#### 3. Add Compartment Configuration Files

```yaml
# config/compartments/Patient.yaml

compartment: Patient
version: 1.0
url: http://hl7.org/fhir/CompartmentDefinition/patient
strategy: precomputed
description: "The patient compartment includes resources that are about or related to a patient"

resources:
  Observation:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Observation about the patient"
      - name: performer
        field: _search.performerId
        description: "Observation performed by the patient"
    precomputeField: "_compartments.Patient"
  
  Condition:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Condition affecting the patient"
    precomputeField: "_compartments.Patient"
  
  MedicationRequest:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Medication prescribed for the patient"
      - name: requester
        field: _search.requesterId
        description: "Medication requested by the patient"
    precomputeField: "_compartments.Patient"
  
  Encounter:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Encounter with the patient"
    precomputeField: "_compartments.Patient"
  
  Appointment:
    parameters:
      - name: actor
        field: _search.actorIds
        description: "Appointment involving the patient as an actor"
    precomputeField: "_compartments.Patient"
  
  DiagnosticReport:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Diagnostic report for the patient"
    precomputeField: "_compartments.Patient"
  
  Procedure:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Procedure performed on the patient"
      - name: performer
        field: _search.performerId
        description: "Procedure performed by the patient"
    precomputeField: "_compartments.Patient"
  
  AllergyIntolerance:
    parameters:
      - name: patient
        field: _search.patientId
        description: "Allergy or intolerance of the patient"
    precomputeField: "_compartments.Patient"
  
  CarePlan:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Care plan for the patient"
    precomputeField: "_compartments.Patient"

settings:
  precomputeField: "_compartments.Patient"
  cacheDefinitions: true
  optimizeQueries: true
  indexRecommendation: "db.{ResourceType}.createIndex({'_compartments.Patient': 1})"
```

---

## Implementation Examples

### Example 1: Patient Compartment Query (PRECOMPUTE)

**Configuration:** Uses `config/compartments/Patient.yaml` with **precomputed strategy**

**Request:**
```
GET /Patient/pat-123/Observation
```

**Conversion Process:**

```python
from fhir_query_mql import FHIRToMQLConverter

converter = FHIRToMQLConverter(
    config_path='config/mappings',
    compartment_defs_path='config/compartments'
)

mql = converter.convert_compartment_query(
    compartment_type="Patient",
    compartment_id="pat-123",
    resource_type="Observation"
)

print(mql)
```

**Configuration Used:**
```yaml
# From config/compartments/Patient.yaml
Observation:
  parameters:
    - name: subject
      field: _search.patientId
    - name: performer
      field: _search.performerId
  precomputeField: "_compartments.Patient"
```

**Output (Precompute Approach - Fast!):**
```javascript
{
  "_compartments.Patient": "pat-123"
}
```

**Performance:** ~2-5ms for 1M documents

**Data Structure Required:**
```javascript
{
  "resourceType": "Observation",
  "id": "obs-001",
  "subject": {"reference": "Patient/pat-123"},
  "performer": [{"reference": "Practitioner/prac-456"}],
  
  "_search": {
    "patientId": "pat-123",
    "performerId": "prac-456"
  },
  
  "_compartments": {
    "Patient": ["pat-123"]  // Pre-computed compartment membership
  }
}
```

---

### Example 2: Patient Compartment with Additional Filters (PRECOMPUTE)

**Request:**
```
GET /Patient/pat-123/Observation?code=8480-6&date=ge2024-01-01
```

**Conversion:**

```python
mql = converter.convert_compartment_query(
    compartment_type="Patient",
    compartment_id="pat-123",
    resource_type="Observation",
    search_params="code=8480-6&date=ge2024-01-01"
)
```

**Output:**
```javascript
{
  "$and": [
    // Compartment scope (precomputed field)
    {"_compartments.Patient": "pat-123"},
    
    // Additional search filters
    {"_search.codeCodes": "8480-6"},
    {"_search.start": {"$gte": "2024-01-01T00:00:00Z"}}
  ]
}
```

**Performance:** ~5-10ms (with compound index on `_compartments.Patient` + filter fields)

---

### Example 3: Encounter Compartment Query (DYNAMIC)

**Configuration:** Uses `config/compartments/Encounter.yaml` with **dynamic strategy**

**Request:**
```
GET /Encounter/enc-789/Observation
```

**Conversion:**

```python
mql = converter.convert_compartment_query(
    compartment_type="Encounter",
    compartment_id="enc-789",
    resource_type="Observation"
)
```

**Configuration Used:**
```yaml
# From config/compartments/Encounter.yaml
Observation:
  parameters:
    - name: encounter
      field: _search.encounterId
  dynamicFields: ["_search.encounterId"]
```

**Output (Dynamic Approach):**
```javascript
{
  "_search.encounterId": "enc-789"
}
```

**Performance:** ~15-30ms for 1M documents

**Data Structure Required:**
```javascript
{
  "resourceType": "Observation",
  "id": "obs-001",
  "encounter": {"reference": "Encounter/enc-789"},
  
  "_search": {
    "encounterId": "enc-789",
    "patientId": "pat-123"
  }
  // No _compartments.Encounter field needed
}
```

**Note:** Simpler because Observation only has **one parameter** (encounter) linking to Encounter.

---

### Example 4: Practitioner Compartment with Multiple Parameters (DYNAMIC)

**Configuration:** Uses `config/compartments/Practitioner.yaml` with **dynamic strategy**

**Request:**
```
GET /Practitioner/prac-456/ServiceRequest
```

**Conversion:**

```python
mql = converter.convert_compartment_query(
    compartment_type="Practitioner",
    compartment_id="prac-456",
    resource_type="ServiceRequest"
)
```

**Configuration Used:**
```yaml
# From config/compartments/Practitioner.yaml
ServiceRequest:
  parameters:
    - name: requester
      field: _search.requesterId
    - name: performer
      field: _search.performerId
  dynamicFields: ["_search.requesterId", "_search.performerId"]
```

**Output (Dynamic with OR Logic):**
```javascript
{
  "$or": [
    {"_search.requesterId": "prac-456"},   // requester parameter
    {"_search.performerId": "prac-456"}    // performer parameter
  ]
}
```

**Explanation:** ServiceRequest has **two parameters** linking to Practitioner:
- `requester`: The practitioner who requested the service
- `performer`: The practitioner who will perform the service

A ServiceRequest is in the Practitioner compartment if the practitioner is **either** the requester **or** the performer.

---

### Example 5: Practitioner with Additional Filters (DYNAMIC)

**Request:**
```
GET /Practitioner/prac-456/Appointment?date=ge2024-05-01&status=booked
```

**Output:**
```javascript
{
  "$and": [
    // Compartment scope (dynamic translation)
    {"_search.actorIds": "prac-456"},
    
    // Additional search filters
    {"_search.start": {"$gte": "2024-05-01T00:00:00Z"}},
    {"_search.statusCode": "booked"}
  ]
}
```

---

### Example 6: Device Compartment with Multiple Parameters (DYNAMIC)

**Configuration:** Uses `config/compartments/Device.yaml` with **dynamic strategy**

**Request:**
```
GET /Device/device-001/Observation
```

**Configuration Used:**
```yaml
# From config/compartments/Device.yaml
Observation:
  parameters:
    - name: device
      field: _search.deviceId
    - name: subject
      field: _search.subjectId
  dynamicFields: ["_search.deviceId", "_search.subjectId"]
```

**Output:**
```javascript
{
  "$or": [
    {"_search.deviceId": "device-001"},    // Observation.device
    {"_search.subjectId": "device-001"}    // Observation.subject (when device is subject)
  ]
}
```

---

### Example 7: All Resources in Patient Compartment (PRECOMPUTE)

**Request:**
```
GET /Patient/pat-123/*
```

**Implementation:**

```python
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017')
db = client['fhir_db']

# Get all resource types defined in Patient compartment config
resource_types = converter.compartment_parser.get_resource_types("Patient")
# Returns: ["Observation", "Condition", "MedicationRequest", "Encounter", ...]

# Query each resource type using precomputed field
results = {}
for resource_type in resource_types:
    # All queries use same simple pattern for Patient compartment
    mql = {"_compartments.Patient": "pat-123"}
    
    results[resource_type] = list(db[resource_type].find(mql))

# Bundle all results
total_count = sum(len(results[rt]) for rt in results)
print(f"Found {total_count} resources across {len(resource_types)} resource types")
```

**Performance:** Very fast (2-5ms per resource type) due to precomputed fields

---

### Example 8: Comparing Patient vs Encounter Compartment Queries

**Scenario:** Get all Observations

**Patient Compartment (Precompute):**
```python
# GET /Patient/pat-123/Observation
mql = {"_compartments.Patient": "pat-123"}
# Uses: config/compartments/Patient.yaml (strategy: precomputed)
# Field: _compartments.Patient
# Performance: 2-5ms
```

**Encounter Compartment (Dynamic):**
```python
# GET /Encounter/enc-789/Observation
mql = {"_search.encounterId": "enc-789"}
# Uses: config/compartments/Encounter.yaml (strategy: dynamic)
# Field: _search.encounterId
# Performance: 15-30ms
```

**Why Different Strategies?**
- **Patient compartment**: Highest query volume (patient portals, EMR access) → Optimize with precompute
- **Encounter compartment**: Lower volume, simple single-parameter queries → Dynamic is sufficient

---

### Configuration to Query Mapping Summary

| Compartment | Config File | Strategy | Query Pattern | Parameters |
|-------------|-------------|----------|---------------|------------|
| **Patient** | `Patient.yaml` | Precomputed | `{"_compartments.Patient": "id"}` | Multiple (OR in precompute) |
| **Encounter** | `Encounter.yaml` | Dynamic | `{"_search.encounterId": "id"}` | Single parameter |
| **Practitioner** | `Practitioner.yaml` | Dynamic | `{"$or": [...]}` or single field | Multiple (OR in query) |
| **Device** | `Device.yaml` | Dynamic | `{"$or": [...]}` or single field | Multiple (OR in query) |
| **RelatedPerson** | `RelatedPerson.yaml` | Dynamic | `{"_search.fieldId": "id"}` | Single parameter typically |

---

## Performance Optimization

### Strategy 1: Pre-Computed Compartment Fields (Patient Only)

**Used By:** `config/compartments/Patient.yaml`

**Data Structure:**

```javascript
{
  "resourceType": "Observation",
  "id": "obs-001",
  "subject": {"reference": "Patient/pat-123"},
  "performer": [{"reference": "Practitioner/prac-456"}],
  
  "_search": {
    "patientId": "pat-123",
    "performerId": "prac-456",
    "encounterId": "enc-789"
  },
  
  "_compartments": {
    "Patient": ["pat-123"]  // Pre-computed Patient compartment ONLY
  }
  // Note: No _compartments for Practitioner/Encounter (use dynamic)
}
```

**Index for Patient Compartment:**
```javascript
// Primary index for Patient compartment queries
db.Observation.createIndex({"_compartments.Patient": 1});
db.Condition.createIndex({"_compartments.Patient": 1});
db.MedicationRequest.createIndex({"_compartments.Patient": 1});
db.Encounter.createIndex({"_compartments.Patient": 1});
// ... for all resources in Patient compartment
```

**Query Performance:**
```javascript
// Ultra-fast: Single field lookup
db.Observation.find({"_compartments.Patient": "pat-123"});

// Typical performance: 2-5ms for 1M documents
```

**Why Only Patient?**
- Patient compartment has **highest query volume** (patient portals, EMR access)
- Worth the storage and maintenance overhead
- Other compartments use dynamic approach with `_search` fields

---

### Strategy 2: Dynamic Translation for Other Compartments

**Used By:** `Encounter.yaml`, `Practitioner.yaml`, `Device.yaml`, `RelatedPerson.yaml`

**Data Structure (No Extra Fields):**

```javascript
{
  "resourceType": "Observation",
  "id": "obs-001",
  "encounter": {"reference": "Encounter/enc-789"},
  
  "_search": {
    "encounterId": "enc-789",
    "practitionerId": "prac-456",
    "patientId": "pat-123"
  }
  // Uses existing _search fields, no _compartments needed
}
```

**Indexes for Dynamic Compartments:**
```javascript
// Standard _search indexes work for all compartments
db.Observation.createIndex({"_search.encounterId": 1});
db.Observation.createIndex({"_search.practitionerId": 1});
db.Observation.createIndex({"_search.deviceId": 1});
```

**Query Performance:**
```javascript
// Single parameter compartments (Encounter)
db.Observation.find({"_search.encounterId": "enc-789"});
// Performance: 15-30ms for 1M documents

// Multiple parameter compartments (Practitioner, Device)
db.Observation.find({
  "$or": [
    {"_search.requesterId": "prac-456"},
    {"_search.performerId": "prac-456"}
  ]
});
// Performance: 20-40ms for 1M documents
```

**Benefits:**
- No additional storage overhead
- No maintenance when references change
- Uses existing `_search` field infrastructure
- Sufficient performance for lower-volume queries

---

### Strategy 3: Compound Indexes for Compartment + Filters

#### For Patient Compartment (Precomputed)

**Common Query Pattern:**
```
GET /Patient/pat-123/Observation?code=8480-6&date=ge2024-01-01
```

**Optimized Compound Index:**
```javascript
db.Observation.createIndex({
  "_compartments.Patient": 1,
  "_search.codeCodes": 1,
  "_search.start": -1
});
```

**Query:**
```javascript
{
  "$and": [
    {"_compartments.Patient": "pat-123"},
    {"_search.codeCodes": "8480-6"},
    {"_search.start": {"$gte": ISODate("2024-01-01T00:00:00Z")}}
  ]
}
```

**Performance:** 5-10ms for highly selective queries

#### For Dynamic Compartments (Encounter, Practitioner, etc.)

**Common Query Pattern:**
```
GET /Encounter/enc-789/Observation?code=8480-6
```

**Optimized Compound Index:**
```javascript
db.Observation.createIndex({
  "_search.encounterId": 1,
  "_search.codeCodes": 1,
  "_search.start": -1
});
```

**Query:**
```javascript
{
  "$and": [
    {"_search.encounterId": "enc-789"},
    {"_search.codeCodes": "8480-6"}
  ]
}
```

**Performance:** 20-35ms

---

### Strategy 4: Configuration-Based Compartment Cache

**Cache CompartmentDefinition in Memory:**

```python
class CompartmentManager:
    def __init__(self, compartment_config_path: str):
        # Load all compartment configs at startup
        self.configs = {}
        self.query_cache = {}
        
        # Load configs for all compartment types
        for comp_type in ['Patient', 'Encounter', 'Practitioner', 'Device', 'RelatedPerson']:
            config_file = f"{compartment_config_path}/{comp_type}.yaml"
            self.configs[comp_type] = self._load_config(config_file)
    
    def get_compartment_strategy(self, compartment_type: str) -> str:
        """
        Get strategy (precomputed or dynamic) for compartment type.
        
        Returns:
            "precomputed" for Patient compartment
            "dynamic" for all others
        """
        config = self.configs.get(compartment_type, {})
        return config.get('strategy', 'dynamic')
    
    def build_compartment_query(self, compartment_type: str, 
                                compartment_id: str,
                                resource_type: str) -> Dict:
        """
        Build optimized query based on compartment strategy.
        """
        cache_key = f"{compartment_type}:{resource_type}:{compartment_id}"
        
        if cache_key in self.query_cache:
            return self.query_cache[cache_key]
        
        strategy = self.get_compartment_strategy(compartment_type)
        
        if strategy == "precomputed":
            # Patient compartment: Use _compartments field
            query = {f"_compartments.{compartment_type}": compartment_id}
        else:
            # Other compartments: Use dynamic _search fields
            query = self._build_dynamic_query(
                compartment_type, compartment_id, resource_type
            )
        
        self.query_cache[cache_key] = query
        return query
    
    def _build_dynamic_query(self, compartment_type: str, 
                            compartment_id: str,
                            resource_type: str) -> Dict:
        """
        Build dynamic query from _search fields.
        """
        config = self.configs[compartment_type]
        resource_config = config['resources'].get(resource_type, {})
        
        fields = []
        for param in resource_config.get('parameters', []):
            field_path = param['field']
            fields.append({field_path: compartment_id})
        
        if len(fields) == 1:
            return fields[0]
        else:
            return {"$or": fields}
```

---

### Performance Comparison Table

| Compartment Type | Strategy | Config File | Query Pattern | Query Time (1M docs) | Storage Overhead |
|------------------|----------|-------------|---------------|---------------------|------------------|
| **Patient** | Precomputed | `Patient.yaml` | `{"_compartments.Patient": "id"}` | **2-5ms** | +5-10% per resource |
| **Encounter** | Dynamic | `Encounter.yaml` | `{"_search.encounterId": "id"}` | 15-30ms | None |
| **Practitioner** | Dynamic | `Practitioner.yaml` | `{"$or": [{...}, {...}]}` | 20-40ms | None |
| **Device** | Dynamic | `Device.yaml` | `{"$or": [{...}, {...}]}` | 20-40ms | None |
| **RelatedPerson** | Dynamic | `RelatedPerson.yaml` | `{"_search.linkIds": "id"}` | 15-30ms | None |

---

### Index Recommendations by Compartment

#### Patient Compartment (Precomputed)
```javascript
// Primary compartment index
db.Observation.createIndex({"_compartments.Patient": 1});

// Compound indexes for common filters
db.Observation.createIndex({
  "_compartments.Patient": 1,
  "_search.codeCodes": 1,
  "_search.start": -1
});

db.Observation.createIndex({
  "_compartments.Patient": 1,
  "_search.statusCode": 1
});
```

#### Encounter Compartment (Dynamic)
```javascript
// Use existing _search indexes
db.Observation.createIndex({"_search.encounterId": 1});

// Compound indexes for common filters
db.Observation.createIndex({
  "_search.encounterId": 1,
  "_search.codeCodes": 1
});
```

#### Practitioner Compartment (Dynamic)
```javascript
// Individual parameter indexes
db.Observation.createIndex({"_search.performerId": 1});
db.ServiceRequest.createIndex({"_search.requesterId": 1});
db.ServiceRequest.createIndex({"_search.performerId": 1});

// MongoDB will use these for $or queries automatically
```

---

### Memory and Storage Impact

**Precomputed Approach (Patient only):**
```
Storage per resource: +20-50 bytes (_compartments.Patient field)
Total overhead: 5-10% for resources with Patient references
Memory cache: Negligible (config files only, ~50KB total)
```

**Dynamic Approach (Others):**
```
Storage per resource: 0 bytes (uses existing _search fields)
Total overhead: 0%
Memory cache: Negligible (config files + query cache ~100KB)
```

**Recommendation:** Current hybrid approach provides optimal balance:
- **Patient**: 10x faster queries, worth the 5-10% storage cost
- **Others**: Zero overhead, adequate performance for lower volumes

---

## Configuration Approach

### Strategy: Separate Configs for Precompute vs Dynamic

We maintain **separate configuration files** for different approaches:

1. **Patient Compartment** → Precompute approach (uses `_compartments.Patient` field)
2. **Other Compartments** → Dynamic approach (uses `_search` fields with OR logic)

Each parameter gets its own dedicated field mapping.

---

### Configuration 1: Patient Compartment (PRECOMPUTE Strategy)

**File:** `config/compartments/Patient.yaml`

This configuration uses **pre-computed** compartment memberships stored in `_compartments.Patient` field for maximum performance.

```yaml
# config/compartments/Patient.yaml

compartment: Patient
version: 1.0
url: http://hl7.org/fhir/CompartmentDefinition/patient
strategy: precomputed  # Use _compartments.Patient field
description: "Patient compartment with pre-computed memberships for high performance"

# Resource inclusion rules with field mappings
resources:
  
  Observation:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Observation about the patient"
      - name: performer
        field: _search.performerId
        description: "Observation performed by the patient"
    precomputeField: "_compartments.Patient"
  
  Condition:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Condition affecting the patient"
    precomputeField: "_compartments.Patient"
  
  MedicationRequest:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Medication prescribed for the patient"
      - name: requester
        field: _search.requesterId
        description: "Medication requested by the patient"
    precomputeField: "_compartments.Patient"
  
  Encounter:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Encounter with the patient"
    precomputeField: "_compartments.Patient"
  
  Appointment:
    parameters:
      - name: actor
        field: _search.actorIds
        description: "Appointment involving the patient as an actor"
    precomputeField: "_compartments.Patient"
  
  DiagnosticReport:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Diagnostic report for the patient"
    precomputeField: "_compartments.Patient"
  
  Procedure:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Procedure performed on the patient"
      - name: performer
        field: _search.performerId
        description: "Procedure performed by the patient"
    precomputeField: "_compartments.Patient"
  
  AllergyIntolerance:
    parameters:
      - name: patient
        field: _search.patientId
        description: "Allergy or intolerance of the patient"
    precomputeField: "_compartments.Patient"
  
  CarePlan:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Care plan for the patient"
    precomputeField: "_compartments.Patient"
  
  DocumentReference:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Document about the patient"
      - name: author
        field: _search.authorId
        description: "Document authored by the patient"
    precomputeField: "_compartments.Patient"
  
  ImagingStudy:
    parameters:
      - name: subject
        field: _search.patientId
        description: "Imaging study of the patient"
    precomputeField: "_compartments.Patient"

settings:
  precomputeField: "_compartments.Patient"
  cacheDefinitions: true
  optimizeQueries: true
  indexRecommendation: "db.{ResourceType}.createIndex({'_compartments.Patient': 1})"

# Query generation strategy
queryStrategy:
  # Use simple field lookup for precomputed compartments
  template: |
    {
      "_compartments.Patient": "{compartment_id}"
    }
  
  # With additional parameters
  templateWithParams: |
    {
      "$and": [
        {"_compartments.Patient": "{compartment_id}"},
        {additional_params_query}
      ]
    }

# Data structure expectation
dataStructure:
  example: |
    {
      "resourceType": "Observation",
      "id": "obs-001",
      "subject": {"reference": "Patient/pat-123"},
      
      "_search": {
        "patientId": "pat-123",
        "performerId": "prac-456"
      },
      
      "_compartments": {
        "Patient": ["pat-123"]  // Pre-computed Patient compartment
      }
    }
```

---

### Configuration 2: Encounter Compartment (DYNAMIC Strategy)

**File:** `config/compartments/Encounter.yaml`

This configuration uses **dynamic translation** to `_search` fields with OR logic.

```yaml
# config/compartments/Encounter.yaml

compartment: Encounter
version: 1.0
url: http://hl7.org/fhir/CompartmentDefinition/encounter
strategy: dynamic  # Translate to _search fields with OR logic
description: "Encounter compartment using dynamic field resolution"

# Resource inclusion rules with explicit field mappings
resources:
  
  Observation:
    parameters:
      - name: encounter
        field: _search.encounterId
        description: "Observations made during the encounter"
    dynamicFields: ["_search.encounterId"]
  
  Condition:
    parameters:
      - name: encounter
        field: _search.encounterId
        description: "Conditions identified during the encounter"
    dynamicFields: ["_search.encounterId"]
  
  Procedure:
    parameters:
      - name: encounter
        field: _search.encounterId
        description: "Procedures performed during the encounter"
    dynamicFields: ["_search.encounterId"]
  
  MedicationRequest:
    parameters:
      - name: encounter
        field: _search.encounterId
        description: "Medications prescribed during the encounter"
    dynamicFields: ["_search.encounterId"]
  
  DiagnosticReport:
    parameters:
      - name: encounter
        field: _search.encounterId
        description: "Diagnostic reports from the encounter"
    dynamicFields: ["_search.encounterId"]
  
  ServiceRequest:
    parameters:
      - name: encounter
        field: _search.encounterId
        description: "Service requests made during the encounter"
    dynamicFields: ["_search.encounterId"]
  
  Communication:
    parameters:
      - name: encounter
        field: _search.encounterId
        description: "Communications during the encounter"
    dynamicFields: ["_search.encounterId"]
  
  Flag:
    parameters:
      - name: encounter
        field: _search.encounterId
        description: "Flags raised during the encounter"
    dynamicFields: ["_search.encounterId"]

settings:
  precompute: false
  cacheDefinitions: true
  optimizeQueries: true
  indexRecommendation: "db.{ResourceType}.createIndex({'_search.encounterId': 1})"

# Query generation strategy
queryStrategy:
  # Most resources have single encounter parameter
  template: |
    {
      "_search.encounterId": "{compartment_id}"
    }
  
  # With additional parameters
  templateWithParams: |
    {
      "$and": [
        {"_search.encounterId": "{compartment_id}"},
        {additional_params_query}
      ]
    }

# Data structure expectation
dataStructure:
  example: |
    {
      "resourceType": "Observation",
      "id": "obs-001",
      "encounter": {"reference": "Encounter/enc-789"},
      
      "_search": {
        "encounterId": "enc-789",
        "patientId": "pat-123"
      }
      // No _compartments.Encounter field needed (dynamic approach)
    }
```

---

### Configuration 3: Practitioner Compartment (DYNAMIC Strategy)

**File:** `config/compartments/Practitioner.yaml`

```yaml
# config/compartments/Practitioner.yaml

compartment: Practitioner
version: 1.0
url: http://hl7.org/fhir/CompartmentDefinition/practitioner
strategy: dynamic
description: "Practitioner compartment using dynamic field resolution"

resources:
  
  Observation:
    parameters:
      - name: performer
        field: _search.performerId
        description: "Observations performed by the practitioner"
    dynamicFields: ["_search.performerId"]
  
  Procedure:
    parameters:
      - name: performer
        field: _search.performerId
        description: "Procedures performed by the practitioner"
    dynamicFields: ["_search.performerId"]
  
  Encounter:
    parameters:
      - name: practitioner
        field: _search.practitionerId
        description: "Encounters with the practitioner"
      - name: participant
        field: _search.participantIds
        description: "Encounters where practitioner was a participant"
    dynamicFields: ["_search.practitionerId", "_search.participantIds"]
  
  Appointment:
    parameters:
      - name: actor
        field: _search.actorIds
        description: "Appointments involving the practitioner"
    dynamicFields: ["_search.actorIds"]
  
  Schedule:
    parameters:
      - name: actor
        field: _search.actorIds
        description: "Schedules for the practitioner"
    dynamicFields: ["_search.actorIds"]
  
  DiagnosticReport:
    parameters:
      - name: performer
        field: _search.performerId
        description: "Reports performed by the practitioner"
    dynamicFields: ["_search.performerId"]
  
  MedicationRequest:
    parameters:
      - name: requester
        field: _search.requesterId
        description: "Medications requested by the practitioner"
    dynamicFields: ["_search.requesterId"]
  
  ServiceRequest:
    parameters:
      - name: requester
        field: _search.requesterId
        description: "Services requested by the practitioner"
      - name: performer
        field: _search.performerId
        description: "Services performed by the practitioner"
    dynamicFields: ["_search.requesterId", "_search.performerId"]

settings:
  precompute: false
  cacheDefinitions: true
  optimizeQueries: true
  indexRecommendation: "db.{ResourceType}.createIndex({'_search.practitionerId': 1})"

# Query generation strategy
queryStrategy:
  # Single parameter resources
  templateSingle: |
    {
      "{field_path}": "{compartment_id}"
    }
  
  # Multiple parameter resources (OR logic)
  templateMultiple: |
    {
      "$or": [
        {"{field_path_1}": "{compartment_id}"},
        {"{field_path_2}": "{compartment_id}"}
      ]
    }
  
  # Example for ServiceRequest (2 parameters)
  exampleMultiple: |
    {
      "$or": [
        {"_search.requesterId": "prac-456"},
        {"_search.performerId": "prac-456"}
      ]
    }

dataStructure:
  example: |
    {
      "resourceType": "Observation",
      "id": "obs-001",
      "performer": [{"reference": "Practitioner/prac-456"}],
      
      "_search": {
        "performerId": "prac-456",
        "patientId": "pat-123"
      }
      // No _compartments.Practitioner field
    }
```

---

### Configuration 4: Device Compartment (DYNAMIC Strategy)

**File:** `config/compartments/Device.yaml`

```yaml
# config/compartments/Device.yaml

compartment: Device
version: 1.0
url: http://hl7.org/fhir/CompartmentDefinition/device
strategy: dynamic
description: "Device compartment using dynamic field resolution"

resources:
  
  Observation:
    parameters:
      - name: device
        field: _search.deviceId
        description: "Observations from the device"
      - name: subject
        field: _search.subjectId
        description: "Observations where device is the subject"
    dynamicFields: ["_search.deviceId", "_search.subjectId"]
  
  DiagnosticReport:
    parameters:
      - name: subject
        field: _search.subjectId
        description: "Reports about the device"
    dynamicFields: ["_search.subjectId"]
  
  Procedure:
    parameters:
      - name: used-reference
        field: _search.usedDeviceIds
        description: "Procedures using the device"
    dynamicFields: ["_search.usedDeviceIds"]
  
  DeviceMetric:
    parameters:
      - name: source
        field: _search.sourceDeviceId
        description: "Metrics from the device"
    dynamicFields: ["_search.sourceDeviceId"]
  
  DeviceUseStatement:
    parameters:
      - name: device
        field: _search.deviceId
        description: "Usage statements for the device"
    dynamicFields: ["_search.deviceId"]

settings:
  precompute: false
  cacheDefinitions: true
  optimizeQueries: true
  indexRecommendation: "db.{ResourceType}.createIndex({'_search.deviceId': 1})"

queryStrategy:
  templateSingle: |
    {
      "{field_path}": "{compartment_id}"
    }
  
  templateMultiple: |
    {
      "$or": [
        {"{field_path_1}": "{compartment_id}"},
        {"{field_path_2}": "{compartment_id}"}
      ]
    }

dataStructure:
  example: |
    {
      "resourceType": "Observation",
      "id": "obs-001",
      "device": {"reference": "Device/device-001"},
      
      "_search": {
        "deviceId": "device-001",
        "patientId": "pat-123"
      }
    }
```

---

### Configuration 5: RelatedPerson Compartment (DYNAMIC Strategy)

**File:** `config/compartments/RelatedPerson.yaml`

```yaml
# config/compartments/RelatedPerson.yaml

compartment: RelatedPerson
version: 1.0
url: http://hl7.org/fhir/CompartmentDefinition/relatedperson
strategy: dynamic
description: "RelatedPerson compartment using dynamic field resolution"

resources:
  
  Patient:
    parameters:
      - name: link
        field: _search.linkIds
        description: "Patients linked to the related person"
    dynamicFields: ["_search.linkIds"]
  
  Appointment:
    parameters:
      - name: actor
        field: _search.actorIds
        description: "Appointments involving the related person"
    dynamicFields: ["_search.actorIds"]
  
  DocumentReference:
    parameters:
      - name: author
        field: _search.authorId
        description: "Documents authored by the related person"
    dynamicFields: ["_search.authorId"]
  
  Observation:
    parameters:
      - name: performer
        field: _search.performerId
        description: "Observations performed by the related person"
    dynamicFields: ["_search.performerId"]
  
  CareTeam:
    parameters:
      - name: participant
        field: _search.participantIds
        description: "Care teams including the related person"
    dynamicFields: ["_search.participantIds"]

settings:
  precompute: false
  cacheDefinitions: true
  optimizeQueries: true
  indexRecommendation: "db.{ResourceType}.createIndex({'_search.linkIds': 1})"

queryStrategy:
  templateSingle: |
    {
      "{field_path}": "{compartment_id}"
    }

dataStructure:
  example: |
    {
      "resourceType": "Appointment",
      "id": "apt-001",
      "participant": [
        {"actor": {"reference": "RelatedPerson/rel-123"}}
      ],
      
      "_search": {
        "actorIds": ["rel-123", "pat-456"],
        "patientId": "pat-456"
      }
    }
```

---

### Loading Configuration

```python
from fhir_query_mql.compartments import CompartmentManager

# Initialize with compartment configurations
manager = CompartmentManager(compartment_config_path='config/compartments')

# Validate all compartment configurations
for compartment in ['Patient', 'Encounter', 'Practitioner', 'Device', 'RelatedPerson']:
    errors = manager.validate_compartment_config(compartment)
    if errors:
        print(f"Configuration errors in {compartment}:", errors)

# Get optimized query based on strategy
# Patient compartment → Uses precomputed field
patient_mql = manager.build_compartment_query(
    compartment_type="Patient",
    compartment_id="pat-123",
    resource_type="Observation"
)
# Output: {"_compartments.Patient": "pat-123"}

# Encounter compartment → Uses dynamic translation
encounter_mql = manager.build_compartment_query(
    compartment_type="Encounter",
    compartment_id="enc-789",
    resource_type="Observation"
)
# Output: {"_search.encounterId": "enc-789"}

# Practitioner with multiple parameters → Uses OR logic
practitioner_mql = manager.build_compartment_query(
    compartment_type="Practitioner",
    compartment_id="prac-456",
    resource_type="ServiceRequest"
)
# Output: {
#   "$or": [
#     {"_search.requesterId": "prac-456"},
#     {"_search.performerId": "prac-456"}
#   ]
# }
```

---

## Complete Integration Example

### Server Implementation

```python
# app.py - FHIR Server with Compartment Support

from flask import Flask, request, jsonify
from fhir_query_mql import FHIRToMQLConverter
from fhir_query_mql.compartments import CompartmentManager
from pymongo import MongoClient

app = Flask(__name__)

# Initialize MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['fhir_db']

# Initialize converters
converter = FHIRToMQLConverter(config_path='config/mappings')
compartment_mgr = CompartmentManager(compartment_config_path='config/compartments')

@app.route('/<compartment_type>/<compartment_id>/<resource_type>', methods=['GET'])
def compartment_search(compartment_type, compartment_id, resource_type):
    """
    Handle compartment-based searches.
    
    Examples:
        GET /Patient/pat-123/Observation
        GET /Encounter/enc-456/Observation?code=8480-6
        GET /Practitioner/prac-789/Appointment?date=ge2024-05-01
    """
    try:
        # Get additional search parameters from query string
        search_params = request.query_string.decode('utf-8')
        
        # Build MQL query
        mql = compartment_mgr.build_compartment_query(
            compartment_type=compartment_type,
            compartment_id=compartment_id,
            resource_type=resource_type,
            additional_params=search_params
        )
        
        # Execute query
        results = list(db[resource_type].find(mql))
        
        # Format as FHIR Bundle
        bundle = {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(results),
            "entry": [{"resource": r} for r in results]
        }
        
        return jsonify(bundle)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/<resource_type>', methods=['GET'])
def regular_search(resource_type):
    """
    Handle regular (non-compartment) searches.
    
    Example:
        GET /Observation?subject=Patient/123&code=8480-6
    """
    try:
        search_params = request.query_string.decode('utf-8')
        
        # Use regular FHIR_TO_MQL conversion
        mql = converter.convert(resource_type, search_params)
        
        results = list(db[resource_type].find(mql))
        
        bundle = {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(results),
            "entry": [{"resource": r} for r in results]
        }
        
        return jsonify(bundle)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
```

### Data Ingestion with Compartment Computation

**Strategy:** Only compute `_compartments.Patient` field. Other compartments use dynamic resolution.

```python
# data_ingestion.py - Compute Patient compartment only (hybrid approach)

from fhir_query_mql.compartments import CompartmentManager
from pymongo import MongoClient

class FHIRResourceIngestion:
    def __init__(self, db, compartment_mgr):
        self.db = db
        self.compartment_mgr = compartment_mgr
    
    def ingest_resource(self, resource):
        """
        Ingest FHIR resource with Patient compartment precomputation.
        
        Args:
            resource: FHIR resource dictionary
        """
        resource_type = resource['resourceType']
        
        # Step 1: Compute _search fields (from FHIR_TO_MQL approach)
        search_fields = self._compute_search_fields(resource)
        resource['_search'] = search_fields
        
        # Step 2: Compute ONLY Patient compartment (precomputed strategy)
        patient_compartment = self._compute_patient_compartment(
            resource, 
            resource_type, 
            search_fields
        )
        if patient_compartment:
            resource['_compartments'] = {
                "Patient": patient_compartment
            }
            # Note: No Practitioner/Encounter/Device/RelatedPerson in _compartments
            # Those use dynamic resolution from _search fields
        
        # Step 3: Insert into MongoDB
        collection = self.db[resource_type]
        collection.insert_one(resource)
    
    def _compute_patient_compartment(self, resource, resource_type, search_fields):
        """
        Compute Patient compartment memberships.
        
        Uses config/compartments/Patient.yaml to determine which search fields
        to check for patient references.
        
        Returns:
            List of patient IDs this resource belongs to, or None
            Example: ["pat-123"] or ["pat-123", "pat-456"] for multiple patients
        """
        patient_config = self.compartment_mgr.get_config('Patient')
        resource_config = patient_config['resources'].get(resource_type)
        
        if not resource_config:
            return None  # This resource type not in Patient compartment
        
        patient_ids = set()
        
        # Check each parameter defined in Patient.yaml
        for param in resource_config['parameters']:
            param_name = param['name']
            field_path = param['field']
            
            # Extract patient ID from search field
            # Example: _search.patientId -> "pat-123"
            field_key = field_path.split('.')[-1]  # Get last part
            patient_id = search_fields.get(field_key)
            
            if patient_id:
                if isinstance(patient_id, list):
                    patient_ids.update(patient_id)
                else:
                    patient_ids.add(patient_id)
        
        return list(patient_ids) if patient_ids else None
    
    def _compute_search_fields(self, resource):
        """
        Compute _search fields using FHIR_TO_MQL approach.
        
        For Observation example:
        {
          "patientId": "pat-123",        # From subject
          "performerId": "prac-456",     # From performer
          "encounterId": "enc-789",      # From encounter
          "deviceId": "device-001",      # From device
          "codeCodes": ["8480-6"],       # From code
          "start": "2024-05-14T10:00:00Z" # From effectiveDateTime
        }
        """
        resource_type = resource['resourceType']
        search_fields = {}
        
        # Extract search fields based on resource structure
        # (Implementation details from FHIR_TO_MQL.md)
        
        if resource_type == "Observation":
            # Subject reference (Patient)
            if resource.get('subject', {}).get('reference'):
                ref = resource['subject']['reference']
                if ref.startswith('Patient/'):
                    search_fields['patientId'] = ref.replace('Patient/', '')
            
            # Performer reference (Practitioner)
            if resource.get('performer'):
                for perf in resource['performer']:
                    ref = perf.get('reference', '')
                    if ref.startswith('Practitioner/'):
                        search_fields['performerId'] = ref.replace('Practitioner/', '')
            
            # Encounter reference
            if resource.get('encounter', {}).get('reference'):
                ref = resource['encounter']['reference']
                if ref.startswith('Encounter/'):
                    search_fields['encounterId'] = ref.replace('Encounter/', '')
            
            # Device reference
            if resource.get('device', {}).get('reference'):
                ref = resource['device']['reference']
                if ref.startswith('Device/'):
                    search_fields['deviceId'] = ref.replace('Device/', '')
            
            # Code
            if resource.get('code', {}).get('coding'):
                codes = []
                for coding in resource['code']['coding']:
                    if coding.get('code'):
                        codes.append(coding['code'])
                if codes:
                    search_fields['codeCodes'] = codes
            
            # Date
            if resource.get('effectiveDateTime'):
                search_fields['start'] = resource['effectiveDateTime']
        
        # Similar logic for other resource types...
        
        return search_fields

# Usage Example
def main():
    client = MongoClient('mongodb://localhost:27017')
    db = client['fhir_db']
    
    compartment_mgr = CompartmentManager(
        compartment_config_path='config/compartments'
    )
    
    ingestion = FHIRResourceIngestion(db, compartment_mgr)
    
    # Ingest Observation
    observation = {
        "resourceType": "Observation",
        "id": "obs-001",
        "subject": {
            "reference": "Patient/pat-123"
        },
        "performer": [
            {"reference": "Practitioner/prac-456"}
        ],
        "encounter": {
            "reference": "Encounter/enc-789"
        },
        "device": {
            "reference": "Device/device-001"
        },
        "code": {
            "coding": [
                {"system": "http://loinc.org", "code": "8480-6"}
            ]
        },
        "effectiveDateTime": "2024-05-14T10:00:00Z",
        "valueQuantity": {
            "value": 120,
            "unit": "mmHg"
        }
    }
    
    ingestion.ingest_resource(observation)
    
    # Result in MongoDB:
    # {
    #   "resourceType": "Observation",
    #   "id": "obs-001",
    #   "subject": {"reference": "Patient/pat-123"},
    #   "performer": [{"reference": "Practitioner/prac-456"}],
    #   "encounter": {"reference": "Encounter/enc-789"},
    #   "device": {"reference": "Device/device-001"},
    #   ...
    #   
    #   "_search": {
    #     "patientId": "pat-123",
    #     "performerId": "prac-456",
    #     "encounterId": "enc-789",
    #     "deviceId": "device-001",
    #     "codeCodes": ["8480-6"],
    #     "start": "2024-05-14T10:00:00Z"
    #   },
    #   
    #   "_compartments": {
    #     "Patient": ["pat-123"]  // ONLY Patient compartment precomputed
    #   }
    # }
    
    print("Observation ingested with Patient compartment precomputed")
    print("Other compartments (Practitioner, Encounter, Device) use dynamic resolution")
```

---

### Compartment Computation Strategy Comparison

**What Gets Stored:**

| Field | Patient Compartment | Other Compartments |
|-------|---------------------|-------------------|
| `_search.patientId` | ✅ Stored | ✅ Stored |
| `_search.performerId` | ✅ Stored | ✅ Stored |
| `_search.encounterId` | ✅ Stored | ✅ Stored |
| `_search.deviceId` | ✅ Stored | ✅ Stored |
| `_compartments.Patient` | ✅ **Precomputed** | N/A |
| `_compartments.Practitioner` | ❌ Not stored | ❌ Not stored (dynamic) |
| `_compartments.Encounter` | ❌ Not stored | ❌ Not stored (dynamic) |
| `_compartments.Device` | ❌ Not stored | ❌ Not stored (dynamic) |

**Query Generation:**

```python
# Patient compartment query (uses precomputed field)
patient_query = {"_compartments.Patient": "pat-123"}
# Fast: 2-5ms

# Practitioner compartment query (uses dynamic _search fields)
practitioner_query = {
    "$or": [
        {"_search.performerId": "prac-456"},
        {"_search.requesterId": "prac-456"}
    ]
}
# Slower but acceptable: 20-40ms

# Encounter compartment query (single _search field)
encounter_query = {"_search.encounterId": "enc-789"}
# Acceptable: 15-30ms
```

---

### Updating Resources: Maintaining Compartment Integrity

**When a resource's references change, only Patient compartment needs update:**

```python
def update_resource_references(self, resource_id, resource_type, new_references):
    """
    Update resource references and recompute Patient compartment only.
    
    Args:
        resource_id: ID of resource to update
        resource_type: Type of resource (Observation, Condition, etc.)
        new_references: Dictionary of new reference values
                       Example: {"subject": "Patient/pat-456", "encounter": "Encounter/enc-999"}
    """
    collection = self.db[resource_type]
    
    # Get current resource
    resource = collection.find_one({"id": resource_id})
    
    # Update references in resource
    for field, value in new_references.items():
        resource[field] = {"reference": value}
    
    # Recompute _search fields (all references)
    new_search_fields = self._compute_search_fields(resource)
    resource['_search'] = new_search_fields
    
    # Recompute ONLY Patient compartment (precomputed)
    new_patient_compartment = self._compute_patient_compartment(
        resource, 
        resource_type, 
        new_search_fields
    )
    
    if new_patient_compartment:
        resource['_compartments'] = {"Patient": new_patient_compartment}
    else:
        # Remove _compartments if no longer in Patient compartment
        resource.pop('_compartments', None)
    
    # Update in MongoDB
    collection.replace_one({"id": resource_id}, resource)
    
    # NOTE: No need to update Practitioner/Encounter/Device compartments
    # because they use dynamic resolution from _search fields
```

**Maintenance Comparison:**

| Event | Patient Compartment | Other Compartments |
|-------|---------------------|-------------------|
| **Resource creation** | Compute `_compartments.Patient` | No action needed |
| **Reference update** | Recompute `_compartments.Patient` | No action needed |
| **Resource deletion** | Delete document (with `_compartments`) | Delete document |
| **Bulk updates** | Must update `_compartments.Patient` | No action needed |

**Storage Overhead:**

```javascript
// With precomputed Patient compartment only:
{
  "_compartments": {
    "Patient": ["pat-123"]  // ~30 bytes
  }
}

// Total storage overhead: 30-50 bytes per resource
// vs. 100-200 bytes if all compartments were precomputed
```

---

## Summary and Best Practices

### Key Takeaways

1. **Compartments are pre-defined resource groupings** based on relationships to a central entity (Patient, Encounter, etc.)

2. **Compartment queries use multiple parameters with OR logic** - a resource is in a compartment if ANY of the defined parameters match

3. **Two distinct implementation strategies in this guide:**
   - **Precomputed (Patient only)**: Uses `_compartments.Patient` field for 10x performance
   - **Dynamic (All others)**: Uses `_search` fields with no storage overhead

4. **Configuration files clearly separate the approaches:**
   - `config/compartments/Patient.yaml` → `strategy: precomputed`
   - `config/compartments/Encounter.yaml` → `strategy: dynamic`
   - `config/compartments/Practitioner.yaml` → `strategy: dynamic`
   - `config/compartments/Device.yaml` → `strategy: dynamic`
   - `config/compartments/RelatedPerson.yaml` → `strategy: dynamic`

5. **Each parameter has its own dedicated field mapping:**
   ```yaml
   # From Patient.yaml
   Observation:
     parameters:
       - name: subject
         field: _search.patientId
       - name: performer
         field: _search.performerId
   ```

6. **FHIR_TO_MQL extension requirements:**
   - Add `CompartmentManager` class
   - Extend `FHIRToMQLConverter` with `convert_compartment_query()` method
   - Load compartment configuration files
   - Implement strategy detection (precomputed vs dynamic)

---

### Configuration File Summary

| Compartment | Config File | Strategy | Key Features |
|-------------|-------------|----------|--------------|
| **Patient** | `Patient.yaml` | `precomputed` | • Uses `_compartments.Patient` field<br>• Highest performance (2-5ms)<br>• Storage overhead: 30-50 bytes/resource<br>• Must be maintained on reference changes |
| **Encounter** | `Encounter.yaml` | `dynamic` | • Uses `_search.encounterId` field<br>• Single parameter per resource typically<br>• No storage overhead<br>• No maintenance needed |
| **Practitioner** | `Practitioner.yaml` | `dynamic` | • Uses multiple `_search` fields<br>• OR logic for multi-parameter queries<br>• No storage overhead<br>• No maintenance needed |
| **Device** | `Device.yaml` | `dynamic` | • Uses `_search.deviceId` and `_search.subjectId`<br>• OR logic when needed<br>• No storage overhead |
| **RelatedPerson** | `RelatedPerson.yaml` | `dynamic` | • Uses `_search.linkIds` and `_search.actorIds`<br>• Simple single-field queries typically |

---

### Best Practices

#### ✅ DO:

1. **Use precomputed `_compartments.Patient` for Patient compartment**
   - Highest query volume (patient portals, EMR access)
   - 10x performance improvement worth the storage cost
   - Index: `db.{ResourceType}.createIndex({"_compartments.Patient": 1})`

2. **Use dynamic resolution for all other compartments**
   - Encounter, Practitioner, Device, RelatedPerson
   - Sufficient performance (15-40ms)
   - Zero storage overhead
   - No maintenance burden

3. **Create separate configuration files for each compartment**
   - Clear separation of concerns
   - Easy to understand and maintain
   - Each parameter has explicit field mapping

4. **Index `_search` fields used by compartments**
   ```javascript
   db.Observation.createIndex({"_search.patientId": 1});
   db.Observation.createIndex({"_search.encounterId": 1});
   db.Observation.createIndex({"_search.performerId": 1});
   db.Observation.createIndex({"_search.deviceId": 1});
   ```

5. **Cache compartment configurations in memory**
   - Load all YAML files at startup
   - Cache resolved query patterns
   - Minimal memory footprint (~100KB total)

6. **Combine compartments with search parameters**
   ```
   GET /Patient/pat-123/Observation?code=8480-6
   ```
   Use compound indexes:
   ```javascript
   db.Observation.createIndex({
     "_compartments.Patient": 1,
     "_search.codeCodes": 1
   });
   ```

7. **Validate compartment memberships on data ingestion**
   - Compute `_compartments.Patient` during insert
   - Update on reference changes
   - Use configuration files to determine fields

---

#### ❌ DON'T:

1. **Don't precompute all compartments**
   - Only Patient compartment justifies the overhead
   - Other compartments: use dynamic approach
   - Storage cost: 30-50 bytes vs 100-200 bytes per resource

2. **Don't forget to update `_compartments.Patient` when references change**
   - Must recompute on subject/performer updates
   - Other compartments auto-update (use _search fields)

3. **Don't use compartments for cross-compartment queries**
   - Example: "Observations for Patient X by Practitioner Y"
   - Use regular search: `/Observation?subject=Patient/X&performer=Practitioner/Y`
   - More efficient than combining compartment queries

4. **Don't ignore security implications**
   - Compartments are ideal for access control
   - Patient portals: restrict to `_compartments.Patient = current_user_patient_id`
   - Consider row-level security with compartments

5. **Don't duplicate field mappings in configs**
   - Each parameter should have ONE dedicated field
   - Example: `subject` → `_search.patientId` (consistent across all resources)

6. **Don't mix strategies in same compartment**
   - Patient: 100% precomputed
   - Others: 100% dynamic
   - Avoid "sometimes precomputed, sometimes dynamic" confusion

---

### Performance Expectations

| Scenario | Query Pattern | Expected Performance | Configuration |
|----------|---------------|---------------------|---------------|
| **Patient portal - single resource type** | `/Patient/123/Observation` | **2-5ms** | `Patient.yaml` (precomputed) |
| **Patient portal - with filter** | `/Patient/123/Observation?code=X` | **5-10ms** | Compound index on compartment + code |
| **Patient portal - all resources** | `/Patient/123/*` (10 resource types) | **20-50ms total** | Precomputed for all types |
| **Encounter review** | `/Encounter/456/Observation` | 15-30ms | `Encounter.yaml` (dynamic) |
| **Practitioner schedule** | `/Practitioner/789/Appointment` | 20-40ms | `Practitioner.yaml` (dynamic, OR logic) |
| **Device data query** | `/Device/001/Observation` | 20-40ms | `Device.yaml` (dynamic, OR logic) |

---

### Recommended Architecture for Hybrid Approach

```
┌─────────────────────────────────────────────────────────────────┐
│                 FHIR Compartment Request                        │
│          GET /{CompartmentType}/{id}/{ResourceType}             │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              CompartmentManager.build_query()                   │
│  • Load config: config/compartments/{CompartmentType}.yaml      │
│  • Read strategy: "precomputed" or "dynamic"                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
                ┌────────┴────────┐
                │                 │
                ▼                 ▼
┌───────────────────────┐   ┌──────────────────────────┐
│  IF Patient           │   │  IF Other Compartment    │
│  (strategy=precomputed)│   │  (strategy=dynamic)      │
├───────────────────────┤   ├──────────────────────────┤
│ Query Pattern:        │   │ Query Pattern:           │
│ {                     │   │ {                        │
│   "_compartments.     │   │   "$or": [               │
│     Patient": "id"    │   │     {"_search.field1": id}│
│ }                     │   │     {"_search.field2": id}│
│                       │   │   ]                      │
│ Performance: 2-5ms    │   │ }                        │
│ Uses: Patient.yaml    │   │                          │
│                       │   │ Performance: 15-40ms     │
│                       │   │ Uses: {Type}.yaml        │
└───────────────────────┘   └──────────────────────────┘
                │                      │
                └──────────┬───────────┘
                           ▼
                ┌──────────────────────┐
                │   MongoDB Query      │
                │   Execution          │
                └──────────────────────┘
```

---

### Migration Path

**Step 1: Start with Dynamic Only (Simplest)**
```
All compartments use _search fields
No _compartments field needed
Good for: Low traffic, proof of concept
```

**Step 2: Add Patient Precompute (Recommended)**
```
Patient → _compartments.Patient (precomputed)
Others → _search fields (dynamic)
Good for: Production with patient portals
```

**Step 3: Optimize High-Traffic Compartments (If Needed)**
```
Patient → Precomputed
Encounter → Precomputed (if high traffic)
Others → Dynamic
Good for: Very high-volume systems
```

---

### Next Steps

1. **Understand**: Read compartment config examples in this document
   - `Patient.yaml` (precomputed strategy)
   - `Encounter.yaml` (dynamic strategy)
   - `Practitioner.yaml` (dynamic with OR logic)

2. **Implement**: Create `CompartmentManager` class
   - Load YAML configuration files
   - Detect strategy (precomputed vs dynamic)
   - Build appropriate MongoDB queries

3. **Configure**: Create compartment configuration files
   - Start with 5 provided examples
   - Map each parameter to specific `_search` field
   - Define strategy for each compartment

4. **Integrate**: Extend `FHIRToMQLConverter`
   - Add `convert_compartment_query()` method
   - Route Patient queries to precomputed field
   - Route others to dynamic resolution

5. **Optimize**: Add MongoDB indexes
   - `_compartments.Patient` for all Patient-referenced resources
   - `_search` fields for all compartments
   - Compound indexes for common filter combinations

6. **Test**: Validate query performance
   - Patient compartment: Should be 2-5ms
   - Other compartments: Should be 15-40ms
   - Adjust indexes as needed

7. **Monitor**: Track usage patterns
   - If Encounter queries become high-volume → Consider precomputing
   - If Practitioner queries are low-volume → Keep dynamic

---

### References

- **FHIR R5 Compartments**: https://www.hl7.org/fhir/compartments.html
- **CompartmentDefinition Resource**: https://www.hl7.org/fhir/compartmentdefinition.html
- **Patient CompartmentDefinition**: https://www.hl7.org/fhir/compartmentdefinition-patient.html
- **Encounter CompartmentDefinition**: https://www.hl7.org/fhir/compartmentdefinition-encounter.html
- **Practitioner CompartmentDefinition**: https://www.hl7.org/fhir/compartmentdefinition-practitioner.html
- **FHIR_TO_MQL.md**: Complete guide for regular search parameter conversion (companion document)
- **MongoDB Compound Indexes**: https://docs.mongodb.com/manual/core/index-compound/

---

**Document Version:** 1.0  
**Last Updated:** May 14, 2026  
**Companion Document:** [FHIR_TO_MQL.md](FHIR_TO_MQL.md)
