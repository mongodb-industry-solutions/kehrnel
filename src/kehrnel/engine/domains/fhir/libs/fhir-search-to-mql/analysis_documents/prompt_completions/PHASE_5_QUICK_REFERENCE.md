# Phase 5: Advanced Converters - Quick Reference

Fast lookup guide for Phase 5 converters with code examples.

---

## 📚 Import

```python
from fhir_search_to_mql.converters import (
    ReferenceConverter,
    URIConverter,
    CompositeConverter,
    SpecialConverter,
    ChainingHandler,
    MultiStepQuery,
    QueryStep,
)
```

---

## 1. ReferenceConverter

### Basic Usage

```python
config = {
    'type': 'reference',
    'fields': [
        {'field': '_search.patientId', 'referenceType': 'Patient'},
        {'field': '_search.subjectId'}  # Generic
    ]
}
converter = ReferenceConverter(config)
```

### Reference Formats

```python
# Simple ID
converter.convert("123")
# → {"_search.patientId": "123"}

# Type/ID
converter.convert("Patient/123")
# → {"_search.patientId": "123"}

# Full URL
converter.convert("http://example.org/fhir/Patient/123")
# → {"_search.patientId": "123"}

# Canonical URL
converter.convert("http://hl7.org/fhir/ValueSet/admin-gender|4.0.1")
# → {"url": "http://hl7.org/fhir/ValueSet/admin-gender", "version": "4.0.1"}
```

### Modifiers

```python
# Type modifier
converter.convert("123", modifier="Patient")
# → {"_search.patientId": "123"}

# Identifier (multi-step)
result = converter.convert("http://hospital.org|MRN-123", modifier="identifier")
# → MultiStepQuery (2 steps)

# Text search (PREFIX match)
converter.convert("John Smith", modifier="text")
# → {"display_lower": {"$gte": "john smith", "$lt": "john smithz"}}

# Missing
converter.convert("true", modifier="missing")
# → {"$or": [{"field": {"$exists": false}}, {"field": null}]}
```

---

## 2. URIConverter

### Basic Usage

```python
config = {
    'type': 'uri',
    'fields': [{'field': 'url', 'query_type': 'range'}]  # or 'regex'
}
converter = URIConverter(config)
```

### URI Searches

```python
# Exact match
converter.convert("http://hl7.org/fhir/ValueSet/observation-codes")
# → {"url": "http://hl7.org/fhir/ValueSet/observation-codes"}

# Below (hierarchical children) - Range query
converter.convert("http://hl7.org/fhir/", modifier="below")
# →
{
    "url": {
        "$gte": "http://hl7.org/fhir/",
        "$lt": "http://hl7.org/fhir/\uffff"
    }
}

# Below - Regex fallback
config = {'type': 'uri', 'fields': [{'field': 'url', 'query_type': 'regex'}]}
converter = URIConverter(config)
converter.convert("http://hl7.org/fhir/", modifier="below")
# → {"url": {"$regex": "^http://hl7\\.org/fhir/"}}

# Above (hierarchical parents)
converter.convert("http://example.org/path/to/resource", modifier="above")
# →
{
    "$or": [
        {"url": "http://example.org/path/to"},
        {"url": "http://example.org/path"},
        {"url": "http://example.org"}
    ]
}

# Missing
converter.convert("true", modifier="missing")
# → {"$or": [{"url": {"$exists": false}}, {"url": null}]}
```

### Performance

- **Range query**: ~5ms (requires index on url)
- **Regex query**: Variable (recommend index)

---

## 3. CompositeConverter

### Basic Usage

```python
config = {
    'type': 'composite',
    'components': [
        {
            'name': 'code',
            'type': 'token',
            'fields': [{'field': 'code.coding', 'tokenType': 'systemCode'}]
        },
        {
            'name': 'value',
            'type': 'quantity',
            'fields': [{'field': 'valueQuantity'}]
        }
    ]
}
converter = CompositeConverter(config)
```

### Composite Searches

```python
# Code + Value ($ separator)
converter.convert("http://loinc.org|2093-3$le5")
# →
{
    "$and": [
        {
            "code.coding": {
                "$elemMatch": {
                    "system": "http://loinc.org",
                    "code": "2093-3"
                }
            }
        },
        {
            "valueQuantity.value": {"$lte": 5}
        }
    ]
}

# Three components
converter.convert("value1$value2$value3")
# → {"$and": [<query1>, <query2>, <query3>]}

# Missing
converter.convert("true", modifier="missing")
# → Checks if any component is missing
```

### Component Types

Composite automatically uses correct converter:
- `token` → TokenConverter
- `quantity` → QuantityConverter
- `number` → NumberConverter
- `date` → DateConverter
- `string` → StringConverter
- `reference` → ReferenceConverter
- `uri` → URIConverter

---

## 4. SpecialConverter

All methods are **static** - no instance needed.

### _id

```python
# Single ID
SpecialConverter.convert_id("123")
# → {"_id": "123"}

# Multiple IDs (comma-separated)
SpecialConverter.convert_id("123,456,789")
# → {"_id": {"$in": ["123", "456", "789"]}}
```

### _lastUpdated

```python
# Date with prefix
SpecialConverter.convert_last_updated("2024-01-01", prefix="ge")
# → {"meta.lastUpdated": {"$gte": "2024-01-01T00:00:00Z"}}

SpecialConverter.convert_last_updated("2024-01-01", prefix="le")
# → {"meta.lastUpdated": {"$lte": "2024-01-01T23:59:59.999Z"}}

# Range
SpecialConverter.convert_last_updated("2024-01-01", prefix="eq")
# → {"$and": [{"meta.lastUpdated": {"$gte": ...}}, {"meta.lastUpdated": {"$lte": ...}}]}
```

### _tag

```python
# System|Code
SpecialConverter.convert_tag("http://terminology.org|tag1")
# →
{
    "meta.tag": {
        "$elemMatch": {
            "system": "http://terminology.org",
            "code": "tag1"
        }
    }
}

# Code only
SpecialConverter.convert_tag("tag1")
# → {"meta.tag": {"$elemMatch": {"code": "tag1"}}}

# :not modifier
SpecialConverter.convert_tag("tag1", modifier="not")
# →
{
    "$nor": [
        {"meta.tag": {"$elemMatch": {"code": "tag1"}}}
    ]
}

# :missing
SpecialConverter.convert_tag("true", modifier="missing")
# → {"$or": [{"meta.tag": {"$exists": false}}, {"meta.tag": {"$size": 0}}]}

# :text (display search)
SpecialConverter.convert_tag("Emergency", modifier="text")
# → {"meta.tag.display": {"$regex": "Emergency", "$options": "i"}}

# :above/:below (for hierarchical code systems)
SpecialConverter.convert_tag("parent-code", modifier="below")
# → Searches for parent code and all descendants
```

### _profile

```python
# URL
SpecialConverter.convert_profile("http://hl7.org/fhir/StructureDefinition/Patient")
# → {"meta.profile": "http://hl7.org/fhir/StructureDefinition/Patient"}

# :missing
SpecialConverter.convert_profile("true", modifier="missing")
# → {"$or": [{"meta.profile": {"$exists": false}}, {"meta.profile": {"$size": 0}}]}

# :above/:below (for profile hierarchies)
SpecialConverter.convert_profile("http://hl7.org/fhir/us/core/Patient", modifier="below")
# → Searches for profile and all derived profiles
```

### _security

```python
# System|Code
SpecialConverter.convert_security("http://terminology.org|RESTRICTED")
# →
{
    "meta.security": {
        "$elemMatch": {
            "system": "http://terminology.org",
            "code": "RESTRICTED"
        }
    }
}

# Modifiers: :not, :missing, :text, :above, :below (same as _tag)
```

### _has (Reverse Chaining)

```python
# Syntax: _has=ResourceType:referenceParam:searchParam=value
# Find Patients who have Observations with code 8480-6
result = SpecialConverter.convert_has("Observation:subject:code=8480-6", "Patient")
# → MultiStepQuery
# Step 1: Find Observations with code=8480-6
# Step 2: Extract subject references
# Step 3: Find Patients with those IDs

# Multiple criteria
result = SpecialConverter.convert_has("Observation:subject:code=8480-6&status=final", "Patient")
# Step 1: Find Observations with code=8480-6 AND status=final
```

### _text (Narrative Search)

```python
# Search in text.div
SpecialConverter.convert_text("diabetes")
# → {"$text": {"$search": "diabetes"}}

# Requires MongoDB text index:
# db.Patient.createIndex({"text.div": "text"})
```

### _content (Full Resource Search)

```python
# Search all fields
SpecialConverter.convert_content("blood pressure")
# → {"$text": {"$search": "blood pressure"}}

# Requires MongoDB text index on all fields:
# db.Observation.createIndex({"$**": "text"})
```

---

## 5. ChainingHandler

### Basic Usage

```python
handler = ChainingHandler()
```

### Forward Chaining

Find resources based on properties of referenced resources:

```python
# Find Observations where subject (Patient) has name=Smith
result = handler.parse_chain("subject:Patient.name", "Smith", "Observation")
# → MultiStepQuery
# Step 1: Patient?name=Smith → Extract IDs
# Step 2: Observation?subject:in=<IDs>

# Find DiagnosticReports where performer (Practitioner) has identifier
result = handler.parse_chain(
    "performer:Practitioner.identifier",
    "http://hospital.org|DOC-123",
    "DiagnosticReport"
)
```

### Deep Chaining

Multiple levels of references:

```python
# Find Observations where subject's organization has name=Hospital
result = handler.parse_chain(
    "subject:Patient.organization:Organization.name",
    "Hospital",
    "Observation"
)
# → MultiStepQuery (3 steps)
# Step 1: Organization?name=Hospital → Extract Org IDs
# Step 2: Patient?organization:in=<Org IDs> → Extract Patient IDs
# Step 3: Observation?subject:in=<Patient IDs>
```

### Helper Functions

```python
# Check if parameter is chained
from fhir_search_to_mql.converters.chaining_handler import is_chained_parameter

is_chained_parameter("subject:Patient.name")      # True
is_chained_parameter("name:exact")                # False (modifier, not chain)

# Parse chained parameter
from fhir_search_to_mql.converters.chaining_handler import parse_chained_parameter

result = parse_chained_parameter("subject:Patient.name", "Smith", "Observation")
# → MultiStepQuery
```

### Aggregation Pipeline

Convert to MongoDB aggregation pipeline (alternative to multi-step):

```python
result = handler.parse_chain("subject:Patient.name", "Smith", "Observation")
pipeline = handler.to_aggregation_pipeline(result, "Observation")
# →
[
    {
        "$lookup": {
            "from": "Patient",
            "localField": "_search.subjectId",
            "foreignField": "_id",
            "as": "subject_data"
        }
    },
    {
        "$match": {
            "subject_data.name": "Smith"
        }
    }
]
```

---

## 6. MultiStepQuery

### Creating Multi-Step Queries

```python
from fhir_search_to_mql.converters import MultiStepQuery, QueryStep

# Create query
query = MultiStepQuery(description="Find Observations for Smith")

# Add step
query.add_step(
    resource_type="Patient",
    query={"name": "Smith"},
    extract_field="id",
    description="Find patients named Smith"
)

# Set final query builder
query.set_final_query_builder(
    lambda ids: {"_search.subjectId": {"$in": ids}}
)

# Get execution plan
plan = query.get_execution_plan()
# →
{
    "is_multi_step": True,
    "num_steps": 1,
    "steps": [
        {
            "resource_type": "Patient",
            "query": {"name": "Smith"},
            "extract_field": "id",
            "description": "Find patients named Smith"
        }
    ],
    "has_final_query_builder": True,
    "description": "Find Observations for Smith"
}
```

### Helper Functions

```python
from fhir_search_to_mql.converters.multi_step_query import (
    is_multi_step_query,
    create_simple_multi_step_query
)

# Check if result is multi-step
result = converter.convert(value, modifier)
if is_multi_step_query(result):
    # Execute multi-step query
    plan = result.get_execution_plan()
else:
    # Execute single query
    db.collection.find(result)

# Create simple multi-step query
query = create_simple_multi_step_query(
    target_resource="Patient",
    target_query={"identifier.value": "MRN-123"},
    final_field="_search.subjectId",
    description="Find by patient identifier"
)
```

---

## 🔍 Quick Decision Guide

### When to use each converter?

| Converter | Use When |
|-----------|----------|
| **ReferenceConverter** | Searching by resource reference (subject, patient, performer, etc.) |
| **URIConverter** | Searching URIs with hierarchical relationships (url, system, etc.) |
| **CompositeConverter** | Multiple parameters must match together (code-value pairs, etc.) |
| **SpecialConverter** | Using FHIR special parameters (_id, _tag, _has, etc.) |
| **ChainingHandler** | Searching by properties of referenced resources |

### When do you get MultiStepQuery?

You get `MultiStepQuery` (not a regular dict) from:
1. `ReferenceConverter` with `:identifier` modifier
2. `SpecialConverter.convert_has()` (reverse chaining)
3. `ChainingHandler.parse_chain()` (forward chaining)

**Handling MultiStepQuery**:
```python
result = converter.convert(value, modifier)

if isinstance(result, MultiStepQuery):
    # Execute multi-step
    for step in result.steps:
        # Query: db[step.resource_type].find(step.query)
        # Extract: result[step.extract_field]
        pass
    # Final: db[source].find(result.final_query_builder(extracted_ids))
else:
    # Execute single query
    db.collection.find(result)
```

---

## 📊 Performance Tips

### 1. Required Indexes

```javascript
// Reference fields
db.Observation.createIndex({"_search.patientId": 1})
db.Observation.createIndex({"_search.subjectId": 1})

// URI fields (for :below)
db.ValueSet.createIndex({"url": 1})

// Meta fields
db.Patient.createIndex({"meta.lastUpdated": 1})
db.Patient.createIndex({"meta.tag.system": 1, "meta.tag.code": 1})
db.Patient.createIndex({"meta.profile": 1})

// Text indexes
db.Patient.createIndex({"text.div": "text"})           // For _text
db.Observation.createIndex({"$**": "text"})            // For _content
```

### 2. Query Optimization

```python
# ✅ GOOD: Direct reference
converter.convert("Patient/123")
# → 1ms

# ⚠️ SLOW: Identifier search (2 queries)
converter.convert("system|value", modifier="identifier")
# → 5-10ms

# ⚠️ SLOWER: Chaining (3+ queries)
handler.parse_chain("subject:Patient.organization:Organization.name", "Hospital", "Observation")
# → 10-30ms

# 💡 TIP: Cache common chains
cache_key = f"{param}:{value}"
if cache_key in chain_cache:
    return chain_cache[cache_key]
```

### 3. Avoid

```python
# ❌ BAD: Don't use regex for text search
# Use SpecialConverter.convert_text() with text index instead

# ❌ BAD: Don't chain unnecessarily
# If you already have the reference ID, use it directly

# ❌ BAD: Don't use _content without text index
# Very slow full collection scan
```

---

## 🧪 Testing Examples

```python
import pytest
from fhir_search_to_mql.converters import ReferenceConverter, MultiStepQuery

def test_reference_simple_id():
    config = {'type': 'reference', 'fields': [{'field': '_search.patientId'}]}
    converter = ReferenceConverter(config)
    
    query = converter.convert("123")
    
    assert '_search.patientId' in query
    assert query['_search.patientId'] == "123"

def test_reference_identifier_multi_step():
    config = {'type': 'reference', 'fields': [{'field': '_search.patientId'}]}
    converter = ReferenceConverter(config)
    
    result = converter.convert("system|value", modifier="identifier")
    
    assert isinstance(result, MultiStepQuery)
    assert result.is_multi_step == True
    assert len(result.steps) > 0
```

---

## 📚 Common Patterns

### Pattern 1: Simple Search

```python
# Direct field match
converter = ReferenceConverter(config)
query = converter.convert("Patient/123")
results = db.Observation.find(query)
```

### Pattern 2: Multi-Step Search

```python
# Identifier search
converter = ReferenceConverter(config)
multi_step = converter.convert("system|value", modifier="identifier")

# Execute steps
ids = []
for step in multi_step.steps:
    results = db[step.resource_type].find(step.query)
    ids = [r[step.extract_field] for r in results]

# Final query
final_query = multi_step.final_query_builder(ids)
results = db.Observation.find(final_query)
```

### Pattern 3: Chained Search

```python
# Parse chain
handler = ChainingHandler()
multi_step = handler.parse_chain("subject:Patient.name", "Smith", "Observation")

# Execute (same as Pattern 2)
```

### Pattern 4: Special Parameters

```python
# Parse search string
params = parse_search_string("?_id=123,456&_tag=system|code&_lastUpdated=ge2024-01-01")

# Convert each parameter
queries = []
queries.append(SpecialConverter.convert_id(params['_id']))
queries.append(SpecialConverter.convert_tag(params['_tag']))
queries.append(SpecialConverter.convert_last_updated(params['_lastUpdated']))

# Combine with AND
final_query = {"$and": queries}
results = db.Patient.find(final_query)
```

### Pattern 5: Composite Search

```python
# Code-value search
converter = CompositeConverter(config)
query = converter.convert("http://loinc.org|2093-3$le5")

# Query has $and with both components
results = db.Observation.find(query)
```

---

## 🚨 Error Handling

```python
from fhir_search_to_mql.core.exceptions import ConversionError

try:
    query = converter.convert(value, modifier)
except ConversionError as e:
    # Invalid parameter format, modifier, or value
    return {"error": str(e), "status": 400}

# Check for multi-step
if isinstance(query, MultiStepQuery):
    # Execute multi-step
    try:
        results = execute_multi_step(query)
    except Exception as e:
        return {"error": "Multi-step execution failed", "status": 500}
else:
    # Execute single query
    results = db.collection.find(query)
```

---

## ✅ Quick Checklist

Before deploying:

- [ ] All reference fields indexed
- [ ] URI fields indexed for :below searches
- [ ] Text indexes created for _text/_content
- [ ] Meta fields indexed (_lastUpdated, _tag, _profile)
- [ ] Multi-step query execution implemented
- [ ] Error handling for ConversionError
- [ ] Cache strategy for common chains
- [ ] Performance testing completed
- [ ] Integration tests passing

---

## 📖 See Also

- **Full Documentation**: `PHASE_5_COMPLETE.md`
- **Test Suite**: `tests/test_advanced_converters.py`
- **Phase 4 Reference**: `PHASE_4_QUICK_REFERENCE.md`
- **FHIR Spec**: https://hl7.org/fhir/search.html

