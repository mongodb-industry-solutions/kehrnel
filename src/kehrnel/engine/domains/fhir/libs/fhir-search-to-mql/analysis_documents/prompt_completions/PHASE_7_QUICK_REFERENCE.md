# Phase 7 Quick Reference: Compartments

Quick reference for developers using Phase 7 compartment features.

---

## 🚀 Quick Start

### Basic Compartment Query

```python
from fhir_search_to_mql import FHIRSearchConverter

converter = FHIRSearchConverter(config_dir="configs")

# Get all Observations for a patient
query = converter.convert_with_compartment(
    compartment_type='Patient',
    compartment_id='pat-123',
    resource_type='Observation'
)

# Execute query
results = db.Observation.find(query)
```

---

## 📦 Components

### FHIRSearchConverter (Compartment Methods)

Main interface for compartment queries.

```python
converter = FHIRSearchConverter(config_dir="configs")
```

**Key Methods**:

| Method | Purpose | Returns |
|--------|---------|---------|
| `convert_with_compartment(type, id, resource, query_string)` | Convert compartment query | MongoDB query dict |
| `list_compartments()` | List available compartments | List[str] |
| `get_compartment_resources(type)` | Get resources in compartment | List[str] |
| `get_compartment_info(type)` | Get compartment information | Dict |

**Examples**:

```python
# Basic compartment query
query = converter.convert_with_compartment(
    'Patient', 'pat-123', 'Observation'
)

# With additional filters
query = converter.convert_with_compartment(
    'Patient', 'pat-123', 'Observation',
    query_string='code=8480-6&status=final'
)

# List compartments
compartments = converter.list_compartments()
# → ['Patient', 'Encounter', 'Practitioner', 'Device', 'RelatedPerson']

# Get resources in compartment
resources = converter.get_compartment_resources('Patient')
# → ['Observation', 'Condition', 'Encounter', ...]

# Get compartment info
info = converter.get_compartment_info('Patient')
# → {'code': 'Patient', 'name': 'Patient', 'resource_count': 60, ...}
```

---

### CompartmentLoader

Load and validate compartment definitions.

```python
from fhir_search_to_mql.compartments import CompartmentLoader

loader = CompartmentLoader()
loader.load_all()
```

**Methods**:

```python
# Load all compartments
compartments = loader.load_all()

# Get specific compartment
patient_comp = loader.get_compartment('Patient')

# Check if resource is in compartment
is_in = loader.is_resource_in_compartment('Patient', 'Observation')

# Get linking parameters
params = loader.get_linking_parameters('Patient', 'Observation')
# → ['subject', 'performer']

# Get resource entry
entry = loader.get_resource_entry('Patient', 'Observation')
# → ResourceEntry(code='Observation', params=['subject', 'performer'])
```

---

### CompartmentResolver

Resolve compartment queries to MongoDB fragments.

```python
from fhir_search_to_mql.compartments import CompartmentResolver

resolver = CompartmentResolver()
```

**Methods**:

```python
# Resolve compartment query
query = resolver.resolve(
    compartment_type='Patient',
    compartment_id='pat-123',
    resource_type='Observation',
    config=observation_config
)
# → {"$or": [{"_search.patientId": "pat-123"}, {"_search.performerId": "pat-123"}]}

# Combine with parameters
compartment_query = {"$or": [...]}
parameter_queries = [{"code": "8480-6"}]
final_query = resolver.combine_with_parameters(
    compartment_query,
    parameter_queries
)
# → {"$and": [{"$or": [...]}, {"code": "8480-6"}]}

# Validate before resolving
is_valid, error = resolver.validate_compartment_query(
    'Patient', 'pat-123', 'Observation'
)
if not is_valid:
    print(f"Error: {error}")

# Get compartment resources
resources = resolver.get_compartment_resources('Patient')

# Get compartment info
info = resolver.get_compartment_info('Patient')
```

---

## 🎯 Common Use Cases

### 1. Patient Compartment Query

```python
# Get all observations for a patient
query = converter.convert_with_compartment(
    compartment_type='Patient',
    compartment_id='patient-123',
    resource_type='Observation'
)

# Result:
{
    "$or": [
        {"_search.patientId": "patient-123"},
        {"_search.performerId": "patient-123"}
    ]
}
```

### 2. Patient Compartment with Filters

```python
# Get blood pressure readings for a patient
query = converter.convert_with_compartment(
    compartment_type='Patient',
    compartment_id='patient-123',
    resource_type='Observation',
    query_string='code=8480-6&date=ge2024-01-01'
)

# Result:
{
    "$and": [
        {
            "$or": [
                {"_search.patientId": "patient-123"},
                {"_search.performerId": "patient-123"}
            ]
        },
        {"_search.codeSystem_code": "8480-6"},
        {"_search.date": {"$gte": datetime(2024, 1, 1)}}
    ]
}
```

### 3. Encounter Compartment

```python
# Get all observations from an encounter
query = converter.convert_with_compartment(
    compartment_type='Encounter',
    compartment_id='encounter-456',
    resource_type='Observation',
    query_string='category=vital-signs'
)

# Result:
{
    "$and": [
        {"_search.encounterId": "encounter-456"},
        {"_search.categorySystem_code": "vital-signs"}
    ]
}
```

### 4. Practitioner Compartment

```python
# Get all procedures performed by a practitioner
query = converter.convert_with_compartment(
    compartment_type='Practitioner',
    compartment_id='practitioner-789',
    resource_type='Procedure',
    query_string='status=completed&date=ge2024-01-01'
)

# Result:
{
    "$and": [
        {"_search.performerId": "practitioner-789"},
        {"_search.status": "completed"},
        {"_search.date": {"$gte": datetime(2024, 1, 1)}}
    ]
}
```

### 5. Device Compartment

```python
# Get all observations from a device
query = converter.convert_with_compartment(
    compartment_type='Device',
    compartment_id='device-001',
    resource_type='Observation',
    query_string='code=heart-rate'
)

# Result:
{
    "$and": [
        {
            "$or": [
                {"_search.deviceId": "device-001"},
                {"_search.subjectId": "device-001"}
            ]
        },
        {"_search.codeSystem_code": "heart-rate"}
    ]
}
```

### 6. Multi-Resource Queries

```python
# Get all resources in Patient compartment
patient_resources = converter.get_compartment_resources('Patient')

for resource_type in patient_resources:
    query = converter.convert_with_compartment(
        'Patient', 'patient-123', resource_type
    )
    results = db[resource_type].find(query)
    print(f"{resource_type}: {results.count()} documents")
```

### 7. Validation Before Query

```python
# Check if query is valid
from fhir_search_to_mql.compartments import CompartmentResolver

resolver = CompartmentResolver()
is_valid, error = resolver.validate_compartment_query(
    compartment_type='Patient',
    compartment_id='patient-123',
    resource_type='Observation'
)

if is_valid:
    query = converter.convert_with_compartment(
        'Patient', 'patient-123', 'Observation'
    )
else:
    raise ValueError(f"Invalid compartment query: {error}")
```

### 8. Multi-Tenant Security

```python
# Ensure users only see their own data
def get_patient_observations(patient_id, filters=None):
    """Get observations for a patient with optional filters."""
    query = converter.convert_with_compartment(
        compartment_type='Patient',
        compartment_id=patient_id,
        resource_type='Observation',
        query_string=filters
    )
    
    return db.Observation.find(query)

# Usage
observations = get_patient_observations('patient-123', 'code=8480-6')
```

### 9. Compartment Introspection

```python
# Explore available compartments
compartments = converter.list_compartments()
print(f"Available compartments: {', '.join(compartments)}")

for comp_type in compartments:
    info = converter.get_compartment_info(comp_type)
    resources = converter.get_compartment_resources(comp_type)
    
    print(f"\n{comp_type} Compartment:")
    print(f"  Description: {info['description']}")
    print(f"  Resources: {len(resources)}")
    print(f"  Top 5: {', '.join(resources[:5])}")
```

---

## 🎓 Best Practices

### 1. Always Validate Input

```python
# Before resolving
is_valid, error = resolver.validate_compartment_query(
    compartment_type, compartment_id, resource_type
)

if not is_valid:
    raise ValueError(f"Invalid: {error}")

query = resolver.resolve(...)
```

### 2. Use Type Hints

```python
from typing import Dict, List, Optional

def get_compartment_query(
    compartment_type: str,
    compartment_id: str,
    resource_type: str,
    filters: Optional[str] = None
) -> Dict:
    """Get compartment query with type hints."""
    return converter.convert_with_compartment(
        compartment_type,
        compartment_id,
        resource_type,
        filters
    )
```

### 3. Handle Errors Gracefully

```python
from fhir_search_to_mql.core.exceptions import ConversionError

try:
    query = converter.convert_with_compartment(
        'Patient', 'pat-123', 'Observation'
    )
except ConversionError as e:
    logger.error(f"Compartment query failed: {e}")
    return {"error": str(e)}
```

### 4. Cache Compartment Info

```python
# Cache compartment information
class CompartmentCache:
    def __init__(self, converter):
        self.converter = converter
        self._resources_cache = {}
    
    def get_resources(self, compartment_type):
        if compartment_type not in self._resources_cache:
            self._resources_cache[compartment_type] = \
                self.converter.get_compartment_resources(compartment_type)
        return self._resources_cache[compartment_type]
```

### 5. Combine with Authorization

```python
def get_authorized_query(
    user,
    compartment_type,
    resource_type,
    filters=None
):
    """Generate query with authorization check."""
    # Check user has access to compartment
    if not user.has_access_to(compartment_type, compartment_id):
        raise PermissionError("Access denied")
    
    # Generate compartment query
    return converter.convert_with_compartment(
        compartment_type,
        compartment_id,
        resource_type,
        filters
    )
```

---

## 📊 Compartment Reference

### Available Compartments

| Compartment | Resources | Common Use Cases |
|-------------|-----------|------------------|
| **Patient** | 60+ | Patient records, observations, conditions |
| **Encounter** | 40+ | Visit-specific data, procedures, observations |
| **Practitioner** | 50+ | Provider-authored resources, appointments |
| **Device** | 40+ | Device-generated observations, measurements |
| **RelatedPerson** | 45+ | Family/proxy access to patient data |

### Linking Parameters by Resource

| Resource | Patient | Encounter | Practitioner | Device |
|----------|---------|-----------|--------------|--------|
| **Observation** | subject, performer | encounter | performer | device, subject, performer |
| **Condition** | patient, subject | encounter | participant | participant |
| **Procedure** | patient, subject, performer | encounter | performer | performer |
| **MedicationRequest** | subject | encounter | requester | requester |
| **DiagnosticReport** | subject | encounter | performer | performer |
| **Encounter** | patient, subject | part-of | practitioner, participant | participant |

---

## 🔧 Configuration Requirements

### Resource Configuration

For compartment queries to work, configure linking parameters:

```yaml
resource: Observation
parameters:
  # Patient compartment
  subject:
    type: reference
    fields:
      - field: _search.patientId
        indexed: true
  
  performer:
    type: reference
    fields:
      - field: _search.performerId
        indexed: true
  
  # Encounter compartment
  encounter:
    type: reference
    fields:
      - field: _search.encounterId
        indexed: true
```

**Important**: If a linking parameter is not configured, it will be skipped.

---

## 🧪 Testing

```python
import pytest
from fhir_search_to_mql import FHIRSearchConverter

def test_patient_compartment():
    converter = FHIRSearchConverter(config_dir="configs")
    
    query = converter.convert_with_compartment(
        'Patient', 'pat-123', 'Observation'
    )
    
    # Verify query structure
    assert query is not None
    assert 'pat-123' in str(query)

def test_compartment_validation():
    converter = FHIRSearchConverter(config_dir="configs")
    
    # Valid compartment
    try:
        query = converter.convert_with_compartment(
            'Patient', 'pat-123', 'Observation'
        )
        assert query is not None
    except Exception as e:
        pytest.fail(f"Valid query failed: {e}")
    
    # Invalid compartment type
    with pytest.raises(ConversionError):
        converter.convert_with_compartment(
            'InvalidType', 'id-123', 'Observation'
        )
```

---

## 🎯 Quick Commands

```python
# Basic compartment query
query = converter.convert_with_compartment('Patient', 'pat-123', 'Observation')

# With filters
query = converter.convert_with_compartment(
    'Patient', 'pat-123', 'Observation', 'code=8480-6'
)

# List compartments
compartments = converter.list_compartments()

# Get resources
resources = converter.get_compartment_resources('Patient')

# Get info
info = converter.get_compartment_info('Patient')

# Validate
from fhir_search_to_mql.compartments import CompartmentResolver
resolver = CompartmentResolver()
is_valid, error = resolver.validate_compartment_query(
    'Patient', 'pat-123', 'Observation'
)
```

---

## 🔗 Related Documentation

- [PHASE_7_COMPLETE.md](PHASE_7_COMPLETE.md) - Complete Phase 7 documentation
- [FHIR Compartments Specification](https://www.hl7.org/fhir/compartmentdefinition.html) - Official FHIR R5 spec
- Resource configuration files - See `configs/*.yaml`

---

**Phase 7 Complete** ✅  
All compartment features ready for use!
