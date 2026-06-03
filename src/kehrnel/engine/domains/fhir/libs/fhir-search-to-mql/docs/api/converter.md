# FHIR Search Converter API Reference

## Overview

The `FHIRSearchConverter` class converts FHIR search queries into MongoDB Query Language (MQL), supporting all FHIR search parameter types, modifiers, and prefixes.

## Class: FHIRSearchConverter

### Constructor

```python
from fhir_search_to_mql import FHIRSearchConverter

converter = FHIRSearchConverter(
    config_dir: Optional[str] = None,
    compartment_definitions_dir: Optional[str] = None
)
```

**Parameters:**
- `config_dir` (Optional[str]): Path to resource configuration directory. Defaults to `configs/`.
- `compartment_definitions_dir` (Optional[str]): Path to compartment definitions. Defaults to `src/fhir_search_to_mql/compartments/definitions/`.

**Example:**
```python
# Use default directories
converter = FHIRSearchConverter()

# Use custom directories
converter = FHIRSearchConverter(
    config_dir="/path/to/configs",
    compartment_definitions_dir="/path/to/compartments"
)
```

---

## Main Methods

### convert()

Convert a FHIR search query string to MongoDB query.

```python
def convert(
    resource_type: str,
    query_string: str,
    fhir_version: Optional[str] = None
) -> Dict[str, Any]:
```

**Parameters:**
- `resource_type` (str): FHIR resource type (e.g., 'Patient', 'Observation')
- `query_string` (str): FHIR search query string
- `fhir_version` (Optional[str]): FHIR version ('R4', 'R5', 'R6')

**Returns:**
- `Dict[str, Any]`: Dictionary containing:
  - `mql_query`: MongoDB query dict
  - `metadata`: Query metadata (parameter count, etc.)

**Raises:**
- `ConversionError`: If query cannot be converted
- `ConfigurationError`: If resource configuration not found

**Example:**
```python
result = converter.convert(
    'Patient',
    'name=Smith&gender=male&birthdate=ge1980-01-01'
)

print(result['mql_query'])
# {
#     "$and": [
#         {"$or": [
#             {"_search.familyName_lower": {"$gte": "smith", "$lt": "smith\uffff"}},
#             {"_search.givenNames_lower": {"$gte": "smith", "$lt": "smith\uffff"}}
#         ]},
#         {"gender": "male"},
#         {"birthDate": {"$gte": "1980-01-01"}}
#     ]
# }

# Execute against MongoDB
from pymongo import MongoClient
client = MongoClient()
db = client['fhir_synthetic']
patients = db.Patient.find(result['mql_query'])
```

### convert_with_compartment()

Convert a compartment-based FHIR search query.

```python
def convert_with_compartment(
    compartment_type: str,
    compartment_id: str,
    resource_type: str,
    query_string: Optional[str] = None,
    fhir_version: Optional[str] = None
) -> Dict[str, Any]:
```

**Parameters:**
- `compartment_type` (str): Compartment type ('Patient', 'Encounter', 'Practitioner', 'Device', 'RelatedPerson')
- `compartment_id` (str): ID of the compartment instance
- `resource_type` (str): Resource type to query
- `query_string` (Optional[str]): Additional FHIR search parameters
- `fhir_version` (Optional[str]): FHIR version

**Returns:**
- `Dict[str, Any]`: Dictionary with `mql_query` and `metadata`

**Example:**
```python
# Get all Observations for a specific Patient
result = converter.convert_with_compartment(
    compartment_type='Patient',
    compartment_id='patient-123',
    resource_type='Observation',
    query_string='code=8480-6&status=final'
)

print(result['mql_query'])
# {
#     "$and": [
#         {"$or": [
#             {"_search.subjectId": "patient-123"},
#             {"_search.performerId": "patient-123"}
#         ]},
#         {"_search.codeSystem_code": "http://loinc.org|8480-6"},
#         {"status": "final"}
#     ]
# }

observations = db.Observation.find(result['mql_query'])
```

---

## Utility Methods

### list_compartments()

Get list of available compartment types.

```python
def list_compartments() -> List[str]:
```

**Returns:**
- `List[str]`: List of compartment types

**Example:**
```python
compartments = converter.list_compartments()
print(compartments)
# ['Patient', 'Encounter', 'Practitioner', 'Device', 'RelatedPerson']
```

### get_compartment_resources()

Get list of resources in a specific compartment.

```python
def get_compartment_resources(compartment_type: str) -> List[str]:
```

**Parameters:**
- `compartment_type` (str): Compartment type

**Returns:**
- `List[str]`: List of resource type codes

**Example:**
```python
resources = converter.get_compartment_resources('Patient')
print(f"Patient compartment has {len(resources)} resource types")
print(resources[:5])
# ['Account', 'AdverseEvent', 'AllergyIntolerance', 'Appointment', ...]
```

### get_compartment_info()

Get detailed information about a compartment.

```python
def get_compartment_info(compartment_type: str) -> Optional[Dict[str, Any]]:
```

**Parameters:**
- `compartment_type` (str): Compartment type

**Returns:**
- `Optional[Dict[str, Any]]`: Compartment information or None if not found

**Example:**
```python
info = converter.get_compartment_info('Patient')
print(info)
# {
#     'id': 'patient',
#     'code': 'Patient',
#     'name': 'Patient',
#     'status': 'active',
#     'description': 'The set of resources associated with a particular patient',
#     'resource_count': 60,
#     'resources': ['Observation', 'Condition', ...]
# }
```

---

## Search Parameter Types

### String Parameters

String parameters support prefix searches, exact matches, and contains searches.

**Modifiers:**
- `:exact` - Exact match (case-sensitive)
- `:contains` - Substring match (case-insensitive)

**Examples:**
```python
# Prefix search (default)
result = converter.convert('Patient', 'name=Smith')
# Matches: Smith, Smithson, Smithers, etc.

# Exact match
result = converter.convert('Patient', 'name:exact=Smith')
# Matches: Only "Smith"

# Contains
result = converter.convert('Patient', 'name:contains=mit')
# Matches: Smith, Smitty, submitted, etc.
```

### Token Parameters

Token parameters match exact values, often in `system|value` format.

**Examples:**
```python
# Value only
result = converter.convert('Patient', 'identifier=MRN12345')

# System and value
result = converter.convert('Patient', 'identifier=http://hospital.example.org|MRN12345')

# Code with system
result = converter.convert('Observation', 'code=http://loinc.org|8480-6')

# Simple token
result = converter.convert('Patient', 'gender=male')
```

### Reference Parameters

Reference parameters link resources together.

**Examples:**
```python
# Reference with type
result = converter.convert('Observation', 'subject=Patient/patient-123')

# Reference ID only
result = converter.convert('Observation', 'patient=patient-123')

# Multiple references
result = converter.convert('Observation', 'subject=Patient/pat-1,Patient/pat-2')
```

### Date Parameters

Date parameters support range queries with prefixes.

**Prefixes:**
- `eq` (default) - Equal (with implicit range)
- `gt` - Greater than
- `lt` - Less than
- `ge` - Greater than or equal
- `le` - Less than or equal
- `sa` - Starts after
- `eb` - Ends before
- `ap` - Approximately

**Examples:**
```python
# Equal (implicit range)
result = converter.convert('Patient', 'birthdate=1980-05-15')

# Date range
result = converter.convert('Patient', 
    'birthdate=ge1980-01-01&birthdate=le2000-12-31')

# Year only
result = converter.convert('Patient', 'birthdate=1980')

# Year-month
result = converter.convert('Patient', 'birthdate=1980-05')

# Greater than
result = converter.convert('Observation', 'date=gt2024-01-01')
```

### Number Parameters

Number parameters support numeric comparisons with optional units.

**Examples:**
```python
# Exact (with implicit range)
result = converter.convert('Observation', 'value-quantity=120')

# Greater than
result = converter.convert('Observation', 'value-quantity=gt100')

# Less than
result = converter.convert('Observation', 'value-quantity=lt200')

# With unit
result = converter.convert('Observation', 'value-quantity=120|mmHg')
```

### Quantity Parameters

Quantity parameters combine value, comparator, system, and code.

**Examples:**
```python
# Value only
result = converter.convert('Observation', 'value-quantity=120')

# Value with unit
result = converter.convert('Observation', 
    'value-quantity=120|http://unitsofmeasure.org|mmHg')

# Range query
result = converter.convert('Observation', 'value-quantity=ge100')
```

---

## Query Builders

The library uses modular converters for each parameter type:

| Converter | Search Type | Description |
|-----------|-------------|-------------|
| `StringParameterConverter` | string | String searches (name, address, etc.) |
| `TokenParameterConverter` | token | Token matches (identifier, code, gender) |
| `ReferenceParameterConverter` | reference | Reference links (subject, patient, etc.) |
| `DateParameterConverter` | date | Date and datetime ranges |
| `NumberParameterConverter` | number | Numeric comparisons |
| `QuantityParameterConverter` | quantity | Quantity with units |
| `URIParameterConverter` | uri | URI exact matches |
| `CompositeParameterConverter` | composite | Multi-component parameters |
| `SpecialParameterConverter` | special | Special FHIR parameters (_id, _lastUpdated, etc.) |

---

## Advanced Features

### Chaining

Chain parameters to search related resources.

```python
# Find Observations where subject's name is "Smith"
result = converter.convert('Observation', 'subject:Patient.name=Smith')

# Find Encounters for patients in a specific city
result = converter.convert('Encounter', 'patient:Patient.address-city=Springfield')
```

### Reverse Chaining

Search resources that reference specific values.

```python
# Find Patients who have Observations with specific code
result = converter.convert('Patient', '_has:Observation:patient:code=8480-6')
```

### Composite Parameters

Search multiple components together.

```python
# Find Observations with specific code AND value
result = converter.convert('Observation', 
    'component-code-value-quantity=http://loinc.org|8480-6$120')
```

### OR Logic

Use comma-separated values for OR logic.

```python
# Find patients with gender male OR female
result = converter.convert('Patient', 'gender=male,female')

# Find observations with multiple codes
result = converter.convert('Observation', 
    'code=8480-6,8462-4')
```

### AND Logic

Multiple parameters are automatically combined with AND.

```python
# Find male patients born after 1980 named Smith
result = converter.convert('Patient',
    'name=Smith&gender=male&birthdate=ge1980-01-01')
```

---

## Examples

### Example 1: Simple Patient Search

```python
from fhir_search_to_mql import FHIRSearchConverter
from pymongo import MongoClient

converter = FHIRSearchConverter(config_dir="configs")
client = MongoClient()
db = client['fhir_synthetic']

# Convert query
result = converter.convert('Patient', 'name=Smith&gender=male')

# Execute
patients = list(db.Patient.find(result['mql_query']))
print(f"Found {len(patients)} patients")
```

### Example 2: Complex Observation Query

```python
# Find vital signs observations for a patient in date range
result = converter.convert('Observation',
    'patient=patient-123&'
    'category=vital-signs&'
    'code=8480-6&'
    'date=ge2024-01-01&'
    'date=le2024-12-31&'
    'status=final'
)

observations = list(db.Observation.find(result['mql_query']))
```

### Example 3: Compartment Query

```python
# Get all resources for a patient
result = converter.convert_with_compartment(
    'Patient',
    'patient-123',
    'Observation'
)

observations = list(db.Observation.find(result['mql_query']))
```

### Example 4: Pagination

```python
# Get patients with pagination
result = converter.convert('Patient', 'name=Smith')

# MongoDB pagination
page_size = 20
page = 0

patients = list(db.Patient.find(result['mql_query'])
    .skip(page * page_size)
    .limit(page_size))
```

### Example 5: Sorting

```python
# Convert query and add sorting
result = converter.convert('Patient', 'name=Smith')

# MongoDB sorting
patients = list(db.Patient.find(result['mql_query'])
    .sort([
        ('_search.familyName_lower', 1),
        ('_search.givenNames_lower', 1)
    ]))
```

---

## Error Handling

### ConversionError

Raised when query cannot be converted.

```python
from fhir_search_to_mql.core.exceptions import ConversionError

try:
    result = converter.convert('Patient', 'invalid:query')
except ConversionError as e:
    print(f"Conversion error: {e}")
```

### ConfigurationError

Raised when resource configuration is missing.

```python
from fhir_search_to_mql.core.exceptions import ConfigurationError

try:
    result = converter.convert('UnknownResource', 'name=test')
except ConfigurationError as e:
    print(f"Configuration error: {e}")
```

---

## Performance Optimization

### Query Optimization Tips

1. **Use Indexes**: Ensure MongoDB indexes match your search patterns
2. **Limit Fields**: Use projection to return only needed fields
3. **Pagination**: Always paginate large result sets
4. **Covered Queries**: Use indexed fields in both filter and projection
5. **Compound Indexes**: Create compound indexes for common multi-parameter queries

```python
# Create indexes for common queries
db.Patient.create_index([
    ('_search.familyName_lower', 1),
    ('gender', 1),
    ('birthDate', 1)
])

# Use projection
result = converter.convert('Patient', 'name=Smith')
patients = list(db.Patient.find(
    result['mql_query'],
    {'name': 1, 'gender': 1, 'birthDate': 1}
))
```

---

## Best Practices

1. **Always Use Configurations**: Define search parameters in YAML configs
2. **Leverage Compartments**: Use compartments for multi-tenant security
3. **Batch Operations**: Process multiple queries together when possible
4. **Cache Configurations**: Configurations are cached automatically
5. **Monitor Performance**: Use MongoDB explain() to analyze query performance
6. **Use Lowercase Fields**: Always search lowercase fields for case-insensitive searches
7. **Index Strategy**: Create indexes based on actual query patterns

---

## Related Documentation

- [Resource Denormalizer API](denormalizer.md)
- [Configuration Guide](../guides/configuration.md)
- [Getting Started Guide](../guides/getting_started.md)
- [Performance Tuning Guide](../guides/performance_tuning.md)
- [Compartment Support](../guides/compartments.md)
