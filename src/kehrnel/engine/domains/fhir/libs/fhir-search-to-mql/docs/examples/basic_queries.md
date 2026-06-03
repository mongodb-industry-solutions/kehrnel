# Basic Query Examples

This file contains simple, common query patterns for getting started with FHIR Search to MQL.

## Setup

```python
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from pymongo import MongoClient

# Initialize
converter = FHIRSearchConverter(config_dir="configs")
denormalizer = ResourceDenormalizer(config_dir="configs")
client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']
```

---

## Patient Queries

### Example 1: Search by Name

```python
# Search for patients with family name starting with "Smith"
result = converter.convert('Patient', 'name=Smith')

patients = list(db.Patient.find(result['mql_query']))
print(f"Found {len(patients)} patients")

# MongoDB Query Generated:
# {
#     '$or': [
#         {'_search.familyName_lower': {'$gte': 'smith', '$lt': 'smith\uffff'}},
#         {'_search.givenNames_lower': {'$gte': 'smith', '$lt': 'smith\uffff'}}
#     ]
# }
```

### Example 2: Search by Gender

```python
# Search for male patients
result = converter.convert('Patient', 'gender=male')

patients = list(db.Patient.find(result['mql_query']))

# MongoDB Query:
# {'gender': 'male'}
```

### Example 3: Search by Birthdate

```python
# Patients born after 1980-01-01
result = converter.convert('Patient', 'birthdate=ge1980-01-01')

patients = list(db.Patient.find(result['mql_query']))

# MongoDB Query:
# {'birthDate': {'$gte': '1980-01-01'}}
```

### Example 4: Combined Search

```python
# Male patients named Smith born after 1980
result = converter.convert('Patient',
    'name=Smith&gender=male&birthdate=ge1980-01-01')

patients = list(db.Patient.find(result['mql_query']))

# MongoDB Query:
# {
#     '$and': [
#         {'$or': [
#             {'_search.familyName_lower': {'$gte': 'smith', '$lt': 'smith\uffff'}},
#             {'_search.givenNames_lower': {'$gte': 'smith', '$lt': 'smith\uffff'}}
#         ]},
#         {'gender': 'male'},
#         {'birthDate': {'$gte': '1980-01-01'}}
#     ]
# }
```

### Example 5: Search by Identifier

```python
# Search by identifier value
result = converter.convert('Patient', 'identifier=MRN12345')

patients = list(db.Patient.find(result['mql_query']))

# With system
result = converter.convert('Patient',
    'identifier=http://hospital.example.org|MRN12345')

# MongoDB Query:
# {'_search.identifierSystem_value': 'http://hospital.example.org|MRN12345'}
```

### Example 6: Search by Address

```python
# Search by city
result = converter.convert('Patient', 'address-city=Springfield')

patients = list(db.Patient.find(result['mql_query']))

# MongoDB Query:
# {'_search.addressCity_lower': {'$gte': 'springfield', '$lt': 'springfield\uffff'}}

# Search by state and city
result = converter.convert('Patient',
    'address-state=IL&address-city=Springfield')

patients = list(db.Patient.find(result['mql_query']))
```

---

## Observation Queries

### Example 7: Search by Code

```python
# Search by LOINC code (Systolic BP)
result = converter.convert('Observation',
    'code=http://loinc.org|8480-6')

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query:
# {'_search.codeSystem_code': 'http://loinc.org|8480-6'}
```

### Example 8: Search by Patient

```python
# All observations for a patient
result = converter.convert('Observation', 'patient=patient-123')

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query:
# {'_search.patientId': 'patient-123'}
```

### Example 9: Search by Status

```python
# Final observations only
result = converter.convert('Observation', 'status=final')

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query:
# {'status': 'final'}
```

### Example 10: Search by Date Range

```python
# Observations from January 2024
result = converter.convert('Observation',
    'date=ge2024-01-01&date=le2024-01-31')

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query:
# {
#     '$and': [
#         {'effectiveDateTime': {'$gte': '2024-01-01'}},
#         {'effectiveDateTime': {'$lte': '2024-01-31'}}
#     ]
# }
```

---

## Appointment Queries

### Example 11: Search by Status

```python
# Search for booked appointments
result = converter.convert('Appointment', 'status=booked')

appointments = list(db.Appointment.find(result['mql_query']))
```

### Example 12: Search by Patient

```python
# All appointments for a patient
result = converter.convert('Appointment', 'patient=patient-123')

appointments = list(db.Appointment.find(result['mql_query']))
```

### Example 13: Search by Date

```python
# Appointments after today
from datetime import datetime

today = datetime.now().strftime('%Y-%m-%d')
result = converter.convert('Appointment', f'date=ge{today}')

appointments = list(db.Appointment.find(result['mql_query']))
```

---

## Condition Queries

### Example 14: Search by Code

```python
# Search for diabetes conditions (SNOMED CT code)
result = converter.convert('Condition',
    'code=http://snomed.info/sct|73211009')

conditions = list(db.Condition.find(result['mql_query']))
```

### Example 15: Search by Category

```python
# Search by condition category
result = converter.convert('Condition',
    'category=encounter-diagnosis')

conditions = list(db.Condition.find(result['mql_query']))
```

### Example 16: Active Conditions for Patient

```python
# Active conditions for a specific patient
result = converter.convert('Condition',
    'patient=patient-123&clinical-status=active')

conditions = list(db.Condition.find(result['mql_query']))
```

---

## Pagination Examples

### Example 17: Basic Pagination

```python
def paginated_query(resource_type, query_string, page=0, page_size=20):
    \"\"\"Execute paginated query.\"\"\"
    result = converter.convert(resource_type, query_string)
    
    # Get total count
    total = db[resource_type].count_documents(result['mql_query'])
    
    # Get page
    cursor = db[resource_type].find(result['mql_query'])
    cursor = cursor.skip(page * page_size).limit(page_size)
    
    return {
        'data': list(cursor),
        'total': total,
        'page': page,
        'page_size': page_size,
        'pages': (total + page_size - 1) // page_size
    }

# Get first page
page1 = paginated_query('Patient', 'name=Smith', page=0, page_size=20)
print(f"Page 1: {len(page1['data'])} of {page1['total']} total results")

# Get second page
page2 = paginated_query('Patient', 'name=Smith', page=1, page_size=20)
```

### Example 18: Sorted Pagination

```python
# Sort by family name
result = converter.convert('Patient', 'name=Smith')

cursor = db.Patient.find(result['mql_query'])
cursor = cursor.sort('_search.familyName_lower', 1)  # 1 = ascending
cursor = cursor.skip(0).limit(20)

patients = list(cursor)
```

---

## Projection Examples

### Example 19: Limit Returned Fields

```python
# Only return specific fields
result = converter.convert('Patient', 'name=Smith')

patients = list(db.Patient.find(
    result['mql_query'],
    {'id': 1, 'name': 1, 'gender': 1, 'birthDate': 1, '_id': 0}
))

# Only essential fields returned, faster query
```

### Example 20: Exclude Large Fields

```python
# Exclude photo field (can be large)
result = converter.convert('Patient', 'name=Smith')

patients = list(db.Patient.find(
    result['mql_query'],
    {'photo': 0}  # Exclude photo
))
```

---

## Count Examples

### Example 21: Count Results

```python
# Count without retrieving documents
result = converter.convert('Patient', 'gender=male')

count = db.Patient.count_documents(result['mql_query'])
print(f"Found {count} male patients")
```

### Example 22: Count by Status

```python
# Count observations by status
statuses = ['final', 'preliminary', 'amended']

for status in statuses:
    result = converter.convert('Observation', f'status={status}')
    count = db.Observation.count_documents(result['mql_query'])
    print(f"{status}: {count}")
```

---

## String Modifier Examples

### Example 23: Exact Match

```python
# Exact match (case-sensitive)
result = converter.convert('Patient', 'name:exact=Smith')

patients = list(db.Patient.find(result['mql_query']))

# MongoDB Query:
# {
#     '$or': [
#         {'_search.familyName': 'Smith'},
#         {'_search.givenNames': 'Smith'}
#     ]
# }
```

### Example 24: Contains Search

```python
# Contains search (substring match)
result = converter.convert('Patient', 'name:contains=mit')

patients = list(db.Patient.find(result['mql_query']))

# MongoDB Query (uses text index):
# {'$text': {'$search': 'mit'}}
```

---

## Complete Workflow Example

### Example 25: End-to-End

```python
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from pymongo import MongoClient

# Setup
converter = FHIRSearchConverter(config_dir=\"configs\")
denormalizer = ResourceDenormalizer(config_dir=\"configs\")
client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']

# Sample patient
patient = {
    \"resourceType\": \"Patient\",
    \"id\": \"patient-123\",
    \"name\": [{
        \"family\": \"Smith\",
        \"given\": [\"John\", \"Michael\"]
    }],
    \"gender\": \"male\",
    \"birthDate\": \"1980-05-15\"
}

# 1. Denormalize
denormalized = denormalizer.denormalize(patient)

# 2. Insert
db.Patient.insert_one(denormalized)

# 3. Search
result = converter.convert('Patient', 'name=Smith&gender=male')

# 4. Retrieve
patients = list(db.Patient.find(result['mql_query']))

# 5. Display
for patient in patients:
    name = patient['name'][0]
    print(f\"{name['family']}, {name['given'][0]} - {patient['gender']} - {patient['birthDate']}\")

# Cleanup
db.Patient.delete_one({'id': 'patient-123'})
```

---

## Tips

1. **Use lowercase fields**: String searches automatically use lowercase fields for performance
2. **Index all search parameters**: Create indexes for fields marked `indexed: true`
3. **Paginate large result sets**: Always use `.limit()` for large queries
4. **Use projection**: Only request fields you need
5. **Count before retrieving**: Use `count_documents()` to check result size
6. **Combine parameters**: Use `&` to combine multiple search criteria
7. **Test queries**: Use `explain()` to analyze query performance

```python
# Explain query
result = converter.convert('Patient', 'name=Smith')
explain = db.Patient.find(result['mql_query']).explain()
print(f"Execution time: {explain['executionStats']['executionTimeMillis']}ms")
print(f"Index used: {explain['executionStats']['executionStages']['inputStage']['indexName']}")
```

---

## Next Steps

- [Complex Query Examples](complex_queries.md) - Advanced query patterns
- [Integration Examples](integration.md) - Real-world integration scenarios
- [Custom Resource Examples](custom_resources.md) - Add your own resources
