# Getting Started with FHIR Search to MQL

This guide will help you get started with the FHIR Search to MQL library.

## Installation

### Prerequisites

- Python 3.9 or higher
- MongoDB 4.0+ (for running queries)
- pip (Python package manager)

### Install from PyPI

```bash
pip install fhir-search-to-mql
```

### Install from Source

```bash
git clone https://github.com/fhir-gen/fhir-search-to-mql.git
cd fhir-search-to-mql

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows:
.\.venv\Scripts\Activate.ps1
# Linux/macOS:
source .venv/bin/activate

# Install in editable mode
pip install -e ".[dev]"
```

---

## Quick Start

### 1. Set Up MongoDB

Ensure MongoDB is running:

```bash
# Start MongoDB (if not already running)
mongod --dbpath /path/to/data

# Or use Docker
docker run -d -p 27017:27017 --name mongodb mongo:latest
```

### 2. Import the Library

```python
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from pymongo import MongoClient
```

### 3. Denormalize a FHIR Resource

```python
# Initialize denormalizer
denormalizer = ResourceDenormalizer(config_dir="configs")

# Sample Patient resource
patient = {
    "resourceType": "Patient",
    "id": "example-patient",
    "name": [
        {
            "use": "official",
            "family": "Smith",
            "given": ["John", "Michael"]
        }
    ],
    "gender": "male",
    "birthDate": "1980-05-15",
    "identifier": [
        {
            "system": "http://hospital.example.org",
            "value": "MRN12345"
        }
    ]
}

# Denormalize (adds _search fields)
denormalized = denormalizer.denormalize(patient)

print("Denormalized patient:")
print(denormalized["_search"])
# Output:
# {
#     'familyName': ['Smith'],
#     'familyName_lower': ['smith'],
#     'givenNames': ['John', 'Michael'],
#     'givenNames_lower': ['john', 'michael'],
#     'identifierValues': ['MRN12345'],
#     'identifierSystem_value': ['http://hospital.example.org|MRN12345']
# }
```

### 4. Store in MongoDB

```python
# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']

# Insert denormalized resource
result = db.Patient.insert_one(denormalized)
print(f"Inserted patient with ID: {result.inserted_id}")
```

### 5. Convert FHIR Search Query

```python
# Initialize converter
converter = FHIRSearchConverter(config_dir="configs")

# Convert FHIR search query to MongoDB query
search_result = converter.convert(
    resource_type='Patient',
    query_string='name=Smith&gender=male&birthdate=ge1980-01-01'
)

print("MongoDB query:")
print(search_result['mql_query'])
# Output:
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

### 6. Execute Query Against MongoDB

```python
# Execute the converted query
patients = list(db.Patient.find(search_result['mql_query']))

print(f"Found {len(patients)} patients matching the criteria")
for patient in patients:
    print(f"- {patient['name'][0]['family']}, {patient['name'][0]['given'][0]}")
```

---

## Common Patterns

### Pattern 1: Search by Name

```python
# Case-insensitive prefix search (default)
result = converter.convert('Patient', 'name=Smith')
patients = list(db.Patient.find(result['mql_query']))

# Exact match
result = converter.convert('Patient', 'name:exact=Smith')
patients = list(db.Patient.find(result['mql_query']))

# Contains search
result = converter.convert('Patient', 'name:contains=mit')
patients = list(db.Patient.find(result['mql_query']))
```

### Pattern 2: Search by Identifier

```python
# Search by identifier value only
result = converter.convert('Patient', 'identifier=MRN12345')
patients = list(db.Patient.find(result['mql_query']))

# Search by system and value
result = converter.convert('Patient', 
    'identifier=http://hospital.example.org|MRN12345')
patients = list(db.Patient.find(result['mql_query']))
```

### Pattern 3: Search by Date Range

```python
# Patients born after 1980
result = converter.convert('Patient', 'birthdate=ge1980-01-01')
patients = list(db.Patient.find(result['mql_query']))

# Patients born in specific range
result = converter.convert('Patient', 
    'birthdate=ge1980-01-01&birthdate=le2000-12-31')
patients = list(db.Patient.find(result['mql_query']))

# Patients born in 1980
result = converter.convert('Patient', 'birthdate=1980')
patients = list(db.Patient.find(result['mql_query']))
```

### Pattern 4: Combined Search

```python
# Multiple criteria with AND logic
result = converter.convert('Patient',
    'name=Smith&'
    'gender=male&'
    'birthdate=ge1980-01-01&'
    'address-city=Springfield'
)

patients = list(db.Patient.find(result['mql_query']))
```

### Pattern 5: Search Observations

```python
# Denormalize and insert observation
observation = {
    "resourceType": "Observation",
    "id": "obs-123",
    "status": "final",
    "code": {
        "coding": [{
            "system": "http://loinc.org",
            "code": "8480-6",
            "display": "Systolic blood pressure"
        }]
    },
    "subject": {
        "reference": "Patient/example-patient"
    },
    "effectiveDateTime": "2024-01-15T10:30:00Z",
    "valueQuantity": {
        "value": 120,
        "unit": "mmHg"
    }
}

denormalized_obs = denormalizer.denormalize(observation)
db.Observation.insert_one(denormalized_obs)

# Search observations
result = converter.convert('Observation',
    'code=http://loinc.org|8480-6&'
    'patient=example-patient&'
    'date=ge2024-01-01&'
    'status=final'
)

observations = list(db.Observation.find(result['mql_query']))
```

### Pattern 6: Compartment Queries

```python
# Get all Observations for a specific Patient
result = converter.convert_with_compartment(
    compartment_type='Patient',
    compartment_id='example-patient',
    resource_type='Observation',
    query_string='code=8480-6&status=final'
)

observations = list(db.Observation.find(result['mql_query']))
print(f"Found {len(observations)} observations for patient")
```

### Pattern 7: Pagination

```python
# Get first page
result = converter.convert('Patient', 'name=Smith')

page_size = 20
page = 0

patients = list(db.Patient.find(result['mql_query'])
    .skip(page * page_size)
    .limit(page_size)
    .sort('_search.familyName_lower', 1))

print(f"Page {page + 1}: {len(patients)} patients")

# Get next page
page = 1
patients = list(db.Patient.find(result['mql_query'])
    .skip(page * page_size)
    .limit(page_size)
    .sort('_search.familyName_lower', 1))
```

### Pattern 8: Batch Processing

```python
# Process multiple resources
patients = [patient1, patient2, patient3, ...]

# Denormalize batch
denormalized_patients = denormalizer.denormalize_batch(patients)

# Insert batch
db.Patient.insert_many(denormalized_patients)

print(f"Inserted {len(denormalized_patients)} patients")
```

---

## Troubleshooting

### Issue 1: Configuration Not Found

**Error:**
```
ConfigurationError: Configuration file not found for Patient
```

**Solution:**
```python
# Check config directory
import os
config_dir = "configs"
print(f"Config directory exists: {os.path.exists(config_dir)}")
print(f"Files in config dir: {os.listdir(config_dir)}")

# Specify correct path
denormalizer = ResourceDenormalizer(config_dir="/path/to/configs")
```

### Issue 2: MongoDB Connection Failed

**Error:**
```
pymongo.errors.ServerSelectionTimeoutError: localhost:27017: [Errno 61] Connection refused
```

**Solution:**
```bash
# Check if MongoDB is running
# Windows:
tasklist | findstr mongod

# Linux/macOS:
ps aux | grep mongod

# Start MongoDB if not running
mongod --dbpath /path/to/data
```

### Issue 3: No Results Returned

**Problem:** Query returns empty results even though data exists.

**Solution:**
```python
# Check if data was denormalized
patient = db.Patient.find_one({"id": "example-patient"})
print("_search" in patient)  # Should be True

# Check query
result = converter.convert('Patient', 'name=Smith')
print(result['mql_query'])

# Test query directly
count = db.Patient.count_documents(result['mql_query'])
print(f"Matching documents: {count}")

# Check indexes
print(db.Patient.list_indexes())
```

### Issue 4: Slow Queries

**Problem:** Queries take too long to execute.

**Solution:**
```python
# Explain query
result = converter.convert('Patient', 'name=Smith')
explain = db.Patient.find(result['mql_query']).explain()
print(explain)

# Create indexes
db.Patient.create_index('_search.familyName_lower')
db.Patient.create_index('_search.givenNames_lower')
db.Patient.create_index('gender')
db.Patient.create_index('birthDate')

# Compound index for common queries
db.Patient.create_index([
    ('_search.familyName_lower', 1),
    ('gender', 1),
    ('birthDate', 1)
])
```

---

## Next Steps

- [Configuration Guide](configuration.md) - Learn how to configure resources
- [API Reference](../api/converter.md) - Complete API documentation
- [Adding Resources](adding_resources.md) - Add support for new resources
- [Performance Tuning](performance_tuning.md) - Optimize for production

---

## Complete Example: End-to-End Workflow

```python
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from pymongo import MongoClient
import json

# 1. Initialize
converter = FHIRSearchConverter(config_dir="configs")
denormalizer = ResourceDenormalizer(config_dir="configs")
client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']

# 2. Load FHIR resources from file
with open('patients.json', 'r') as f:
    patients = json.load(f)

# 3. Denormalize and insert
denormalized = denormalizer.denormalize_batch(patients)
db.Patient.insert_many(denormalized)
print(f"Inserted {len(denormalized)} patients")

# 4. Create indexes
db.Patient.create_index('_search.familyName_lower')
db.Patient.create_index('_search.givenNames_lower')
db.Patient.create_index('gender')
db.Patient.create_index('birthDate')
print("Indexes created")

# 5. Execute searches
queries = [
    'name=Smith',
    'gender=male',
    'birthdate=ge1980-01-01',
    'name=Smith&gender=male',
    'address-city=Springfield'
]

for query_string in queries:
    result = converter.convert('Patient', query_string)
    count = db.Patient.count_documents(result['mql_query'])
    print(f"Query '{query_string}': {count} results")

# 6. Pagination example
result = converter.convert('Patient', 'name=Smith')
total = db.Patient.count_documents(result['mql_query'])
page_size = 10

print(f"\nTotal matching patients: {total}")
print(f"Pages: {(total + page_size - 1) // page_size}")

for page in range(min(3, (total + page_size - 1) // page_size)):
    patients = list(db.Patient.find(result['mql_query'])
        .skip(page * page_size)
        .limit(page_size)
        .sort('_search.familyName_lower', 1))
    
    print(f"\nPage {page + 1}:")
    for patient in patients:
        name = patient['name'][0]
        print(f"  - {name.get('family', '')}, {name.get('given', [''])[0]}")
```

This example demonstrates:
- Loading resources from file
- Batch denormalization
- Index creation
- Multiple search patterns
- Pagination
- Result display

You're now ready to use FHIR Search to MQL in your projects!
