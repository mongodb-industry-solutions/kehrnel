# Complex Query Examples

This file demonstrates advanced query patterns including chaining, reverse chaining, composite parameters, and OR logic.

## Setup

```python
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from pymongo import MongoClient

converter = FHIRSearchConverter(config_dir="configs")
denormalizer = ResourceDenormalizer(config_dir="configs")
client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']
```

---

## Reference Chaining

### Example 1: Simple Chain

```python
# Find observations where the patient's name is "Smith"
# Observation?patient.name=Smith

result = converter.convert('Observation', 'patient.name=Smith')

# This generates a two-step query:
# 1. Find patients with name=Smith
# 2. Find observations referencing those patients

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query:
# {'_search.patientId': {'$in': ['patient-1', 'patient-2', ...]}}
```

### Example 2: Multi-Level Chain

```python
# Find observations where the patient's organization is "General Hospital"
# Observation?patient.organization.name=General Hospital

result = converter.convert('Observation',
    'patient.organization.name=General Hospital')

# Three-step chain:
# 1. Find organizations with name="General Hospital"
# 2. Find patients with organization in those IDs
# 3. Find observations for those patients

observations = list(db.Observation.find(result['mql_query']))
```

### Example 3: Chain with Additional Criteria

```python
# Find observations for patients named Smith with status=final
# Observation?patient.name=Smith&status=final

result = converter.convert('Observation',
    'patient.name=Smith&status=final')

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query combines chain result with status filter:
# {
#     '$and': [
#         {'_search.patientId': {'$in': ['patient-1', 'patient-2', ...]}},
#         {'status': 'final'}
#     ]
# }
```

---

## Reverse Chaining

### Example 4: Reverse Chain (_has)

```python
# Find patients who have observations with code 8480-6 (Systolic BP)
# Patient?_has:Observation:patient:code=8480-6

result = converter.convert('Patient',
    '_has:Observation:patient:code=8480-6')

# Process:
# 1. Find observations with code=8480-6
# 2. Extract patient IDs from observations
# 3. Return patients with those IDs

patients = list(db.Patient.find(result['mql_query']))

# MongoDB Query:
# {'id': {'$in': ['patient-1', 'patient-2', ...]}}
```

### Example 5: Complex Reverse Chain

```python
# Find patients with final observations in the last month
from datetime import datetime, timedelta

last_month = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

result = converter.convert('Patient',
    f'_has:Observation:patient:status=final&'
    f'_has:Observation:patient:date=ge{last_month}')

patients = list(db.Patient.find(result['mql_query']))
```

---

## Composite Parameters

### Example 6: Quantity Composite

```python
# Find observations with value between 120-140 mmHg
# Observation?value-quantity=120||mmHg to 140||mmHg

# Note: Exact syntax depends on your implementation
result = converter.convert('Observation',
    'code=8480-6&value-quantity=ge120&value-quantity=le140')

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query:
# {
#     '$and': [
#         {'_search.codeSystem_code': 'http://loinc.org|8480-6'},
#         {'valueQuantity.value': {'$gte': 120}},
#         {'valueQuantity.value': {'$lte': 140}}
#     ]
# }
```

### Example 7: Token and Date Composite

```python
# Find conditions diagnosed in 2024 with specific code
result = converter.convert('Condition',
    'code=73211009&'
    'onset-date=ge2024-01-01&onset-date=le2024-12-31')

conditions = list(db.Condition.find(result['mql_query']))
```

---

## OR Logic

### Example 8: Multiple Values (OR)

```python
# Find patients with gender male OR female (using comma)
# Patient?gender=male,female

result = converter.convert('Patient', 'gender=male,female')

patients = list(db.Patient.find(result['mql_query']))

# MongoDB Query:
# {'gender': {'$in': ['male', 'female']}}
```

### Example 9: Multiple Codes

```python
# Find observations with multiple LOINC codes
# Observation?code=8480-6,8462-4  (Systolic and Diastolic BP)

result = converter.convert('Observation',
    'code=http://loinc.org|8480-6,http://loinc.org|8462-4')

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query:
# {
#     '_search.codeSystem_code': {
#         '$in': [
#             'http://loinc.org|8480-6',
#             'http://loinc.org|8462-4'
#         ]
#     }
# }
```

### Example 10: Complex OR with AND

```python
# Male patients named Smith OR female patients named Jones
# Patient?name=Smith&gender=male,name=Jones&gender=female

# Use multiple queries and combine results
query1 = converter.convert('Patient', 'name=Smith&gender=male')
query2 = converter.convert('Patient', 'name=Jones&gender=female')

# Combine with $or
combined_query = {
    '$or': [
        query1['mql_query'],
        query2['mql_query']
    ]
}

patients = list(db.Patient.find(combined_query))
```

---

## Compartment Queries

### Example 11: Patient Compartment

```python
# All observations for a specific patient
result = converter.convert_with_compartment(
    compartment_type='Patient',
    compartment_id='patient-123',
    resource_type='Observation',
    query_string='status=final'
)

observations = list(db.Observation.find(result['mql_query']))

# MongoDB Query:
# {
#     '$and': [
#         {'_search.patientId': 'patient-123'},
#         {'status': 'final'}
#     ]
# }
```

### Example 12: Encounter Compartment

```python
# All resources related to an encounter
result = converter.convert_with_compartment(
    compartment_type='Encounter',
    compartment_id='encounter-456',
    resource_type='Observation',
    query_string='code=8480-6'
)

observations = list(db.Observation.find(result['mql_query']))
```

### Example 13: Multiple Resource Types in Compartment

```python
# Get all resources for a patient across multiple types
patient_id = 'patient-123'

# Observations
obs_result = converter.convert_with_compartment(
    'Patient', patient_id, 'Observation', 'status=final')
observations = list(db.Observation.find(obs_result['mql_query']))

# Conditions
cond_result = converter.convert_with_compartment(
    'Patient', patient_id, 'Condition', 'clinical-status=active')
conditions = list(db.Condition.find(cond_result['mql_query']))

# Appointments
appt_result = converter.convert_with_compartment(
    'Patient', patient_id, 'Appointment', 'status=booked')
appointments = list(db.Appointment.find(appt_result['mql_query']))

print(f"Patient {patient_id}:")
print(f"  {len(observations)} observations")
print(f"  {len(conditions)} conditions")
print(f"  {len(appointments)} appointments")
```

---

## Date Range Queries

### Example 14: Year Range

```python
# Patients born in the 1980s
result = converter.convert('Patient',
    'birthdate=ge1980-01-01&birthdate=lt1990-01-01')

patients = list(db.Patient.find(result['mql_query']))
```

### Example 15: Relative Date Range

```python
# Observations in the last 7 days
from datetime import datetime, timedelta

end_date = datetime.now()
start_date = end_date - timedelta(days=7)

result = converter.convert('Observation',
    f'date=ge{start_date.strftime("%Y-%m-%d")}&'
    f'date=le{end_date.strftime("%Y-%m-%d")}')

observations = list(db.Observation.find(result['mql_query']))
```

### Example 16: Period Overlap

```python
# Encounters overlapping with a specific date range
# (searches both Encounter.period.start and Encounter.period.end)

result = converter.convert('Encounter',
    'date=ge2024-01-01&date=le2024-01-31')

encounters = list(db.Encounter.find(result['mql_query']))
```

---

## Quantity Queries

### Example 17: Numeric Range

```python
# Observations with value between 120-140
result = converter.convert('Observation',
    'code=8480-6&'
    'value-quantity=ge120&value-quantity=le140')

observations = list(db.Observation.find(result['mql_query']))
```

### Example 18: Unit Conversion

```python
# Observations with temperature > 38°C
# (Assumes temperature in Celsius)

result = converter.convert('Observation',
    'code=8310-5&'  # Body temperature
    'value-quantity=gt38')

observations = list(db.Observation.find(result['mql_query']))
```

---

## Aggregation Examples

### Example 19: Count by Status

```python
# Count observations by status using aggregation
result = converter.convert('Observation', 'patient=patient-123')

pipeline = [
    {'$match': result['mql_query']},
    {'$group': {
        '_id': '$status',
        'count': {'$sum': 1}
    }},
    {'$sort': {'count': -1}}
]

status_counts = list(db.Observation.aggregate(pipeline))

for item in status_counts:
    print(f"{item['_id']}: {item['count']}")
```

### Example 20: Average Values

```python
# Average systolic blood pressure for a patient
result = converter.convert('Observation',
    'patient=patient-123&code=8480-6')

pipeline = [
    {'$match': result['mql_query']},
    {'$group': {
        '_id': None,
        'average': {'$avg': '$valueQuantity.value'},
        'min': {'$min': '$valueQuantity.value'},
        'max': {'$max': '$valueQuantity.value'},
        'count': {'$sum': 1}
    }}
]

stats = list(db.Observation.aggregate(pipeline))

if stats:
    print(f"Average: {stats[0]['average']:.1f} mmHg")
    print(f"Range: {stats[0]['min']}-{stats[0]['max']} mmHg")
    print(f"Count: {stats[0]['count']}")
```

### Example 21: Time-Series Grouping

```python
# Group observations by month
result = converter.convert('Observation',
    'patient=patient-123&code=8480-6')

pipeline = [
    {'$match': result['mql_query']},
    {'$project': {
        'year': {'$year': {'$toDate': '$effectiveDateTime'}},
        'month': {'$month': {'$toDate': '$effectiveDateTime'}},
        'value': '$valueQuantity.value'
    }},
    {'$group': {
        '_id': {
            'year': '$year',
            'month': '$month'
        },
        'average': {'$avg': '$value'},
        'count': {'$sum': 1}
    }},
    {'$sort': {'_id.year': 1, '_id.month': 1}}
]

monthly_data = list(db.Observation.aggregate(pipeline))

for item in monthly_data:
    year = item['_id']['year']
    month = item['_id']['month']
    avg = item['average']
    count = item['count']
    print(f"{year}-{month:02d}: {avg:.1f} mmHg ({count} readings)")
```

---

## Text Search Examples

### Example 22: Full-Text Search

```python
# Requires text index on name fields
# db.Patient.create_index([
#     ('_search.familyName', 'text'),
#     ('_search.givenNames', 'text')
# ])

# Full-text search
result = converter.convert('Patient', 'name:contains=smith john')

patients = list(db.Patient.find(result['mql_query']))
```

---

## Complex Multi-Step Queries

### Example 23: Find Related Resources

```python
def find_patient_care_team(patient_id):
    \"\"\"Find all practitioners involved in a patient's care.\"\"\"
    
    # Step 1: Find encounters for patient
    enc_result = converter.convert_with_compartment(
        'Patient', patient_id, 'Encounter', '')
    encounters = list(db.Encounter.find(enc_result['mql_query']))
    
    # Step 2: Extract practitioner IDs from encounters
    practitioner_ids = set()
    for encounter in encounters:
        for participant in encounter.get('participant', []):
            individual = participant.get('individual', {})
            ref = individual.get('reference', '')
            if ref.startswith('Practitioner/'):
                practitioner_ids.add(ref.split('/')[1])
    
    # Step 3: Get practitioner details
    practitioners = list(db.Practitioner.find({'id': {'$in': list(practitioner_ids)}}))
    
    return {
        'patient_id': patient_id,
        'encounters': len(encounters),
        'practitioners': practitioners
    }

# Execute
care_team = find_patient_care_team('patient-123')
print(f"Patient has {care_team['encounters']} encounters")
print(f"Seen by {len(care_team['practitioners'])} practitioners")
```

### Example 24: Clinical Summary

```python
def generate_clinical_summary(patient_id):
    \"\"\"Generate comprehensive clinical summary for a patient.\"\"\"
    
    # Demographics
    patient = db.Patient.find_one({'id': patient_id})
    
    # Active conditions
    cond_result = converter.convert_with_compartment(
        'Patient', patient_id, 'Condition', 'clinical-status=active')
    conditions = list(db.Condition.find(cond_result['mql_query']))
    
    # Recent observations (last 30 days)
    from datetime import datetime, timedelta
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    obs_result = converter.convert_with_compartment(
        'Patient', patient_id, 'Observation', f'date=ge{start_date}')
    observations = list(db.Observation.find(obs_result['mql_query']))
    
    # Upcoming appointments
    today = datetime.now().strftime('%Y-%m-%d')
    appt_result = converter.convert_with_compartment(
        'Patient', patient_id, 'Appointment', f'date=ge{today}&status=booked')
    appointments = list(db.Appointment.find(appt_result['mql_query']))
    
    return {
        'patient': patient,
        'conditions': conditions,
        'recent_observations': observations,
        'upcoming_appointments': appointments
    }

# Execute
summary = generate_clinical_summary('patient-123')

print(f"Patient: {summary['patient']['name'][0]['family']}")
print(f"Active conditions: {len(summary['conditions'])}")
print(f"Recent observations: {len(summary['recent_observations'])}")
print(f"Upcoming appointments: {len(summary['upcoming_appointments'])}")
```

### Example 25: Cohort Analysis

```python
def analyze_diabetic_cohort():
    \"\"\"Analyze patients with diabetes.\"\"\"
    
    # Find patients with diabetes diagnosis (SNOMED CT: 73211009)
    cond_result = converter.convert('Condition',
        'code=http://snomed.info/sct|73211009&clinical-status=active')
    conditions = list(db.Condition.find(cond_result['mql_query']))
    
    # Extract patient IDs
    patient_ids = list(set(c['subject']['reference'].split('/')[1]
                          for c in conditions if 'subject' in c))
    
    # Get patient demographics
    patients = list(db.Patient.find({'id': {'$in': patient_ids}}))
    
    # Analyze demographics
    gender_counts = {}
    age_groups = {'<40': 0, '40-60': 0, '>60': 0}
    
    from datetime import datetime
    current_year = datetime.now().year
    
    for patient in patients:
        # Gender
        gender = patient.get('gender', 'unknown')
        gender_counts[gender] = gender_counts.get(gender, 0) + 1
        
        # Age
        birthdate = patient.get('birthDate', '')
        if birthdate:
            birth_year = int(birthdate.split('-')[0])
            age = current_year - birth_year
            
            if age < 40:
                age_groups['<40'] += 1
            elif age < 60:
                age_groups['40-60'] += 1
            else:
                age_groups['>60'] += 1
    
    return {
        'total_patients': len(patients),
        'gender_distribution': gender_counts,
        'age_distribution': age_groups
    }

# Execute
cohort = analyze_diabetic_cohort()

print(f"Diabetic cohort: {cohort['total_patients']} patients")
print(f"Gender: {cohort['gender_distribution']}")
print(f"Age groups: {cohort['age_distribution']}")
```

---

## Performance Optimization Examples

### Example 26: Query with Explain

```python
# Analyze query performance
result = converter.convert('Patient', 'name=Smith&gender=male')

explain = db.Patient.find(result['mql_query']).explain('executionStats')

print(f"Execution time: {explain['executionStats']['executionTimeMillis']}ms")
print(f"Documents examined: {explain['executionStats']['totalDocsExamined']}")
print(f"Documents returned: {explain['executionStats']['nReturned']}")
print(f"Index used: {explain['executionStats']['executionStages'].get('inputStage', {}).get('indexName', 'None')}")
```

### Example 27: Cached Queries

```python
from functools import lru_cache

class CachedQueryExecutor:
    def __init__(self, converter, db):
        self.converter = converter
        self.db = db
    
    @lru_cache(maxsize=1000)
    def execute_query(self, resource_type, query_string):
        \"\"\"Execute query with caching.\"\"\"
        result = self.converter.convert(resource_type, query_string)
        return tuple(self.db[resource_type].find(result['mql_query']).limit(100))
    
    def clear_cache(self):
        self.execute_query.cache_clear()

# Use cached executor
executor = CachedQueryExecutor(converter, db)

# First call: converts and executes
patients1 = executor.execute_query('Patient', 'name=Smith')

# Second call: returns from cache (much faster)
patients2 = executor.execute_query('Patient', 'name=Smith')
```

---

## Related Documentation

- [Basic Query Examples](basic_queries.md)
- [Integration Examples](integration.md)
- [Custom Resource Examples](custom_resources.md)
- [Performance Tuning Guide](../guides/performance_tuning.md)
