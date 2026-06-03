# Integration Examples

This file demonstrates real-world integration scenarios with FHIR servers, web APIs, and applications.

## Overview

These examples show how to integrate FHIR Search to MQL into production systems.

---

## Example 1: REST API Integration (Flask)

### Complete Flask API

```python
from flask import Flask, request, jsonify
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from pymongo import MongoClient
import os

app = Flask(__name__)

# Initialize
CONFIG_DIR = os.getenv('FHIR_CONFIG_DIR', 'configs')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.getenv('FHIR_DB_NAME', 'fhir_synthetic')

converter = FHIRSearchConverter(config_dir=CONFIG_DIR)
denormalizer = ResourceDenormalizer(config_dir=CONFIG_DIR)
client = MongoClient(MONGO_URI)
db = client[DB_NAME]


@app.route('/fhir/<resource_type>', methods=['GET'])
def search_resources(resource_type):
    """FHIR search endpoint."""
    try:
        # Get query parameters
        query_string = '&'.join([f"{k}={v}" for k, v in request.args.items() 
                                 if not k.startswith('_')])
        
        # Pagination parameters
        count = int(request.args.get('_count', 20))
        offset = int(request.args.get('_offset', 0))
        
        # Convert query
        result = converter.convert(resource_type, query_string)
        
        # Execute query with pagination
        cursor = db[resource_type].find(result['mql_query'])
        total = cursor.count()
        resources = list(cursor.skip(offset).limit(count))
        
        # Build FHIR Bundle response
        bundle = {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": total,
            "link": [
                {
                    "relation": "self",
                    "url": request.url
                }
            ],
            "entry": [
                {
                    "fullUrl": f"{request.url_root}fhir/{resource_type}/{r['id']}",
                    "resource": r
                } for r in resources
            ]
        }
        
        return jsonify(bundle), 200
    
    except Exception as e:
        return jsonify({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "processing",
                "diagnostics": str(e)
            }]
        }), 400


@app.route('/fhir/<resource_type>/<resource_id>', methods=['GET'])
def read_resource(resource_type, resource_id):
    """FHIR read endpoint."""
    resource = db[resource_type].find_one({'id': resource_id}, {'_search': 0, '_id': 0})
    
    if resource:
        return jsonify(resource), 200
    else:
        return jsonify({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "not-found",
                "diagnostics": f"{resource_type}/{resource_id} not found"
            }]
        }), 404


@app.route('/fhir/<resource_type>', methods=['POST'])
def create_resource(resource_type):
    """FHIR create endpoint."""
    try:
        resource = request.json
        
        # Validate resource type
        if resource.get('resourceType') != resource_type:
            raise ValueError("Resource type mismatch")
        
        # Denormalize
        denormalized = denormalizer.denormalize(resource)
        
        # Insert
        result = db[resource_type].insert_one(denormalized)
        
        return jsonify(resource), 201, {
            'Location': f"{request.url_root}fhir/{resource_type}/{resource['id']}"
        }
    
    except Exception as e:
        return jsonify({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "invalid",
                "diagnostics": str(e)
            }]
        }), 400


@app.route('/fhir/<compartment_type>/<compartment_id>/<resource_type>', methods=['GET'])
def compartment_search(compartment_type, compartment_id, resource_type):
    """FHIR compartment search endpoint."""
    try:
        query_string = '&'.join([f"{k}={v}" for k, v in request.args.items() 
                                 if not k.startswith('_')])
        
        # Convert with compartment
        result = converter.convert_with_compartment(
            compartment_type, compartment_id, resource_type, query_string)
        
        # Execute query
        resources = list(db[resource_type].find(result['mql_query']))
        
        # Build bundle
        bundle = {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(resources),
            "entry": [
                {
                    "fullUrl": f"{request.url_root}fhir/{resource_type}/{r['id']}",
                    "resource": r
                } for r in resources
            ]
        }
        
        return jsonify(bundle), 200
    
    except Exception as e:
        return jsonify({
            "resourceType": "OperationOutcome",
            "issue": [{
                "severity": "error",
                "code": "processing",
                "diagnostics": str(e)
            }]
        }), 400


if __name__ == '__main__':
    app.run(debug=True, port=5000)
```

### API Usage

```bash
# Search patients
curl "http://localhost:5000/fhir/Patient?name=Smith&gender=male"

# Read patient
curl "http://localhost:5000/fhir/Patient/patient-123"

# Create patient
curl -X POST "http://localhost:5000/fhir/Patient" \
  -H "Content-Type: application/json" \
  -d '{"resourceType": "Patient", "id": "new-patient", "name": [{"family": "Doe"}]}'

# Compartment search
curl "http://localhost:5000/fhir/Patient/patient-123/Observation?code=8480-6"
```

---

## Example 2: FastAPI Integration

```python
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List
from fhir_search_to_mql import FHIRSearchConverter, ResourceDenormalizer
from pymongo import MongoClient

app = FastAPI(title="FHIR API")

# Initialize
converter = FHIRSearchConverter(config_dir="configs")
denormalizer = ResourceDenormalizer(config_dir="configs")
client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']


class FHIRResource(BaseModel):
    resourceType: str
    id: Optional[str]


@app.get("/fhir/{resource_type}")
async def search(
    resource_type: str,
    name: Optional[str] = None,
    gender: Optional[str] = None,
    birthdate: Optional[str] = None,
    _count: int = Query(20, ge=1, le=100),
    _offset: int = Query(0, ge=0)
):
    """Search resources."""
    # Build query string
    params = []
    if name:
        params.append(f"name={name}")
    if gender:
        params.append(f"gender={gender}")
    if birthdate:
        params.append(f"birthdate={birthdate}")
    
    query_string = '&'.join(params)
    
    # Convert and execute
    result = converter.convert(resource_type, query_string)
    cursor = db[resource_type].find(result['mql_query'], {'_search': 0, '_id': 0})
    
    total = cursor.count()
    resources = list(cursor.skip(_offset).limit(_count))
    
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total,
        "entry": [{"resource": r} for r in resources]
    }


@app.get("/fhir/{resource_type}/{resource_id}")
async def read(resource_type: str, resource_id: str):
    """Read resource."""
    resource = db[resource_type].find_one(
        {'id': resource_id}, {'_search': 0, '_id': 0})
    
    if not resource:
        raise HTTPException(status_code=404, detail="Resource not found")
    
    return resource


@app.post("/fhir/{resource_type}")
async def create(resource_type: str, resource: dict):
    """Create resource."""
    # Denormalize
    denormalized = denormalizer.denormalize(resource)
    
    # Insert
    db[resource_type].insert_one(denormalized)
    
    return resource
```

---

## Example 3: Bulk Data Import

```python
import json
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from fhir_search_to_mql import ResourceDenormalizer
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BulkImporter:
    def __init__(self, config_dir="configs", mongo_uri="mongodb://localhost:27017/", db_name="fhir_synthetic"):
        self.denormalizer = ResourceDenormalizer(config_dir=config_dir)
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
    
    def import_file(self, file_path, batch_size=100):
        """Import resources from NDJSON file."""
        file_path = Path(file_path)
        resource_type = file_path.stem  # Assume filename is resource type
        
        logger.info(f"Importing {file_path}...")
        
        batch = []
        total = 0
        
        with open(file_path, 'r') as f:
            for line in f:
                resource = json.loads(line)
                batch.append(resource)
                
                if len(batch) >= batch_size:
                    self._insert_batch(batch, resource_type)
                    total += len(batch)
                    logger.info(f"Imported {total} {resource_type} resources")
                    batch = []
            
            # Insert remaining
            if batch:
                self._insert_batch(batch, resource_type)
                total += len(batch)
        
        logger.info(f"Completed: {total} {resource_type} resources imported")
        return total
    
    def _insert_batch(self, batch, resource_type):
        """Insert batch of resources."""
        # Denormalize batch
        denormalized = self.denormalizer.denormalize_batch(batch)
        
        # Insert
        self.db[resource_type].insert_many(denormalized, ordered=False)
    
    def import_directory(self, directory, max_workers=4):
        """Import all NDJSON files in directory (parallel)."""
        directory = Path(directory)
        files = list(directory.glob('*.ndjson'))
        
        logger.info(f"Found {len(files)} files to import")
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(self.import_file, f): f for f in files
            }
            
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                try:
                    count = future.result()
                    results[file.name] = count
                except Exception as e:
                    logger.error(f"Error importing {file}: {e}")
                    results[file.name] = 0
        
        return results
    
    def create_indexes(self, resource_types):
        """Create indexes for imported resources."""
        import yaml
        from pathlib import Path
        
        config_dir = Path('configs')
        
        for resource_type in resource_types:
            config_file = config_dir / f"{resource_type}.yaml"
            
            if not config_file.exists():
                logger.warning(f"Config not found for {resource_type}")
                continue
            
            with open(config_file) as f:
                config = yaml.safe_load(f)
            
            # Create indexes from config
            for param_name, param_config in config.get('search_parameters', {}).items():
                fields = param_config.get('fields', [])
                
                if isinstance(fields, dict):
                    field_list = []
                    for modifier_fields in fields.values():
                        field_list.extend(modifier_fields)
                else:
                    field_list = fields
                
                for field_config in field_list:
                    if field_config.get('indexed', False):
                        field_name = field_config['field']
                        self.db[resource_type].create_index(field_name)
                        logger.info(f"Created index: {resource_type}.{field_name}")


# Usage
if __name__ == '__main__':
    importer = BulkImporter()
    
    # Import directory
    results = importer.import_directory('/path/to/fhir/data', max_workers=4)
    
    print("\nImport Results:")
    for file, count in results.items():
        print(f"  {file}: {count} resources")
    
    # Create indexes
    resource_types = [Path(f).stem for f in results.keys()]
    importer.create_indexes(resource_types)
```

---

## Example 4: Data Synchronization

```python
import time
import logging
from datetime import datetime
from fhir_search_to_mql import ResourceDenormalizer
from pymongo import MongoClient
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FHIRSynchronizer:
    def __init__(self, source_url, mongo_uri, db_name="fhir_synthetic"):
        self.source_url = source_url.rstrip('/')
        self.denormalizer = ResourceDenormalizer(config_dir="configs")
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
    
    def sync_resource_type(self, resource_type, since=None):
        """Sync resources from FHIR server."""
        logger.info(f"Syncing {resource_type}...")
        
        # Build query
        url = f"{self.source_url}/{resource_type}"
        params = {'_count': 100}
        
        if since:
            params['_lastUpdated'] = f'ge{since}'
        
        page = 0
        total_synced = 0
        
        while url:
            page += 1
            logger.info(f"Fetching page {page}...")
            
            # Fetch page
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            bundle = response.json()
            
            # Process entries
            if 'entry' in bundle:
                resources = [entry['resource'] for entry in bundle['entry']]
                
                # Denormalize
                denormalized = self.denormalizer.denormalize_batch(resources)
                
                # Upsert
                for resource in denormalized:
                    self.db[resource_type].replace_one(
                        {'id': resource['id']},
                        resource,
                        upsert=True
                    )
                
                total_synced += len(resources)
                logger.info(f"Synced {len(resources)} resources (total: {total_synced})")
            
            # Get next page URL
            url = None
            if 'link' in bundle:
                for link in bundle['link']:
                    if link['relation'] == 'next':
                        url = link['url']
                        params = {}  # URL already contains params
                        break
        
        logger.info(f"Completed: {total_synced} {resource_type} resources synced")
        return total_synced
    
    def continuous_sync(self, resource_types, interval=300):
        """Continuously sync resources."""
        last_sync = {}
        
        while True:
            for resource_type in resource_types:
                try:
                    since = last_sync.get(resource_type)
                    count = self.sync_resource_type(resource_type, since)
                    last_sync[resource_type] = datetime.now().isoformat()
                    logger.info(f"Synced {count} {resource_type} resources")
                except Exception as e:
                    logger.error(f"Error syncing {resource_type}: {e}")
            
            logger.info(f"Waiting {interval} seconds...")
            time.sleep(interval)


# Usage
if __name__ == '__main__':
    syncer = FHIRSynchronizer(
        source_url='http://hapi.fhir.org/baseR4',
        mongo_uri='mongodb://localhost:27017/'
    )
    
    # One-time sync
    syncer.sync_resource_type('Patient')
    
    # Continuous sync
    # syncer.continuous_sync(['Patient', 'Observation', 'Condition'], interval=300)
```

---

## Example 5: GraphQL Integration

```python
import graphene
from graphene import ObjectType, String, Int, List, Field
from fhir_search_to_mql import FHIRSearchConverter
from pymongo import MongoClient

# Initialize
converter = FHIRSearchConverter(config_dir="configs")
client = MongoClient('mongodb://localhost:27017/')
db = client['fhir_synthetic']


class Patient(ObjectType):
    id = String()
    family_name = String()
    given_name = String()
    gender = String()
    birth_date = String()
    
    def resolve_family_name(self, info):
        if self.get('name'):
            return self['name'][0].get('family')
        return None
    
    def resolve_given_name(self, info):
        if self.get('name'):
            given = self['name'][0].get('given', [])
            return given[0] if given else None
        return None


class Observation(ObjectType):
    id = String()
    status = String()
    code = String()
    value = String()
    effective_date_time = String()
    
    def resolve_code(self, info):
        if self.get('code', {}).get('coding'):
            return self['code']['coding'][0].get('display')
        return None
    
    def resolve_value(self, info):
        if 'valueQuantity' in self:
            vq = self['valueQuantity']
            return f"{vq.get('value')} {vq.get('unit')}"
        return None


class Query(ObjectType):
    patients = List(
        Patient,
        name=String(),
        gender=String(),
        birth_date=String(),
        limit=Int(default_value=20)
    )
    
    observations = List(
        Observation,
        patient_id=String(required=True),
        code=String(),
        status=String(),
        limit=Int(default_value=20)
    )
    
    def resolve_patients(self, info, name=None, gender=None, birth_date=None, limit=20):
        params = []
        if name:
            params.append(f"name={name}")
        if gender:
            params.append(f"gender={gender}")
        if birth_date:
            params.append(f"birthdate={birth_date}")
        
        query_string = '&'.join(params)
        result = converter.convert('Patient', query_string)
        
        patients = list(db.Patient.find(
            result['mql_query'],
            {'_search': 0, '_id': 0}
        ).limit(limit))
        
        return patients
    
    def resolve_observations(self, info, patient_id, code=None, status=None, limit=20):
        params = [f"patient={patient_id}"]
        if code:
            params.append(f"code={code}")
        if status:
            params.append(f"status={status}")
        
        query_string = '&'.join(params)
        result = converter.convert('Observation', query_string)
        
        observations = list(db.Observation.find(
            result['mql_query'],
            {'_search': 0, '_id': 0}
        ).limit(limit))
        
        return observations


schema = graphene.Schema(query=Query)

# Example query:
# {
#   patients(name: "Smith", gender: "male") {
#     id
#     familyName
#     givenName
#     gender
#     birthDate
#   }
# }
```

---

## Related Documentation

- [Basic Query Examples](basic_queries.md)
- [Complex Query Examples](complex_queries.md)
- [Getting Started Guide](../guides/getting_started.md)
- [Performance Tuning](../guides/performance_tuning.md)
