# kehrnel — openEHR **Document‑Model** utilities

> **Disclaimer** `kehrnel` and its companion projects are **learning tools**: they showcase best‑practice modelling and querying of clinical data in an **openEHR Document Model**. They are **not** production‑ready libraries or applications.

`kehrnel` provides a modular set of Python libraries, command-line interfaces (CLIs), and a companion REST API. Together, these tools help model, generate, map, validate, transform, and ingest openEHR compositions — following a document-centric approach. They are designed for rapid prototyping, teaching, and building proof-of-concepts.

It also powers the **MongoDB Healthcare Playground**, a demo stack with MongoDB Atlas Local, FastAPI, AQL dashboards, and semantic search layers:

```
 ┌─────────────┐       REST/CLI        ┌────────────────────────┐
 │  kehrnel    │ <───────────────────▶ │ MongoDB Healthcare Play│
 │ (utilities) │  bidirectional sync  │   ground (demo stack)   │
 └─────────────┘                       └────────────────────────┘
```

---

## Features

| #   | Utility                    | CLI | Python API | REST API | Purpose                                                |
| --- | -------------------------- | --- | ---------- | -------- | ------------------------------------------------------ |
| 0.0 | **kehrnel‑map**           | ✔︎  | ✔︎         | ✔︎       | Map HL7 CDA / FHIR / other formats → openEHR JSON      |
| 0.1 | **kehrnel‑generate**      | ✔︎  | ✔︎         | –        | Generate minimal or random openEHR compositions        |
| 0.2 | **kehrnel‑validate**      | ✔︎  | ✔︎         | ✔︎       | Validate compositions against an openEHR OPT           |
| 0.3 | **kehrnel‑transform**     | ✔︎  | ✔︎         | ✔︎       | Transform: Canonical ⇄ Flattened JSON                  |
| 0.4 | **kehrnel‑ingest**        | ✔︎  | ✔︎         | –        | Ingest JSON into MongoDB Atlas (bulk or single)        |
| 0.5 | **document‑identifier**   | –   | ✔︎         | ✔︎       | Auto-identify document types and assign handlers       |
| 0.6 | **mapping‑studio‑api**    | –   | –          | ✔︎       | REST API for Mapping Studio integration                |
| 0.7 | **openEHR Demo API**      | –   | ✔︎         | ✔︎       | Minimal REST PoC for experimentation                   |

---

## Installation

### 1. Development Mode (Library + CLI + API)

```bash
git clone https://github.com/your-org/kehrnel.git
cd kehrnel
python -m venv .venv && source .venv/bin/activate

# Install with all features
pip install -e .[all]

# Or install specific features
pip install -e .[cli,mongo,api]
```

This will install all required dependencies including Typer (for CLI), PyMongo (for Atlas ingestion), and FastAPI (for the REST API).

### 2. Start the API Server

```bash
# Using the installed command
kehrnel-api

# Or using Python module
python -m api.internal.api_server

# The API will be available at:
# - http://localhost:8000 (endpoints)
# - http://localhost:8000/docs (interactive documentation)
```

---

## Quick Usage

### 2.1 Generate a minimal or random composition

You can generate a valid openEHR composition from a template directly — either empty, minimal, or filled with random values.

```bash
kehrnel-generate \                                                              
  -t samples/templates/T-IGR-TUMOUR-SUMMARY.opt \
  -o samples/out/tomourExample.json \
  --random
```
**Parameters:**

- `-t`, `--template`: path to the .opt template
- `--random`: Generate a full composition with randomized values
- `--minimal`: Generate a minimal valid skeleton (no values)
- `-o`: (optional) Path to write the output instead of printing to stdout

### 2.2 Validate a composition against its template

Ensures the generated composition structure conforms to the constraints of its openEHR OPT template.

```bash
kehrnel-validate \
  -c samples/out/tumourNew.json \
  -t samples/templates/T-IGR-TUMOUR-SUMMARY.opt \
  -v
```

**Parameters:**

- `-c`, `--composition`: Path to the composition JSON file to validate
- `-t`, `--template`: Path to the OPT template file
- `-v`, `--verbose`: *(optional)* Show all validation issues, including warnings and info

You can also pipe the composition through stdin:

```bash
comp=$(kehrnel-generate -t templates/TUMOUR.opt --random)
printf "%s" "$comp" | kehrnel-validate -t templates/TUMOUR.opt -
```

### 2.3 Map a source file into an openEHR Composition

You can convert external source documents (e.g., CDA, HL7v2, custom XML/CSV) into a canonical openEHR composition JSON using a YAML mapping definition.

```bash
kehrnel-map \
  -m samples/mappings/tumour_mapping.yaml \
  -s samples/in/fiche_tumour.xml \
  -t samples/templates/T-IGR-TUMOUR-SUMMARY.opt \
  -o samples/out/tumourExample.json \
  --trace
```

**Parameters:**

- `-m`, `--mapping`: Path to the YAML mapping file (defines source-to-openEHR path rules)
- `-s`, `--source`: Path to the input source file (e.g., XML, CSV, FHIR JSON)
- `-t`, `--template`: Path to the OPT template (optional but improves path validation)
- `-o`, `--output`: Output file for the resulting composition JSON
- `--trace`: *(optional)* Shows detailed mapping trace and intermediate resolution steps

### 2.4 Transform canonical JSON to flattened and back

```bash
# Flatten a canonical JSON file
kehrnel-transform canonical.json --flatten -o flat.json

# Expand a flattened JSON file back to canonical structure
kehrnel-transform flat.json --expand -o canonical.json
```

**Parameters:**

- Input file: Either canonical or flattened JSON
- `--flatten`: Convert canonical → flattened representation
- `--expand`: Convert flattened → canonical representation
- `-o`, `--output`: Path to save the result

---

## API Usage

### Document Identification API

The system can automatically identify document types and suggest appropriate handlers:

```python
from mapper.document_identifier import DocumentIdentifier

identifier = DocumentIdentifier()
result = identifier.identify_document("path/to/document.xml")

print(result)
# {
#   "documentType": "pmsi_cda",
#   "handler": "xml",
#   "confidence": 0.95,
#   "sampleData": {...},
#   "structure": {...}
# }
```

### REST API Endpoints

#### Identify Document Type
```bash
curl -X POST "http://localhost:8000/api/internal/identify-document" \
  -F "document=@sample.xml"
```

#### List/Save Mappings
```bash
# Get all mappings
curl "http://localhost:8000/api/internal/mappings"

# Save a mapping
curl -X POST "http://localhost:8000/api/internal/mappings" \
  -H "Content-Type: application/json" \
  -d '{
    "documentType": "pmsi_cda",
    "targetTemplate": "template_id",
    "content": "{ ... mapping json ... }",
    "version": "1.0"
  }'
```

#### Transform Documents
```bash
curl -X POST "http://localhost:8000/api/internal/transform" \
  -F "document=@input.xml" \
  -F "mappingId=mapping_id" \
  -F "templateId=template_id"
```

#### Validate Composition
```bash
curl -X POST "http://localhost:8000/api/internal/validate-composition" \
  -H "Content-Type: application/json" \
  -d '{
    "composition": { ... },
    "templateId": "template_id"
  }'
```

---

## Supported Document Types

The document identifier can automatically recognize:

### XML/CDA Documents
- **PMSI CDA**: French hospital activity records
- **Fiche Tumeur**: Tumor registry documents
- **SIMBAD**: Medication administration reports
- **Generic CDA**: Any HL7 CDA R2 document

### CSV Documents
- **Biology Results**: Lab test results with standard headers
- **Generic Lab Results**: Various laboratory formats

### HL7v2 Messages
- **ADT**: Admission, Discharge, Transfer messages
- **ORU**: Observation results
- **ORM**: Order messages

### Custom Formats
You can add custom document patterns:

```python
from mapper.document_identifier import DocumentPattern

pattern = DocumentPattern(
    name="custom_format",
    handler="xml",
    required_elements=["CustomRoot", "RequiredElement"],
    optional_elements=["OptionalElement"],
    xpath_patterns=["//CustomRoot[@version='1.0']"]
)

identifier.add_pattern(pattern)
```

---

## Project Structure

```
kehrnel/
├── pyproject.toml            # Project configuration and dependencies
├── README.md                 # You are here!
├── LICENSE                   # License information
├── samples/                  # Example files and test data
│   ├── in/                   # Input sample documents
│   │   ├── fiche_tumour.xml  # Tumor record CDA example
│   │   └── pmsi.xml          # PMSI CDA example
│   ├── out/                  # Generated output examples
│   │   ├── tumour*.json      # Various tumor compositions
│   │   └── pmsi*.json        # PMSI compositions
│   ├── mappings/             # YAML mapping definitions
│   │   ├── tumour_mapping.yaml
│   │   └── pmsi-openehr-complete-mapping.yaml
│   └── templates/            # openEHR OPT templates
│       ├── T-IGR-TUMOUR-SUMMARY.opt
│       └── T-IGR-PMSI-EXTRACT.opt
└── src/                      # Source code
    ├── cli/                  # Command-line interfaces (Typer-based)
    │   ├── __init__.py
    │   ├── generate.py       # kehrnel-generate command
    │   ├── validate.py       # kehrnel-validate command
    │   ├── map.py           # kehrnel-map command
    │   ├── transform.py      # kehrnel-transform command
    │   └── ingest.py        # kehrnel-ingest command
    ├── core/                 # Core openEHR logic
    │   ├── __init__.py
    │   ├── generator.py      # Composition generation
    │   ├── validator.py      # Composition validation
    │   ├── parser.py         # Template parsing
    │   ├── models.py         # Data models
    │   └── store/            # Storage abstractions
    │       ├── base.py       # Base store interface
    │       └── factory.py    # Store factory pattern
    ├── mapper/               # Document mapping engine
    │   ├── __init__.py
    │   ├── mapping_engine.py # Core mapping logic
    │   ├── document_identifier.py  # Auto document type detection
    │   ├── transforms.py     # Value transformation functions
    │   ├── handlers/         # Format-specific handlers
    │   │   ├── __init__.py
    │   │   ├── xml_handler.py    # XML/CDA handler
    │   │   └── csv_handler.py    # CSV handler
    │   └── utils/            # Mapping utilities
    │       └── trace_mapping.py   # Debug tracing
    ├── transform/            # JSON transformation utilities
    │   ├── __init__.py
    │   ├── core.py           # Core transformation logic
    │   ├── single.py         # Single document transforms
    │   ├── reverse_unflatten.py  # Unflatten JSON
    │   ├── shortcuts.py      # Path shortcuts
    │   ├── at_code_codec.py  # at-code handling
    │   ├── rules_engine.py   # Transformation rules
    │   └── config/           # Configuration files
    │       ├── mappings.yaml
    │       ├── shortcuts.json
    │       └── default_config.jsonc
    ├── persistence/          # Data persistence layer
    │   ├── __init__.py
    │   ├── mongo.py          # MongoDB adapter
    │   ├── memory.py         # In-memory store
    │   └── fs.py             # File system store
    ├── ingest/               # Bulk data ingestion
    │   ├── __init__.py
    │   ├── ingest.py         # Core ingestion logic
    │   ├── bulk.py           # Bulk operations
    │   ├── mongo.py          # MongoDB ingestion
    │   ├── fs.py             # File system ingestion
    │   └── api.py            # API ingestion
    ├── api/                  # REST API endpoints
    │   ├── __init__.py
    │   ├── internal/         # Internal APIs
    │   │   ├── __init__.py
    │   │   └── api_server.py # Mapping Studio API server
    │   └── openehr/          # OpenEHR REST API (WIP)
    │       ├── __init__.py
    │       ├── template.py    # Template endpoints
    │       └── composition.py # Composition endpoints
    ├── aql/                  # AQL query engine (WIP)
    │   ├── __init__.py
    │   └── router.py         # AQL-to-MQL translation
    └── tests/                # Test suite
        ├── test_flatten.py
        ├── test_reverse.py
        ├── test_roundtrip.py
        └── fixtures/         # Test data
```

---

## Integration with MongoDB OpenEHR Playground

kehrnel powers the Mapping Studio feature in the MongoDB OpenEHR Playground:

1. **Document Upload**: Upload clinical documents in various formats
2. **Auto-Identification**: System identifies document type and structure
3. **Visual Mapping**: Create mappings visually or with code editor
4. **Batch Processing**: Transform multiple documents at once
5. **Validation**: Ensure outputs conform to openEHR templates

### Environment Variables

```bash
# MongoDB connection
MONGODB_URL=mongodb+srv://usr:pwd@cluster.mongodb.net/openehr_playground

# API configuration
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=true
API_LOG_LEVEL=info

# Backend URL for Next.js integration
BACKEND_URL=http://localhost:8000
```

---

## Extending `kehrnel`

You can easily extend `kehrnel` by:

- Adding new **source handlers** (`mapper/handlers/*.py`) to support other formats (CSV, FHIR, etc.)
- Creating custom **document patterns** for auto-identification
- Writing custom **generators** or **transformers** (`core/`)
- Implementing new **persistence adapters** (e.g., Elastic, SQL)
- Adding CLI commands by dropping files in `cli/` with Typer apps
- Extending the REST API with new endpoints

### Adding a Custom Handler

```python
# mapper/handlers/custom_handler.py
from mapper.handlers.base import BaseHandler

class CustomHandler(BaseHandler):
    def parse(self, file_path):
        # Parse your custom format
        pass
    
    def extract_value(self, xpath, namespaces=None):
        # Extract values using your format's query language
        pass
```

### Adding Document Patterns

```python
# In your code or via API
pattern = DocumentPattern(
    name="my_custom_doc",
    handler="custom",
    required_elements=["root", "data"],
    xpath_patterns=["//root[@type='custom']"]
)

# Via API
curl -X POST "http://localhost:8000/api/internal/patterns" \
  -H "Content-Type: application/json" \
  -d '{ ... pattern definition ... }'
```

---

## API Documentation

When running the API server, interactive documentation is available at:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## 🤝 Acknowledgements

Built with ❤️ by openEHR and MongoDB practitioners, this project explores semantic modelling, flexible persistence, and future-proof architectures for healthcare.