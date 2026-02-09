# kehrnel — openEHR® **Document‑Model** utilities

> **⚠️ Disclaimer**
>
> This project is an experimental, non-production environment for demonstration purposes only. It is not an official MongoDB product and is not formally supported by MongoDB. MongoDB makes no representation or warranty as to the accuracy, adequacy, completeness, and fitness for a particular purpose in respect of any materials made available through the Healthcare Data Lab.

`kehrnel` provides a modular set of Python libraries, command-line interfaces (CLIs), and a companion REST API. Together, these tools help model, generate, map, validate, transform, and ingest openEHR compositions — following a document-centric approach. They are designed for rapid prototyping, teaching, and building proof-of-concepts.

> Looking for the active runtime? See `README-dev.md` (`uvicorn kehrnel.api.app:app`).

It also powers the **MongoDB Healthcare Playground**, a demo stack with MongoDB Atlas Local, FastAPI, AQL dashboards, and semantic search layers:

```
 ┌─────────────┐       REST/CLI        ┌────────────────────────┐
 │  kehrnel    │ <───────────────────▶ │ MongoDB Healthcare Play│
 │ (utilities) │  bidirectional sync   │   ground (demo stack)  │
 └─────────────┘                       └────────────────────────┘
```

---

## Features

| #   | Utility                    | CLI | Python API | REST API | Purpose                                                |
| --- | -------------------------- | --- | ---------- | -------- | ------------------------------------------------------ |
| 0.0 | **kehrnel‑map**           | ✔︎  | ✔︎         | ✔︎       | Map HL7 CDA / FHIR / other formats → openEHR JSON      |
| 0.1 | **kehrnel‑generate**      | ✔︎  | ✔︎         | –       | Generate minimal or random openEHR compositions        |
| 0.2 | **kehrnel‑validate**      | ✔︎  | ✔︎         | ✔︎       | Validate compositions against an openEHR OPT           |
| 0.3 | **kehrnel‑transform**     | ✔︎  | ✔︎         | ✔︎       | Transform: Canonical ⇄ Flattened JSON                  |
| 0.4 | **kehrnel‑ingest**        | ✔︎  | ✔︎         | –       | Ingest JSON into MongoDB Atlas (bulk or single)        |
| 0.5 | **kehrnel‑identify**      | ✔︎  | ✔︎         | ✔︎       | Auto-identify document types with pattern matching     |
| 0.6 | **mapping‑studio‑api**    | –  | –         | ✔︎       | REST API for Mapping Studio integration                |
| 0.7 | **openEHR Demo API**      | –  | ✔︎         | ✔︎       | Minimal REST PoC for experimentation                   |

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
python -m kehrnel.api.app

# The API will be available at:
# - http://localhost:8000 (endpoints)
# - http://localhost:8000/docs (interactive documentation)
```

---

## Quick Usage

### Generate a minimal or random composition

Generate a valid openEHR composition from a template — either empty, minimal, or filled with random values.

```bash
kehrnel-generate \                                                              
  -t path/to/template.opt \
  -o output/composition.json \
  --random
```
**Parameters:**

- `-t`, `--template`: path to the .opt template
- `--random`: Generate a full composition with randomized values
- `--minimal`: Generate a minimal valid skeleton (no values)
- `-o`: (optional) Path to write the output instead of printing to stdout

### Validate a composition against its template

Ensure the generated composition structure conforms to the constraints of its openEHR OPT template.

```bash
kehrnel-validate \
  -c path/to/composition.json \
  -t path/to/template.opt \
  -v
```

**Parameters:**

- `-c`, `--composition`: Path to the composition JSON file to validate
- `-t`, `--template`: Path to the OPT template file
- `-v`, `--verbose`: *(optional)* Show all validation issues, including warnings and info

You can also pipe the composition through stdin:

```bash
comp=$(kehrnel-generate -t template.opt --random)
printf "%s" "$comp" | kehrnel-validate -t template.opt -
```

### Validate a strategy pack for portability

Use the validator to lint a strategy folder (manifest + defaults + schema + entrypoint) before dropping it into discovery paths:

```bash
kehrnel-validate-pack ./path/to/strategy-pack
```

Add `--json` for machine-readable diagnostics. A starter skeleton lives in `strategy-pack-template/`.

### Map a source file into an openEHR Composition

Convert external source documents (e.g., CDA, HL7v2, custom XML/CSV) into a canonical openEHR composition JSON using a YAML mapping definition.

```bash
kehrnel-map \
  -m path/to/mapping.yaml \
  -s path/to/source.xml \
  -t path/to/template.opt \
  -o output/composition.json \
  --trace
```

**Parameters:**

- `-m`, `--mapping`: Path to the YAML mapping file (defines source-to-openEHR path rules)
- `-s`, `--source`: Path to the input source file (e.g., XML, CSV, FHIR JSON)
- `-t`, `--template`: Path to the OPT template (optional but improves path validation)
- `-o`, `--output`: Output file for the resulting composition JSON
- `--trace`: *(optional)* Shows detailed mapping trace and intermediate resolution steps

### Transform canonical JSON to flattened and back

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

## Document Identification System

`kehrnel-identify` and the Mapping-Studio API share the **same pattern engine** (`DocumentIdentifier`). Patterns are loaded from multiple sources:

1. **patterns.yaml** – the default pattern file next to `document_identifier.py` (can be excluded)
2. **Extra pattern files** – optional YAML or JSON files passed by the caller
3. **MongoDB collection `patterns`** – patterns persisted through the API (loaded at runtime by FastAPI)

By default, the built-in `patterns.yaml` is always loaded. Use `--no-default` to load **only** your custom pattern files.

### CLI – identify files or directories

```bash
# Single file with default patterns only
kehrnel-identify -d path/to/doc.xml -o result.json

# Default patterns + customer-specific patterns
kehrnel-identify -d path/to/doc.xml -o result.json -p customer_patterns.yaml

# ONLY customer patterns (exclude built-in patterns.yaml)
kehrnel-identify -d path/to/doc.xml -o result.json -p customer.yml --no-default

# Chain multiple pattern files (option can be repeated)
kehrnel-identify -d samples/in/data -o results.json -p core.yml -p extra.json

# Whole tree with extra patterns and debug output
kehrnel-identify -d samples/in/data -o results.json --recursive -p custom.yaml --debug
```

| **Options** | **Default** | **Description** |
|-------------|-------------|-----------------|
| -p, --patterns | None | Extra YAML or JSON pattern files (can be repeated) |
| --no-default | False | Ignore the built-in src/mapper/patterns.yaml |
| --glob | * | Glob pattern to select files |
| --recursive / --no-recursive | --recursive | Descend into sub-folders |
| --debug | *off* | Prints every rule that was tested |

### Pattern definition (YAML or JSON)

Patterns can be defined in YAML or JSON files. Each file must contain a list/array of pattern objects:

```yaml
# patterns.yaml or custom_patterns.yaml
- name: imaging_cda
  handler: xml
  priority: 90
  xpath_patterns:
    - "//cda:code[@code='11528-7']"
  namespaces: {cda: "urn:hl7-org:v3"}

- name: laboratory_csv
  handler: csv
  priority: 80
  csv_headers:
    - patientid
    - test
    - resultat
```

```json
// patterns.json
[
  {
    "name": "dicom_csv",
    "handler": "csv",
    "priority": 75,
    "csv_headers": [
      "accessionnumber",
      "patientid",
      "modality"
    ]
  }
]
```

*All keys are **AND-ed** together – a document either matches 100% or it does not.*

### Python API usage

```python
from mapper.document_identifier import DocumentIdentifier

# Default behaviour (patterns.yaml only)
id1 = DocumentIdentifier()

# Default patterns + one customer-specific file
id2 = DocumentIdentifier(pattern_files=["customer_patterns.yaml"])

# ONLY customer patterns (exclude built-in patterns.yaml)
id3 = DocumentIdentifier(
    pattern_files=["customer_patterns.yaml"],
    include_default=False
)

# Multiple files without default patterns
id4 = DocumentIdentifier(
    pattern_files=["core.yml", "extra.json"],
    include_default=False
)

# With runtime patterns from your application
runtime_patterns = [DocumentPattern(...), ...]
id5 = DocumentIdentifier(patterns=runtime_patterns)
```

### Managing patterns through the API

| **Method & Path** | **Purpose** |
|-------------------|-------------|
| POST /api/internal/patterns | **Upsert** one pattern (JSON body = pattern fields) |
| GET /api/internal/patterns | List every active pattern |
| *(startup)* | API automatically loads all Mongo patterns into the in-memory singleton |

Example:

```bash
curl -X POST http://localhost:8000/api/internal/patterns \
     -H "Content-Type: application/json" \
     -d '{
           "name": "dicom_csv",
           "handler": "csv",
           "csv_headers": [
             "accessionnumber","patientid","modality","seriesinstanceuid",
             "studydate","studydescription","studyinstanceuid"
           ]
         }'
```

### Identification result schema

```json
{
  "documentType": "laboratory_csv",   // pattern name
  "handler":     "csv",               // xml | csv | json | hl7v2
  "file":        "path/to/file.csv",
  "sampleData":  {                    // one representative row / snippet
    "patientid": "12345",
    "test":      "HbA1c",
    "resultat":  "6.8"
  },
  "structure":   {
    "headers":   ["patientid","test","resultat"],
    "delimiter": ";"
  }
}
```

**Note**: Because every pattern is unique and evaluated deterministically, no *"ambiguous_csv"* result is emitted any more.

### Pattern loading order and conflicts

Pattern loading behavior depends on the `include_default` parameter:

**Default behavior** (`include_default=True` or `--no-default` not used):
1. **Base patterns** from `src/mapper/patterns.yaml`
2. **Extra files** specified via `-p` / `--patterns` (in order)
3. **MongoDB patterns** (API runtime only)

**Exclusive mode** (`include_default=False` or `--no-default` flag):
1. **Only extra files** specified via `-p` / `--patterns`
2. **MongoDB patterns** (API runtime only)
3. Built-in `patterns.yaml` is completely ignored

When patterns share the same `name`:
- The **last loaded pattern wins**
- This allows overriding built-in patterns by reusing their names
- Final ordering is still determined by the `priority` field

### Bootstrapping a new environment

1. **For shared patterns**: Add core patterns to `src/mapper/patterns.yaml`
2. **For customer-specific patterns**: Create separate YAML/JSON files
3. Deploy/launch the API – it loads patterns based on configuration
4. POST additional patterns to `/api/internal/patterns` for persistence
5. Use CLI with appropriate flags:
   - Include defaults: `kehrnel-identify -d data/ -p customer.yaml`
   - Exclude defaults: `kehrnel-identify -d data/ -p customer.yaml --no-default`

---

## API Usage

### Document Identification API

```python
from mapper.document_identifier import DocumentIdentifier

# Single file identification
identifier = DocumentIdentifier(debug=True)
result = identifier.identify_document("path/to/document.xml")

print(result)
# {
#   "documentType": "identified_type",
#   "handler": "xml",
#   "sampleData": {...},
#   "structure": {...}
# }
```

### REST API Endpoints

#### Identify Document Type
```bash
curl -X POST "http://localhost:8000/api/internal/identify-document" \
  -F "document=@document.xml"
```

#### List/Save Mappings
```bash
# Get all mappings
curl "http://localhost:8000/api/internal/mappings"

# Save a mapping
curl -X POST "http://localhost:8000/api/internal/mappings" \
  -H "Content-Type: application/json" \
  -d '{
    "documentType": "my_document_type",
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

#### Pattern Management
```bash
# List all patterns
curl "http://localhost:8000/api/internal/patterns"

# Add/Update a pattern (persisted to MongoDB)
curl -X POST "http://localhost:8000/api/internal/patterns" \
  -H "Content-Type: application/json" \
  -d '{ ... pattern definition ... }'
```

---

## Supported Document Types

The document identifier supports pattern-based recognition of various healthcare document formats:

### XML-based Documents
- HL7 CDA R2 documents
- Custom XML formats
- FHIR XML resources
- Proprietary clinical formats

### CSV/Delimited Files
- Laboratory results
- Patient registries
- Clinical measurements
- Custom delimited formats

### HL7v2 Messages
- ADT (Admission, Discharge, Transfer)
- ORU (Observation Result)
- ORM (Order Message)
- Custom message types

### JSON Documents
- FHIR JSON resources
- Custom JSON structures
- API responses

---

## Project Structure

```
kehrnel/
├── pyproject.toml            # Project configuration and dependencies
├── README.md                 # This file
├── src/                      # Source code
│   ├── cli/                  # Command-line interfaces
│   │   ├── generate.py       # kehrnel-generate command
│   │   ├── validate.py       # kehrnel-validate command
│   │   ├── map.py           # kehrnel-map command
│   │   ├── transform.py      # kehrnel-transform command
│   │   ├── identify.py       # kehrnel-identify command
│   │   └── ingest.py        # kehrnel-ingest command
│   ├── core/                 # Core openEHR logic
│   │   ├── generator.py      # Composition generation
│   │   ├── validator.py      # Composition validation
│   │   ├── parser.py         # Template parsing
│   │   └── models.py         # Data models
│   ├── mapper/               # Document mapping engine
│   │   ├── mapping_engine.py # Core mapping logic
│   │   ├── document_identifier.py  # Document type detection
│   │   ├── patterns.yaml     # Document identification patterns
│   │   ├── transforms.py     # Value transformation functions
│   │   └── handlers/         # Format-specific handlers
│   │       ├── xml_handler.py
│   │       ├── csv_handler.py
│   │       ├── json_handler.py
│   │       └── hl7v2_handler.py
│   ├── transform/            # JSON transformation utilities
│   │   ├── core.py           # Core transformation logic
│   │   ├── reverse_unflatten.py  # Unflatten JSON
│   │   └── config/           # Configuration files
│   ├── persistence/          # Data persistence layer
│   │   ├── mongo.py          # MongoDB adapter
│   │   ├── memory.py         # In-memory store
│   │   └── fs.py             # File system store
│   └── api/                  # REST API endpoints
│       └── internal/         
│           └── api_server.py # Mapping Studio API server
```

---

## Integration with MongoDB OpenEHR Playground

kehrnel powers the Mapping Studio feature in the MongoDB OpenEHR Playground:

1. **Document Upload**: Upload clinical documents in various formats
2. **Auto-Identification**: System identifies document type and structure using pattern matching
3. **Visual Mapping**: Create mappings visually or with code editor
4. **Batch Processing**: Transform multiple documents at once
5. **Validation**: Ensure outputs conform to openEHR templates

### Environment Variables

```bash
# MongoDB connection
MONGODB_URL=mongodb+srv://usr:pwd@cluster.mongodb.net/database

# API configuration
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=true
API_LOG_LEVEL=info

# Backend URL for Next.js integration
BACKEND_URL=http://localhost:8000
```

---

## Performance and Scalability

### Document Identification
- **Pattern Caching**: Patterns loaded once and cached in memory
- **Priority-based Evaluation**: Higher priority patterns checked first
- **Early Exit**: First matching pattern wins
- **Batch Processing**: Identify entire directories efficiently

### MongoDB Integration
- **Pattern Persistence**: Custom patterns stored in MongoDB
- **Lazy Loading**: Patterns loaded on startup from both YAML and database
- **Singleton Pattern**: One DocumentIdentifier instance per process

---

## Extending kehrnel

### Adding a Custom Handler

```python
# mapper/handlers/custom_handler.py
from mapper.handlers.base import BaseHandler

class CustomHandler(BaseHandler):
    def parse(self, file_path):
        # Parse your custom format
        pass
    
    def extract_value(self, query, namespaces=None):
        # Extract values using your format's query language
        pass
```

### Creating Document Patterns

```yaml
# src/mapper/patterns.yaml
- name: your_document_type
  handler: xml|csv|json|hl7v2|custom
  priority: 1-100  # Higher = checked first
  required_elements: [...]
  xpath_patterns: [...]  # XML only
  csv_headers: [...]     # CSV only
  exclude_elements: [...] 
```

### Adding CLI Commands

Create a new file in `cli/` with a Typer app:

```python
# cli/custom_command.py
import typer

app = typer.Typer()

@app.command()
def main(input_file: Path, output_file: Path):
    """Your custom command description"""
    # Implementation
```

---

## API Documentation

When running the API server, interactive documentation is available at:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run specific test module
pytest tests/test_identifier.py

# Run with coverage
pytest --cov=src --cov-report=html
```

---

## License

The data strategies, templates, schemas, and design artifacts in `src/kehrnel/strategies/` are licensed under the [Creative Commons Attribution 4.0 International License (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/). You may use, share, adapt, and build upon these materials, provided you give appropriate attribution. See the [LICENSE](src/kehrnel/strategies/LICENSE) file for details.

---

## Trademark Notices

- **FHIR®** is the registered trademark of Health Level Seven International and its use does not constitute endorsement by HL7.
- **openEHR®** is the registered trademark of the openEHR Foundation and use of the mark does not constitute endorsement by openEHR International or openEHR Foundation.

---

## Acknowledgements

Built with care by openEHR® and MongoDB practitioners, this project explores semantic modelling, flexible persistence, and future-proof architectures for healthcare.
