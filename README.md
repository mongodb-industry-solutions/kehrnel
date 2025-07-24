# kehrnel — openEHR **Document‑Model** utilities

> **Disclaimer** `kehrnel` and its companion projects are **learning tools**: they showcase best‑practice modelling and querying of clinical data in an **openEHR Document Model**. They are **not** production‑ready libraries or applications.

`kehrnel` provides a modular set of Python libraries, command-line interfaces (CLIs), and a companion REST API. Together, these tools help model, generate, map, validate, transform, and ingest openEHR compositions — following a document-centric approach. They are designed for rapid prototyping, teaching, and building proof-of-concepts.

It also powers the **MongoDB Healthcare Playground**, a demo stack with MongoDB Atlas Local, FastAPI, AQL dashboards, and semantic search layers:

```
 ┌─────────────┐       REST/CLI        ┌────────────────────────┐
 │  kehrnel    │ <───────────────────▶ │ MongoDB Healthcare Play│
 │ (utilities) │  bidirectional sync  │   ground (demo stack)   │
 └─────────────┘                       └────────────────────────┘
```

---

## 🚀 Features

| #   | Utility               | CLI | Python API | Purpose                                                |
| --- | --------------------- | --- | ---------- | ------------------------------------------------------ |
| 0.0 | **kehrnel‑map**       | ✔︎  | ✔︎         | Map HL7 CDA / FHIR / other formats → openEHR JSON      |
| 0.1 | **kehrnel‑generate**  | ✔︎  | ✔︎         | Generate minimal or random openEHR compositions        |
| 0.2 | **kehrnel‑validate**  | ✔︎  | ✔︎         | Validate compositions against an openEHR OPT           |
| 0.3 | **kehrnel‑transform** | ✔︎  | ✔︎         | Transform: Canonical ⇄ Flattened JSON                  |
| 0.4 | **kehrnel‑ingest**    | ✔︎  | ✔︎         | Ingest JSON into MongoDB Atlas (bulk or single)        |
| 0.5 | **openEHR Demo API**  | –   | ✔︎         | Minimal REST PoC for experimentation                   |

---

## 🛠 Installation

### 1. Development Mode (Library + CLI)

```bash
git clone https://github.com/your-org/kehrnel.git
cd kehrnel
python -m venv .venv && source .venv/bin/activate
pip install -e .[cli,mongo]
```

This will install all required dependencies including Typer (for CLI) and PyMongo (for Atlas ingestion).

---

## ⚡ Quick Usage

### 2.1 Generate a minimal or random composition

You can generate a valid openEHR composition from a template directly — either empty, minimal, or filled with random values.

```bash
kehrnel-generate \
  -t templates/TUMOUR.opt \
  --random \
  -o out/generated_random.json
```
**Parameters:**

- `-t`, `--template`: path to the .opt template.
- `--random`: Generate a full composition with randomized values.
- `--minimal`: Generate a minimal valid skeleton (no values).
- `-o`: (optional) Path to write the output instead of printing to stdout.
```
### 2.2 Map a source file into an openEHR Composition

You can convert external source documents (e.g., CDA, HL7v2, custom XML/CSV) into a canonical openEHR composition JSON using a YAML mapping definition.

```bash
kehrnel-map \
  -m samples/mappings/tumour_mapping.yaml \
  -s samples/in/fiche_tumour.xml \
  -t samples/templates/T-IGR-TUMOUR-SUMMARY.opt \
  -o samples/out/tumourNew.json \
  --trace
```

**Parameters:**

- `-m`, `--mapping`: Path to the YAML mapping file (defines source-to-openEHR path rules)
- `-s`, `--source`: Path to the input source file (e.g., XML, CSV, FHIR JSON)
- `-t`, `--template`: Path to the OPT template (optional but improves path validation)
- `-o`, `--output`: Output file for the resulting composition JSON
- `--trace`: *(optional)* Shows detailed mapping trace and intermediate resolution steps

---

### 2.3 Validate a composition against its template

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

---

### 2.3 Transform canonical JSON to flattened and back

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

## 🧱 Project Structure

```
kehrnel/
├── src/
│   ├── cli/                  # All CLI apps (typer-based)
│   │   ├── map.py
│   │   ├── generate.py
│   │   ├── validate.py
│   │   ├── transform.py
│   │   └── ingest.py
│   ├── core/                 # Core logic (generator, validator, transformer)
│   ├── mapper/               # Mapping engine + source/handlers (XML, etc.)
│   ├── persistence/          # MongoDB ingestion logic (and future drivers)
│   ├── api/                  # FastAPI endpoints (optional)
│   └── aql/                  # (WIP) AQL-to-MQL translation engine
├── samples/                  # Example XMLs, mappings, templates
├── README.md                 # You are here!
└── pyproject.toml            # Project configuration
```

---

## 🧩 Extending `kehrnel`

You can easily extend `kehrnel` by:

- Adding new **source handlers** (`mapper/handlers/*.py`) to support other formats (CSV, FHIR, etc.)
- Writing custom **generators** or **transformers** (`core/`)
- Implementing new **persistence adapters** (e.g., Elastic, SQL)
- Adding CLI commands by dropping files in `cli/` with Typer apps

### 💡 Tip for CLI typing

Typer 0.12 still prefers:

```python
from typing import Optional
def main(opt: Optional[Path] = None): ...
```

Avoid using modern `Path | None` typing to ensure compatibility.
---

## 🤝 Acknowledgements

Built with ❤️ by openEHR and MongoDB practitioners, this project explores semantic modelling, flexible persistence, and future-proof architectures for healthcare.
