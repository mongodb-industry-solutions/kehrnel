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

### 2. Full Playground Environment

To test `kehrnel` inside a working MongoDB + FastAPI + Dashboard stack:

```bash
git clone https://github.com/your-org/healthcare-playground.git
cd healthcare-playground
docker compose up -d
```

The containers mount your local `kehrnel/` directory, so any code changes are hot-reloaded.

---

## ⚡ Quick Usage

### 2.1 Convert a CDA file into a composition

```bash
kehrnel-map \
  -m mappings/pmsi_mapping.yaml \
  -s samples/in/fiche_tumour.xml \
  -t templates/PMSI.opt \
  -o out/pmsi.json \
  --trace
```

### 2.2 Generate a random composition and validate it

```bash
comp=$(kehrnel-generate -t templates/TUMOUR.opt --random)
printf "%s" "$comp" | kehrnel-validate -t templates/TUMOUR.opt -
```

### 2.3 Transform canonical JSON to flattened and back

```bash
# Flatten a canonical JSON file
kehrnel-transform canonical.json --flatten -o flat.json

# Expand it back to canonical form
kehrnel-transform flat.json --expand -o canonical.json
```

### 2.4 Ingest into MongoDB Atlas

```bash
kehrnel-ingest flat.json \
  --dsn "$MONGODB_URI" \
  --db ehr \
  --collection comps
```

Check the playground Grafana dashboards or query from the API to validate ingestion.

---

## 🌐 Minimal openEHR REST API (PoC)

Once the **healthcare-playground** is running, an experimental openEHR-compatible FastAPI service is available:

| Method | Path                                   | Description                            |
|--------|----------------------------------------|----------------------------------------|
| GET    | `/rest/openehr/v1/definition/template` | List available OPT templates            |
| POST   | `/rest/openehr/v1/composition`         | Validate + store a composition          |
| GET    | `/rest/openehr/v1/composition/{uid}`   | Fetch by UID                            |
| GET    | `/rest/openehr/v1/composition`         | Filter by template, date, subject, etc. |

Under the hood, this service reuses the exact same modules (`validate.py`, `generate.py`, etc.).

---

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

## 📅 Roadmap

- ✅ July 2025 — Canonical to Flat transformer
- ✅ July 2025 — MongoDB Atlas ingestor
- ◻︎ Sept 2025 — ElasticSearch persistence backend
- ◻︎ Oct 2025 — Smart-on-FHIR wrapper for REST API
- ◻︎ Q4 2025 — Streamlit GUI for drag-and-drop mapping

Community contributions welcome! Open an issue to share feedback or start a PR.

---

## 📜 License

MIT License — see [`LICENSE`](./LICENSE)

---

## 🤝 Acknowledgements

Built with ❤️ by openEHR and MongoDB practitioners, this project explores semantic modelling, flexible persistence, and future-proof architectures for healthcare.