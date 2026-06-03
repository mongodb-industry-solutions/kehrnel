# FHIR Data Generation Library — Complete Build Prompts

> **Usage:** Run prompts **in order**. Each prompt is one implementation step; paste only that prompt block into the LLM.
> **Stack:** Python 3.11+, MongoDB, Click (CLI), Pytest, Faker, PyYAML
> **Canonical repo inputs** (do not rename):
> - Schema: `fhir.schema.v5.json` (857 definitions, 158 FHIR R5 resources)
> - Datatypes checklist: `FHIR_DATATYPES.txt`
> - Resource spec URLs: `FHIR_RESOURCE_URLS.txt` (157 resources)
> - Terminology: `hl7_codes/healthcare_codes.yaml`
> - Requirements: `INSTRUCTIONS.txt`

### Token rules for codegen
- Implement **only** files named in the current prompt.
- Reuse types/functions from prior prompts; do not restate earlier code.
- Prefer schema-driven generation; enrichers are optional overlays for clinical realism.
- Run `pip install -e .[dev]` and the verification step after each prompt.

### Requirements traceability (`INSTRUCTIONS.txt`)
| # | Requirement | Primary prompt(s) |
|---|-------------|-------------------|
| 1 | Generic generator for all resources | 2, 8, 15 |
| 2 | All datatypes in `FHIR_DATATYPES.txt` | 4, 5, 6 |
| 3 | Correct field datatypes | 2, 4–6, 8 |
| 4–5 | Reference linking; deps generated first | 7, 8 |
| 6 | Optional custom schema path | 1, 2, 8, 15 |
| 7 | Multiple docs for polymorphic / choice fields | 8, 15 |
| 8 | Healthcare-standard correctness | 3, 9–13 |
| 9–10 | `hl7_codes/healthcare_codes.yaml` + enrichment | 3 |
| 11 | MongoDB persistence | 14 |
| 12 | Installable library + CLI | 1, 2, 15, 16 |
| 13 | Searchable cross-resource data | 7, 8, 14, 15 |
| 14 | Full test coverage | 17, 18 |

---

## TABLE OF CONTENTS

0. [Prerequisites](#prompt-0-prerequisites)
1. [Project Scaffold, pyproject & Config](#prompt-1-project-scaffold-pyproject--config)
2. [FHIR Schema Parser](#prompt-2-fhir-schema-parser)
3. [Healthcare Codes YAML & Loader](#prompt-3-healthcare-codes-yaml--loader)
4. [Primitive Datatype Generators](#prompt-4-primitive-datatype-generators)
5. [Complex Datatype Generators](#prompt-5-complex-datatype-generators)
6. [Special & Metadata Datatype Generators](#prompt-6-special--metadata-datatype-generators)
7. [Reference Resolver & Dependency Graph](#prompt-7-reference-resolver--dependency-graph)
8. [Base Resource Generator Engine](#prompt-8-base-resource-generator-engine)
9. [Clinical Core Resources](#prompt-9-clinical-core-resources)
10. [Medication Resources](#prompt-10-medication-resources)
11. [Workflow & Administrative Resources](#prompt-11-workflow--administrative-resources)
12. [Financial & Coverage Resources](#prompt-12-financial--coverage-resources)
13. [Specialized Domain Resources](#prompt-13-specialized-domain-resources)
14. [MongoDB Persistence Layer](#prompt-14-mongodb-persistence-layer)
15. [CLI Interface](#prompt-15-cli-interface)
16. [README & Package Polish](#prompt-16-package-setup--distribution)
17. [Test Suite — Datatypes & Engine](#prompt-17-test-suite--datatypes--engine)
18. [Test Suite — Resources & Integration](#prompt-18-test-suite--resources--integration)

---

## PROMPT 0: Prerequisites

```
Before Prompt 1, confirm repo inputs exist:
- `fhir.schema.v5.json`, `FHIR_DATATYPES.txt`, `FHIR_RESOURCE_URLS.txt`, `INSTRUCTIONS.txt`
- `hl7_codes/healthcare_codes.yaml`

No code in this step. Read `INSTRUCTIONS.txt` and skim `FHIR_DATATYPES.txt` + `FHIR_RESOURCE_URLS.txt`.
All 158 schema resources must be generatable via the generic engine (Prompt 8); enrichers (Prompts 9–13) cover ~55 high-value clinical/workflow types only.
```

---

## PROMPT 1: Project Scaffold, pyproject & Config

```
Create the Python package scaffold for a FHIR R5 synthetic data generation library called `fhir_gen`.

Directory structure to create (all empty __init__.py files included):
fhir_gen/
  __init__.py
  config.py
  schema/
    __init__.py
    parser.py
    registry.py
  generators/
    __init__.py
    primitives.py
    complex_types.py
    special_types.py
    base.py
    resources/
      __init__.py
      clinical.py
      medication.py
      workflow.py
      financial.py
      specialized.py
  resolvers/
    __init__.py
    reference.py
    dependency.py
  persistence/
    __init__.py
    mongo.py
  codes/
    __init__.py
    loader.py
  cli/
    __init__.py
    main.py
tests/
  __init__.py
  conftest.py
  test_primitives.py
  test_complex.py
  test_resources.py
  test_integration.py
hl7_codes/
  healthcare_codes.yaml
pyproject.toml
README.md

### config.py content:
```python
from pydantic_settings import BaseSettings
from pathlib import Path

PACKAGE_ROOT = Path(__file__).parent
SCHEMA_PATH = PACKAGE_ROOT / "schema" / "fhir.schema.v5.json"
CODES_PATH = PACKAGE_ROOT.parent / "hl7_codes" / "healthcare_codes.yaml"

class Settings(BaseSettings):
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db: str = "fhir_synthetic"
    default_locale: str = "en-US"
    seed: int | None = None
    schema_path: Path = SCHEMA_PATH
    codes_path: Path = CODES_PATH
    log_level: str = "INFO"

    class Config:
        env_prefix = "FHIR_GEN_"
        env_file = ".env"

settings = Settings()
```

### fhir_gen/__init__.py (stub until Prompt 8 adds ResourceGenerator):
```python
from .config import settings

__version__ = "1.0.0"
__all__ = ["settings"]
```

Copy repository-root `fhir.schema.v5.json` → `fhir_gen/schema/fhir.schema.v5.json` (package-data, UTF-8).

### pyproject.toml (minimal; Prompt 16 expands README):
```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "fhir-gen"
version = "1.0.0"
description = "FHIR R5 synthetic healthcare data generator"
requires-python = ">=3.11"
dependencies = [
  "pydantic>=2.0", "pydantic-settings>=2.0", "pymongo>=4.6",
  "click>=8.1", "faker>=24.0", "PyYAML>=6.0",
]

[project.optional-dependencies]
dev = ["pytest>=7.4", "pytest-cov>=4.1", "mongomock>=4.1", "ruff>=0.1"]

[project.scripts]
fhir-gen = "fhir_gen.cli.main:main"

[tool.setuptools.packages.find]
include = ["fhir_gen*"]

[tool.setuptools.package-data]
fhir_gen = ["schema/fhir.schema.v5.json"]
```

**Verify:** `pip install -e .` then `python -c "from fhir_gen.config import settings; print(settings.schema_path.exists())"`
```

---

## PROMPT 2: FHIR Schema Parser

```
Create `fhir_gen/schema/parser.py` and `fhir_gen/schema/registry.py`.

### parser.py
Parse `fhir.schema.v5.json` (FHIR R5, 857 definitions). Schema structure:
- Top-level keys: $schema, id, description, discriminator, oneOf, definitions
- definitions[ResourceName] has: properties, required, description
- Properties reference other definitions via "$ref": "#/definitions/TypeName"
- Polymorphic fields use multiple keys: valueQuantity, valueString, valueBoolean, etc. (same base name, different type suffixes)
- Array fields have: {"items": {"$ref": "..."}, "type": "array"}

```python
import json
from dataclasses import dataclass, field
from pathlib import Path
from functools import lru_cache

@dataclass
class FieldDef:
    name: str
    ref: str | None          # resolved definition name e.g. "Quantity"
    is_array: bool
    is_required: bool
    description: str
    is_primitive: bool       # True if ref is a primitive type
    const_value: str | None  # for resourceType fields

@dataclass
class ResourceDef:
    name: str
    description: str
    fields: dict[str, FieldDef]
    required: list[str]
    is_resource: bool        # True if has resourceType property
    # Polymorphic groups: {"value": ["valueQuantity","valueString",...]}
    poly_groups: dict[str, list[str]]

class FHIRSchemaParser:
    PRIMITIVES = {
        "base64Binary","boolean","canonical","code","date","dateTime",
        "decimal","id","instant","integer","integer64","markdown","oid",
        "positiveInt","string","time","unsignedInt","uri","url","uuid",
        "xhtml"
    }

    def __init__(self, schema_path: Path):
        with open(schema_path) as f:
            self._raw = json.load(f)
        self._defs = self._raw["definitions"]

    def parse_all(self) -> dict[str, ResourceDef]:
        """Return dict of all parsed definitions."""
        ...

    def parse_definition(self, name: str) -> ResourceDef:
        """Parse a single definition by name."""
        ...

    def _extract_ref(self, prop: dict) -> str | None:
        """Extract definition name from $ref, items.$ref, or allOf[0].$ref"""
        ...

    def _find_poly_groups(self, fields: dict[str, FieldDef]) -> dict[str, list[str]]:
        """Group polymorphic fields: valueQuantity+valueString -> {"value": [...]}"""
        # Fields starting with _ are extension shadows, skip them
        # Group by common prefix before the type suffix
        ...

    @lru_cache(maxsize=1)
    def get_all_resources(self) -> list[str]:
        """Return names of definitions that are FHIR resources (have resourceType const)."""
        ...

    def get_references_for(self, resource_name: str) -> list[str]:
        """Return list of resource names this resource references via Reference fields."""
        ...
```

### registry.py
```python
from functools import lru_cache
from .parser import FHIRSchemaParser, ResourceDef
from ..config import settings

class SchemaRegistry:
    _instance: "SchemaRegistry | None" = None

    def __init__(self):
        self._parser = FHIRSchemaParser(settings.schema_path)
        self._cache: dict[str, ResourceDef] = {}

    @classmethod
    def get(cls) -> "SchemaRegistry":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def definition(self, name: str) -> ResourceDef:
        if name not in self._cache:
            self._cache[name] = self._parser.parse_definition(name)
        return self._cache[name]

    def all_resources(self) -> list[str]:
        return self._parser.get_all_resources()

    def references_for(self, name: str) -> list[str]:
        return self._parser.get_references_for(name)

    @classmethod
    def reload(cls, schema_path: Path | None = None) -> "SchemaRegistry":
        """Reload parser when CLI passes custom schema (INSTRUCTIONS #6)."""
        if schema_path:
            settings.schema_path = schema_path
        cls._instance = None
        return cls.get()

registry = SchemaRegistry.get()
```
```

---

## PROMPT 3: Healthcare Codes YAML & Loader

```
Do NOT regenerate `hl7_codes/healthcare_codes.yaml` from scratch — the repo file already exists.

### Task A — `fhir_gen/codes/loader.py`
```python
import yaml
from functools import lru_cache
from ..config import settings

@lru_cache(maxsize=1)
def load_codes() -> dict:
    path = settings.codes_path
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def get_codes(section: str) -> list[dict]:
    data = load_codes()
    section_data = data.get(section, {})
    if isinstance(section_data, list):
        return section_data
    return section_data.get("codes", [])

def get_system(section: str) -> str | None:
    data = load_codes()
    section_data = data.get(section, {})
    if isinstance(section_data, dict):
        return section_data.get("system")
    return None

def random_code(section: str, rng) -> dict | None:
    codes = get_codes(section)
    return rng.choice(codes) if codes else None
```

### Task B — Enrich existing YAML (INSTRUCTIONS #9–10)
Add or extend sections **only when missing** or when generators reference them. Preserve existing keys (appointment/slot/participation codes).

YAML shape per section:
```yaml
section_name:
  system: http://...
  codes:
    - {code: "...", display: "..."}
```
Or flat list for simple enums: `name_use: {codes: [usual, official, ...]}`

**Required sections** (used by prompts 4–13): languages, mime_types, gender, marital_status, contact_relationship, identifier_types, name_use, address_use, address_type, telecom_system, telecom_use, appointment_status, slot_status, participation_status, encounter_status, encounter_class, observation_status, condition_clinical_status, condition_verification_status, allergy_clinical_status, allergy_verification_status, procedure_status, immunization_status, medication_request_status, medication_admin_status, medication_dispense_status, claim_status, coverage_status, care_plan_status, care_plan_intent, goal_status, task_status, document_reference_status, composition_status, dosage_routes, dosage_timing, service_category, service_type, loinc_observations (value + unit + referenceRange low/high per code), snomed_conditions, snomed_procedures, snomed_allergies, snomed_medications, body_sites, countries, us_states.

**Sources:** https://www.hl7.org/fhir/terminologies.html and binding pages from `FHIR_RESOURCE_URLS.txt`. Target 15–30 codes per high-traffic section.

**Verification:** `python -c "from fhir_gen.codes.loader import random_code; import random; print(random_code('gender', random.Random(1)))"`
```

## PROMPT 4: Primitive Datatype Generators

```
Create `fhir_gen/generators/primitives.py`.

Implement a `PrimitiveGenerator` class that generates valid FHIR R5 primitive values.
Use `faker` for realistic data. Accept a `random.Random` instance for seeded reproducibility.

```python
import random as _random
import uuid
import base64
from datetime import date, datetime, timedelta
from faker import Faker

class PrimitiveGenerator:
    """Generates FHIR R5 primitive datatype values."""

    def __init__(self, seed: int | None = None):
        self._seed = seed
        self.rng = _random.Random(seed)
        self.faker = Faker()
        if seed:
            Faker.seed(seed)

    def generate(self, type_name: str, **kwargs) -> object:
        """Dispatch to the correct generator by FHIR primitive type name."""
        method = getattr(self, f"gen_{type_name}", self.gen_string)
        return method(**kwargs)

    def gen_id(self, **_) -> str:
        """FHIR id: [A-Za-z0-9\-\.]{1,64}"""
        return str(uuid.uuid4())

    def gen_string(self, max_length: int = 100, **_) -> str:
        return self.faker.sentence(nb_words=4).rstrip(".")[:max_length]

    def gen_boolean(self, **_) -> bool:
        return self.rng.choice([True, False])

    def gen_integer(self, min_val: int = -2**31, max_val: int = 2**31 - 1, **_) -> int:
        return self.rng.randint(max(min_val, -1000), min(max_val, 1000))

    def gen_integer64(self, **_) -> int:
        return self.rng.randint(-10**9, 10**9)

    def gen_decimal(self, min_val: float = 0.0, max_val: float = 1000.0,
                    precision: int = 2, **_) -> float:
        return round(self.rng.uniform(min_val, max_val), precision)

    def gen_unsignedInt(self, max_val: int = 1000, **_) -> int:
        return self.rng.randint(0, max_val)

    def gen_positiveInt(self, max_val: int = 1000, **_) -> int:
        return self.rng.randint(1, max_val)

    def gen_date(self, min_year: int = 1920, max_year: int = 2024, **_) -> str:
        """FHIR date: YYYY, YYYY-MM, or YYYY-MM-DD"""
        start = date(min_year, 1, 1)
        end = date(max_year, 12, 31)
        delta = (end - start).days
        d = start + timedelta(days=self.rng.randint(0, delta))
        fmt = self.rng.choice(["%Y", "%Y-%m", "%Y-%m-%d"])
        return d.strftime(fmt)

    def gen_dateTime(self, min_year: int = 2000, max_year: int = 2024, **_) -> str:
        """FHIR dateTime: YYYY-MM-DDThh:mm:ss+zz:zz"""
        start = datetime(min_year, 1, 1)
        end = datetime(max_year, 12, 31, 23, 59, 59)
        delta = int((end - start).total_seconds())
        dt = start + timedelta(seconds=self.rng.randint(0, delta))
        return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def gen_instant(self, **_) -> str:
        """FHIR instant: always full YYYY-MM-DDThh:mm:ss.sssZ"""
        return self.gen_dateTime().replace("+00:00", "Z")

    def gen_time(self, **_) -> str:
        """FHIR time: hh:mm:ss"""
        h = self.rng.randint(0, 23)
        m = self.rng.randint(0, 59)
        s = self.rng.randint(0, 59)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def gen_uri(self, **_) -> str:
        return f"urn:uuid:{uuid.uuid4()}"

    def gen_url(self, **_) -> str:
        return self.faker.url()

    def gen_canonical(self, base: str = "http://example.org/fhir", **_) -> str:
        resource = self.rng.choice(["StructureDefinition", "ValueSet", "CodeSystem"])
        name = self.faker.word().lower()
        version = f"{self.rng.randint(1,3)}.{self.rng.randint(0,9)}"
        return f"{base}/{resource}/{name}|{version}"

    def gen_code(self, code_set: list[str] | None = None, **_) -> str:
        if code_set:
            return self.rng.choice(code_set)
        # Default: short alphanumeric code
        return self.faker.lexify("????").lower()

    def gen_oid(self, **_) -> str:
        """OID: urn:oid:x.x.x..."""
        parts = [str(self.rng.randint(1, 9))] + [
            str(self.rng.randint(0, 999)) for _ in range(self.rng.randint(3, 7))
        ]
        return "urn:oid:" + ".".join(parts)

    def gen_uuid(self, **_) -> str:
        return f"urn:uuid:{uuid.uuid4()}"

    def gen_markdown(self, **_) -> str:
        return f"## {self.faker.sentence()}\n\n{self.faker.paragraph()}"

    def gen_base64Binary(self, byte_count: int = 64, **_) -> str:
        data = bytes(self.rng.getrandbits(8) for _ in range(byte_count))
        return base64.b64encode(data).decode()

    def gen_xhtml(self, **_) -> str:
        text = self.faker.sentence()
        return f'<div xmlns="http://www.w3.org/1999/xhtml"><p>{text}</p></div>'
```
```

---

## PROMPT 5: Complex Datatype Generators

```
Create `fhir_gen/generators/complex_types.py`.

Implement `ComplexTypeGenerator` for all FHIR R5 General-Purpose Complex Data Types.
Import `PrimitiveGenerator` and `codes.loader` for standard codes.
All methods return Python dicts (FHIR JSON representation).

```python
from .primitives import PrimitiveGenerator
from ..codes.loader import get_codes, get_system, random_code

class ComplexTypeGenerator:
    def __init__(self, prim: PrimitiveGenerator):
        self.p = prim
        self.rng = prim.rng

    def generate(self, type_name: str, **kwargs) -> dict:
        method = getattr(self, f"gen_{type_name}", None)
        if method:
            return method(**kwargs)
        raise ValueError(f"No generator for complex type: {type_name}")

    def gen_Identifier(self, system: str | None = None, value: str | None = None) -> dict:
        type_code = random_code("identifier_types", self.rng)
        return {
            "use": self.rng.choice(["usual", "official", "temp", "secondary"]),
            "type": self.gen_CodeableConcept(
                system=get_system("identifier_types"),
                code=type_code["code"] if type_code else "MR",
                display=type_code["display"] if type_code else "Medical Record Number"
            ),
            "system": system or f"http://hospital.example.org/identifiers",
            "value": value or self.p.gen_id()
        }

    def gen_HumanName(self, use: str | None = None) -> dict:
        first = self.p.faker.first_name()
        last = self.p.faker.last_name()
        use = use or self.rng.choice(["usual", "official", "nickname"])
        return {
            "use": use,
            "family": last,
            "given": [first],
            "text": f"{first} {last}",
            "prefix": [self.rng.choice(["Mr.", "Mrs.", "Ms.", "Dr.", "Prof."])]
                       if self.rng.random() < 0.3 else []
        }

    def gen_Address(self, country: str | None = None) -> dict:
        f = self.p.faker
        use_code = random_code("address_use", self.rng)
        return {
            "use": use_code["code"] if use_code else "home",
            "type": self.rng.choice(["postal", "physical", "both"]),
            "line": [f.street_address()],
            "city": f.city(),
            "state": f.state_abbr() if hasattr(f, "state_abbr") else f.state(),
            "postalCode": f.postcode(),
            "country": country or "US",
            "text": f.address()
        }

    def gen_ContactPoint(self, system: str | None = None) -> dict:
        system = system or self.rng.choice(["phone", "email", "fax", "url"])
        use_code = random_code("telecom_use", self.rng)
        value_map = {
            "phone": self.p.faker.phone_number(),
            "email": self.p.faker.email(),
            "fax": self.p.faker.phone_number(),
            "url": self.p.faker.url(),
            "sms": self.p.faker.phone_number(),
        }
        return {
            "system": system,
            "value": value_map.get(system, self.p.faker.phone_number()),
            "use": use_code["code"] if use_code else "home",
            "rank": self.p.gen_positiveInt(max_val=5)
        }

    def gen_CodeableConcept(self, system: str | None = None,
                             code: str | None = None,
                             display: str | None = None,
                             text: str | None = None) -> dict:
        return {
            "coding": [self.gen_Coding(system=system, code=code, display=display)],
            "text": text or display or self.p.faker.word()
        }

    def gen_Coding(self, system: str | None = None, code: str | None = None,
                   display: str | None = None, version: str | None = None) -> dict:
        result = {
            "system": system or "http://snomed.info/sct",
            "code": code or self.p.gen_code(),
            "display": display or self.p.faker.sentence(nb_words=3).rstrip(".")
        }
        if version:
            result["version"] = version
        return result

    def gen_Period(self, start_dt: str | None = None, duration_days: int | None = None) -> dict:
        start = start_dt or self.p.gen_dateTime(min_year=2020, max_year=2024)
        if duration_days is None:
            duration_days = self.rng.randint(1, 365)
        from datetime import datetime, timedelta
        start_obj = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_obj = start_obj + timedelta(days=duration_days)
        return {
            "start": start,
            "end": end_obj.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        }

    def gen_Quantity(self, value: float | None = None, unit: str | None = None,
                     system: str = "http://unitsofmeasure.org",
                     code: str | None = None) -> dict:
        return {
            "value": value if value is not None else round(self.rng.uniform(0.1, 500.0), 2),
            "unit": unit or "mg",
            "system": system,
            "code": code or unit or "mg"
        }

    def gen_SimpleQuantity(self, **kwargs) -> dict:
        q = self.gen_Quantity(**kwargs)
        q.pop("comparator", None)
        return q

    def gen_Range(self, low_val: float | None = None, high_val: float | None = None,
                  unit: str = "mg") -> dict:
        low = low_val if low_val is not None else round(self.rng.uniform(0, 100), 1)
        high = high_val if high_val is not None else round(low + self.rng.uniform(10, 100), 1)
        return {
            "low": self.gen_SimpleQuantity(value=low, unit=unit),
            "high": self.gen_SimpleQuantity(value=high, unit=unit)
        }

    def gen_Ratio(self, unit: str = "mg/mL") -> dict:
        return {
            "numerator": self.gen_Quantity(value=round(self.rng.uniform(1, 100), 2), unit="mg"),
            "denominator": self.gen_Quantity(value=round(self.rng.uniform(1, 10), 1), unit="mL")
        }

    def gen_Annotation(self, author_ref: dict | None = None) -> dict:
        result: dict = {"text": self.p.gen_markdown()}
        if author_ref:
            result["authorReference"] = author_ref
        else:
            result["authorString"] = self.p.faker.name()
        result["time"] = self.p.gen_dateTime()
        return result

    def gen_Attachment(self, content_type: str | None = None) -> dict:
        ct = content_type or self.rng.choice(["application/pdf", "image/jpeg", "text/plain"])
        return {
            "contentType": ct,
            "language": "en",
            "data": self.p.gen_base64Binary(byte_count=32),
            "title": self.p.faker.sentence(nb_words=4).rstrip("."),
            "creation": self.p.gen_dateTime()
        }

    def gen_Money(self, currency: str = "USD") -> dict:
        return {
            "value": round(self.rng.uniform(10.0, 5000.0), 2),
            "currency": currency
        }

    def gen_Timing(self) -> dict:
        timing = random_code("dosage_timing", self.rng)
        result: dict = {
            "repeat": {
                "frequency": timing.get("frequency", 1) if timing else 1,
                "period": timing.get("period", 1) if timing else 1,
                "periodUnit": timing.get("period_unit", "d") if timing else "d",
                "boundsPeriod": self.gen_Period()
            }
        }
        if timing:
            result["code"] = self.gen_CodeableConcept(
                system=get_system("dosage_timing"),
                code=timing["code"],
                display=timing.get("display")
            )
        return result

    def gen_Age(self, value: float | None = None) -> dict:
        return {
            "value": value or float(self.rng.randint(1, 100)),
            "unit": "years",
            "system": "http://unitsofmeasure.org",
            "code": "a"
        }

    def gen_Duration(self, value: float | None = None, unit: str = "d") -> dict:
        unit_map = {"d": "days", "h": "hours", "min": "minutes", "s": "seconds",
                    "wk": "weeks", "mo": "months", "a": "years"}
        return {
            "value": value or float(self.rng.randint(1, 30)),
            "unit": unit_map.get(unit, unit),
            "system": "http://unitsofmeasure.org",
            "code": unit
        }

    def gen_SampledData(self) -> dict:
        count = self.rng.randint(10, 50)
        data = " ".join(str(round(self.rng.uniform(-100, 100), 2)) for _ in range(count))
        return {
            "origin": self.gen_Quantity(value=0.0, unit="uV"),
            "interval": round(self.rng.uniform(0.001, 1.0), 3),
            "intervalUnit": "s",
            "factor": 1.0,
            "lowerLimit": -200.0,
            "upperLimit": 200.0,
            "dimensions": 1,
            "data": data
        }

    def gen_Count(self, **kwargs) -> dict:
        return self.gen_Quantity(**kwargs)

    def gen_Distance(self, value: float | None = None, unit: str = "km", **_) -> dict:
        return self.gen_Quantity(value=value, unit=unit, code=unit)

    def gen_MoneyQuantity(self, **kwargs) -> dict:
        return self.gen_Quantity(**kwargs)

    def gen_RatioRange(self, **_) -> dict:
        return {"lowRatio": self.gen_Ratio(), "highRatio": self.gen_Ratio()}

    def gen_Signature(self) -> dict:
        return {
            "type": [self.gen_Coding(system="http://hl7.org/fhir/signature-type", code="1.2.840.10065.1.12.1.1")],
            "when": self.p.gen_instant(),
            "who": self.gen_Reference(resource_type="Practitioner"),
            "data": self.p.gen_base64Binary(byte_count=16),
        }
```

Implement all types in `FHIR_DATATYPES.txt` §2. `BackboneElement` / nested types use schema backbone logic (Prompt 8).
```

---

## PROMPT 6: Special & Metadata Datatype Generators

```
Create `fhir_gen/generators/special_types.py`.

Implement generators for FHIR R5 Special Purpose and Metadata types.
Extends `ComplexTypeGenerator`. These types appear in resource meta, narrative, references, and dosage.

```python
from .complex_types import ComplexTypeGenerator

class SpecialTypeGenerator(ComplexTypeGenerator):

    def gen_Meta(self, profile: str | None = None) -> dict:
        """FHIR Meta — resource metadata."""
        import uuid
        result: dict = {
            "versionId": str(self.rng.randint(1, 10)),
            "lastUpdated": self.p.gen_instant(),
            "source": f"urn:uuid:{uuid.uuid4()}"
        }
        if profile:
            result["profile"] = [profile]
        return result

    def gen_Narrative(self, status: str = "generated", text: str | None = None) -> dict:
        """FHIR Narrative — human-readable summary."""
        return {
            "status": status,
            "div": text or self.p.gen_xhtml()
        }

    def gen_Reference(self, resource_type: str | None = None,
                      resource_id: str | None = None,
                      display: str | None = None) -> dict:
        """FHIR Reference — link to another resource."""
        import uuid
        rid = resource_id or str(uuid.uuid4())
        result: dict = {}
        if resource_type:
            result["reference"] = f"{resource_type}/{rid}"
            result["type"] = resource_type
        else:
            result["reference"] = f"urn:uuid:{rid}"
        if display:
            result["display"] = display
        return result

    def gen_Extension(self, url: str | None = None, value_type: str = "string") -> dict:
        """FHIR Extension."""
        url = url or f"http://example.org/fhir/StructureDefinition/{self.p.faker.word()}"
        value_map = {
            "string": {"valueString": self.p.gen_string()},
            "boolean": {"valueBoolean": self.p.gen_boolean()},
            "integer": {"valueInteger": self.p.gen_integer(0, 100)},
            "decimal": {"valueDecimal": self.p.gen_decimal(0, 10)},
            "code": {"valueCode": self.p.gen_code()},
            "uri": {"valueUri": self.p.gen_uri()},
        }
        ext = {"url": url}
        ext.update(value_map.get(value_type, value_map["string"]))
        return ext

    def gen_Dosage(self, route_code: dict | None = None) -> dict:
        """FHIR Dosage — medication dosage instructions."""
        from ..codes.loader import random_code, get_system
        route = route_code or random_code("dosage_routes", self.rng)
        dose_value = round(self.rng.uniform(50.0, 1000.0), 1)
        return {
            "sequence": self.rng.randint(1, 3),
            "text": f"Take {dose_value}mg by mouth",
            "timing": self.gen_Timing(),
            "route": self.gen_CodeableConcept(
                system=get_system("dosage_routes"),
                code=route["code"] if route else "26643006",
                display=route["display"] if route else "Oral route"
            ),
            "doseAndRate": [{
                "type": self.gen_CodeableConcept(
                    system="http://terminology.hl7.org/CodeSystem/dose-rate-type",
                    code="ordered",
                    display="Ordered"
                ),
                "doseQuantity": self.gen_Quantity(
                    value=dose_value,
                    unit="mg",
                    code="mg"
                )
            }],
            "maxDosePerPeriod": [self.gen_Ratio()]
        }

    def gen_ContactDetail(self, name: str | None = None) -> dict:
        return {
            "name": name or self.p.faker.name(),
            "telecom": [self.gen_ContactPoint("email"), self.gen_ContactPoint("phone")]
        }

    def gen_UsageContext(self) -> dict:
        return {
            "code": self.gen_Coding(
                system="http://terminology.hl7.org/CodeSystem/usage-context-type",
                code=self.rng.choice(["gender", "age", "focus", "user", "workflow", "task"]),
            ),
            "valueCodeableConcept": self.gen_CodeableConcept()
        }

    def gen_DataRequirement(self) -> dict:
        return {
            "type": self.rng.choice(["Patient", "Observation", "Condition", "MedicationRequest"]),
            "mustSupport": [self.p.faker.word() for _ in range(self.rng.randint(1, 3))]
        }

    def gen_Expression(self) -> dict:
        return {
            "language": self.rng.choice(["text/fhirpath", "text/cql", "application/x-fhir-query"]),
            "expression": f"Patient.where(id = '{self.p.gen_id()}')"
        }

    def gen_TriggerDefinition(self) -> dict:
        return {
            "type": self.rng.choice(["named-event", "periodic", "data-changed",
                                     "data-accessed", "data-access-ended"]),
            "name": self.p.faker.word()
        }

    def gen_Availability(self) -> dict:
        return {"availableTime": [self.gen_Timing()], "notAvailableTime": []}

    def gen_Contributor(self) -> dict:
        return {"type": self.rng.choice(["author", "editor", "reviewer", "endorser"]),
                "name": self.p.faker.name(), "contact": [self.gen_ContactDetail()]}

    def gen_ExtendedContactDetail(self) -> dict:
        return {"purpose": self.gen_CodeableConcept(), "name": [self.gen_HumanName()],
                "telecom": [self.gen_ContactPoint()]}

    def gen_MonetaryComponent(self) -> dict:
        return {"type": self.rng.choice(["base", "surcharge", "deduction", "discount", "tax", "informational"]),
                "code": self.gen_CodeableConcept(), "factor": self.p.gen_decimal(0.8, 1.2),
                "amount": self.gen_Money()}

    def gen_ParameterDefinition(self) -> dict:
        return {"name": self.p.faker.word(), "use": self.rng.choice(["in", "out"]),
                "type": self.rng.choice(["string", "integer", "boolean", "CodeableConcept"])}

    def gen_VirtualServiceDetail(self) -> dict:
        return {"channelType": self.gen_Coding(system="http://terminology.hl7.org/CodeSystem/service-mode",
                                               code=self.rng.choice(["phone", "video", "chat"]))}

    def gen_ElementDefinition(self) -> dict:
        return {"path": self.p.faker.word(), "min": 0, "max": "1",
                "type": [{"code": self.rng.choice(["string", "CodeableConcept", "Reference"])}]}
```

Cover `FHIR_DATATYPES.txt` §3–4 (Metadata + Special). Rotate `value_type` in `gen_Extension`.
```

---

## PROMPT 7: Reference Resolver & Dependency Graph

```
Create `fhir_gen/resolvers/dependency.py` and `fhir_gen/resolvers/reference.py`.

### dependency.py
Builds a dependency graph of FHIR resource references to determine generation order.

```python
from collections import defaultdict, deque
from ..schema.registry import registry

# Resources that are commonly referenced and should be pre-generated
CORE_DEPENDENCIES = {
    "Patient": [],
    "Practitioner": [],
    "Organization": [],
    "Location": [],
    "PractitionerRole": ["Practitioner", "Organization"],
    "Encounter": ["Patient", "Practitioner", "Organization", "Location"],
    "Observation": ["Patient", "Practitioner", "Encounter"],
    "Condition": ["Patient", "Practitioner", "Encounter"],
    "MedicationRequest": ["Patient", "Practitioner", "Encounter", "Medication"],
    "MedicationAdministration": ["Patient", "Practitioner", "Encounter", "Medication"],
    "MedicationDispense": ["Patient", "Practitioner", "MedicationRequest"],
    "AllergyIntolerance": ["Patient", "Practitioner", "Encounter"],
    "Procedure": ["Patient", "Practitioner", "Encounter", "Location"],
    "DiagnosticReport": ["Patient", "Practitioner", "Encounter", "Observation"],
    "ServiceRequest": ["Patient", "Practitioner", "Encounter"],
    "CarePlan": ["Patient", "Practitioner", "CareTeam", "Condition", "Goal"],
    "CareTeam": ["Patient", "Practitioner", "Organization"],
    "Goal": ["Patient"],
    "Immunization": ["Patient", "Practitioner", "Location"],
    "Coverage": ["Patient", "Organization"],
    "Claim": ["Patient", "Practitioner", "Organization", "Coverage", "Encounter"],
    "ClaimResponse": ["Patient", "Practitioner", "Organization", "Claim"],
    "Appointment": ["Patient", "Practitioner", "Location"],
    "DocumentReference": ["Patient", "Practitioner", "Organization"],
    "Medication": [],
    "Substance": [],
    "Device": ["Organization"],
    "Group": [],
    "RelatedPerson": ["Patient"],
    "Schedule": ["Practitioner", "Location"],
    "Slot": ["Schedule"],
    "EpisodeOfCare": ["Patient", "Organization", "Practitioner"],
    "HealthcareService": ["Organization", "Location"],
    "RiskAssessment": ["Patient", "Practitioner", "Encounter"],
    "Task": ["Patient", "Practitioner"],
    "Communication": ["Patient", "Practitioner", "Encounter"],
    "Flag": ["Patient", "Practitioner"],
    "AuditEvent": ["Patient", "Practitioner"],
    "Consent": ["Patient", "Organization"],
    "Contract": ["Patient", "Organization"],
    "NutritionOrder": ["Patient", "Practitioner", "Encounter"],
    "Specimen": ["Patient", "Practitioner"],
    "ImagingStudy": ["Patient", "Practitioner", "Encounter"],
    "FamilyMemberHistory": ["Patient"],
    "ClinicalImpression": ["Patient", "Practitioner", "Encounter"],
    "DetectedIssue": ["Patient", "Practitioner"],
    "QuestionnaireResponse": ["Patient", "Practitioner"],
    "PaymentNotice": ["Organization", "Practitioner"],
    "PaymentReconciliation": ["Organization"],
    "Account": ["Patient", "Organization"],
    "ChargeItem": ["Patient", "Practitioner", "Encounter"],
    "Invoice": ["Patient", "Practitioner", "Organization"],
    "ResearchStudy": ["Organization", "Practitioner"],
    "ResearchSubject": ["Patient", "ResearchStudy"],
}

def resolve_order(resource_names: list[str]) -> list[str]:
    """
    Topological sort of resource names based on dependency graph.
    Returns ordered list — dependencies come before dependents.
    Uses CORE_DEPENDENCIES for known resources; for others call
    `registry.references_for(name)` to add schema-derived Reference targets.
    Raises ValueError for circular dependencies.
    """
    # Build full graph including transitive deps
    all_nodes: set[str] = set()
    for name in resource_names:
        all_nodes.add(name)
        for dep in CORE_DEPENDENCIES.get(name, []):
            all_nodes.add(dep)

    # Kahn's algorithm
    in_degree: dict[str, int] = defaultdict(int)
    graph: dict[str, list[str]] = defaultdict(list)

    for node in all_nodes:
        for dep in CORE_DEPENDENCIES.get(node, []):
            if dep in all_nodes:
                graph[dep].append(node)
                in_degree[node] += 1

    queue = deque(n for n in all_nodes if in_degree[n] == 0)
    result = []
    while queue:
        node = queue.popleft()
        result.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(all_nodes):
        raise ValueError("Circular dependency detected in resource graph")

    # Return only originally requested resources (plus required deps)
    requested_set = set(resource_names)
    return [r for r in result if r in all_nodes]
```

### reference.py
```python
import uuid
from typing import Any

class ReferenceStore:
    """
    In-memory store of generated resources for cross-referencing.
    Maps resource_type -> list of {id, display, resource} dicts.
    """

    def __init__(self):
        self._store: dict[str, list[dict]] = {}

    def register(self, resource: dict) -> None:
        """Register a generated resource for future referencing."""
        rtype = resource.get("resourceType")
        rid = resource.get("id")
        if not rtype or not rid:
            return
        entry = {
            "id": rid,
            "reference": f"{rtype}/{rid}",
            "display": self._extract_display(resource, rtype),
            "resource": resource
        }
        self._store.setdefault(rtype, []).append(entry)

    def get_reference(self, resource_type: str, rng) -> dict | None:
        """Return a FHIR Reference dict for a random registered resource of given type."""
        entries = self._store.get(resource_type, [])
        if not entries:
            return None
        entry = rng.choice(entries)
        return {
            "reference": entry["reference"],
            "type": resource_type,
            "display": entry["display"]
        }

    def get_id(self, resource_type: str, rng) -> str | None:
        entries = self._store.get(resource_type, [])
        if not entries:
            return None
        return rng.choice(entries)["id"]

    def has(self, resource_type: str) -> bool:
        return bool(self._store.get(resource_type))

    def count(self, resource_type: str) -> int:
        return len(self._store.get(resource_type, []))

    def _extract_display(self, resource: dict, rtype: str) -> str:
        """Extract human-readable display string from resource."""
        if rtype == "Patient":
            names = resource.get("name", [])
            if names:
                n = names[0]
                given = " ".join(n.get("given", []))
                family = n.get("family", "")
                return f"{given} {family}".strip()
        elif rtype in ("Practitioner", "RelatedPerson"):
            names = resource.get("name", [])
            if names:
                n = names[0]
                return f"Dr. {n.get('family', '')}"
        elif rtype == "Organization":
            return resource.get("name", "Unknown Organization")
        elif rtype == "Medication":
            cc = resource.get("code", {})
            codings = cc.get("coding", [{}])
            return codings[0].get("display", "Unknown Medication") if codings else "Unknown Medication"
        return f"{rtype}/{resource.get('id', 'unknown')}"

    def clear(self, resource_type: str | None = None) -> None:
        if resource_type:
            self._store.pop(resource_type, None)
        else:
            self._store.clear()
```
```

---

## PROMPT 8: Base Resource Generator Engine

```
Create `fhir_gen/generators/base.py`.

This is the core engine that drives generation of ANY FHIR resource using the schema registry.
It handles polymorphic fields, required fields, references, and schema-driven generation.

```python
import uuid
import random
from pathlib import Path
from typing import Any
from ..schema.registry import registry, SchemaRegistry
from ..schema.parser import FieldDef, ResourceDef
from ..resolvers.reference import ReferenceStore
from ..resolvers.dependency import resolve_order, CORE_DEPENDENCIES
from ..generators.special_types import SpecialTypeGenerator
from ..generators.primitives import PrimitiveGenerator
from ..codes.loader import random_code, get_codes, get_system


class ResourceGenerator:
    """
    Generic FHIR R5 resource generator.
    Generates any resource from the schema, handling all field types,
    polymorphism, and inter-resource references.
    """

    # Fields to always skip (FHIR infrastructure, not data)
    SKIP_FIELDS = {"resourceType", "id", "meta", "implicitRules", "language",
                   "text", "contained", "extension", "modifierExtension"}

    # Probability of generating optional fields
    OPTIONAL_FIELD_PROB = 0.7

    def __init__(self, seed: int | None = None, store: ReferenceStore | None = None):
        self.seed = seed
        self.rng = random.Random(seed)
        self._prim = PrimitiveGenerator(seed=seed)
        self._types = SpecialTypeGenerator(self._prim)
        self._store = store or ReferenceStore()
        self._registry = registry

    @property
    def store(self) -> ReferenceStore:
        return self._store

    def generate(self, resource_type: str, count: int = 1,
                 overrides: dict | None = None,
                 schema_path: str | None = None) -> list[dict]:
        """
        Generate `count` instances of `resource_type`.
        Automatically generates required dependencies first if not in store.
        `overrides`: field values to force on every generated resource.
        `schema_path`: optional alternate JSON schema (INSTRUCTIONS #6).
        """
        if schema_path:
            from ..schema.registry import SchemaRegistry
            SchemaRegistry.reload(Path(schema_path))
            self._registry = SchemaRegistry.get()
        # Resolve and pre-generate dependencies
        deps = resolve_order([resource_type])
        for dep in deps:
            if dep != resource_type and not self._store.has(dep):
                self._generate_one(dep)

        results = []
        for _ in range(count):
            resource = self._generate_one(resource_type, overrides=overrides)
            results.append(resource)
        return results

    def generate_many(self, resource_types: list[str],
                      counts: dict[str, int] | None = None) -> dict[str, list[dict]]:
        """
        Generate multiple resource types in dependency order.
        counts: {resource_type: n} — defaults to 1 per type.
        Returns dict of {resource_type: [resources]}.
        """
        counts = counts or {}
        ordered = resolve_order(resource_types)
        all_needed = set(ordered)
        # Also resolve deps not in the original list
        for rt in resource_types:
            for dep in CORE_DEPENDENCIES.get(rt, []):
                all_needed.add(dep)
        ordered_all = resolve_order(list(all_needed))

        results: dict[str, list[dict]] = {}
        for rt in ordered_all:
            n = counts.get(rt, 1)
            generated = []
            for _ in range(n):
                r = self._generate_one(rt)
                generated.append(r)
            results[rt] = generated
        return results

    def generate_variants(self, resource_type: str,
                          variant_fields: list[str] | None = None) -> list[dict]:
        """
        INSTRUCTIONS #7: emit one document per polymorphic/choice variant.
        For each group in `poly_groups` (or `variant_fields` subset), force each
        variant key in turn; return len(variants) resources with shared deps generated once.
        """
        resource_def = self._registry.definition(resource_type)
        groups = resource_def.poly_groups
        if variant_fields:
            groups = {k: v for k, v in groups.items() if k in variant_fields}
        if not groups:
            return self.generate(resource_type, count=1)

        deps = resolve_order([resource_type])
        for dep in deps:
            if dep != resource_type and not self._store.has(dep):
                self._generate_one(dep)

        variants: list[dict] = []
        for base, keys in groups.items():
            for key in keys:
                overrides = {k: None for k in keys if k != key}  # strip siblings later
                resource = self._generate_one(resource_type)
                for sib in keys:
                    resource.pop(sib, None)
                field = resource_def.fields.get(key)
                if field:
                    resource[key] = self._generate_field(field, resource_type)
                variants.append(resource)
                self._store.register(resource)
        return variants

    def _generate_one(self, resource_type: str, overrides: dict | None = None) -> dict:
        """Generate a single resource instance."""
        resource_def = self._registry.definition(resource_type)

        resource: dict[str, Any] = {
            "resourceType": resource_type,
            "id": str(uuid.uuid4()),
            "meta": self._types.gen_Meta()
        }

        # Generate required fields first
        for field_name in resource_def.required:
            if field_name in self.SKIP_FIELDS:
                continue
            field = resource_def.fields.get(field_name)
            if field:
                value = self._generate_field(field, resource_type)
                if value is not None:
                    resource[field_name] = value

        # Generate optional fields
        for field_name, field in resource_def.fields.items():
            if field_name in self.SKIP_FIELDS or field_name in resource:
                continue
            if field_name.startswith("_"):  # Extension shadow field
                continue
            if self.rng.random() > self.OPTIONAL_FIELD_PROB:
                continue
            value = self._generate_field(field, resource_type)
            if value is not None:
                resource[field_name] = value

        # Handle polymorphic field groups — pick ONE variant per group
        for base, variants in resource_def.poly_groups.items():
            # Remove any already set (required ones)
            existing = [v for v in variants if v in resource]
            if existing:
                # Remove others
                for v in variants:
                    if v not in existing:
                        resource.pop(v, None)
            else:
                chosen = self.rng.choice(variants)
                field = resource_def.fields.get(chosen)
                if field:
                    val = self._generate_field(field, resource_type)
                    if val is not None:
                        resource[chosen] = val

        # Apply resource-specific enrichment
        resource = self._enrich(resource, resource_type)

        # Apply overrides
        if overrides:
            resource.update(overrides)

        self._store.register(resource)
        return resource

    def _generate_field(self, field: FieldDef, context_resource: str) -> Any:
        """Generate a value for a single field based on its type."""
        if field.const_value is not None:
            return field.const_value

        ref = field.ref
        if ref is None:
            return self._prim.gen_string()

        # Primitive type
        if field.is_primitive:
            value = self._prim.generate(ref)
            if field.is_array:
                return [value]
            return value

        # Reference to another resource
        if ref == "Reference":
            return self._generate_reference_field(field)

        # Known complex types
        if hasattr(self._types, f"gen_{ref}"):
            value = getattr(self._types, f"gen_{ref}")()
            return [value] if field.is_array else value

        # Nested backbone element (defined inline in schema)
        nested_def = None
        try:
            nested_def = self._registry.definition(ref)
        except Exception:
            pass

        if nested_def:
            nested = self._generate_backbone(nested_def, context_resource)
            return [nested] if field.is_array else nested

        return None

    def _generate_reference_field(self, field: FieldDef) -> dict:
        """
        Generate a FHIR Reference. Use store if available, else placeholder.
        Infers target resource type from field description/name.
        """
        # Common field name → resource type mappings
        target_map = {
            "subject": ["Patient", "Group"],
            "patient": ["Patient"],
            "performer": ["Practitioner", "PractitionerRole", "Organization"],
            "author": ["Practitioner", "PractitionerRole"],
            "requester": ["Practitioner", "PractitionerRole"],
            "recorder": ["Practitioner", "PractitionerRole"],
            "asserter": ["Practitioner", "PractitionerRole"],
            "encounter": ["Encounter"],
            "organization": ["Organization"],
            "managingOrganization": ["Organization"],
            "location": ["Location"],
            "medication": ["Medication"],
            "careTeam": ["CareTeam"],
            "coverage": ["Coverage"],
            "insurer": ["Organization"],
            "provider": ["Practitioner", "PractitionerRole", "Organization"],
            "basedOn": ["ServiceRequest", "CarePlan"],
            "partOf": ["Procedure", "Observation"],
            "hasMember": ["Observation"],
            "derivedFrom": ["Observation", "QuestionnaireResponse"],
            "specimen": ["Specimen"],
            "device": ["Device"],
            "goal": ["Goal"],
            "condition": ["Condition"],
            "focus": ["Condition", "Observation"],
            "reasonReference": ["Condition", "Observation"],
        }
        fname = field.name
        candidates = target_map.get(fname, [])

        for candidate in candidates:
            if self._store.has(candidate):
                ref = self._store.get_reference(candidate, self.rng)
                if ref:
                    return ref

        # Fallback: placeholder reference
        return self._types.gen_Reference(
            resource_type=candidates[0] if candidates else "Resource"
        )

    def _generate_backbone(self, nested_def: ResourceDef, context: str) -> dict:
        """Generate a backbone element (nested object)."""
        result: dict = {}
        for fname, field in nested_def.fields.items():
            if fname.startswith("_"):
                continue
            is_req = fname in nested_def.required
            if not is_req and self.rng.random() > self.OPTIONAL_FIELD_PROB:
                continue
            val = self._generate_field(field, context)
            if val is not None:
                result[fname] = val
        return result

    def _enrich(self, resource: dict, resource_type: str) -> dict:
        """
        Apply resource-type-specific enrichment for clinical correctness.
        Import per-resource enrichers from generators/resources/ modules.
        """
        from .resources import clinical, medication, workflow, financial, specialized
        enrichers = {
            **clinical.ENRICHERS,
            **medication.ENRICHERS,
            **workflow.ENRICHERS,
            **financial.ENRICHERS,
            **specialized.ENRICHERS,
        }
        enricher = enrichers.get(resource_type)
        if enricher:
            return enricher(resource, self._types, self._store, self.rng)
        return resource
```

Update `fhir_gen/__init__.py`:
```python
from .generators.base import ResourceGenerator
from .config import settings
__all__ = ["ResourceGenerator", "settings"]
```
```

---

## PROMPT 9: Clinical Core Resources

```
Create `fhir_gen/generators/resources/clinical.py`.

Optional enrichers for high-traffic resources from `FHIR_RESOURCE_URLS.txt`. Resources without an enricher still generate via Prompt 8 (schema-only). Each enricher takes
(resource, types: SpecialTypeGenerator, store: ReferenceStore, rng: Random) and returns enriched dict.
Import codes from loader. These override/supplement schema-generated fields with clinically correct data.

```python
import uuid
import random
from datetime import datetime, timedelta
from ...codes.loader import random_code, get_codes, get_system
from ...resolvers.reference import ReferenceStore
from ..special_types import SpecialTypeGenerator


def enrich_Patient(r, t, store, rng):
    from faker import Faker
    f = Faker()
    gender = rng.choice(["male", "female", "other", "unknown"])
    r["gender"] = gender
    # Realistic birthDate (5-95 years ago)
    days_ago = rng.randint(5*365, 95*365)
    bd = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    r["birthDate"] = bd
    # Names
    r["name"] = [t.gen_HumanName(use="official"), t.gen_HumanName(use="nickname")]
    # Telecom
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    # Address
    r["address"] = [t.gen_Address()]
    # Identifiers (MRN + SSN)
    r["identifier"] = [
        t.gen_Identifier(system="http://hospital.example.org/mrn"),
        t.gen_Identifier(system="http://hl7.org/fhir/sid/us-ssn")
    ]
    # Marital status
    ms = random_code("marital_status", rng)
    if ms:
        r["maritalStatus"] = t.gen_CodeableConcept(
            system=get_system("marital_status"), code=ms["code"], display=ms["display"]
        )
    # Language
    lang = random_code("languages", rng)
    if lang:
        r["communication"] = [{"language": t.gen_CodeableConcept(
            system=lang.get("system"), code=lang["code"], display=lang.get("display")
        ), "preferred": True}]
    r["active"] = rng.random() > 0.05  # 95% active
    r["deceasedBoolean"] = rng.random() < 0.05  # 5% deceased
    return r


def enrich_Practitioner(r, t, store, rng):
    r["active"] = True
    r["name"] = [t.gen_HumanName(use="official")]
    r["name"][0]["prefix"] = [rng.choice(["Dr.", "Prof.", "Mr.", "Ms.", "Mrs."])]
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    r["address"] = [t.gen_Address()]
    r["gender"] = rng.choice(["male", "female", "other", "unknown"])
    r["identifier"] = [t.gen_Identifier(
        system="http://hl7.org/fhir/sid/us-npi",
        value=str(rng.randint(1000000000, 9999999999))
    )]
    spec = random_code("specialties", rng)
    if spec:
        r["qualification"] = [{
            "code": t.gen_CodeableConcept(
                system=get_system("specialties"), code=spec["code"], display=spec["display"]
            ),
            "identifier": [t.gen_Identifier()],
            "period": t.gen_Period()
        }]
    return r


def enrich_PractitionerRole(r, t, store, rng):
    r["active"] = True
    r["period"] = t.gen_Period()
    if store.has("Practitioner"):
        r["practitioner"] = store.get_reference("Practitioner", rng)
    if store.has("Organization"):
        r["organization"] = store.get_reference("Organization", rng)
    role = random_code("practitioner_roles", rng)
    if role:
        r["code"] = [t.gen_CodeableConcept(
            system=get_system("practitioner_roles"), code=role["code"], display=role["display"]
        )]
    spec = random_code("specialties", rng)
    if spec:
        r["specialty"] = [t.gen_CodeableConcept(
            system=get_system("specialties"), code=spec["code"], display=spec["display"]
        )]
    if store.has("Location"):
        r["location"] = [store.get_reference("Location", rng)]
    return r


def enrich_Organization(r, t, store, rng):
    from faker import Faker
    f = Faker()
    r["active"] = True
    r["name"] = rng.choice([
        f"{f.last_name()} Medical Center",
        f"{f.city()} General Hospital",
        f"{f.last_name()} Clinic",
        f"St. {f.first_name()} Healthcare",
        f"{f.city()} Health System"
    ])
    org_type = random_code("organization_types", rng)
    if org_type:
        r["type"] = [t.gen_CodeableConcept(
            system=get_system("organization_types"), code=org_type["code"], display=org_type["display"]
        )]
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    r["address"] = [t.gen_Address()]
    r["identifier"] = [t.gen_Identifier(
        system="http://hl7.org/fhir/sid/us-npi",
        value=str(rng.randint(1000000000, 9999999999))
    )]
    return r


def enrich_Location(r, t, store, rng):
    loc_type = random_code("location_types", rng)
    r["status"] = rng.choice(["active", "inactive", "suspended"])
    r["mode"] = "instance"
    r["name"] = f"{t.p.faker.word().title()} {loc_type['display'] if loc_type else 'Unit'}"
    if loc_type:
        r["type"] = [t.gen_CodeableConcept(
            system=get_system("location_types"), code=loc_type["code"], display=loc_type["display"]
        )]
    r["address"] = t.gen_Address()
    r["telecom"] = [t.gen_ContactPoint("phone")]
    if store.has("Organization"):
        r["managingOrganization"] = store.get_reference("Organization", rng)
    r["physicalType"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/location-physical-type",
        code=rng.choice(["si", "bu", "wi", "wa", "lvl", "ro", "bd", "ve"]),
        display=rng.choice(["Site", "Building", "Wing", "Ward", "Level", "Room", "Bed", "Vehicle"])
    )
    return r


def enrich_Encounter(r, t, store, rng):
    status = random_code("encounter_status", rng)
    r["status"] = status["code"] if status else "finished"
    enc_class = random_code("encounter_class", rng)
    r["class"] = [t.gen_CodeableConcept(
        system=get_system("encounter_class"),
        code=enc_class["code"] if enc_class else "AMB",
        display=enc_class["display"] if enc_class else "ambulatory"
    )]
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["participant"] = [{
            "type": [t.gen_CodeableConcept(
                system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                code="ATND", display="attender"
            )],
            "actor": store.get_reference("Practitioner", rng)
        }]
    r["actualPeriod"] = t.gen_Period()
    if store.has("Organization"):
        r["serviceProvider"] = store.get_reference("Organization", rng)
    if store.has("Location"):
        r["location"] = [{
            "location": store.get_reference("Location", rng),
            "status": rng.choice(["planned", "active", "reserved", "completed"])
        }]
    r["identifier"] = [t.gen_Identifier(system="http://hospital.example.org/encounters")]
    return r


def enrich_Condition(r, t, store, rng):
    # Clinical status
    cs = random_code("condition_clinical_status", rng)
    r["clinicalStatus"] = t.gen_CodeableConcept(
        system=get_system("condition_clinical_status"),
        code=cs["code"] if cs else "active"
    )
    vs = random_code("condition_verification_status", rng)
    r["verificationStatus"] = t.gen_CodeableConcept(
        system=get_system("condition_verification_status"),
        code=vs["code"] if vs else "confirmed"
    )
    sev = random_code("condition_severity", rng)
    if sev:
        r["severity"] = t.gen_CodeableConcept(
            system=get_system("condition_severity"), code=sev["code"], display=sev["display"]
        )
    # SNOMED condition code
    cond = random_code("snomed_conditions", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("snomed_conditions"),
        code=cond["code"] if cond else "73211009",
        display=cond["display"] if cond else "Diabetes mellitus"
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["recorder"] = store.get_reference("Practitioner", rng)
    r["onsetDateTime"] = t.p.gen_dateTime(min_year=2015, max_year=2023)
    r["recordedDate"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/condition-category",
        code=rng.choice(["problem-list-item", "encounter-diagnosis"]),
        display=rng.choice(["Problem List Item", "Encounter Diagnosis"])
    )]
    return r


def enrich_Observation(r, t, store, rng):
    r["status"] = rng.choice(["registered", "preliminary", "final", "amended"])
    cat = random_code("observation_categories", rng)
    r["category"] = [t.gen_CodeableConcept(
        system=get_system("observation_categories"),
        code=cat["code"] if cat else "vital-signs",
        display=cat["display"] if cat else "Vital Signs"
    )]
    # LOINC code with realistic value
    loinc = random_code("loinc_observations", rng)
    if loinc:
        r["code"] = t.gen_CodeableConcept(
            system=get_system("loinc_observations"),
            code=loinc["code"],
            display=loinc["display"]
        )
        if not loinc.get("is_panel") and "low" in loinc and "high" in loinc:
            val = round(rng.uniform(loinc.get("typical_low", loinc["low"]),
                                    loinc.get("typical_high", loinc["high"])), 2)
            r["valueQuantity"] = t.gen_Quantity(
                value=val, unit=loinc.get("unit"), code=loinc.get("ucum"),
                system="http://unitsofmeasure.org"
            )
            r["referenceRange"] = [{
                "low": t.gen_SimpleQuantity(value=loinc["low"], unit=loinc.get("unit")),
                "high": t.gen_SimpleQuantity(value=loinc["high"], unit=loinc.get("unit"))
            }]
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [store.get_reference("Practitioner", rng)]
    r["effectiveDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["issued"] = t.p.gen_instant()
    return r


def enrich_AllergyIntolerance(r, t, store, rng):
    cs = random_code("allergy_clinical_status", rng)
    r["clinicalStatus"] = t.gen_CodeableConcept(
        system=get_system("allergy_clinical_status"), code=cs["code"] if cs else "active"
    )
    vs = random_code("allergy_verification_status", rng)
    r["verificationStatus"] = t.gen_CodeableConcept(
        system=get_system("allergy_verification_status"), code=vs["code"] if vs else "confirmed"
    )
    r["type"] = t.gen_CodeableConcept(
        system="http://hl7.org/fhir/allergy-intolerance-type",
        code=rng.choice(["allergy", "intolerance"])
    )
    r["category"] = [rng.choice(["food", "medication", "environment", "biologic"])]
    r["criticality"] = rng.choice(["low", "high", "unable-to-assess"])
    sub = random_code("allergy_substances", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("allergy_substances"),
        code=sub["code"] if sub else "7980",
        display=sub["display"] if sub else "Penicillin"
    )
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    r["onsetDateTime"] = t.p.gen_dateTime(min_year=2010, max_year=2023)
    r["recordedDate"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    sev = random_code("reaction_severity", rng)
    r["reaction"] = [{
        "substance": t.gen_CodeableConcept(
            system=get_system("allergy_substances"),
            code=sub["code"] if sub else "7980",
            display=sub["display"] if sub else "Penicillin"
        ),
        "manifestation": [t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code=rng.choice(["271807003", "39579001", "418290006", "49727002"]),
            display=rng.choice(["Rash", "Anaphylaxis", "Itching", "Cough"])
        )],
        "severity": sev["code"] if sev else "moderate",
        "onset": t.p.gen_dateTime(min_year=2020, max_year=2023)
    }]
    return r


def enrich_Procedure(r, t, store, rng):
    proc = random_code("snomed_procedures", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("snomed_procedures"),
        code=proc["code"] if proc else "71388002",
        display=proc["display"] if proc else "Procedure"
    )
    r["status"] = rng.choice(["preparation", "in-progress", "completed", "stopped"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    r["occurredDateTime"] = t.p.gen_dateTime(min_year=2022, max_year=2024)
    r["category"] = [t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["387713003", "103693007", "46947000"]),
        display=rng.choice(["Surgical procedure", "Diagnostic procedure", "Chiropractic manipulation"])
    )]
    body = random_code("body_sites", rng)
    if body:
        r["bodySite"] = [t.gen_CodeableConcept(
            system=get_system("body_sites"), code=body["code"], display=body["display"]
        )]
    return r


def enrich_DiagnosticReport(r, t, store, rng):
    status = random_code("diagnostic_report_status", rng)
    r["status"] = status["code"] if status else "final"
    cat = random_code("diagnostic_report_categories", rng)
    r["category"] = [t.gen_CodeableConcept(
        system=get_system("diagnostic_report_categories"),
        code=cat["code"] if cat else "LAB",
        display=cat["display"] if cat else "Laboratory"
    )]
    loinc = random_code("loinc_observations", rng)
    r["code"] = t.gen_CodeableConcept(
        system="http://loinc.org",
        code=loinc["code"] if loinc else "58410-2",
        display=loinc["display"] if loinc else "CBC panel"
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [store.get_reference("Practitioner", rng)]
    r["effectiveDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["issued"] = t.p.gen_instant()
    if store.has("Observation"):
        r["result"] = [store.get_reference("Observation", rng)
                       for _ in range(rng.randint(1, 4))]
    r["conclusion"] = t.p.faker.sentence()
    return r


def enrich_Immunization(r, t, store, rng):
    vaccine = random_code("vaccines", rng)
    r["vaccineCode"] = t.gen_CodeableConcept(
        system=get_system("vaccines"),
        code=vaccine["code"] if vaccine else "88",
        display=vaccine["display"] if vaccine else "Influenza, unspecified formulation"
    )
    r["status"] = rng.choice(["completed", "not-done"])
    r["primarySource"] = rng.random() > 0.2
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    if store.has("Location"):
        r["location"] = store.get_reference("Location", rng)
    r["occurrenceDateTime"] = t.p.gen_dateTime(min_year=2020, max_year=2024)
    body = random_code("body_sites", rng)
    if body:
        r["site"] = t.gen_CodeableConcept(
            system=get_system("body_sites"), code=body["code"], display=body["display"]
        )
    r["route"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-RouteOfAdministration",
        code="IM", display="Injection, intramuscular"
    )
    r["doseQuantity"] = t.gen_Quantity(value=0.5, unit="mL", code="mL")
    return r


def enrich_FamilyMemberHistory(r, t, store, rng):
    from faker import Faker
    f = Faker()
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    r["status"] = rng.choice(["partial", "completed", "entered-in-error", "health-unknown"])
    r["relationship"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        code=rng.choice(["MTH", "FTH", "SIB", "CHILD", "GRPRN", "AUNT", "UNCLE"]),
        display=rng.choice(["Mother", "Father", "Sibling", "Child", "Grandparent", "Aunt", "Uncle"])
    )
    cond = random_code("snomed_conditions", rng)
    r["condition"] = [{
        "code": t.gen_CodeableConcept(
            system=get_system("snomed_conditions"),
            code=cond["code"] if cond else "73211009",
            display=cond["display"] if cond else "Diabetes mellitus"
        ),
        "outcome": t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code=rng.choice(["182992009", "370996005", "418715001"]),
            display=rng.choice(["Treatment completed", "Patient well", "Symptom resolved"])
        ),
        "onsetAge": t.gen_Age(value=float(rng.randint(30, 80)))
    }]
    r["name"] = f.first_name()
    r["sex"] = t.gen_CodeableConcept(
        system="http://hl7.org/fhir/administrative-gender",
        code=rng.choice(["male", "female", "other", "unknown"])
    )
    return r


def enrich_ClinicalImpression(r, t, store, rng):
    r["status"] = rng.choice(["in-progress", "completed", "entered-in-error"])
    r["description"] = t.p.faker.sentence()
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = store.get_reference("Practitioner", rng)
    r["effectiveDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    cond = random_code("snomed_conditions", rng)
    r["finding"] = [{
        "item": t.gen_CodeableConcept(
            system=get_system("snomed_conditions"),
            code=cond["code"] if cond else "73211009",
            display=cond["display"] if cond else "Diabetes mellitus"
        )
    }]
    return r


def enrich_RiskAssessment(r, t, store, rng):
    r["status"] = rng.choice(["registered", "preliminary", "final", "amended"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = store.get_reference("Practitioner", rng)
    r["occurrenceDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["method"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code="413556000",
        display="Risk assessment using assessment tool"
    )
    prob = round(rng.uniform(0.0, 1.0), 2)
    r["prediction"] = [{
        "outcome": t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code=rng.choice(["363346000", "414545008", "230690007"]),
            display=rng.choice(["Malignant neoplasm", "Ischemic heart disease", "Stroke"])
        ),
        "probabilityDecimal": prob,
        "whenPeriod": t.gen_Period(),
        "relativeRisk": round(rng.uniform(0.5, 5.0), 2)
    }]
    return r


ENRICHERS = {
    "Patient": enrich_Patient,
    "Practitioner": enrich_Practitioner,
    "PractitionerRole": enrich_PractitionerRole,
    "Organization": enrich_Organization,
    "Location": enrich_Location,
    "Encounter": enrich_Encounter,
    "Condition": enrich_Condition,
    "Observation": enrich_Observation,
    "AllergyIntolerance": enrich_AllergyIntolerance,
    "Procedure": enrich_Procedure,
    "DiagnosticReport": enrich_DiagnosticReport,
    "Immunization": enrich_Immunization,
    "FamilyMemberHistory": enrich_FamilyMemberHistory,
    "ClinicalImpression": enrich_ClinicalImpression,
    "RiskAssessment": enrich_RiskAssessment,
}
```
```

---

## PROMPT 10: Medication Resources

```
Create `fhir_gen/generators/resources/medication.py`.

Implement enricher functions for medication-related FHIR resources.

```python
from ...codes.loader import random_code, get_codes, get_system

def enrich_Medication(r, t, store, rng):
    med = random_code("rxnorm_medications", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("rxnorm_medications"),
        code=med["code"] if med else "313782",
        display=med["display"] if med else "Acetaminophen 325 MG"
    )
    status = random_code("medication_status", rng)
    r["status"] = status["code"] if status else "active"
    if store.has("Organization"):
        r["marketingAuthorizationHolder"] = store.get_reference("Organization", rng)
    r["doseForm"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["385055001", "385061003", "385049006", "385229008"]),
        display=rng.choice(["Tablet dose form", "Capsule dose form", "Oral solution", "Injection"])
    )
    dose_val = round(rng.uniform(50.0, 1000.0), 1)
    r["ingredient"] = [{
        "item": t.gen_CodeableConcept(
            system="http://www.nlm.nih.gov/research/umls/rxnorm",
            code=med["code"] if med else "161",
            display=rng.choice(["Acetaminophen", "Amoxicillin", "Ibuprofen", "Metformin"])
        ),
        "isActive": True,
        "strengthRatio": t.gen_Ratio()
    }]
    return r


def enrich_MedicationRequest(r, t, store, rng):
    status = random_code("medication_request_status", rng)
    r["status"] = status["code"] if status else "active"
    intent = random_code("medication_request_intent", rng)
    r["intent"] = intent["code"] if intent else "order"
    med = random_code("rxnorm_medications", rng)
    r["medication"] = t.gen_CodeableConcept(
        system=get_system("rxnorm_medications"),
        code=med["code"] if med else "313782",
        display=med["display"] if med else "Acetaminophen 325 MG"
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["requester"] = store.get_reference("Practitioner", rng)
        r["recorder"] = store.get_reference("Practitioner", rng)
    r["authoredOn"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["dosageInstruction"] = [t.gen_Dosage()]
    r["dispenseRequest"] = {
        "validityPeriod": t.gen_Period(),
        "numberOfRepeatsAllowed": rng.randint(0, 5),
        "quantity": t.gen_SimpleQuantity(value=float(rng.choice([30, 60, 90])), unit="each"),
        "expectedSupplyDuration": t.gen_Duration(value=float(rng.choice([30, 60, 90])), unit="d")
    }
    return r


def enrich_MedicationAdministration(r, t, store, rng):
    r["status"] = rng.choice(["in-progress", "not-done", "on-hold", "completed", "entered-in-error", "stopped"])
    med = random_code("rxnorm_medications", rng)
    r["medication"] = t.gen_CodeableConcept(
        system=get_system("rxnorm_medications"),
        code=med["code"] if med else "313782",
        display=med["display"] if med else "Acetaminophen 325 MG"
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    if store.has("MedicationRequest"):
        r["basedOn"] = [store.get_reference("MedicationRequest", rng)]
    r["occurredDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    route = random_code("dosage_routes", rng)
    r["dosage"] = {
        "route": t.gen_CodeableConcept(
            system=get_system("dosage_routes"),
            code=route["code"] if route else "26643006",
            display=route["display"] if route else "Oral route"
        ),
        "dose": t.gen_Quantity(value=round(rng.uniform(100, 500), 1), unit="mg", code="mg")
    }
    return r


def enrich_MedicationDispense(r, t, store, rng):
    r["status"] = rng.choice(["preparation", "in-progress", "cancelled", "on-hold",
                              "completed", "entered-in-error", "stopped", "declined", "unknown"])
    med = random_code("rxnorm_medications", rng)
    r["medication"] = t.gen_CodeableConcept(
        system=get_system("rxnorm_medications"),
        code=med["code"] if med else "313782",
        display=med["display"] if med else "Acetaminophen 325 MG"
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    if store.has("MedicationRequest"):
        r["authorizingPrescription"] = [store.get_reference("MedicationRequest", rng)]
    r["quantity"] = t.gen_SimpleQuantity(value=float(rng.choice([30, 60, 90])), unit="each")
    r["daysSupply"] = t.gen_SimpleQuantity(value=float(rng.choice([30, 60, 90])), unit="d")
    r["whenHandedOver"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["dosageInstruction"] = [t.gen_Dosage()]
    return r


def enrich_MedicationStatement(r, t, store, rng):
    r["status"] = rng.choice(["recorded", "entered-in-error", "draft"])
    med = random_code("rxnorm_medications", rng)
    r["medication"] = t.gen_CodeableConcept(
        system=get_system("rxnorm_medications"),
        code=med["code"] if med else "313782",
        display=med["display"] if med else "Acetaminophen 325 MG"
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    r["effectiveDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["dateAsserted"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["dosage"] = [t.gen_Dosage()]
    return r


def enrich_MedicationKnowledge(r, t, store, rng):
    med = random_code("rxnorm_medications", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("rxnorm_medications"),
        code=med["code"] if med else "313782",
        display=med["display"] if med else "Acetaminophen 325 MG"
    )
    status = random_code("medication_status", rng)
    r["status"] = t.gen_CodeableConcept(
        system=get_system("medication_status"),
        code=status["code"] if status else "active"
    )
    r["doseForm"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code="385055001", display="Tablet dose form"
    )
    r["indicationGuideline"] = [{
        "indication": [t.gen_CodeableConcept()],
        "dosingGuideline": [{"dosage": [{"type": t.gen_CodeableConcept(), "dosage": [t.gen_Dosage()]}]}]
    }]
    return r


ENRICHERS = {
    "Medication": enrich_Medication,
    "MedicationRequest": enrich_MedicationRequest,
    "MedicationAdministration": enrich_MedicationAdministration,
    "MedicationDispense": enrich_MedicationDispense,
    "MedicationStatement": enrich_MedicationStatement,
    "MedicationKnowledge": enrich_MedicationKnowledge,
}
```
```

---

## PROMPT 11: Workflow & Administrative Resources

```
Create `fhir_gen/generators/resources/workflow.py`.

Implement enrichers for workflow, scheduling, care coordination, and administrative resources.

```python
from ...codes.loader import random_code, get_codes, get_system


def enrich_Appointment(r, t, store, rng):
    status = random_code("appointment_status", rng)
    r["status"] = status["code"] if status else "booked"
    r["start"] = t.p.gen_dateTime(min_year=2024, max_year=2025)
    from datetime import datetime, timedelta
    start = datetime.fromisoformat(r["start"].replace("Z", "+00:00"))
    r["end"] = (start + timedelta(minutes=rng.choice([15, 30, 45, 60]))).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    r["minutesDuration"] = rng.choice([15, 30, 45, 60])
    r["serviceType"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/service-type",
        code=str(rng.randint(1, 600)),
        display=rng.choice(["General Practice", "Specialist", "Physiotherapy", "Cardiology", "Radiology"])
    )]
    participants = []
    if store.has("Patient"):
        participants.append({
            "actor": store.get_reference("Patient", rng),
            "status": "accepted",
            "type": [t.gen_CodeableConcept(
                system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                code="SBJ", display="subject"
            )]
        })
    if store.has("Practitioner"):
        participants.append({
            "actor": store.get_reference("Practitioner", rng),
            "status": rng.choice(["accepted", "tentative"]),
            "type": [t.gen_CodeableConcept(
                system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                code="ATND", display="attender"
            )]
        })
    if store.has("Location"):
        participants.append({
            "actor": store.get_reference("Location", rng),
            "status": "accepted"
        })
    r["participant"] = participants
    r["description"] = t.p.faker.sentence(nb_words=6)
    return r


def enrich_CarePlan(r, t, store, rng):
    r["status"] = rng.choice(["draft", "active", "on-hold", "revoked", "completed"])
    r["intent"] = rng.choice(["proposal", "plan", "order", "option"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["author"] = store.get_reference("Practitioner", rng)
    r["period"] = t.gen_Period()
    r["title"] = rng.choice(["Diabetes Management Plan", "Hypertension Care Plan",
                              "Post-Surgery Recovery Plan", "Cardiac Care Plan",
                              "Mental Health Care Plan"])
    r["description"] = t.p.faker.paragraph(nb_sentences=2)
    r["category"] = [t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["736055001", "735321000", "734163000"]),
        display=rng.choice(["Nursing care plan", "Medication management plan", "Care plan"])
    )]
    if store.has("CareTeam"):
        r["careTeam"] = [store.get_reference("CareTeam", rng)]
    if store.has("Condition"):
        r["addresses"] = [t.gen_CodeableConcept()]
    if store.has("Goal"):
        r["goal"] = [store.get_reference("Goal", rng)]
    return r


def enrich_CareTeam(r, t, store, rng):
    r["status"] = rng.choice(["proposed", "active", "suspended", "inactive", "entered-in-error"])
    r["name"] = rng.choice(["Primary Care Team", "Oncology Team", "Cardiology Team",
                             "Diabetes Management Team", "Mental Health Team"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    r["period"] = t.gen_Period()
    participants = []
    if store.has("Practitioner"):
        for _ in range(rng.randint(1, 3)):
            role = random_code("practitioner_roles", rng)
            participants.append({
                "role": t.gen_CodeableConcept(
                    system=get_system("practitioner_roles"),
                    code=role["code"] if role else "112247003",
                    display=role["display"] if role else "Medical doctor"
                ),
                "member": store.get_reference("Practitioner", rng),
                "period": t.gen_Period()
            })
    r["participant"] = participants
    if store.has("Organization"):
        r["managingOrganization"] = [store.get_reference("Organization", rng)]
    return r


def enrich_Goal(r, t, store, rng):
    r["lifecycleStatus"] = rng.choice(["proposed", "planned", "accepted", "active",
                                        "on-hold", "completed", "cancelled"])
    r["achievementStatus"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/goal-achievement",
        code=rng.choice(["in-progress", "improving", "worsening", "no-change",
                          "achieved", "sustaining", "not-achieved"]),
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    r["description"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["281078001", "395082007", "386490003"]),
        display=rng.choice(["Maintain blood pressure", "Weight reduction", "Blood glucose control"])
    )
    r["startDate"] = t.p.gen_date(min_year=2023, max_year=2024)
    r["target"] = [{
        "measure": t.gen_CodeableConcept(
            system="http://loinc.org",
            code=rng.choice(["8480-6", "29463-7", "4548-4"]),
            display=rng.choice(["Systolic BP", "Body weight", "HbA1c"])
        ),
        "detailQuantity": t.gen_Quantity(value=round(rng.uniform(50, 150), 1), unit="mm[Hg]"),
        "dueDate": t.p.gen_date(min_year=2024, max_year=2025)
    }]
    if store.has("Practitioner"):
        r["expressedBy"] = store.get_reference("Practitioner", rng)
    return r


def enrich_ServiceRequest(r, t, store, rng):
    r["status"] = rng.choice(["draft", "active", "on-hold", "revoked", "completed", "unknown"])
    r["intent"] = rng.choice(["proposal", "plan", "order", "original-order"])
    r["priority"] = rng.choice(["routine", "urgent", "asap", "stat"])
    proc = random_code("snomed_procedures", rng)
    r["code"] = t.gen_CodeableConcept(
        system=get_system("snomed_procedures"),
        code=proc["code"] if proc else "71388002",
        display=proc["display"] if proc else "Procedure"
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["requester"] = store.get_reference("Practitioner", rng)
        r["performer"] = [store.get_reference("Practitioner", rng)]
    r["authoredOn"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["occurrenceDateTime"] = t.p.gen_dateTime(min_year=2024, max_year=2025)
    return r


def enrich_Task(r, t, store, rng):
    r["status"] = rng.choice(["draft", "requested", "received", "accepted",
                               "in-progress", "completed", "cancelled"])
    r["intent"] = rng.choice(["unknown", "proposal", "plan", "order", "original-order"])
    r["priority"] = rng.choice(["routine", "urgent", "asap", "stat"])
    r["code"] = t.gen_CodeableConcept(
        system="http://hl7.org/fhir/CodeSystem/task-code",
        code=rng.choice(["approve", "fulfill", "abort", "replace", "change", "suspend", "resume"]),
    )
    r["description"] = t.p.faker.sentence()
    if store.has("Patient"):
        r["for"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["requester"] = store.get_reference("Practitioner", rng)
        r["owner"] = store.get_reference("Practitioner", rng)
    r["authoredOn"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["lastModified"] = t.p.gen_dateTime(min_year=2024, max_year=2024)
    r["executionPeriod"] = t.gen_Period()
    return r


def enrich_Communication(r, t, store, rng):
    r["status"] = rng.choice(["preparation", "in-progress", "not-done", "on-hold",
                               "stopped", "completed", "entered-in-error", "unknown"])
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/communication-category",
        code=rng.choice(["alert", "notification", "reminder", "instruction"]),
    )]
    r["priority"] = rng.choice(["routine", "urgent", "asap", "stat"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["sender"] = store.get_reference("Practitioner", rng)
        r["recipient"] = [store.get_reference("Practitioner", rng)]
    r["sent"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["payload"] = [{
        "contentString": t.p.faker.paragraph(nb_sentences=2)
    }]
    return r


def enrich_DocumentReference(r, t, store, rng):
    r["status"] = rng.choice(["current", "superseded", "entered-in-error"])
    r["docStatus"] = rng.choice(["preliminary", "final", "amended", "entered-in-error"])
    r["type"] = t.gen_CodeableConcept(
        system="http://loinc.org",
        code=rng.choice(["11488-4", "34117-2", "51847-2", "11506-3"]),
        display=rng.choice(["Consultation Note", "History and Physical Note",
                             "Assessment Note", "Progress Note"])
    )
    r["category"] = [t.gen_CodeableConcept(
        system="http://loinc.org",
        code="clinical-note", display="Clinical Note"
    )]
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["author"] = [store.get_reference("Practitioner", rng)]
    if store.has("Organization"):
        r["custodian"] = store.get_reference("Organization", rng)
    r["date"] = t.p.gen_instant()
    r["content"] = [{
        "attachment": t.gen_Attachment(content_type="text/plain"),
        "profile": [{"valueCoding": t.gen_Coding(
            system="http://ihe.net/fhir/ihe.formatcode.fhir/CodeSystem/formatcode",
            code="urn:ihe:iti:xds:2017:mimeTypeSufficient"
        )}]
    }]
    if store.has("Encounter"):
        r["context"] = [{"encounter": [store.get_reference("Encounter", rng)]}]
    return r


def enrich_Schedule(r, t, store, rng):
    r["active"] = rng.random() > 0.1
    r["serviceType"] = [t.gen_CodeableConcept()]
    spec = random_code("specialties", rng)
    if spec:
        r["specialty"] = [t.gen_CodeableConcept(
            system=get_system("specialties"), code=spec["code"], display=spec["display"]
        )]
    actors = []
    if store.has("Practitioner"):
        actors.append(store.get_reference("Practitioner", rng))
    if store.has("Location"):
        actors.append(store.get_reference("Location", rng))
    r["actor"] = actors or [t.gen_Reference("Practitioner")]
    r["planningHorizon"] = t.gen_Period()
    r["comment"] = t.p.faker.sentence()
    return r


def enrich_Slot(r, t, store, rng):
    r["status"] = rng.choice(["busy", "free", "busy-unavailable", "busy-tentative", "entered-in-error"])
    if store.has("Schedule"):
        r["schedule"] = store.get_reference("Schedule", rng)
    r["start"] = t.p.gen_dateTime(min_year=2024, max_year=2025)
    from datetime import datetime, timedelta
    start = datetime.fromisoformat(r["start"].replace("Z", "+00:00"))
    r["end"] = (start + timedelta(minutes=rng.choice([15, 30, 45, 60]))).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    r["serviceType"] = [t.gen_CodeableConcept()]
    return r


def enrich_Flag(r, t, store, rng):
    r["status"] = rng.choice(["active", "inactive", "entered-in-error"])
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/flag-category",
        code=rng.choice(["diet", "drug", "lab", "admin", "contact", "clinical",
                          "behavioral", "research", "advance-directive", "safety"]),
    )]
    r["code"] = t.gen_CodeableConcept()
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["author"] = store.get_reference("Practitioner", rng)
    r["period"] = t.gen_Period()
    return r


def enrich_Consent(r, t, store, rng):
    r["status"] = rng.choice(["draft", "active", "inactive", "not-done",
                               "entered-in-error", "unknown"])
    r["date"] = t.p.gen_date(min_year=2023, max_year=2024)
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code=rng.choice(["59284-0", "64292-6", "57016-8"]),
        display=rng.choice(["Patient Consent", "Privacy Consent", "Privacy policy"])
    )]
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Organization"):
        r["controller"] = [{"organization": store.get_reference("Organization", rng)}]
    return r


def enrich_NutritionOrder(r, t, store, rng):
    r["status"] = rng.choice(["draft", "active", "on-hold", "revoked", "completed",
                               "entered-in-error", "unknown"])
    r["intent"] = rng.choice(["proposal", "plan", "order"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["orderer"] = store.get_reference("Practitioner", rng)
    r["dateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["oralDiet"] = {
        "type": [t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code=rng.choice(["435801000124108", "437421000124105", "182922004"]),
            display=rng.choice(["Low sodium diet", "Low fat diet", "Diabetic diet"])
        )],
        "texture": [{"modifier": t.gen_CodeableConcept(
            system="http://snomed.info/sct",
            code="228055009", display="Regular diet"
        )}]
    }
    return r


ENRICHERS = {
    "Appointment": enrich_Appointment,
    "CarePlan": enrich_CarePlan,
    "CareTeam": enrich_CareTeam,
    "Goal": enrich_Goal,
    "ServiceRequest": enrich_ServiceRequest,
    "Task": enrich_Task,
    "Communication": enrich_Communication,
    "DocumentReference": enrich_DocumentReference,
    "Schedule": enrich_Schedule,
    "Slot": enrich_Slot,
    "Flag": enrich_Flag,
    "Consent": enrich_Consent,
    "NutritionOrder": enrich_NutritionOrder,
}
```
```

---

## PROMPT 12: Financial & Coverage Resources

```
Create `fhir_gen/generators/resources/financial.py`.

Implement enrichers for financial, insurance, and billing FHIR resources.

```python
from ...codes.loader import random_code, get_codes, get_system


def enrich_Coverage(r, t, store, rng):
    r["status"] = rng.choice(["active", "cancelled", "draft", "entered-in-error"])
    cov_type = random_code("coverage_type", rng)
    r["type"] = t.gen_CodeableConcept(
        system=get_system("coverage_type"),
        code=cov_type["code"] if cov_type else "EHCPOL",
        display=cov_type["display"] if cov_type else "Extended healthcare"
    )
    if store.has("Patient"):
        r["beneficiary"] = store.get_reference("Patient", rng)
        r["subscriber"] = store.get_reference("Patient", rng)
    if store.has("Organization"):
        r["insurer"] = store.get_reference("Organization", rng)
    r["subscriberId"] = [t.gen_Identifier(
        system="http://insurance.example.org/subscribers",
        value=f"SUB{rng.randint(100000, 999999)}"
    )]
    r["relationship"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/subscriber-relationship",
        code=rng.choice(["self", "spouse", "child", "parent", "common"]),
    )
    r["period"] = t.gen_Period()
    r["class"] = [{
        "type": t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/coverage-class",
            code=rng.choice(["group", "plan", "subplan", "class"])
        ),
        "value": t.gen_Identifier(value=f"GRP{rng.randint(10000, 99999)}")
    }]
    r["order"] = rng.randint(1, 3)
    return r


def enrich_Claim(r, t, store, rng):
    r["status"] = rng.choice(["active", "cancelled", "draft", "entered-in-error"])
    r["type"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/claim-type",
        code=rng.choice(["institutional", "oral", "pharmacy", "professional", "vision"]),
    )
    r["use"] = rng.choice(["claim", "preauthorization", "predetermination"])
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    r["created"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    if store.has("Organization"):
        r["insurer"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["provider"] = store.get_reference("Practitioner", rng)
    r["priority"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/processpriority",
        code=rng.choice(["stat", "normal", "deferred"]),
    )
    if store.has("Coverage"):
        r["insurance"] = [{
            "sequence": 1,
            "focal": True,
            "coverage": store.get_reference("Coverage", rng),
            "identifier": t.gen_Identifier()
        }]
    if store.has("Encounter"):
        r["item"] = [{
            "sequence": i + 1,
            "encounter": [store.get_reference("Encounter", rng)],
            "productOrService": t.gen_CodeableConcept(
                system="http://snomed.info/sct",
                code=rng.choice(["371883000", "308335008"]),
                display=rng.choice(["Outpatient procedure", "Patient encounter"])
            ),
            "unitPrice": t.gen_Money(),
            "net": t.gen_Money()
        } for i in range(rng.randint(1, 3))]
    r["total"] = t.gen_Money()
    return r


def enrich_ClaimResponse(r, t, store, rng):
    r["status"] = rng.choice(["active", "cancelled", "draft", "entered-in-error"])
    r["type"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/claim-type",
        code=rng.choice(["institutional", "professional"])
    )
    r["use"] = rng.choice(["claim", "preauthorization", "predetermination"])
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    r["created"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    if store.has("Organization"):
        r["insurer"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["requestor"] = store.get_reference("Practitioner", rng)
    r["outcome"] = rng.choice(["queued", "complete", "error", "partial"])
    r["disposition"] = rng.choice(["Claim adjudicated as submitted",
                                    "Partial payment approved",
                                    "Claim denied",
                                    "Approved for processing"])
    r["total"] = [{
        "category": t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/adjudication",
            code=rng.choice(["submitted", "copay", "eligible", "deductible", "benefit"]),
        ),
        "amount": t.gen_Money()
    }]
    return r


def enrich_Account(r, t, store, rng):
    r["status"] = rng.choice(["active", "inactive", "entered-in-error", "on-hold", "unknown"])
    r["type"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code=rng.choice(["PBILLACCT", "PUBLICPOL"]),
        display=rng.choice(["patient billing account", "public healthcare policy"])
    )
    r["name"] = f"Account-{rng.randint(10000, 99999)}"
    if store.has("Patient"):
        r["subject"] = [store.get_reference("Patient", rng)]
    r["servicePeriod"] = t.gen_Period()
    if store.has("Organization"):
        r["owner"] = store.get_reference("Organization", rng)
    r["description"] = t.p.faker.sentence()
    if store.has("Coverage"):
        r["coverage"] = [{
            "coverage": store.get_reference("Coverage", rng),
            "priority": 1
        }]
    return r


def enrich_Invoice(r, t, store, rng):
    r["status"] = rng.choice(["draft", "issued", "balanced", "cancelled", "entered-in-error"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["participant"] = [{"actor": store.get_reference("Practitioner", rng)}]
    if store.has("Organization"):
        r["issuer"] = store.get_reference("Organization", rng)
    if store.has("Account"):
        r["account"] = store.get_reference("Account", rng)
    r["date"] = t.p.gen_date(min_year=2023, max_year=2024)
    r["lineItem"] = [{
        "sequence": i + 1,
        "chargeItem": t.gen_CodeableConcept(),
        "priceComponent": [{
            "type": rng.choice(["base", "surcharge", "deduction", "discount", "tax", "informational"]),
            "amount": t.gen_Money()
        }]
    } for i in range(rng.randint(1, 4))]
    r["totalNet"] = t.gen_Money()
    r["totalGross"] = t.gen_Money()
    return r


def enrich_ChargeItem(r, t, store, rng):
    r["status"] = rng.choice(["planned", "billable", "not-billable", "aborted",
                               "billed", "entered-in-error", "unknown"])
    r["code"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["308335008", "371883000"]),
        display=rng.choice(["Patient encounter procedure", "Outpatient procedure"])
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["performer"] = [{"actor": store.get_reference("Practitioner", rng)}]
    r["occurrenceDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["quantity"] = t.gen_Quantity(value=1.0, unit="each")
    r["unitPriceComponent"] = [{
        "type": "base",
        "amount": t.gen_Money()
    }]
    return r


def enrich_CoverageEligibilityRequest(r, t, store, rng):
    r["status"] = rng.choice(["active", "cancelled", "draft", "entered-in-error"])
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    r["created"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    if store.has("Practitioner"):
        r["provider"] = store.get_reference("Practitioner", rng)
    if store.has("Organization"):
        r["insurer"] = store.get_reference("Organization", rng)
    if store.has("Coverage"):
        r["insurance"] = [{"coverage": store.get_reference("Coverage", rng), "focal": True}]
    r["purpose"] = [rng.choice(["auth-requirements", "benefits", "discovery", "validation"])]
    return r


ENRICHERS = {
    "Coverage": enrich_Coverage,
    "Claim": enrich_Claim,
    "ClaimResponse": enrich_ClaimResponse,
    "Account": enrich_Account,
    "Invoice": enrich_Invoice,
    "ChargeItem": enrich_ChargeItem,
    "CoverageEligibilityRequest": enrich_CoverageEligibilityRequest,
}
```
```

---

## PROMPT 13: Specialized Domain Resources

```
Create `fhir_gen/generators/resources/specialized.py`.

Implement enrichers for specimen, imaging, research, device, genomics, and other specialized resources.

```python
from ...codes.loader import random_code, get_codes, get_system


def enrich_Specimen(r, t, store, rng):
    r["status"] = rng.choice(["available", "unavailable", "unsatisfactory", "entered-in-error"])
    r["type"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["119297000", "119342007", "122555007", "258527002", "119323008"]),
        display=rng.choice(["Blood specimen", "Saliva specimen", "Venous blood specimen",
                             "Nail specimen", "Urine specimen"])
    )
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    r["receivedTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    body = random_code("body_sites", rng)
    r["collection"] = {
        "collector": store.get_reference("Practitioner", rng) if store.has("Practitioner") else None,
        "collectedDateTime": t.p.gen_dateTime(min_year=2023, max_year=2024),
        "bodySite": t.gen_CodeableConcept(
            system=get_system("body_sites"),
            code=body["code"] if body else "368209003",
            display=body["display"] if body else "Right arm"
        ) if body else None,
        "quantity": t.gen_Quantity(value=round(rng.uniform(1.0, 50.0), 1), unit="mL")
    }
    # Remove None values
    r["collection"] = {k: v for k, v in r["collection"].items() if v is not None}
    r["identifier"] = [t.gen_Identifier(system="http://lab.example.org/specimens")]
    return r


def enrich_ImagingStudy(r, t, store, rng):
    r["status"] = rng.choice(["registered", "available", "cancelled", "entered-in-error", "unknown"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    r["started"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    if store.has("Practitioner"):
        r["referrer"] = store.get_reference("Practitioner", rng)
    r["numberOfSeries"] = rng.randint(1, 5)
    r["numberOfInstances"] = rng.randint(10, 500)
    r["modality"] = [t.gen_Coding(
        system="http://dicom.nema.org/resources/ontology/DCM",
        code=rng.choice(["CT", "MR", "US", "XR", "PET", "NM", "PT", "CR", "DX"]),
        display=rng.choice(["Computed Tomography", "Magnetic Resonance", "Ultrasound",
                             "X-Ray", "Positron Emission Tomography", "Nuclear Medicine"])
    )]
    r["description"] = rng.choice([
        "CT Chest with contrast", "MRI Brain without contrast",
        "Ultrasound abdomen", "Chest X-Ray PA and Lateral",
        "PET Scan whole body", "MRI Spine lumbar"
    ])
    r["series"] = [{
        "uid": t.p.gen_oid(),
        "number": i + 1,
        "modality": t.gen_Coding(
            system="http://dicom.nema.org/resources/ontology/DCM",
            code=rng.choice(["CT", "MR", "US"])
        ),
        "description": rng.choice(["Axial", "Coronal", "Sagittal"]),
        "numberOfInstances": rng.randint(10, 200),
        "started": t.p.gen_dateTime(min_year=2023, max_year=2024),
        "instance": [{
            "uid": t.p.gen_oid(),
            "sopClass": t.gen_Coding(
                system="urn:ietf:rfc:3986",
                code="urn:oid:1.2.840.10008.5.1.4.1.1.2"
            ),
            "number": j + 1
        } for j in range(rng.randint(1, 5))]
    } for i in range(rng.randint(1, 3))]
    return r


def enrich_Device(r, t, store, rng):
    r["status"] = rng.choice(["active", "inactive", "entered-in-error"])
    r["name"] = [{
        "value": rng.choice(["Pulse Oximeter", "Blood Pressure Monitor",
                              "Insulin Pump", "Cardiac Monitor",
                              "Ventilator", "Defibrillator",
                              "Infusion Pump", "ECG Machine"]),
        "type": t.gen_CodeableConcept(
            system="http://hl7.org/fhir/device-nametype",
            code="user-friendly-name"
        )
    }]
    r["type"] = [t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["706767009", "19257004", "360008003"]),
        display=rng.choice(["Patient monitor", "Defibrillator", "Immunization agent"])
    )]
    r["manufacturer"] = rng.choice(["Medtronic", "Philips", "GE Healthcare",
                                     "Siemens", "Abbott", "Boston Scientific"])
    r["manufactureDate"] = t.p.gen_date(min_year=2020, max_year=2023)
    r["expirationDate"] = t.p.gen_date(min_year=2025, max_year=2030)
    r["serialNumber"] = f"SN{rng.randint(100000000, 999999999)}"
    if store.has("Organization"):
        r["owner"] = store.get_reference("Organization", rng)
    if store.has("Location"):
        r["location"] = store.get_reference("Location", rng)
    return r


def enrich_ResearchStudy(r, t, store, rng):
    r["status"] = rng.choice(["active", "administratively-completed", "approved",
                               "closed-to-accrual", "closed-to-accrual-and-intervention",
                               "completed", "disapproved", "in-review",
                               "temporarily-closed-to-accrual",
                               "temporarily-closed-to-accrual-and-intervention", "withdrawn"])
    r["title"] = rng.choice([
        "Effect of Exercise on Type 2 Diabetes Outcomes",
        "Novel Therapy for Hypertension Management",
        "Genetic Factors in Cardiovascular Disease",
        "Immunotherapy for Non-Small Cell Lung Cancer",
        "Cognitive Behavioral Therapy for Depression"
    ])
    r["phase"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/research-study-phase",
        code=rng.choice(["n-a", "early-phase-1", "phase-1", "phase-1-phase-2",
                          "phase-2", "phase-2-phase-3", "phase-3", "phase-4"]),
    )
    if store.has("Organization"):
        r["sponsor"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["principalInvestigator"] = store.get_reference("Practitioner", rng)
    if store.has("Location"):
        r["site"] = [store.get_reference("Location", rng)]
    r["description"] = t.p.gen_markdown()
    r["period"] = t.gen_Period()
    return r


def enrich_ResearchSubject(r, t, store, rng):
    r["status"] = rng.choice(["candidate", "eligible", "follow-up", "ineligible",
                               "not-registered", "off-study", "on-study",
                               "on-study-intervention", "on-study-observation",
                               "pending-on-study", "potential-candidate",
                               "screening", "withdrawn"])
    if store.has("ResearchStudy"):
        r["study"] = store.get_reference("ResearchStudy", rng)
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    r["period"] = t.gen_Period()
    r["assignedComparisonGroup"] = t.p.gen_id()
    r["actualComparisonGroup"] = t.p.gen_id()
    return r


def enrich_QuestionnaireResponse(r, t, store, rng):
    r["status"] = rng.choice(["in-progress", "completed", "amended", "entered-in-error", "stopped"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Encounter"):
        r["encounter"] = store.get_reference("Encounter", rng)
    if store.has("Practitioner"):
        r["author"] = store.get_reference("Practitioner", rng)
    r["authored"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["item"] = [{
        "linkId": f"question-{i+1}",
        "text": t.p.faker.sentence(nb_words=8) + "?",
        "answer": [{"valueString": t.p.faker.sentence(nb_words=5)}]
    } for i in range(rng.randint(2, 6))]
    return r


def enrich_AuditEvent(r, t, store, rng):
    r["action"] = rng.choice(["C", "R", "U", "D", "E"])
    r["recorded"] = t.p.gen_instant()
    r["outcome"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/audit-event-outcome",
        code=rng.choice(["0", "4", "8", "12"]),
        display=rng.choice(["Success", "Minor failure", "Serious failure", "Major failure"])
    )
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/audit-event-type",
        code=rng.choice(["rest", "hl7-v2", "hl7-v3", "dicom"]),
    )]
    r["code"] = t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/audit-event-sub-type",
        code=rng.choice(["read", "create", "update", "delete", "search"]),
    )
    r["agent"] = [{
        "type": t.gen_CodeableConcept(),
        "requestor": True,
        "who": store.get_reference("Practitioner", rng) if store.has("Practitioner") else t.gen_Reference(),
        "network": {"address": t.p.faker.ipv4(), "type": t.gen_CodeableConcept(
            system="http://hl7.org/fhir/network-type", code="2", display="IP Address"
        )}
    }]
    r["source"] = {
        "site": t.gen_Reference("Location") if not store.has("Location") else store.get_reference("Location", rng),
        "observer": t.gen_Reference("Device"),
        "type": [t.gen_CodeableConcept()]
    }
    r["entity"] = [{
        "what": store.get_reference("Patient", rng) if store.has("Patient") else t.gen_Reference(),
        "role": t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/object-role",
            code=rng.choice(["1", "2", "3", "4"]),
            display=rng.choice(["Patient", "Location", "Report", "Domain Resource"])
        )
    }]
    return r


def enrich_EpisodeOfCare(r, t, store, rng):
    r["status"] = rng.choice(["planned", "waitlist", "active", "onhold",
                               "finished", "cancelled", "entered-in-error"])
    r["type"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/episodeofcare-type",
        code=rng.choice(["hacc", "pac", "diab", "da", "cacp", "posad", "oncol"]),
    )]
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    if store.has("Organization"):
        r["managingOrganization"] = store.get_reference("Organization", rng)
    if store.has("Practitioner"):
        r["careManager"] = store.get_reference("Practitioner", rng)
    r["period"] = t.gen_Period()
    if store.has("Condition"):
        r["diagnosis"] = [{
            "condition": [{"reference": store.get_reference("Condition", rng)}],
            "use": t.gen_CodeableConcept(
                system="http://snomed.info/sct",
                code="8319008", display="Principal diagnosis"
            )
        }]
    return r


def enrich_HealthcareService(r, t, store, rng):
    r["active"] = rng.random() > 0.1
    if store.has("Organization"):
        r["providedBy"] = store.get_reference("Organization", rng)
    if store.has("Location"):
        r["location"] = [store.get_reference("Location", rng)]
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/service-category",
        code=str(rng.randint(1, 40)),
        display=rng.choice(["Aged Care", "Child Care", "Dental", "General Practice",
                             "Mental Health", "Physiotherapy", "Podiatry"])
    )]
    r["type"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/service-type",
        code=str(rng.randint(1, 600)),
    )]
    r["name"] = rng.choice(["General Practice Clinic", "Outpatient Diabetes Clinic",
                              "Cardiac Rehabilitation", "Physical Therapy",
                              "Mental Health Services", "Oncology Outpatient"])
    r["comment"] = t.p.faker.sentence()
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    return r


def enrich_RelatedPerson(r, t, store, rng):
    r["active"] = True
    if store.has("Patient"):
        r["patient"] = store.get_reference("Patient", rng)
    rel = random_code("contact_relationship", rng)
    r["relationship"] = [t.gen_CodeableConcept(
        system=get_system("contact_relationship"),
        code=rel["code"] if rel else "N",
        display=rel["display"] if rel else "Next-of-Kin"
    )]
    r["name"] = [t.gen_HumanName(use="official")]
    r["telecom"] = [t.gen_ContactPoint("phone"), t.gen_ContactPoint("email")]
    r["gender"] = rng.choice(["male", "female", "other", "unknown"])
    r["address"] = [t.gen_Address()]
    r["period"] = t.gen_Period()
    return r


def enrich_Group(r, t, store, rng):
    r["type"] = rng.choice(["person", "animal", "practitioner", "device",
                              "careteam", "healthcareservice", "location",
                              "organization", "relatedperson", "specimen"])
    r["membership"] = rng.choice(["definitional", "enumerated"])
    r["active"] = True
    r["quantity"] = rng.randint(5, 500)
    r["name"] = rng.choice(["Diabetes Patients Group", "Hypertension Cohort",
                              "Oncology Trial Participants", "Post-Surgical Patients"])
    r["code"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["73211009", "38341003"]),
        display=rng.choice(["Diabetes mellitus", "Hypertensive disorder"])
    )
    if store.has("Patient") and r["type"] == "person":
        r["member"] = [{
            "entity": store.get_reference("Patient", rng),
            "period": t.gen_Period(),
            "inactive": False
        } for _ in range(min(3, store.count("Patient")))]
    return r


def enrich_DetectedIssue(r, t, store, rng):
    r["status"] = rng.choice(["preliminary", "final", "entered-in-error", "mitigated"])
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
        code=rng.choice(["DRG", "DI", "TIME", "DOSE", "DOSEIND", "ALG"]),
        display=rng.choice(["Drug Interaction Alert", "Drug Intolerance Alert",
                             "Timing Detected Issue", "Dosage Alert",
                             "Dose Indicator", "Allergy Alert"])
    )]
    r["severity"] = rng.choice(["high", "moderate", "low"])
    if store.has("Patient"):
        r["subject"] = store.get_reference("Patient", rng)
    if store.has("Practitioner"):
        r["author"] = store.get_reference("Practitioner", rng)
    r["identifiedDateTime"] = t.p.gen_dateTime(min_year=2023, max_year=2024)
    r["detail"] = t.p.faker.sentence()
    r["mitigation"] = [{
        "action": t.gen_CodeableConcept(
            system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
            code=rng.choice(["13", "2", "4", "5", "6", "7", "10", "11"]),
            display=rng.choice(["Consulted Prescriber", "Assessed Patient",
                                 "Consulted other prescriber", "Substituted different drug",
                                 "Provided patient education", "Instituted ongoing monitoring program"])
        ),
        "date": t.p.gen_dateTime(min_year=2023, max_year=2024)
    }]
    return r


def enrich_Substance(r, t, store, rng):
    r["status"] = rng.choice(["active", "inactive", "entered-in-error"])
    r["category"] = [t.gen_CodeableConcept(
        system="http://terminology.hl7.org/CodeSystem/substance-category",
        code=rng.choice(["allergen", "biological", "body", "chemical",
                          "food", "drug", "material"]),
    )]
    r["code"] = t.gen_CodeableConcept(
        system="http://snomed.info/sct",
        code=rng.choice(["372687004", "387207008", "7980", "1191"]),
        display=rng.choice(["Amoxicillin", "Ibuprofen", "Penicillin", "Aspirin"])
    )
    r["description"] = t.p.faker.sentence()
    return r


ENRICHERS = {
    "Specimen": enrich_Specimen,
    "ImagingStudy": enrich_ImagingStudy,
    "Device": enrich_Device,
    "ResearchStudy": enrich_ResearchStudy,
    "ResearchSubject": enrich_ResearchSubject,
    "QuestionnaireResponse": enrich_QuestionnaireResponse,
    "AuditEvent": enrich_AuditEvent,
    "EpisodeOfCare": enrich_EpisodeOfCare,
    "HealthcareService": enrich_HealthcareService,
    "RelatedPerson": enrich_RelatedPerson,
    "Group": enrich_Group,
    "DetectedIssue": enrich_DetectedIssue,
    "Substance": enrich_Substance,
}
```

Also create `fhir_gen/generators/resources/__init__.py` that imports ENRICHERS from all modules and provides a combined dict.
```

---

## PROMPT 14: MongoDB Persistence Layer

```
Create `fhir_gen/persistence/mongo.py`.

Implement a MongoDB storage layer for FHIR resources with search capabilities.

```python
from datetime import datetime
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from typing import Iterator
from ..config import settings
import logging

logger = logging.getLogger(__name__)


class FHIRMongoStore:
    """
    MongoDB persistence for FHIR resources.
    One collection per resource type: fhir_{ResourceType}
    Also maintains a fhir_all collection for cross-resource search.
    """

    def __init__(self, uri: str | None = None, db_name: str | None = None):
        self._client = MongoClient(uri or settings.mongodb_uri)
        self._db = self._client[db_name or settings.mongodb_db]
        self._ensure_indexes()

    def _collection(self, resource_type: str) -> Collection:
        return self._db[f"fhir_{resource_type}"]

    def _ensure_indexes(self) -> None:
        """Create standard FHIR search indexes."""
        common_indexes = [
            [("id", ASCENDING)],
            [("meta.lastUpdated", DESCENDING)],
        ]
        resource_specific = {
            "Patient": [[("identifier.value", ASCENDING)], [("name.family", ASCENDING)],
                         [("birthDate", ASCENDING)], [("gender", ASCENDING)]],
            "Observation": [[("subject.reference", ASCENDING)], [("code.coding.code", ASCENDING)],
                             [("effectiveDateTime", DESCENDING)], [("status", ASCENDING)]],
            "Condition": [[("subject.reference", ASCENDING)], [("code.coding.code", ASCENDING)],
                           [("clinicalStatus.coding.code", ASCENDING)]],
            "Encounter": [[("subject.reference", ASCENDING)], [("status", ASCENDING)],
                           [("actualPeriod.start", DESCENDING)]],
            "MedicationRequest": [[("subject.reference", ASCENDING)], [("status", ASCENDING)],
                                    [("authoredOn", DESCENDING)]],
            "Practitioner": [[("identifier.value", ASCENDING)], [("name.family", ASCENDING)]],
            "Organization": [[("name", ASCENDING)], [("identifier.value", ASCENDING)]],
        }
        for rtype, extra_indexes in resource_specific.items():
            col = self._collection(rtype)
            for idx in common_indexes + extra_indexes:
                try:
                    col.create_index(idx)
                except Exception as e:
                    logger.debug(f"Index creation skipped for {rtype}: {e}")

    def save(self, resource: dict) -> str:
        """Upsert a single resource. Returns the resource id."""
        rtype = resource.get("resourceType")
        rid = resource.get("id")
        if not rtype or not rid:
            raise ValueError("Resource must have resourceType and id")

        doc = {**resource, "_fhir_resource_type": rtype, "_stored_at": datetime.utcnow()}
        self._collection(rtype).replace_one({"id": rid}, doc, upsert=True)
        return rid

    def save_many(self, resources: list[dict], batch_size: int = 500) -> dict[str, int]:
        """
        Bulk upsert resources. Groups by type for efficient batch writes.
        Returns {resource_type: count} dict.
        """
        by_type: dict[str, list[dict]] = {}
        for r in resources:
            rtype = r.get("resourceType")
            if rtype:
                by_type.setdefault(rtype, []).append(r)

        counts: dict[str, int] = {}
        for rtype, docs in by_type.items():
            col = self._collection(rtype)
            for i in range(0, len(docs), batch_size):
                batch = docs[i:i + batch_size]
                ops = [
                    UpdateOne(
                        {"id": d["id"]},
                        {"$set": {**d, "_fhir_resource_type": rtype,
                                  "_stored_at": datetime.utcnow()}},
                        upsert=True
                    ) for d in batch
                ]
                try:
                    result = col.bulk_write(ops, ordered=False)
                    counts[rtype] = counts.get(rtype, 0) + result.upserted_count + result.modified_count
                except BulkWriteError as e:
                    logger.error(f"Bulk write error for {rtype}: {e.details}")
        return counts

    def find(self, resource_type: str, query: dict | None = None,
             limit: int = 100, skip: int = 0) -> list[dict]:
        """Search resources by MongoDB query dict."""
        col = self._collection(resource_type)
        cursor = col.find(query or {}, {"_id": 0, "_fhir_resource_type": 0, "_stored_at": 0})
        return list(cursor.skip(skip).limit(limit))

    def find_by_reference(self, reference: str) -> dict | None:
        """Find a resource by its FHIR reference string e.g. 'Patient/abc123'"""
        parts = reference.split("/")
        if len(parts) != 2:
            return None
        rtype, rid = parts
        return self.get(rtype, rid)

    def get(self, resource_type: str, resource_id: str) -> dict | None:
        """Get a single resource by type and id."""
        col = self._collection(resource_type)
        doc = col.find_one({"id": resource_id}, {"_id": 0, "_fhir_resource_type": 0, "_stored_at": 0})
        return doc

    def search_patient(self, family: str | None = None, given: str | None = None,
                       birthdate: str | None = None, identifier: str | None = None,
                       gender: str | None = None) -> list[dict]:
        """FHIR-style patient search."""
        query: dict = {}
        if family:
            query["name.family"] = {"$regex": family, "$options": "i"}
        if given:
            query["name.given"] = {"$regex": given, "$options": "i"}
        if birthdate:
            query["birthDate"] = birthdate
        if identifier:
            query["identifier.value"] = identifier
        if gender:
            query["gender"] = gender
        return self.find("Patient", query)

    def search_observations_for_patient(self, patient_id: str,
                                         code: str | None = None,
                                         status: str | None = None) -> list[dict]:
        """Get all observations for a patient, optionally filtered by LOINC code."""
        query: dict = {"subject.reference": f"Patient/{patient_id}"}
        if code:
            query["code.coding.code"] = code
        if status:
            query["status"] = status
        return self.find("Observation", query)

    def search_conditions_for_patient(self, patient_id: str,
                                       clinical_status: str | None = None) -> list[dict]:
        query: dict = {"subject.reference": f"Patient/{patient_id}"}
        if clinical_status:
            query["clinicalStatus.coding.code"] = clinical_status
        return self.find("Condition", query)

    def search_encounters_for_patient(self, patient_id: str,
                                       status: str | None = None) -> list[dict]:
        query: dict = {"subject.reference": f"Patient/{patient_id}"}
        if status:
            query["status"] = status
        return self.find("Encounter", query)

    def count(self, resource_type: str, query: dict | None = None) -> int:
        return self._collection(resource_type).count_documents(query or {})

    def delete_all(self, resource_type: str | None = None) -> None:
        """Delete all documents. If resource_type given, only that collection."""
        if resource_type:
            self._collection(resource_type).drop()
        else:
            for name in self._db.list_collection_names():
                if name.startswith("fhir_"):
                    self._db[name].drop()

    def list_resource_types(self) -> list[str]:
        return [n.replace("fhir_", "") for n in self._db.list_collection_names()
                if n.startswith("fhir_")]

    def stats(self) -> dict[str, int]:
        """Return count per resource type."""
        return {rt: self.count(rt) for rt in self.list_resource_types()}

    def close(self) -> None:
        self._client.close()
```
```

---

## PROMPT 15: CLI Interface

```
Create `fhir_gen/cli/main.py`.

Implement a Click CLI for the FHIR data generation library.

```python
import json
import sys
import click
from pathlib import Path
from ..generators.base import ResourceGenerator
from ..persistence.mongo import FHIRMongoStore
from ..config import settings
from ..schema.registry import registry


@click.group()
@click.option("--seed", type=int, default=None, help="Random seed for reproducibility")
@click.option("--schema-path", type=click.Path(exists=True, path_type=Path), default=None,
              help="Custom FHIR JSON schema (default: packaged fhir.schema.v5.json)")
@click.option("--mongo-uri", default=None, envvar="FHIR_GEN_MONGODB_URI",
              help="MongoDB URI (default: mongodb://localhost:27017)")
@click.option("--db", default=None, envvar="FHIR_GEN_MONGODB_DB",
              help="MongoDB database name (default: fhir_synthetic)")
@click.pass_context
def cli(ctx, seed, schema_path, mongo_uri, db):
    """FHIR R5 Synthetic Data Generator CLI"""
    ctx.ensure_object(dict)
    ctx.obj["seed"] = seed
    ctx.obj["schema_path"] = str(schema_path) if schema_path else None
    ctx.obj["mongo_uri"] = mongo_uri or settings.mongodb_uri
    ctx.obj["db"] = db or settings.mongodb_db
    if schema_path:
        from ..schema.registry import SchemaRegistry
        SchemaRegistry.reload(schema_path)


@cli.command()
@click.argument("resource_type")
@click.option("-n", "--count", default=1, show_default=True, help="Number of resources to generate")
@click.option("--save/--no-save", default=True, show_default=True, help="Save to MongoDB")
@click.option("--output", type=click.Path(), default=None,
              help="Output JSON file path (- for stdout)")
@click.option("--pretty/--no-pretty", default=True, show_default=True,
              help="Pretty-print JSON output")
@click.option("--with-deps/--no-deps", default=True, show_default=True,
              help="Auto-generate required dependency resources")
@click.option("--variants/--no-variants", default=False, show_default=True,
              help="Emit one resource per polymorphic field variant (INSTRUCTIONS #7)")
@click.pass_context
def generate(ctx, resource_type, count, save, output, pretty, with_deps, variants):
    """Generate FHIR resources of a given type.

    \b
    Examples:
      fhir-gen generate Patient --count 10
      fhir-gen generate Observation --count 5 --no-save --output obs.json
      fhir-gen generate MedicationRequest -n 20
    """
    seed = ctx.obj["seed"]
    gen = ResourceGenerator(seed=seed)

    # Validate resource type
    valid_resources = registry.all_resources()
    if resource_type not in valid_resources:
        similar = [r for r in valid_resources if r.lower().startswith(resource_type.lower()[:3])]
        click.echo(f"Unknown resource: {resource_type}", err=True)
        if similar:
            click.echo(f"Did you mean: {', '.join(similar[:5])}", err=True)
        sys.exit(1)

    click.echo(f"Generating {count} {resource_type} resource(s)...", err=True)

    if variants:
        resources = gen.generate_variants(resource_type)
    else:
        resources = gen.generate(
            resource_type, count=count,
            schema_path=ctx.obj.get("schema_path"),
        )

    if save:
        store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
        all_resources = []
        # Collect everything from the store
        for rtype in gen.store._store:
            all_resources.extend([e["resource"] for e in gen.store._store[rtype]])
        counts = store_obj.save_many(all_resources)
        click.echo(f"Saved to MongoDB: {counts}", err=True)

    indent = 2 if pretty else None
    if output and output != "-":
        Path(output).write_text(json.dumps(resources, indent=indent))
        click.echo(f"Written to {output}", err=True)
    elif output == "-" or not save:
        click.echo(json.dumps(resources, indent=indent))
    else:
        click.echo(f"Generated {len(resources)} {resource_type} resource(s)", err=True)


@cli.command()
@click.argument("resource_types", nargs=-1, required=True)
@click.option("--counts", default=None, help='JSON map e.g. \'{"Patient":10,"Encounter":20}\'')
@click.option("--save/--no-save", default=True, show_default=True)
@click.option("--output-dir", type=click.Path(), default=None,
              help="Directory to write per-resource JSON files")
@click.pass_context
def generate_many(ctx, resource_types, counts, save, output_dir):
    """Generate multiple resource types in dependency order.

    \b
    Examples:
      fhir-gen generate-many Patient Encounter Observation
      fhir-gen generate-many Patient Encounter --counts '{"Patient":5,"Encounter":10}'
      fhir-gen generate-many Patient Condition --output-dir ./output
    """
    import json as _json
    seed = ctx.obj["seed"]
    gen = ResourceGenerator(seed=seed)

    count_map = {}
    if counts:
        count_map = _json.loads(counts)

    click.echo(f"Generating resources: {', '.join(resource_types)}", err=True)
    results = gen.generate_many(list(resource_types), counts=count_map)

    if save:
        all_resources = [r for rlist in results.values() for r in rlist]
        store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
        saved = store_obj.save_many(all_resources)
        click.echo(f"Saved: {saved}", err=True)

    if output_dir:
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        for rtype, resources in results.items():
            file_path = out_path / f"{rtype}.json"
            file_path.write_text(_json.dumps(resources, indent=2))
            click.echo(f"  Written {rtype}: {file_path}", err=True)

    summary = {rtype: len(res) for rtype, res in results.items()}
    click.echo(f"Summary: {_json.dumps(summary, indent=2)}")


@cli.command()
@click.pass_context
def list_resources(ctx):
    """List all available FHIR resource types."""
    resources = sorted(registry.all_resources())
    click.echo(f"Available resources ({len(resources)} total):")
    for i, r in enumerate(resources, 1):
        click.echo(f"  {i:3d}. {r}")


@cli.command()
@click.argument("resource_type")
@click.pass_context
def schema_info(ctx, resource_type):
    """Show schema info for a resource type."""
    try:
        defn = registry.definition(resource_type)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)

    click.echo(f"\n{resource_type}")
    click.echo("=" * len(resource_type))
    click.echo(f"Description: {defn.description}")
    click.echo(f"Required fields: {defn.required}")
    click.echo(f"Total fields: {len(defn.fields)}")
    click.echo(f"Polymorphic groups: {list(defn.poly_groups.keys())}")
    click.echo(f"\nFields:")
    for fname, fdef in list(defn.fields.items())[:30]:
        req = "* " if fname in defn.required else "  "
        arr = "[]" if fdef.is_array else ""
        click.echo(f"  {req}{fname}: {fdef.ref or 'any'}{arr}")


@cli.command()
@click.pass_context
def db_stats(ctx):
    """Show MongoDB database statistics."""
    store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
    stats = store_obj.stats()
    if not stats:
        click.echo("No data found in database.")
        return
    total = sum(stats.values())
    click.echo(f"Database: {ctx.obj['db']}")
    click.echo(f"{'Resource Type':<35} {'Count':>10}")
    click.echo("-" * 46)
    for rtype, count in sorted(stats.items()):
        click.echo(f"{rtype:<35} {count:>10,}")
    click.echo("-" * 46)
    click.echo(f"{'TOTAL':<35} {total:>10,}")


@cli.command()
@click.argument("resource_type", required=False)
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def clear(ctx, resource_type, yes):
    """Clear resources from MongoDB database."""
    store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
    target = resource_type or "ALL RESOURCES"
    if not yes:
        click.confirm(f"Delete {target} from {ctx.obj['db']}?", abort=True)
    store_obj.delete_all(resource_type)
    click.echo(f"Deleted: {target}")


@cli.command()
@click.argument("resource_type")
@click.option("--patient-id", help="Filter by patient ID")
@click.option("--code", help="Filter by code (LOINC/SNOMED)")
@click.option("--status", help="Filter by status")
@click.option("--limit", default=10, show_default=True)
@click.option("--pretty/--no-pretty", default=True)
@click.pass_context
def search(ctx, resource_type, patient_id, code, status, limit, pretty):
    """Search resources in MongoDB.

    \b
    Examples:
      fhir-gen search Observation --patient-id abc-123
      fhir-gen search Condition --code 73211009 --status active
      fhir-gen search Patient --limit 5
    """
    store_obj = FHIRMongoStore(ctx.obj["mongo_uri"], ctx.obj["db"])
    query = {}
    if patient_id:
        if resource_type == "Patient":
            query["id"] = patient_id
        else:
            query["subject.reference"] = f"Patient/{patient_id}"
    if code:
        query["$or"] = [{"code.coding.code": code}, {"vaccineCode.coding.code": code}]
    if status:
        query["status"] = status

    results = store_obj.find(resource_type, query, limit=limit)
    indent = 2 if pretty else None
    click.echo(json.dumps(results, indent=indent, default=str))


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
```

In `pyproject.toml` add:
```toml
[project.scripts]
fhir-gen = "fhir_gen.cli.main:main"
```
```

---

## PROMPT 16: Package Setup & Distribution

```
Expand packaging docs (pyproject.toml skeleton is in Prompt 1). Create `README.md` and `.env.example`.

### pyproject.toml — add dev tooling to existing file
```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "fhir-gen"
version = "1.0.0"
description = "FHIR R5 Synthetic Healthcare Data Generator"
readme = "README.md"
requires-python = ">=3.11"
license = {text = "MIT"}
authors = [{name = "Your Name", email = "you@example.com"}]
keywords = ["fhir", "healthcare", "synthetic-data", "hl7", "r5"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Intended Audience :: Healthcare Industry",
    "Topic :: Scientific/Engineering :: Medical Science Apps.",
    "License :: OSI Approved :: MIT License",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]
dependencies = [
    "pydantic>=2.0",
    "pydantic-settings>=2.0",
    "pymongo>=4.6",
    "click>=8.1",
    "faker>=24.0",
    "PyYAML>=6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4",
    "pytest-asyncio>=0.21",
    "pytest-cov>=4.1",
    "mongomock>=4.1",
    "ruff>=0.1",
    "mypy>=1.5",
]

[project.scripts]
fhir-gen = "fhir_gen.cli.main:main"

[project.urls]
Repository = "https://github.com/your-org/fhir-gen"
Documentation = "https://github.com/your-org/fhir-gen#readme"

[tool.setuptools.packages.find]
include = ["fhir_gen*"]

[tool.setuptools.package-data]
fhir_gen = ["schema/fhir.schema.v5.json"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "--cov=fhir_gen --cov-report=term-missing --cov-fail-under=75"
asyncio_mode = "auto"

[tool.ruff]
line-length = 120
target-version = "py311"

[tool.mypy]
python_version = "3.11"
ignore_missing_imports = true
```

### .env.example
```
FHIR_GEN_MONGODB_URI=mongodb://localhost:27017
FHIR_GEN_MONGODB_DB=fhir_synthetic
FHIR_GEN_SEED=42
FHIR_GEN_LOG_LEVEL=INFO
```

### README.md — include:
- Installation: `pip install fhir-gen` and `pip install -e .[dev]`
- Quick start Python API example
- CLI usage examples for all commands
- MongoDB setup section
- Supported resources table
- Architecture overview (schema → generator → enricher → store)
- Environment variables table
- Contributing section
```

---

## PROMPT 17: Test Suite — Datatypes & Engine

```
Create `tests/conftest.py` and `tests/test_primitives.py` and `tests/test_complex.py`.

### conftest.py
```python
import pytest
from fhir_gen.generators.primitives import PrimitiveGenerator
from fhir_gen.generators.special_types import SpecialTypeGenerator
from fhir_gen.generators.base import ResourceGenerator
from fhir_gen.resolvers.reference import ReferenceStore

SEED = 42

@pytest.fixture(scope="session")
def prim():
    return PrimitiveGenerator(seed=SEED)

@pytest.fixture(scope="session")
def types(prim):
    return SpecialTypeGenerator(prim)

@pytest.fixture
def store():
    return ReferenceStore()

@pytest.fixture
def gen():
    return ResourceGenerator(seed=SEED)

@pytest.fixture
def gen_with_core(gen):
    """Generator with Patient, Practitioner, Organization pre-generated."""
    gen.generate("Patient", count=3)
    gen.generate("Practitioner", count=2)
    gen.generate("Organization", count=2)
    gen.generate("Location", count=2)
    return gen
```

### test_primitives.py
```python
import re
import pytest
from fhir_gen.generators.primitives import PrimitiveGenerator


@pytest.fixture(scope="module")
def p():
    return PrimitiveGenerator(seed=42)


class TestPrimitiveTypes:
    def test_id_format(self, p):
        id_val = p.gen_id()
        assert re.match(r'^[A-Za-z0-9\-\.]{1,64}$', id_val)

    def test_boolean(self, p):
        assert p.gen_boolean() in (True, False)

    def test_integer_range(self, p):
        val = p.gen_integer(0, 100)
        assert 0 <= val <= 100

    def test_positive_int(self, p):
        assert p.gen_positiveInt(max_val=100) > 0

    def test_unsigned_int(self, p):
        assert p.gen_unsignedInt(max_val=100) >= 0

    def test_decimal_precision(self, p):
        val = p.gen_decimal(0.0, 10.0, precision=2)
        assert isinstance(val, float)
        assert 0.0 <= val <= 10.0
        assert len(str(val).split(".")[-1]) <= 2

    def test_date_formats(self, p):
        for _ in range(20):
            d = p.gen_date()
            assert re.match(r'^\d{4}(-\d{2}(-\d{2})?)?$', d)

    def test_datetime_format(self, p):
        dt = p.gen_dateTime()
        assert re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$', dt)

    def test_instant_format(self, p):
        inst = p.gen_instant()
        assert inst.endswith("Z")

    def test_time_format(self, p):
        t = p.gen_time()
        assert re.match(r'^\d{2}:\d{2}:\d{2}$', t)

    def test_uri_format(self, p):
        uri = p.gen_uri()
        assert uri.startswith("urn:")

    def test_canonical_format(self, p):
        c = p.gen_canonical()
        assert "|" in c
        assert c.startswith("http")

    def test_oid_format(self, p):
        oid = p.gen_oid()
        assert oid.startswith("urn:oid:")

    def test_uuid_format(self, p):
        u = p.gen_uuid()
        assert u.startswith("urn:uuid:")

    def test_base64_binary(self, p):
        import base64
        b64 = p.gen_base64Binary(32)
        base64.b64decode(b64)  # should not raise

    def test_xhtml_structure(self, p):
        x = p.gen_xhtml()
        assert x.startswith('<div xmlns=')
        assert '</div>' in x

    def test_seeded_reproducibility(self):
        p1 = PrimitiveGenerator(seed=123)
        p2 = PrimitiveGenerator(seed=123)
        assert p1.gen_id() == p2.gen_id()
        assert p1.gen_dateTime() == p2.gen_dateTime()

    def test_code_from_set(self, p):
        codes = ["active", "inactive", "entered-in-error"]
        c = p.gen_code(code_set=codes)
        assert c in codes

    def test_dispatch(self, p):
        for type_name in ["id", "string", "boolean", "integer", "decimal",
                           "date", "dateTime", "instant", "uri", "code"]:
            val = p.generate(type_name)
            assert val is not None
```

### test_complex.py
```python
import pytest
from fhir_gen.generators.complex_types import ComplexTypeGenerator
from fhir_gen.generators.primitives import PrimitiveGenerator
from fhir_gen.generators.special_types import SpecialTypeGenerator


@pytest.fixture(scope="module")
def ct():
    return ComplexTypeGenerator(PrimitiveGenerator(seed=42))


@pytest.fixture(scope="module")
def st():
    p = PrimitiveGenerator(seed=42)
    return SpecialTypeGenerator(p)


class TestComplexTypes:
    def test_identifier(self, ct):
        ident = ct.gen_Identifier()
        assert "system" in ident and "value" in ident

    def test_human_name(self, ct):
        name = ct.gen_HumanName()
        assert "family" in name and "given" in name
        assert isinstance(name["given"], list)

    def test_address(self, ct):
        addr = ct.gen_Address()
        assert all(k in addr for k in ["line", "city", "postalCode"])

    def test_contact_point_phone(self, ct):
        cp = ct.gen_ContactPoint(system="phone")
        assert cp["system"] == "phone"
        assert "value" in cp

    def test_contact_point_email(self, ct):
        cp = ct.gen_ContactPoint(system="email")
        assert "@" in cp["value"]

    def test_codeable_concept(self, ct):
        cc = ct.gen_CodeableConcept(system="http://snomed.info/sct", code="123", display="Test")
        assert "coding" in cc and "text" in cc
        assert cc["coding"][0]["code"] == "123"

    def test_coding(self, ct):
        coding = ct.gen_Coding(system="http://loinc.org", code="8867-4", display="Heart rate")
        assert coding["code"] == "8867-4"

    def test_period(self, ct):
        period = ct.gen_Period()
        assert "start" in period and "end" in period
        # end must be after start
        assert period["end"] > period["start"]

    def test_quantity(self, ct):
        q = ct.gen_Quantity(value=5.5, unit="mg")
        assert q["value"] == 5.5 and q["unit"] == "mg"

    def test_range(self, ct):
        r = ct.gen_Range(low_val=10.0, high_val=20.0)
        assert r["low"]["value"] < r["high"]["value"]

    def test_ratio(self, ct):
        ratio = ct.gen_Ratio()
        assert "numerator" in ratio and "denominator" in ratio

    def test_timing(self, ct):
        timing = ct.gen_Timing()
        assert "repeat" in timing
        assert timing["repeat"]["frequency"] >= 1

    def test_annotation(self, ct):
        ann = ct.gen_Annotation()
        assert "text" in ann and "time" in ann

    def test_money(self, ct):
        m = ct.gen_Money()
        assert "value" in m and m["currency"] == "USD"

    def test_age(self, ct):
        age = ct.gen_Age(value=45.0)
        assert age["value"] == 45.0 and age["code"] == "a"

    def test_sampled_data(self, ct):
        sd = ct.gen_SampledData()
        assert "data" in sd and "origin" in sd
        assert len(sd["data"].split(" ")) >= 10


class TestSpecialTypes:
    def test_meta(self, st):
        meta = st.gen_Meta()
        assert "versionId" in meta and "lastUpdated" in meta

    def test_narrative(self, st):
        n = st.gen_Narrative()
        assert n["status"] == "generated"
        assert n["div"].startswith("<div")

    def test_reference_with_type(self, st):
        ref = st.gen_Reference("Patient", "abc-123")
        assert ref["reference"] == "Patient/abc-123"
        assert ref["type"] == "Patient"

    def test_extension(self, st):
        ext = st.gen_Extension(url="http://example.org/ext", value_type="boolean")
        assert ext["url"] == "http://example.org/ext"
        assert "valueBoolean" in ext

    def test_dosage(self, st):
        d = st.gen_Dosage()
        assert "timing" in d and "doseAndRate" in d
        assert d["doseAndRate"][0]["doseQuantity"]["unit"] == "mg"
```
```

---

## PROMPT 18: Test Suite — Resources & Integration

```
Create `tests/test_resources.py` and `tests/test_integration.py`.

### test_resources.py
```python
import pytest
from fhir_gen.generators.base import ResourceGenerator

SEED = 42


def make_gen():
    return ResourceGenerator(seed=SEED)


class TestClinicalResources:
    def test_patient_structure(self):
        gen = make_gen()
        patients = gen.generate("Patient", count=3)
        assert len(patients) == 3
        for p in patients:
            assert p["resourceType"] == "Patient"
            assert "id" in p and "meta" in p
            assert p["gender"] in ["male", "female", "other", "unknown"]
            assert "birthDate" in p
            assert "name" in p and len(p["name"]) > 0
            assert "identifier" in p and len(p["identifier"]) > 0

    def test_practitioner_structure(self):
        gen = make_gen()
        pr = gen.generate("Practitioner")[0]
        assert pr["resourceType"] == "Practitioner"
        assert "name" in pr and "identifier" in pr
        assert pr["active"] is True

    def test_organization_structure(self):
        gen = make_gen()
        org = gen.generate("Organization")[0]
        assert org["resourceType"] == "Organization"
        assert "name" in org and len(org["name"]) > 0

    def test_encounter_references_patient(self):
        gen = make_gen()
        gen.generate("Patient", count=2)
        enc = gen.generate("Encounter")[0]
        assert "subject" in enc
        assert enc["subject"]["reference"].startswith("Patient/")

    def test_observation_has_value(self):
        gen = make_gen()
        gen.generate("Patient")
        obs = gen.generate("Observation")[0]
        assert obs["resourceType"] == "Observation"
        assert "code" in obs
        # Should have at least one value type
        value_fields = [k for k in obs if k.startswith("value")]
        assert len(value_fields) > 0

    def test_condition_has_clinical_status(self):
        gen = make_gen()
        gen.generate("Patient")
        cond = gen.generate("Condition")[0]
        assert "clinicalStatus" in cond
        assert "coding" in cond["clinicalStatus"]
        cs_code = cond["clinicalStatus"]["coding"][0]["code"]
        assert cs_code in ["active", "recurrence", "relapse", "inactive", "remission", "resolved"]

    def test_allergy_intolerance(self):
        gen = make_gen()
        gen.generate("Patient")
        allergy = gen.generate("AllergyIntolerance")[0]
        assert "clinicalStatus" in allergy
        assert "code" in allergy
        assert "reaction" in allergy
        assert allergy["criticality"] in ["low", "high", "unable-to-assess"]

    def test_procedure_has_body_site(self):
        gen = make_gen()
        gen.generate("Patient")
        proc = gen.generate("Procedure")[0]
        assert "code" in proc and "status" in proc
        assert "subject" in proc

    def test_diagnostic_report(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Observation", count=3)
        dr = gen.generate("DiagnosticReport")[0]
        assert "status" in dr and "code" in dr

    def test_immunization(self):
        gen = make_gen()
        gen.generate("Patient")
        imm = gen.generate("Immunization")[0]
        assert "vaccineCode" in imm
        assert "patient" in imm
        assert imm["status"] in ["completed", "not-done"]


class TestMedicationResources:
    def test_medication_has_rxnorm_code(self):
        gen = make_gen()
        med = gen.generate("Medication")[0]
        assert "code" in med
        coding = med["code"]["coding"][0]
        assert coding["system"] == "http://www.nlm.nih.gov/research/umls/rxnorm"

    def test_medication_request_chain(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Practitioner")
        mr = gen.generate("MedicationRequest")[0]
        assert "subject" in mr
        assert mr["subject"]["reference"].startswith("Patient/")
        assert "medication" in mr
        assert "dosageInstruction" in mr

    def test_medication_administration(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("MedicationRequest")
        ma = gen.generate("MedicationAdministration")[0]
        assert "medication" in ma and "subject" in ma
        assert "dosage" in ma

    def test_medication_dispense(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("MedicationRequest")
        md = gen.generate("MedicationDispense")[0]
        assert "medication" in md and "subject" in md
        assert "quantity" in md


class TestWorkflowResources:
    def test_appointment_has_participants(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Practitioner")
        appt = gen.generate("Appointment")[0]
        assert "participant" in appt
        assert len(appt["participant"]) >= 1
        assert "start" in appt and "end" in appt

    def test_care_plan_has_goal(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Goal")
        cp = gen.generate("CarePlan")[0]
        assert "status" in cp
        assert "subject" in cp

    def test_task_has_status(self):
        gen = make_gen()
        gen.generate("Patient")
        task = gen.generate("Task")[0]
        assert task["status"] in ["draft", "requested", "received", "accepted",
                                   "in-progress", "completed", "cancelled"]

    def test_service_request(self):
        gen = make_gen()
        gen.generate("Patient")
        sr = gen.generate("ServiceRequest")[0]
        assert "code" in sr and "status" in sr and "subject" in sr


class TestFinancialResources:
    def test_coverage_structure(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        cov = gen.generate("Coverage")[0]
        assert "beneficiary" in cov and "insurer" in cov
        assert "period" in cov

    def test_claim_with_coverage(self):
        gen = make_gen()
        gen.generate("Patient")
        gen.generate("Organization")
        gen.generate("Coverage")
        gen.generate("Practitioner")
        claim = gen.generate("Claim")[0]
        assert "patient" in claim and "insurer" in claim
        assert "insurance" in claim


class TestSpecializedResources:
    def test_specimen_collection(self):
        gen = make_gen()
        gen.generate("Patient")
        spec = gen.generate("Specimen")[0]
        assert "type" in spec and "collection" in spec

    def test_imaging_study(self):
        gen = make_gen()
        gen.generate("Patient")
        img = gen.generate("ImagingStudy")[0]
        assert "modality" in img
        assert "series" in img and len(img["series"]) > 0

    def test_device(self):
        gen = make_gen()
        dev = gen.generate("Device")[0]
        assert "name" in dev and len(dev["name"]) > 0

    def test_research_study(self):
        gen = make_gen()
        gen.generate("Organization")
        rs = gen.generate("ResearchStudy")[0]
        assert "title" in rs and "status" in rs

    def test_group(self):
        gen = make_gen()
        gen.generate("Patient", count=3)
        grp = gen.generate("Group")[0]
        assert "type" in grp and "quantity" in grp
```

### test_integration.py
```python
import pytest
from fhir_gen.generators.base import ResourceGenerator
from fhir_gen.resolvers.dependency import resolve_order

SEED = 99


class TestDependencyResolution:
    def test_patient_has_no_deps(self):
        order = resolve_order(["Patient"])
        assert "Patient" in order

    def test_observation_after_patient(self):
        order = resolve_order(["Observation", "Patient"])
        assert order.index("Patient") < order.index("Observation")

    def test_encounter_after_patient(self):
        order = resolve_order(["Encounter", "Patient"])
        assert order.index("Patient") < order.index("Encounter")

    def test_claim_after_coverage(self):
        order = resolve_order(["Claim", "Coverage", "Patient", "Organization"])
        assert order.index("Coverage") < order.index("Claim")
        assert order.index("Patient") < order.index("Claim")

    def test_medication_request_chain(self):
        order = resolve_order(["MedicationRequest", "Patient", "Practitioner", "Encounter"])
        assert order.index("Patient") < order.index("MedicationRequest")
        assert order.index("Practitioner") < order.index("MedicationRequest")


class TestCrossResourceReferences:
    def test_encounter_references_registered_patient(self):
        gen = ResourceGenerator(seed=SEED)
        patients = gen.generate("Patient", count=3)
        patient_ids = {f"Patient/{p['id']}" for p in patients}
        encounters = gen.generate("Encounter", count=5)
        for enc in encounters:
            if "subject" in enc:
                assert enc["subject"]["reference"] in patient_ids

    def test_observation_references_registered_encounter(self):
        gen = ResourceGenerator(seed=SEED)
        gen.generate("Patient", count=2)
        encounters = gen.generate("Encounter", count=2)
        enc_ids = {f"Encounter/{e['id']}" for e in encounters}
        observations = gen.generate("Observation", count=5)
        for obs in observations:
            if "encounter" in obs:
                assert obs["encounter"]["reference"] in enc_ids

    def test_medication_request_references_patient(self):
        gen = ResourceGenerator(seed=SEED)
        patients = gen.generate("Patient", count=2)
        gen.generate("Practitioner", count=1)
        patient_ids = {f"Patient/{p['id']}" for p in patients}
        mrs = gen.generate("MedicationRequest", count=3)
        for mr in mrs:
            if "subject" in mr:
                assert mr["subject"]["reference"] in patient_ids


class TestPolymorphicVariants:
    def test_generate_variants_observation(self):
        gen = ResourceGenerator(seed=SEED)
        gen.generate("Patient", count=1)
        variants = gen.generate_variants("Observation")
        assert len(variants) >= 2
        value_keys = {k for v in variants for k in v if k.startswith("value")}
        assert len(value_keys) >= 2

class TestGenerateMany:
    def test_generate_many_basic(self):
        gen = ResourceGenerator(seed=SEED)
        results = gen.generate_many(
            ["Patient", "Practitioner", "Organization", "Encounter"],
            counts={"Patient": 3, "Practitioner": 2, "Organization": 2, "Encounter": 5}
        )
        assert len(results["Patient"]) == 3
        assert len(results["Practitioner"]) == 2
        assert len(results["Encounter"]) == 5

    def test_generate_many_clinical_bundle(self):
        gen = ResourceGenerator(seed=SEED)
        resources = [
            "Patient", "Practitioner", "Organization", "Location",
            "Encounter", "Condition", "Observation", "MedicationRequest",
            "AllergyIntolerance", "Procedure"
        ]
        counts = {r: 2 for r in resources}
        counts["Patient"] = 5
        results = gen.generate_many(resources, counts=counts)
        for rtype in resources:
            assert rtype in results
            assert len(results[rtype]) > 0

    def test_all_resources_have_required_fields(self):
        gen = ResourceGenerator(seed=SEED)
        results = gen.generate_many(
            ["Patient", "Condition", "Observation", "MedicationRequest"],
            counts={"Patient": 2, "Condition": 2, "Observation": 2, "MedicationRequest": 2}
        )
        for rtype, resources in results.items():
            for resource in resources:
                assert "resourceType" in resource, f"{rtype} missing resourceType"
                assert "id" in resource, f"{rtype} missing id"
                assert resource["resourceType"] == rtype


class TestDataCorrectness:
    def test_observation_values_in_range(self):
        gen = ResourceGenerator(seed=SEED)
        gen.generate("Patient", count=2)
        observations = gen.generate("Observation", count=20)
        for obs in observations:
            if "valueQuantity" in obs and "referenceRange" in obs:
                val = obs["valueQuantity"]["value"]
                low = obs["referenceRange"][0]["low"]["value"]
                high = obs["referenceRange"][0]["high"]["value"]
                assert low <= val <= high, f"Value {val} outside range [{low}, {high}]"

    def test_patient_age_reasonable(self):
        gen = ResourceGenerator(seed=SEED)
        patients = gen.generate("Patient", count=20)
        from datetime import datetime
        for p in patients:
            if "birthDate" in p and len(p["birthDate"]) == 10:
                bd = datetime.strptime(p["birthDate"], "%Y-%m-%d")
                age_days = (datetime.now() - bd).days
                assert 0 <= age_days <= 365 * 120, f"Unreasonable age: {age_days} days"

    def test_medication_request_status_valid(self):
        gen = ResourceGenerator(seed=SEED)
        gen.generate("Patient")
        gen.generate("Practitioner")
        mrs = gen.generate("MedicationRequest", count=10)
        valid = ["active", "on-hold", "cancelled", "completed",
                 "entered-in-error", "stopped", "draft", "unknown"]
        for mr in mrs:
            assert mr["status"] in valid

    def test_encounter_period_order(self):
        gen = ResourceGenerator(seed=SEED)
        gen.generate("Patient")
        encounters = gen.generate("Encounter", count=5)
        for enc in encounters:
            if "actualPeriod" in enc:
                start = enc["actualPeriod"].get("start", "")
                end = enc["actualPeriod"].get("end", "")
                if start and end:
                    assert start <= end


class TestReferenceIntegrity:
    def test_store_registers_generated_resources(self):
        gen = ResourceGenerator(seed=SEED)
        patients = gen.generate("Patient", count=3)
        assert gen.store.count("Patient") == 3
        for p in patients:
            ref = gen.store.get_reference("Patient", gen.rng)
            assert ref is not None
            assert ref["reference"].startswith("Patient/")

    def test_find_resource_by_reference(self):
        gen = ResourceGenerator(seed=SEED)
        patients = gen.generate("Patient", count=2)
        p = patients[0]
        entry = gen.store._store["Patient"][0]
        assert entry["id"] == p["id"]
        assert entry["reference"] == f"Patient/{p['id']}"
```
```

---

## EXECUTION ORDER SUMMARY

```
Run prompts in this exact order:

 0. PROMPT 0  → verify repo inputs (no code)
 1. PROMPT 1  → scaffold, pyproject.toml, config, copy fhir.schema.v5.json
 2. PROMPT 2  → schema parser & registry
 3. PROMPT 3  → codes loader + enrich healthcare_codes.yaml (do not rewrite file)
 4. PROMPT 4  → primitive generators
 5. PROMPT 5  → complex type generators
 6. PROMPT 6  → special & metadata type generators
 7. PROMPT 7  → reference resolver & dependency graph
 8. PROMPT 8  → base resource generator engine
 9. PROMPT 9  → clinical resource enrichers
10. PROMPT 10 → medication resource enrichers
11. PROMPT 11 → workflow resource enrichers
12. PROMPT 12 → financial resource enrichers
13. PROMPT 13 → specialized resource enrichers
14. PROMPT 14 → MongoDB persistence layer
15. PROMPT 15 → CLI interface
16. PROMPT 16 → README, .env.example, polish pyproject metadata
17. PROMPT 17 → tests: primitives & complex types
18. PROMPT 18 → tests: resources & integration

Verification after each prompt:
  pip install -e .[dev]
  python -c "from fhir_gen.config import settings; print('OK')"

After Prompt 8:
  python -c "from fhir_gen import ResourceGenerator; print(ResourceGenerator(seed=1).generate('Patient',1)[0]['resourceType'])"

Run tests after Prompt 18:
  pytest tests/ -v --cov=fhir_gen

Run CLI smoke test after Prompt 15:
  fhir-gen list-resources
  fhir-gen generate Patient --count 2 --no-save --output -
  fhir-gen generate Observation --variants --no-save --output -
  fhir-gen generate-many Patient Encounter Observation --counts '{"Patient":3,"Encounter":5,"Observation":10}' --output-dir ./output
```

---

## KEY DESIGN DECISIONS

| Decision | Rationale |
|---|---|
| Schema-first for all 158 resources | Meets INSTRUCTIONS #1/#6; enrichers optional for ~55 types |
| One enricher per high-value resource | Keeps engine generic; enrichers are pure functions, easy to test |
| `generate_variants()` + `--variants` | INSTRUCTIONS #7: separate docs per polymorphic choice |
| CORE_DEPENDENCIES dict | Fast ordering; extend from `FHIR_RESOURCE_URLS.txt` as needed |
| ReferenceStore in-memory | Session-scoped; cross-resource refs for search (INSTRUCTIONS #13) |
| Seeded `random.Random` | Reproducible datasets for testing and debugging |
| One MongoDB collection per resource type | Type-specific indexes; FHIR-style search helpers |
| Polymorphic groups from parser | Picks one variant by default; variants mode enumerates all |
| Existing `healthcare_codes.yaml` | Avoids ~800 lines of duplicate YAML in prompts (token cost) |
| Kahn's topological sort | Dependencies generated before dependents (INSTRUCTIONS #5) |

---

## APPENDIX A: FHIR R5 datatype coverage (`FHIR_DATATYPES.txt`)

| Section | Types | Prompt |
|---------|-------|--------|
| Primitives (§1) | base64Binary … uuid, xhtml | 4 |
| General (§2) | Address … Timing, Count, Distance, MoneyQuantity, RatioRange, Signature | 5 |
| Metadata (§3) | Availability … VirtualServiceDetail | 6 |
| Special (§4) | Dosage, ElementDefinition, Extension, Meta, Narrative, Reference | 6 |
| Resources | All URLs in `FHIR_RESOURCE_URLS.txt` | 2, 8 (+ enrichers 9–13) |

---

## APPENDIX B: Resource scope

- **158** resources in `fhir.schema.v5.json` — all must pass `generate(ResourceType)` with required fields populated.
- **157** HL7 spec URLs in `FHIR_RESOURCE_URLS.txt` — use for binding/enricher research.
- **Infrastructure types** (Bundle, CapabilityStatement, StructureDefinition, etc.): schema-only generation is sufficient unless a use case needs enrichers.
