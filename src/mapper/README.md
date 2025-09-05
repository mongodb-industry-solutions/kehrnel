# kehrnel-mapper — Convert clinical documents to openEHR JSON
*Part of the [kehrnel](../..) tool-chain – can be used stand-alone.*

---

## 🎯 Why?

Most clinical integration work ends up **mapping** legacy documents (CDA, CSV, HL7v2 …) into openEHR compositions.  
`kehrnel-mapper` gives you:

* **Canonical JSON output** ready for `/composition` REST calls – no extra flat-to-canonical step.
* A **human-first YAML DSL** with shortcuts for the openEHR datatypes you use every day.
* Plug-in handlers for XML/CDA, CSV and JSON; more can be added in one class.
* A full validation round-trip so every generated composition is standards-compliant.

---

## ✨ Feature Highlights

| 🔢 | Feature | Description |
|----|---------|-------------|
|1|**Skeleton generator**|One command generates a *minimal* YAML mapping stub from any `.opt` or web-template.|
|2|**Rule macro system**|`@code`, `default:`, `null_flavour:` macros expand into long canonical paths.|
|3|**Iterators & conditionals**|`{i}` index placeholders, `{append}` injectors, and `when:` guards let you build dynamic clusters without Python code.|
|4|**Transform registry ⟶ Jinja**|Use built-in transforms (`hl7_to_iso8601`, `to_int`, …) or any Jinja2 expression.|
|5|**Batch-safe mapping**|Map one file or 100 000; errors gathered per-document so long jobs keep running.|
|6|**Strict validation loop**|Every result can be schema-checked (*offline*) or sent to an openEHR `/validate` end-point (*online*).|
|7|**Trace & metrics**|`--trace` prints rule-by-rule resolution.  A CSV report logs hit counts and null-flavour usage.|

---

## 🚀 Quick-start

```bash
pip install kehrnel[mapper]          # only the mapper extras
kehrnel-skeleton -t T-IGR-TUMOUR-SUMMARY.opt > tumour.yaml    # ①
# edit tumour.yaml …

kehrnel-map  \                       # ② Map one file
  -m tumour.yaml \
  -s samples/2163431_1026922.xml \
  -o out/2163431.json \
  --validate

kehrnel-map  \                       # ③ Batch + report
  -m tumour.yaml \
  -s samples/cda_zip/ \
  -o out/ --batch-report report.csv
```

---

## 📝 Mapping DSL (CDA focus)

```yaml
# ─ Template meta info ───────────────────────────
_metadata:
  source_format : "fiche_tumeur_cda"
  target_template: "T-IGR-TUMOUR-SUMMARY"
_options:
  prune_empty: true

# ─ Composition header ───────────────────────────
ctx/language:
  code:   "en"            # macro expands into canonical fields
  system: "ISO_639-1"

ctx/start_time:
  xpath: //cda:ficheTumeur/cda:dateCreation/text()
  transform: hl7_to_iso8601
  null_flavour: unknown

# ─ Problem–Diagnosis cluster ────────────────────
content[0]/items[0]/diagnosis:
  value:
    xpath: //cda:codeLesionnel/text()

content[0]/items[0]/anatomical_location/laterality:
  code:
    xpath: //cda:cote/text()
    map:
      droit: at0003
      gauche: at0004
  term:
    xpath: //cda:cote/text()
    map:
      droit: Left
      gauche: Right
  when: "normalize-space(//cda:cote/text()) != ''"
```

### Macros explained

| Macro | Expands to | Example |
|-------|------------|---------|
| `code:` | `value/defining_code/code_string` | `code: at0003` |
| `term:` | `value/value` | `term: Left` |
| `system:` | `value/defining_code/terminology_id/value` | `system: local` |
| `default:` | If rule resolves to None fill with default | `default: 'Not specified'` |
| `null_flavour:` | Auto-create null_flavour subtree | `null_flavour: unknown` |

---

## 🔧 CLI Reference

### kehrnel-skeleton

Generate starter YAML.

```bash
kehrnel-skeleton -t TEMPLATE.opt [--macros/--raw]
```

### kehrnel-map

Transform documents.

```bash
kehrnel-map -m MAP.yaml -s SRC (file|dir) [opts]
```

| Option | Description |
|--------|-------------|
| `--template/-t` | Load OPT/web-template – enables path verification. |
| `--batch-report` | Write per-file status & metrics CSV. |
| `--trace` | Verbose resolution log. |

### kehrnel-validate

Validate a canonical composition (used internally when `--validate` is passed to `kehrnel-map`).

---

## 🏗️ Architecture

```
             ┌────────────┐
 XML/CDA ───▶│ XMLHandler │
 CSV     ───▶│ CSVHandler │──┐
 JSON    ───▶│ JSONHandler │  │ extract_value()
             └─────┬──────┘  │
                   │         │
           mapping YAML   transforms.py
                   │         │
                   ▼         │
              MappingEngine──┘
                   │ _set_value_at_path()
                   ▼
            Composition skeleton
                   │
         prune_empty / defaults
                   ▼
            Canonical JSON
                   │
             openEHR validator
```

Handlers are plug-ins; add your own in `mapper/handlers/`.

---

## 🔌 Extending

* **New document type** – subclass `BaseHandler`, register in `HANDLERS` list.
* **New transform** – add a function to `transforms.py`, it becomes a Jinja filter automatically.
* **New macro** – implement in `dsl/macro_expander.py`, declare in `MACROS` dict.

---

## 📅 Status & Roadmap

| Status | Timeline | Features |
|--------|----------|----------|
| ✅ | 2024-Q2 | Skeleton generator, macros, CDA handler, unit tests |
| 🔄 | 2024-Q3 | CSV & JSON handlers, VS-Code JSON-Schema for DSL |
| 🛠 | 2024-Q4 | HL7v2 handler, Wizard to learn patterns from examples |

---

## 📄 License

Apache-2.0 – see LICENSE.

Built with ❤️ by the openEHR & MongoDB Playground team.