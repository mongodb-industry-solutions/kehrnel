# kehrnel Mapping Grammar & Expression DSL

This document explains how to write **kehrnel** mapping files to transform CSV/XML sources
into openEHR compositions, plus the tiny, safe **Expression DSL** used in `where` / `when`.

> TL;DR: `input` → `select.where` → `group_by` → `compose`.  
> In `compose.content[*].map`, each rule writes a value into an openEHR JSON path.
>
> No Python `eval()` anywhere — conditions use a small, safe DSL.

---

## 1) Top-level sections

```yaml
input:            # how to read the source
  kind: csv
  columns: [paciente, centro, servicio, consulta, etapa, prom, fecha, pregunta, respuesta]
  date_format: "%d/%m/%Y"

_output:          # how to write results
  filename_template: "example_{{ first.paciente }}_{{ first.consulta }}_{{ index|int }}.json"

_options:
  prune_empty: true            # drop empty nodes from the final JSON

_translation:                  # optional - rule-level override with `translate:`
  enable: true
  source_lang: es
  target_lang: en
  cache: .kehrnel/translations.json

select:                         # pre-filter rows
  where:
    - servicio == "VIH eVIHa"
    - respuesta != "N/A"

group_by: [paciente, servicio, consulta]   # one COMPOSITION per visit

compose:
  header:
    language: en
    territory: ES
    category: event
    context.start_time: ${fecha}            # ${col} means take from CSV column

  content:
    - observation: openEHR-EHR-OBSERVATION.example.v0
      map:
        - path: "data[at0001]/origin/value"
          from: fecha
          coerce: date_iso
          list_mode: last
```
Notes:
- `filename_template` is Jinja — you can use `first`, `rows`, and `index`.
- `list_mode: last` is common when many rows in a group set the same path.
- Translation: rule-level `translate: off | on | no-cache` (defaults to on unless the path
  targets code fields like `/defining_code/code_string`, `/language/code_string`, etc.).

---

## 2) Mapping rules (inside `map:`)

A rule writes a value into an openEHR path.

**Common fields**
- `path` (required): e.g., `"data[at0001]/events[at0002]/time/value"`
- `from`: CSV/XML field name
- `literal`: constant value
- `when`: condition (Expression DSL). If false, rule is skipped
- `coerce` / `transform`: a transform name from the `transforms` registry (e.g., `date_iso`, `int`, `trim`)
- `default`: fallback value when input is empty
- `null_flavour`: sets an openEHR null_flavour code
- `list_mode`: `last` (keep final value), `first` (keep first), `append` (if path is a list)
- `translate`: `off | on | no-cache`

**Value types**
- **DV_ORDINAL**
  ```yaml
  - path: "data[...]/items[atXXXX]/value"
    type: ordinal
    from: respuesta
    choices:
      "Nunca":         { code: "at1001", ordinal: 1 }
      "Rara vez":      { code: "at1002", ordinal: 2 }
      "Algunas veces": { code: "at1003", ordinal: 3 }
      "A menudo":      { code: "at1004", ordinal: 4 }
      "Siempre":       { code: "at1005", ordinal: 5 }
  ```

- **DV_COUNT**
  ```yaml
  - path: "data[...]/items[at0150]/value"
    type: count
    from: respuesta
    coerce: int
  ```

- **DV_QUANTITY**
  ```yaml
  - path: "data[...]/items[at1805]"
    quantity:
      magnitude_from: respuesta
      units: "1"
  ```

- **DV_TEXT**
  ```yaml
  - path: "data[...]/items[at1900]/value"
    from: respuesta
    null_if_empty: true
    translate: no-cache
  ```

---

## 3) Expression DSL (safe)

Supported operators:
- Boolean: `and`, `or`, `not`, parentheses
- Comparisons: `== != < <= > >=`
- Membership: `in`, `not in` (`respuesta in ["Nunca", "Siempre"]`)
- Regex: `~=` (match), `!~=` (no match), `?=` (case-insensitive match shorthand)
- Text helpers: `a contains b`, `a startswith b`, `a endswith b`
- Literals: strings `'...'` or `"..."`, numbers, `true/false/null`

Whitelisted functions:
- `regex(text, pattern, flags="")`
- `contains(a, b)`, `startswith(a, b)`, `endswith(a, b)`
- `len(x)`, `lower(x)`, `upper(x)`, `trim(x)`
- `to_int(x)`, `to_float(x)`, `parse_date(text, fmt)`

**Examples**
```
servicio == "VIH eVIHa" and respuesta != "N/A"
pregunta ?= "(?i)excluido|excluida"
respuesta in ["Nunca","Rara vez","Siempre"]
contains(pregunta, "Observa") and not regex(respuesta, "N/A|NA", "i")
```

---

## 4) Tips
- Prefer the DSL for all `where`/`when` logic for consistency across handlers.
- Use tolerant regex for question stems (spaces, punctuation, diacritics).
- Put free text under `translate: no-cache` so it’s translated once and cached.
- Keep code/terminology targets on `translate: off`.
