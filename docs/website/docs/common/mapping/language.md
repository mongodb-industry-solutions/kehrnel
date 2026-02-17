---
sidebar_position: 3
---

# Mapping Language

The mapping language is YAML-based and intentionally constrained to keep transformations deterministic and auditable.

## Design principles

- Declarative over imperative
- Explicit over implicit
- Secure-by-default (no arbitrary template execution)

## Minimum structure

Most mappings use these blocks:

- `input`: input shape definition (`csv`, `xml`, etc.)
- `group_by`: grouping keys (especially for row-based multi-patient files)
- `compose`: target composition layout

## Core expressions

### `xpath`

Extract values from XML/CDA sources.

```yaml
composer/name:
  xpath: //cda:representedCustodianOrganization/cda:name/text()
```

### `constant`

Set fixed values for required fields.

```yaml
"/content/0/items/0/language/code_string": "constant: en"
```

### `map`

Map source values to controlled target values.

```yaml
value/defining_code/code_string:
  xpath: //cda:ficheTumeur/cda:cote/text()
  map: { droit: at0004, gauche: at0003 }
```

### `when`

Conditionally include target blocks.

```yaml
when: "normalize-space(//cda:ficheTumeur/cda:cote/text()) != ''"
```

### `group_by`

Group row-level records before composition building.

```yaml
group_by:
  - IPP
  - IdentifiantAnalyse
```

## Example A: Patient-scoped CDA mapping

```yaml
language:
  code: en
  system: ISO_639-1

composer/name:
  xpath: //cda:representedCustodianOrganization/cda:name/text()

context/start_time:
  xpath: "concat(//cda:ficheTumeur/cda:dateCreation/text(),'T00:00:00')"

content/0/items/0/data/items/0/value/value:
  xpath: //cda:ficheTumeur/cda:codeLesionnel/text()
```

Use this pattern when one XML document carries mostly one patient/event context.

## Example B: Multi-patient CSV mapping

```yaml
input:
  kind: csv

group_by:
  - IPP
  - IdentifiantAnalyse

compose:
  header:
    language: en
    territory: FR
```

Use this pattern when one CSV contains many patients and repeated measurements.

## Security model

Template execution syntax is blocked in strict mode.

### Not allowed

```yaml
context/start_time:
  template: "{{ xpath('//cda:ficheTumeur/cda:dateCreation/text()') ~ 'T00:00:00' }}"
```

### Allowed rewrite

```yaml
context/start_time:
  xpath: "concat(//cda:ficheTumeur/cda:dateCreation/text(),'T00:00:00')"
```

Blocked syntax (for example `{{ ... }}`) should be rewritten using supported YAML expressions.

## Validation checklist

Before saving a mapping:

1. YAML parses cleanly.
2. No disabled template markers (`{{`, `{%`, `%}`).
3. Required target fields have either `xpath` or `constant` values.
4. `group_by` is defined for multi-row CSV inputs where needed.

Before promoting to production:

1. Run preview on attached samples.
2. Transform at least one document per type.
3. Validate output against selected OPT.
4. Confirm error payloads are readable in UI/API.

## Troubleshooting examples

### "Mapping YAML contains disabled template syntax"

Cause: Jinja/template marker found.

Fix: Replace template blocks with `xpath`, `constant`, `map`, and `when`.

### "0 documents transformed"

Likely causes:

- wrong `documentType -> mapping` association
- missing/incorrect `group_by` for CSV
- required path expressions resolve empty values

Start by validating identification result, then preview sample output.
