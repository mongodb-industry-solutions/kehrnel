---
sidebar_position: 1
---

# Mapping

The mapping layer converts heterogeneous source documents into openEHR-ready compositions with deterministic YAML rules.

This section is intentionally practical. It focuses on:

- how a document gets matched to the correct mapping
- how to write secure, maintainable mapping YAML
- how to debug mapping and transform failures quickly

## End-to-end view

```text
Source document(s) -> Identification -> Mapping YAML -> Transform -> Validate (OPT)
```

## Two concrete source shapes

### 1) Patient-scoped XML/CDA

- One document mostly represents one patient context.
- Typical extraction style uses `xpath`.
- Example source: `fiche_tumour.xml`.

### 2) Population-scoped CSV

- One file can contain rows from many patients.
- Mapping usually needs grouping keys (`group_by`) before composing output.
- Example source: `biology.csv`.

## Read next

- [Mapping Identification](./identification)
- [Mapping Language](./language)
