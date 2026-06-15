# Local FHIR indexes (generated)

**Data root:** repo `schema/` (outside `src/fhir_search_to_mql/`). Contains
`fhir.schema.v5.json`, `hl7.fhir.r5.search/`, and this `indexes/` folder.

Regenerate after updating schema JSON, HL7 search packages, or shipped configs:

```powershell
.\.venv\Scripts\python -m fhir_search_to_mql.schema.build_indexes
.\.venv\Scripts\python -m fhir_search_to_mql.schema.build_indexes --version R6
```

| File | Contents |
|------|----------|
| `resources.r5.json` | All R5 resource structures |
| `search-parameters.r5.json` | HL7 search parameters by resource |
| `search-parameters-shipped.r5.json` | Params from shipped `configs/*.yaml` |

Read one resource:

```powershell
.\.venv\Scripts\python -m fhir_search_to_mql.schema.resource_spec Condition
```

Override data location: `set FHIR_SCHEMA_ROOT=D:\path\to\schema`
