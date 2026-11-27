# FHIR domain scripts

| Script | Purpose |
|--------|---------|
| `sync-fhir-libs.ps1` | Copy standalone **fhir-data-generation** / **fhir-search-to-mql** repos into `../libs/` |
| `sync-fhir-libs.sh` | Same (Bash) |

Run from the **kehrnel repository root**:

```powershell
.\src\kehrnel\engine\domains\fhir\scripts\sync-fhir-libs.ps1
```

```bash
./src/kehrnel/engine/domains/fhir/scripts/sync-fhir-libs.sh
```

Default source root: `code_repositories/` (three levels above the kehrnel repo). Override with `-SourceRoot` (PowerShell) or the first argument (Bash).
