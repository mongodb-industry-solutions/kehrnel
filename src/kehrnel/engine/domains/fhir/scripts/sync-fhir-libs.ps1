# Sync vendored FHIR libraries into ../libs/ (no submodules).
# Usage: ./src/kehrnel/engine/domains/fhir/scripts/sync-fhir-libs.ps1 [-SourceRoot <path-to-code_repositories>]

param(
    [string]$SourceRoot = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = $PSScriptRoot
$LibsRoot = Resolve-Path (Join-Path $ScriptDir "../libs")
$KehrnelRoot = Resolve-Path (Join-Path $ScriptDir "../../../../../../")

if (-not $SourceRoot) {
    $SourceRoot = (Resolve-Path (Join-Path $KehrnelRoot "../../..")).Path
}

$Pairs = @(
    @{ Name = "fhir-data-generation"; Src = Join-Path $SourceRoot "fhir-data-generation" },
    @{ Name = "fhir-search-to-mql"; Src = Join-Path $SourceRoot "fhir-search-to-mql" }
)
$ExcludeDirs = @(
    ".git", ".venv", "venv", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache",
    "dist", "build", ".eggs", "node_modules", "htmlcov", ".tox", "coverage", ".cursor"
)

foreach ($pair in $Pairs) {
    if (-not (Test-Path $pair.Src)) {
        Write-Error "Source not found: $($pair.Src)"
    }
    $dest = Join-Path $LibsRoot $pair.Name
    if (Test-Path $dest) {
        $item = Get-Item $dest -Force
        if ($item.LinkType) {
            cmd /c "rmdir `"$dest`"" | Out-Null
        } else {
            Remove-Item $dest -Recurse -Force
        }
    }
    $xd = ($ExcludeDirs | ForEach-Object { "/XD"; $_ }) -join " "
    $cmd = "robocopy `"$($pair.Src)`" `"$dest`" /E /MT:16 $xd /NFL /NDL /NJH /NJS"
    Invoke-Expression $cmd | Out-Null
    if ($LASTEXITCODE -ge 8) {
        Write-Error "robocopy failed for $($pair.Name) (exit $LASTEXITCODE)"
    }
    Write-Host "Synced $($pair.Name) -> $dest"
}

Write-Host "Done. From kehrnel root, reinstall:"
Write-Host "  pip install -e src/kehrnel/engine/domains/fhir/libs/fhir-data-generation -e src/kehrnel/engine/domains/fhir/libs/fhir-search-to-mql"
Write-Host "  pip install -e `".[fhir]`""
