<#
    Ingest the synthetic SNOMED release + synthetic FHIR Conditions into the
    running Kehrnel, then verify.

    SYNTHETIC DEMONSTRATION CONTENT. The SNOMED-shaped release is NOT an
    official SNOMED CT distribution and the patients are not real people.
    See .kehrnel/snomedct/releases/README-hdl-synthetic-2.md.

    Both steps are ADDITIVE (upsert). Nothing is dropped:
      * snomed_ingest_release writes under releaseId 'hdl-synthetic-2', leaving
        'hdl-demo-synthetic-1' untouched in the same collection.
      * fhir_import_resources runs mode=upsert, so the existing 60 Conditions
        remain; the 890 new ones have distinct ids (hdl-syn-c*).

    USAGE
      # 1. See what WOULD happen - writes nothing:
      .\scripts\ingest_hdl_synthetic.ps1 -DryRun

      # 2. Do it for real:
      .\scripts\ingest_hdl_synthetic.ps1 -Confirm

      # Optional: skip a stage, or point elsewhere
      .\scripts\ingest_hdl_synthetic.ps1 -Confirm -SkipFhir
      .\scripts\ingest_hdl_synthetic.ps1 -Confirm -BaseUrl http://127.0.0.1:8080
#>

[CmdletBinding()]
param(
    [switch] $DryRun,
    [switch] $Confirm,
    [switch] $SkipSnomed,
    [switch] $SkipFhir,
    [switch] $RepointRelease = $true,
    [string] $BaseUrl = "http://127.0.0.1:8080",
    [string] $EnvId = "env-1768371326510",
    [string] $ReleaseId = "hdl-synthetic-2",
    [string] $ReleaseFile = "hdl-synthetic-2.json",
    [int]    $ChunkSize = 250
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$hdlEnv = Join-Path (Split-Path -Parent $repo) "HealthcareDataLab\.env.local"

if (-not $DryRun -and -not $Confirm) {
    Write-Host ""
    Write-Host "Refusing to write without an explicit flag." -ForegroundColor Yellow
    Write-Host "  Preview (writes nothing):  .\scripts\ingest_hdl_synthetic.ps1 -DryRun"
    Write-Host "  Execute:                   .\scripts\ingest_hdl_synthetic.ps1 -Confirm"
    Write-Host ""
    exit 2
}
$isDry = [bool]$DryRun

# ---------------------------------------------------------------- api key
$apiKey = $env:KEHRNEL_API_KEY
if (-not $apiKey -and (Test-Path $hdlEnv)) {
    foreach ($line in Get-Content $hdlEnv) {
        if ($line -match "^\s*KEHRNEL_API_KEY\s*=\s*(.+?)\s*$") { $apiKey = $Matches[1] }
    }
}
if (-not $apiKey) { throw "No API key. Set `$env:KEHRNEL_API_KEY or add KEHRNEL_API_KEY to $hdlEnv" }

$headers = @{ "Accept" = "application/json"; "X-API-Key" = $apiKey; "X-Active-Env" = $EnvId }
$runUrl  = "$BaseUrl/environments/$EnvId/run"

function Invoke-Op {
    param([string] $Operation, [string] $Domain, [hashtable] $Payload, [int] $TimeoutSec = 600)
    $body = @{ operation = $Operation; domain = $Domain; payload = $Payload } | ConvertTo-Json -Depth 10 -Compress
    $r = Invoke-WebRequest -Uri $runUrl -Method POST -Headers $headers -ContentType "application/json" `
                           -Body ([System.Text.Encoding]::UTF8.GetBytes($body)) -UseBasicParsing -TimeoutSec $TimeoutSec
    return ($r.Content | ConvertFrom-Json).result
}

function Show-Err {
    param($ErrorRecord, [string] $What)
    Write-Host ("  FAILED: " + $What) -ForegroundColor Red
    if ($ErrorRecord.Exception.Response) {
        $sr = New-Object System.IO.StreamReader($ErrorRecord.Exception.Response.GetResponseStream())
        $t = $sr.ReadToEnd()
        if ($t.Length -gt 1500) { $t = $t.Substring(0,1500) + " ..." }
        Write-Host ("  " + $t) -ForegroundColor Red
    } else {
        Write-Host ("  " + $ErrorRecord.Exception.Message) -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "=======================================================================" -ForegroundColor Cyan
Write-Host ("  Kehrnel : " + $BaseUrl)
Write-Host ("  Env     : " + $EnvId)
Write-Host ("  Mode    : " + $(if ($isDry) { "DRY RUN (no writes)" } else { "LIVE - WILL WRITE" }))
Write-Host "  Content : SYNTHETIC. Not SNOMED CT. Not real patients."
Write-Host "=======================================================================" -ForegroundColor Cyan

# ---------------------------------------------------------------- preflight
Write-Host ""
Write-Host "[0] Preflight" -ForegroundColor Cyan
try {
    $h = Invoke-WebRequest -Uri "$BaseUrl/health" -UseBasicParsing -TimeoutSec 10
    Write-Host ("  health              : " + $h.Content)
} catch { Show-Err $_ "Kehrnel is not answering at $BaseUrl"; exit 1 }

try {
    $before = Invoke-Op -Operation "snomed_readiness" -Domain "snomedct" -Payload @{} -TimeoutSec 60
    Write-Host ("  snomed release now  : " + $before.releaseId + "  (" + $before.canonical.concept_count + " concepts, " + $before.sidecar.term_count + " terms)")
} catch { Show-Err $_ "snomed_readiness"; }

try {
    $fb = Invoke-Op -Operation "fhir_stats" -Domain "fhir" -Payload @{ resource_types = @("Condition") } -TimeoutSec 120
    $c = $fb.collections | Where-Object { $_.resource_type -eq "Condition" }
    Write-Host ("  Condition docs now  : " + $c.document_count + " (denormalized " + $c.denormalized_count + ")")
} catch { Show-Err $_ "fhir_stats" }

# ---------------------------------------------------------------- 1. SNOMED
if (-not $SkipSnomed) {
    Write-Host ""
    Write-Host "[1] SNOMED release -> $ReleaseId" -ForegroundColor Cyan
    $p = @{
        file_name    = $ReleaseFile
        release_id   = $ReleaseId
        release_label = "Synthetic SNOMED-shaped demonstration hierarchy (NOT an official SNOMED CT release)"
        dry_run      = $isDry
        # Additive: never drop the existing release.
        drop_before_ingest = $false
        rebuild_sidecar    = $true
    }
    # The engine refuses a real write without explicit acknowledgement that the
    # tenant is authorized to use the staged content. This content is synthetic.
    if (-not $isDry) { $p["license_acknowledged"] = $true }

    try {
        $res = Invoke-Op -Operation "snomed_ingest_release" -Domain "snomedct" -Payload $p -TimeoutSec 1800
        Write-Host ("  ok            : " + $res.ok)
        Write-Host ("  dry_run       : " + $res.dry_run)
        Write-Host ("  collection    : " + $res.collection)
        Write-Host ("  concepts_seen : " + $res.concepts_seen)
        Write-Host ("  upserted      : " + $res.upserted + "   modified: " + $res.modified)
        if ($res.sidecar) {
            Write-Host ("  sidecar       : " + $res.sidecar.term_documents + " term docs over " + $res.sidecar.concepts_seen + " concepts")
        }
    } catch { Show-Err $_ "snomed_ingest_release"; exit 1 }
}

# ------------------------------------------------- 2. repoint activation
# Ingest alone is not enough: every read resolves release.id from the ACTIVATION
# CONFIG, so without this the 2978 new concepts sit in the collection unused and
# readiness keeps reporting hdl-demo-synthetic-1.
if ($RepointRelease -and -not $SkipSnomed) {
    Write-Host ""
    Write-Host "[2] Repoint snomedct activation release.id -> $ReleaseId" -ForegroundColor Cyan
    # NOT done over HTTP, deliberately.
    #
    # POST /environments/{env}/activate requires `bindings_ref` whenever auth is
    # enabled (BINDINGS_REF_REQUIRED), and this activation has bindings_ref=None
    # because it was created with plaintext bindings. Re-activating therefore
    # returns 400, and forcing it through would discard the Mongo binding the
    # strategy needs. The surgical registry edit is the only safe route.
    try {
        $acts = (Invoke-WebRequest -Uri "$BaseUrl/environments/$EnvId/activations" -Headers $headers `
                 -UseBasicParsing -TimeoutSec 30).Content | ConvertFrom-Json
        $sn = $acts.activations.snomedct
        Write-Host ("  current release.id  : " + $sn.config.release.id)
    } catch { Show-Err $_ "read activation" }

    Write-Host ""
    Write-Host "  The activation is repointed by a separate, explicit step:" -ForegroundColor Yellow
    Write-Host ("    .venv\Scripts\python.exe scripts\repoint_snomed_release.py --release-id " + $ReleaseId) -ForegroundColor Gray
    Write-Host "    (backs up the registry, rewrites only config.release, recomputes config_hash)"
    Write-Host "  Then RESTART Kehrnel - the registry is read at startup." -ForegroundColor Yellow
}

# ---------------------------------------------------------------- 3. FHIR
if (-not $SkipFhir) {
    Write-Host ""
    Write-Host "[3] FHIR Patients + Conditions (upsert, additive)" -ForegroundColor Cyan
    $dataDir = Join-Path $repo "samples\in\data\FHIR-HDL-Synthetic-Conditions"
    foreach ($file in @("patients.ndjson", "conditions.ndjson")) {
        $path = Join-Path $dataDir $file
        if (-not (Test-Path $path)) { Write-Host ("  MISSING: " + $path) -ForegroundColor Red; continue }

        $lines = [System.IO.File]::ReadAllLines($path) | Where-Object { $_.Trim() -ne "" }
        Write-Host ("  " + $file + ": " + $lines.Count + " resources, chunks of " + $ChunkSize)

        $sent = 0; $written = 0; $failed = 0
        for ($i = 0; $i -lt $lines.Count; $i += $ChunkSize) {
            $chunk = $lines[$i..([Math]::Min($i + $ChunkSize - 1, $lines.Count - 1))]
            $p = @{
                ndjson  = ($chunk -join "`n")
                mode    = "upsert"
                dry_run = $isDry
                fail_on_error = $true
            }
            try {
                $res = Invoke-Op -Operation "fhir_import_resources" -Domain "fhir" -Payload $p -TimeoutSec 900
                $sent += $chunk.Count
                $w = 0
                foreach ($k in @("written","upserted","persisted","imported","created")) {
                    if ($null -ne $res.$k) { $w = [int]$res.$k; break }
                }
                $written += $w
                Write-Host ("    chunk " + [Math]::Floor($i / $ChunkSize + 1) + ": sent " + $chunk.Count + ", written " + $w)
            } catch {
                $failed += $chunk.Count
                Show-Err $_ ("chunk starting at line " + ($i + 1))
            }
        }
        Write-Host ("  -> sent " + $sent + ", written " + $written + ", failed " + $failed)
    }

    if (-not $isDry) {
        Write-Host ""
        Write-Host "  denormalize + ensure indexes" -ForegroundColor Cyan
        foreach ($op in @("fhir_denormalize", "fhir_ensure_indexes")) {
            try {
                $res = Invoke-Op -Operation $op -Domain "fhir" -Payload @{ resource_types = @("Condition","Patient") } -TimeoutSec 900
                Write-Host ("    " + $op + ": ok=" + $res.ok)
            } catch { Show-Err $_ $op }
        }
    }
}

# ---------------------------------------------------------------- 4. verify
Write-Host ""
Write-Host "[4] Verify" -ForegroundColor Cyan
try {
    $after = Invoke-Op -Operation "snomed_readiness" -Domain "snomedct" -Payload @{ release_id = $ReleaseId } -TimeoutSec 60
    Write-Host ("  readiness($ReleaseId): concepts=" + $after.canonical.concept_count + " terms=" + $after.sidecar.term_count + " ready=" + $after.canonical.ready)
} catch { Show-Err $_ "snomed_readiness($ReleaseId)" }

try {
    $fa = Invoke-Op -Operation "fhir_stats" -Domain "fhir" -Payload @{ resource_types = @("Condition") } -TimeoutSec 120
    $c = $fa.collections | Where-Object { $_.resource_type -eq "Condition" }
    Write-Host ("  Condition docs      : " + $c.document_count + " (denormalized " + $c.denormalized_count + ")")
} catch { Show-Err $_ "fhir_stats" }

Write-Host ""
Write-Host "Done. Reminder: synthetic demonstration content, not for clinical use." -ForegroundColor Cyan
Write-Host ""
