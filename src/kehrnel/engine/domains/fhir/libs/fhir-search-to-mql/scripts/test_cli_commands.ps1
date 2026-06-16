# Smoke-test every executable command documented in CLI_COMMANDS.md
# Usage: .\scripts\test_cli_commands.ps1 [-SkipPytest] [-SkipMongoBulk]
$ErrorActionPreference = "Continue"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $RepoRoot

$SkipPytest = $args -contains "-SkipPytest"
$SkipMongoBulk = $args -contains "-SkipMongoBulk"

$URI = if ($env:MONGODB_URI) { $env:MONGODB_URI } else { "mongodb://localhost:27017/" }
$DB  = if ($env:MONGODB_DB)  { $env:MONGODB_DB }  else { "fhir_synthetic" }
$env:MONGODB_URI = $URI
$env:MONGODB_DB  = $DB

$passed = 0
$failed = 0
$skipped = 0
$results = [System.Collections.Generic.List[object]]::new()

function Test-Cmd {
    param(
        [string]$Name,
        [string]$Command,
        [switch]$Skip,
        [string[]]$MustNotContain = @(),
        [int]$ExpectExit = 0
    )
    if ($Skip) {
        $script:skipped++
        $results.Add([pscustomobject]@{ Status = "SKIP"; Name = $Name; Detail = "skipped" })
        Write-Host "[SKIP] $Name" -ForegroundColor Yellow
        return
    }
    Write-Host "[RUN ] $Name" -ForegroundColor DarkGray
    $out = Invoke-Expression "$Command 2>&1" | Out-String
    $exit = $LASTEXITCODE
    $bad = $false
    $detail = ""
    if ($exit -ne $ExpectExit) {
        $bad = $true
        $detail = "exit=$exit (expected $ExpectExit)"
    }
    foreach ($pat in $MustNotContain) {
        if ($out -match [regex]::Escape($pat)) {
            $bad = $true
            $detail += " contains '$pat'"
        }
    }
    if ($bad) {
        $script:failed++
        $results.Add([pscustomobject]@{ Status = "FAIL"; Name = $Name; Detail = $detail.Trim() })
        Write-Host "[FAIL] $Name — $detail" -ForegroundColor Red
        if ($out.Length -lt 800) { Write-Host $out }
    } else {
        $script:passed++
        $results.Add([pscustomobject]@{ Status = "PASS"; Name = $Name; Detail = "" })
        Write-Host "[PASS] $Name" -ForegroundColor Green
    }
}

# --- Install & verify ---
Test-Cmd "fhir-mql --version" "fhir-mql --version"
Test-Cmd "fhir-mql resources" "fhir-mql resources"
Test-Cmd "fhir-mql resources --format json" "fhir-mql resources --format json"
Test-Cmd "ConfigLoader list" "python -c `"from fhir_search_to_mql import ConfigLoader; print(sorted(ConfigLoader().list_resources()))`""

# Mongo ping
Test-Cmd "MongoDB ping" "python -c `"from pymongo import MongoClient; print(MongoClient('$URI', serverSelectionTimeoutMS=3000).server_info()['version'])`""

# --- Convert only ---
$convertCommands = @(
    'fhir-mql convert Patient "_id=p1"',
    'fhir-mql convert Observation "_lastUpdated=ge2024-01-01"',
    'fhir-mql convert Patient "name:exact=Smith"',
    'fhir-mql convert Patient "identifier:missing=false"',
    'fhir-mql convert Appointment "status:not=cancelled"',
    'fhir-mql convert Patient "name=Smith&gender=male"',
    'fhir-mql convert Patient "birthdate=ge1980-01-01&birthdate=le1990-12-31"',
    'fhir-mql convert Patient "identifier=http://hospital.org/mrn|MRN-1001"',
    'fhir-mql convert Patient "active=true&address-city=Springfield"',
    'fhir-mql convert Patient "telecom=555-0100"',
    'fhir-mql convert Patient "deceased=false"',
    'fhir-mql convert Patient "language=en-US"',
    'fhir-mql convert Patient "organization=org-1"',
    'fhir-mql convert Practitioner "name=Jones&active=true"',
    'fhir-mql convert Practitioner "identifier=http://npi|1234567890"',
    'fhir-mql convert PractitionerRole "practitioner=pr-1&organization=org-1"',
    'fhir-mql convert PractitionerRole "location=loc-er&service=hs-cardiology"',
    'fhir-mql convert PractitionerRole "specialty=394814009"',
    'fhir-mql convert Organization "name=General Hospital&active=true"',
    'fhir-mql convert Organization "identifier=urn:oid:2.16.840.1.113883.4.6|123"',
    'fhir-mql convert Location "name=ER&status=active"',
    'fhir-mql convert Location "address-city=Boston&organization=org-1"',
    'fhir-mql convert Observation "code=http://loinc.org|8480-6"',
    'fhir-mql convert Observation "patient=p1&status=final"',
    'fhir-mql convert Observation "date=ge2024-06-01&code=8480-6"',
    'fhir-mql convert Observation "value-quantity=120"',
    'fhir-mql convert Observation "category=vital-signs"',
    'fhir-mql convert Observation "encounter=enc-1"',
    'fhir-mql convert Appointment "status=booked&patient=p1"',
    'fhir-mql convert Appointment "date=ge2024-07-01&actor=Practitioner/pr-1"',
    'fhir-mql convert Appointment "reason-code=185345009"',
    'fhir-mql convert Appointment "reason-reference=Condition/cond-1"',
    'fhir-mql convert Schedule "active=true&actor=Practitioner/pr-1"',
    'fhir-mql convert Schedule "service-type=11429006"',
    'fhir-mql convert Schedule "service-type-reference=HealthcareService/hs-1"',
    'fhir-mql convert Schedule "date=ge2024-07-01"',
    'fhir-mql convert Slot "status=free&schedule=sched-1&start=ge2024-07-15"',
    'fhir-mql convert Encounter "status=in-progress&patient=p1"',
    'fhir-mql convert Encounter "class=AMB&type=185349003"',
    'fhir-mql convert Encounter "date=ge2024-07-01&practitioner=pr-1"',
    'fhir-mql convert Encounter "date-start=ge2024-07-01&end-date=le2024-07-31"',
    'fhir-mql convert Encounter "location=loc-1&service-provider=org-1"',
    'fhir-mql convert Encounter "diagnosis-code=44054006"',
    'fhir-mql convert Encounter "part-of=enc-parent"',
    'fhir-mql convert Condition "clinical-status=active&patient=p1"',
    'fhir-mql convert Condition "code=44054006&verification-status=confirmed"',
    'fhir-mql convert Condition "encounter=enc-1&onset-date=ge2020-01-01"',
    'fhir-mql convert Condition "category=problem-list-item"',
    'fhir-mql convert Device "status=active&patient=p1"',
    'fhir-mql convert Device "type=182722004&manufacturer=Acme"',
    'fhir-mql convert Device "expiration-date=le2025-12-31"',
    'fhir-mql convert Group "name=Cohort-A&type=person"',
    'fhir-mql convert Group "member=Patient/p1&membership=enumerated"',
    'fhir-mql convert Group "characteristic=73211009"',
    'fhir-mql convert Patient "name=Smith&gender=male&birthdate=ge1980-01-01"',
    'fhir-mql convert Observation "patient=p1&code=8480-6&date=ge2024-01-01&status=final"',
    'fhir-mql convert Appointment "status=booked&patient=p1&date=ge2024-07-01"'
)
$i = 0
foreach ($c in $convertCommands) {
    $i++
    Test-Cmd "convert[$i]" $c -MustNotContain @("Warning: Parameter", "Error:", "Traceback")
}

# --- Compartment convert ---
Test-Cmd "compartment Observation Patient" 'fhir-mql convert Observation "code=8480-6" --compartment-type Patient --compartment-id p1' -MustNotContain @("Warning: Parameter")
Test-Cmd "compartment Schedule Practitioner" 'fhir-mql convert Schedule "" --compartment-type Practitioner --compartment-id pr-1'
Test-Cmd "compartment Observation Device" 'fhir-mql convert Observation "code=8480-6" --compartment-type Device --compartment-id dev-1'
Test-Cmd "compartment Encounter" 'fhir-mql convert Encounter "status=in-progress" --compartment-type Encounter --compartment-id enc-1'
Test-Cmd "compartment Schedule RelatedPerson" 'fhir-mql convert Schedule "" --compartment-type RelatedPerson --compartment-id rp-1'

# --- Search (needs MongoDB) ---
$searchCommands = @(
    "fhir-mql search Patient `"name=Smith&gender=male`" --limit 10",
    "fhir-mql search Patient `"name=Smith`" --uri `"$URI`" --db `"$DB`" --format json",
    "fhir-mql search Observation `"code=http://loinc.org|8480-6&status=final`" --limit 50",
    "fhir-mql search Slot `"status=free&start=ge2024-07-01`" --limit 20",
    'fhir-mql search Patient "name=Smith" --explain',
    "fhir-mql search Observation `"status=final`" --compartment-type Patient --compartment-id p1 --limit 25",
    "fhir-mql search Schedule `"active=true`" --compartment-type Practitioner --compartment-id pr-1 --limit 5",
    'fhir-mql search Patient "identifier=http://hospital.org/mrn|MRN-1001" --limit 5',
    'fhir-mql search Patient "name=Smith&birthdate=1980-05-15" --limit 10',
    'fhir-mql search Patient "name:exact=Smith&gender=male" --limit 5',
    'fhir-mql search Practitioner "name=Jones&active=true" --limit 20',
    'fhir-mql search PractitionerRole "organization=org-1&active=true" --limit 50',
    'fhir-mql search PractitionerRole "practitioner=pr-1" --limit 5',
    'fhir-mql search Organization "name=Hospital&active=true" --limit 10',
    'fhir-mql search Location "name=ER&status=active" --limit 10',
    'fhir-mql search Location "organization=org-1" --limit 25',
    'fhir-mql search Schedule "active=true&actor=Practitioner/pr-1" --limit 10',
    'fhir-mql search Slot "status=free&schedule=sched-1&start=ge2024-07-15&start=le2024-07-31" --limit 100',
    'fhir-mql search Appointment "status=booked&patient=p1&date=ge2024-07-01" --limit 20',
    'fhir-mql search Encounter "status=in-progress&location=loc-er" --limit 50',
    'fhir-mql search Encounter "patient=p1&status=in-progress" --limit 5',
    'fhir-mql search Encounter "practitioner=pr-1&date=ge2024-07-01" --limit 30',
    'fhir-mql search Condition "patient=p1&clinical-status=active" --limit 50',
    'fhir-mql search Condition "code=44054006&verification-status=confirmed" --limit 10',
    'fhir-mql search Condition "clinical-status=active" --compartment-type Patient --compartment-id p1 --limit 50',
    'fhir-mql search Observation "patient=p1&category=vital-signs&date=ge2024-06-01" --limit 100',
    'fhir-mql search Device "status=active&patient=p1" --limit 20',
    'fhir-mql search Device "manufacturer=Acme&expiration-date=le2025-12-31" --limit 50',
    'fhir-mql search Device "identifier=DEV-001" --limit 5',
    'fhir-mql search Group "name=Diabetes-Cohort&type=person" --limit 10',
    'fhir-mql search Group "member=Patient/p1" --limit 5',
    'fhir-mql search Group "type=person&characteristic=73211009" --limit 20',
    'fhir-mql search Appointment "actor=Practitioner/pr-1&status=booked" --limit 30',
    'fhir-mql search Encounter "participant=Practitioner/pr-1&status=completed" --limit 30',
    'fhir-mql search Encounter "careteam=ct-1" --limit 20'
)
$j = 0
foreach ($c in $searchCommands) {
    $j++
    Test-Cmd "search[$j]" $c -MustNotContain @("Warning: Parameter", "Traceback")
}

if (-not $SkipMongoBulk) {
    Test-Cmd "indexes Patient" "fhir-mql indexes Patient --uri `"$URI`" --db `"$DB`""
    Test-Cmd "indexes multi dry-run" "fhir-mql indexes Patient Observation Encounter --uri `"$URI`" --db `"$DB`" --dry-run"
    Test-Cmd "indexes --all dry-run" "fhir-mql indexes --all --dry-run"
    Test-Cmd "indexes Slot Schedule dry-run" "fhir-mql indexes Slot Schedule --dry-run"
    Test-Cmd "denormalize Patient dry-run" "fhir-mql denormalize Patient --uri `"$URI`" --db `"$DB`" --dry-run"
    Test-Cmd "denormalize --all dry-run" "fhir-mql denormalize --all --uri `"$URI`" --db `"$DB`" --dry-run"
    Test-Cmd "denormalize --all limit 1" "fhir-mql denormalize --all --uri `"$URI`" --db `"$DB`" --limit 1"
    Test-Cmd "stats Patient" "fhir-mql stats Patient --uri `"$URI`" --db `"$DB`""
    Test-Cmd "stats --all json" "fhir-mql stats --all --uri `"$URI`" --db `"$DB`" --format json"
    Test-Cmd "stats multi" "fhir-mql stats Patient Observation Encounter Appointment --uri `"$URI`" --db `"$DB`""
} else {
    Test-Cmd "bulk ops" "" -Skip
}

# Skip destructive reset / full reindex on live data
Test-Cmd "reset Patient" "fhir-mql reset Patient --uri `"$URI`" --db `"$DB`" --limit 1" -Skip
Test-Cmd "reset --all" "" -Skip
Test-Cmd "disaster recovery pipeline" "" -Skip

# Hybrid DB (optional)
$hybridDb = "fhir_schedule_appointment_hybrid"
Test-Cmd "hybrid denormalize dry-run" "fhir-mql denormalize --uri `"$URI`" --db `"$hybridDb`" Patient --dry-run" -Skip:(-not (python -c "from pymongo import MongoClient; d=Client('$URI',serverSelectionTimeoutMS=2000)['$hybridDb'].list_collection_names(); exit(0 if d else 1)" 2>$null; $LASTEXITCODE -ne 0))

# Python API one-liners
Test-Cmd "API convert" 'python -c "from fhir_search_to_mql import FHIRSearchConverter as C; print(C().convert(''Patient'',''name=Smith''))"'
Test-Cmd "API denormalize" 'python -c "from fhir_search_to_mql import ResourceDenormalizer as D; r={''resourceType'':''Patient'',''id'':''x'',''name'':[{''family'':''Smith''}]}; print(D().denormalize(r).get(''_search'',{}))"'
Test-Cmd "API compartment" 'python -c "from fhir_search_to_mql import FHIRSearchConverter as C; print(C().convert_with_compartment(''Patient'',''p1'',''Observation'',''code=8480-6''))"'

# Schema tooling (read-only spec)
Test-Cmd "resource_spec Encounter" "python -m fhir_search_to_mql.schema.resource_spec Encounter"
Test-Cmd "resource_spec Condition" "python -m fhir_search_to_mql.schema.resource_spec Condition"
Test-Cmd "resource_spec Patient" "python -m fhir_search_to_mql.schema.resource_spec Patient"

# Troubleshooting
Test-Cmd "where fhir-mql" "where.exe fhir-mql"
Test-Cmd "pip show" "pip show fhir-search-to-mql"
Test-Cmd "denormalize Bogus (expect fail)" "fhir-mql denormalize Bogus" -ExpectExit 1

if (-not $SkipPytest) {
    Test-Cmd "pytest unit" "python -m pytest tests/unit/ -q --no-cov"
} else {
    Test-Cmd "pytest unit" "" -Skip
}

Write-Host ""
Write-Host "======== SUMMARY ========" -ForegroundColor Cyan
Write-Host "PASS: $passed  FAIL: $failed  SKIP: $skipped"
$results | Where-Object { $_.Status -eq "FAIL" } | Format-Table -AutoSize
if ($failed -gt 0) { exit 1 }
exit 0
