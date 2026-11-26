# PowerShell entrypoint for the POSIX `startKehrnel` script.
# Resolves a real Git Bash (never the WindowsApps WSL stub) and delegates, forwarding
# every argument verbatim and propagating the exit code.
#
# Usage: .\startKehrnel.ps1 [options] [-- <extra uvicorn args>]
#        .\startKehrnel.ps1 --skip-sync
#        .\startKehrnel.ps1 --local               # use a local MongoDB (auto-starts mongod)
#        .\startKehrnel.ps1 -- --timeout-keep-alive 10
#        .\startKehrnel.ps1 --print-bash-only     # print the resolved bash and exit
#
# Options are those of ./startKehrnel (--host, --port, --reload, --no-reload,
# --build-docs, --skip-docs, --skip-sync, --force-sync, --local, -h/--help) and are passed
# through unchanged, including everything after a `--` separator. Set
# $env:KEHRNEL_BASH to override bash resolution.
#
# NOTE: this script deliberately declares NO param() block. PowerShell matches
# parameter names by prefix, so a bare `--` separator binds to any declared
# parameter as an abbreviation -- swallowing the separator, or failing with
# "the parameter name '' is ambiguous". Reading $args directly is the only way
# to forward `--` to the bash script untouched, so the wrapper's own flag uses
# the same `--flag` style as the script it wraps.

$ErrorActionPreference = "Stop"

function Resolve-Bash {
    # $env:KEHRNEL_BASH wins when it points at a real file.
    if ($env:KEHRNEL_BASH) {
        if (Test-Path -LiteralPath $env:KEHRNEL_BASH -PathType Leaf) {
            return (Resolve-Path -LiteralPath $env:KEHRNEL_BASH).Path
        }
        Write-Error "KEHRNEL_BASH is set to '$env:KEHRNEL_BASH' but that file does not exist. Point it at bash.exe from Git for Windows (https://git-scm.com/download/win), or unset it to auto-detect."
    }

    $candidates = @(
        "C:\Program Files\Git\bin\bash.exe",
        "C:\Program Files (x86)\Git\bin\bash.exe"
    )
    if ($env:ProgramFiles) {
        $candidates += (Join-Path $env:ProgramFiles "Git\bin\bash.exe")
    }
    if (${env:ProgramFiles(x86)}) {
        $candidates += (Join-Path ${env:ProgramFiles(x86)} "Git\bin\bash.exe")
    }

    # git.exe-relative: <git root>\cmd\git.exe -> <git root>\bin\bash.exe
    $gitCmd = Get-Command git -ErrorAction SilentlyContinue
    if ($gitCmd) {
        $gitRoot = Split-Path (Split-Path $gitCmd.Source)
        if ($gitRoot) {
            $candidates += (Join-Path $gitRoot "bin\bash.exe")
        }
    }

    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    # Last resort: a bash.exe on PATH that is NOT the WindowsApps WSL stub.
    foreach ($onPath in @(Get-Command bash.exe -All -ErrorAction SilentlyContinue)) {
        if ($onPath.Source -and $onPath.Source -notmatch 'WindowsApps') {
            return $onPath.Source
        }
    }

    Write-Error "No usable bash found. startKehrnel is a bash script; install Git for Windows (https://git-scm.com/download/win) so that 'C:\Program Files\Git\bin\bash.exe' exists, or set `$env:KEHRNEL_BASH to a bash.exe. Note that the 'bash' shipped via WindowsApps is the WSL launcher stub and cannot run this script."
}

# Consume the wrapper's own flag, but only before a `--` separator -- everything
# from `--` onward belongs to the bash script and is forwarded untouched.
$printBashOnly = $false
$forwarded = @()
$seenSeparator = $false
foreach ($arg in $args) {
    if (-not $seenSeparator -and $arg -eq '--print-bash-only') {
        $printBashOnly = $true
        continue
    }
    if ($arg -eq '--') {
        $seenSeparator = $true
    }
    $forwarded += $arg
}

$bash = Resolve-Bash

if ($printBashOnly) {
    Write-Output $bash
    exit 0
}

$script = Join-Path $PSScriptRoot "startKehrnel"
if (-not (Test-Path -LiteralPath $script -PathType Leaf)) {
    Write-Error "Cannot find the startKehrnel script next to this wrapper (expected at '$script')."
}

if ($forwarded.Count -gt 0) {
    & $bash $script @forwarded
} else {
    & $bash $script
}
exit $LASTEXITCODE
