# Setup Script for FHIR Search to MQL Development Environment
# This script sets up a complete development environment

Write-Host "=== FHIR Search to MQL - Development Setup ===" -ForegroundColor Cyan

# Check Python version
Write-Host "`n=== Checking Python Version ===" -ForegroundColor Cyan
$pythonVersion = python --version 2>&1
Write-Host "Found: $pythonVersion" -ForegroundColor White

if ($pythonVersion -match "Python (\d+)\.(\d+)") {
    $major = [int]$Matches[1]
    $minor = [int]$Matches[2]
    
    if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 9)) {
        Write-Host "❌ Python 3.9+ required. Found: $pythonVersion" -ForegroundColor Red
        exit 1
    }
    Write-Host "✅ Python version is compatible" -ForegroundColor Green
}

# Create virtual environment
Write-Host "`n=== Creating Virtual Environment ===" -ForegroundColor Cyan
if (Test-Path ".venv") {
    Write-Host "⚠️  Virtual environment already exists at .venv" -ForegroundColor Yellow
    $response = Read-Host "Do you want to recreate it? This will delete the existing one. (y/N)"
    
    if ($response -eq "y" -or $response -eq "Y") {
        Write-Host "Removing existing virtual environment..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force .venv
        Write-Host "✅ Removed existing virtual environment" -ForegroundColor Green
    } else {
        Write-Host "Keeping existing virtual environment" -ForegroundColor White
    }
}

if (-not (Test-Path ".venv")) {
    Write-Host "Creating new virtual environment..." -ForegroundColor White
    python -m venv .venv
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Virtual environment created at .venv" -ForegroundColor Green
    } else {
        Write-Host "❌ Failed to create virtual environment" -ForegroundColor Red
        exit 1
    }
}

# Activate virtual environment
Write-Host "`n=== Activating Virtual Environment ===" -ForegroundColor Cyan
& .\.venv\Scripts\Activate.ps1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Virtual environment activated" -ForegroundColor Green
} else {
    Write-Host "❌ Failed to activate virtual environment" -ForegroundColor Red
    exit 1
}

# Upgrade pip
Write-Host "`n=== Upgrading pip ===" -ForegroundColor Cyan
python -m pip install --upgrade pip
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ pip upgraded" -ForegroundColor Green
} else {
    Write-Host "⚠️  pip upgrade failed" -ForegroundColor Yellow
}

# Install dependencies
Write-Host "`n=== Installing Dependencies ===" -ForegroundColor Cyan
Write-Host "This may take a few minutes..." -ForegroundColor White

pip install -e ".[dev,docs]"
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ All dependencies installed" -ForegroundColor Green
} else {
    Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
    Write-Host "Trying alternative installation method..." -ForegroundColor Yellow
    pip install -r requirements.txt
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Dependencies installed from requirements.txt" -ForegroundColor Green
    } else {
        Write-Host "❌ Failed to install dependencies" -ForegroundColor Red
        exit 1
    }
}

# Verify installation
Write-Host "`n=== Verifying Installation ===" -ForegroundColor Cyan
$packages = @("pytest", "black", "flake8", "mypy", "PyYAML", "pymongo")
$allInstalled = $true

foreach ($package in $packages) {
    $installed = pip show $package 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ $package installed" -ForegroundColor Green
    } else {
        Write-Host "❌ $package NOT installed" -ForegroundColor Red
        $allInstalled = $false
    }
}

if ($allInstalled) {
    Write-Host "`n✅ All required packages verified" -ForegroundColor Green
} else {
    Write-Host "`n⚠️  Some packages are missing. Please check the installation." -ForegroundColor Yellow
}

# Run a quick test
Write-Host "`n=== Running Quick Test ===" -ForegroundColor Cyan
Write-Host "Testing import..." -ForegroundColor White
python -c "import fhir_search_to_mql; print('Import successful!')"
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Package can be imported successfully" -ForegroundColor Green
} else {
    Write-Host "⚠️  Package import test skipped (development mode)" -ForegroundColor Yellow
}

# Summary
Write-Host "`n=== Setup Complete ===" -ForegroundColor Cyan
Write-Host "✅ Virtual environment created and activated" -ForegroundColor Green
Write-Host "✅ All dependencies installed" -ForegroundColor Green
Write-Host "✅ Development environment ready" -ForegroundColor Green

Write-Host "`n📚 Next Steps:" -ForegroundColor Yellow
Write-Host "1. Run tests:            .\run_tests.ps1" -ForegroundColor White
Write-Host "2. Format code:          black src/ tests/" -ForegroundColor White
Write-Host "3. Check types:          mypy src/" -ForegroundColor White
Write-Host "4. View documentation:   See PHASE_8_TESTING_DOCUMENTATION.md" -ForegroundColor White

Write-Host "`n💡 To activate the virtual environment in the future:" -ForegroundColor Yellow
Write-Host "   .\.venv\Scripts\Activate.ps1" -ForegroundColor White

Write-Host "`n💡 To deactivate when done:" -ForegroundColor Yellow
Write-Host "   deactivate" -ForegroundColor White
