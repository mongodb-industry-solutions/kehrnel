# Quick Test Script for FHIR Search to MQL
# Run this script to execute all quality checks

Write-Host "=== FHIR Search to MQL - Quality Checks ===" -ForegroundColor Cyan

# Check if virtual environment is activated
if (-not $env:VIRTUAL_ENV) {
    Write-Host "`n⚠️  Virtual environment not activated!" -ForegroundColor Yellow
    Write-Host "Activating virtual environment..." -ForegroundColor Yellow
    
    if (Test-Path ".\.venv\Scripts\Activate.ps1") {
        & .\.venv\Scripts\Activate.ps1
    } else {
        Write-Host "❌ Virtual environment not found. Please create it first:" -ForegroundColor Red
        Write-Host "   python -m venv .venv" -ForegroundColor White
        Write-Host "   .\.venv\Scripts\Activate.ps1" -ForegroundColor White
        exit 1
    }
}

Write-Host "✅ Virtual environment: $env:VIRTUAL_ENV" -ForegroundColor Green

# Format code
Write-Host "`n=== 1. Formatting with Black ===" -ForegroundColor Cyan
black src/ tests/
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Code formatting complete" -ForegroundColor Green
} else {
    Write-Host "❌ Code formatting failed" -ForegroundColor Red
    exit 1
}

# Lint
Write-Host "`n=== 2. Linting with Flake8 ===" -ForegroundColor Cyan
flake8 src/ tests/ --max-line-length=100
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Linting passed" -ForegroundColor Green
} else {
    Write-Host "⚠️  Linting warnings found" -ForegroundColor Yellow
}

# Type check
Write-Host "`n=== 3. Type Checking with MyPy ===" -ForegroundColor Cyan
mypy src/
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Type checking passed" -ForegroundColor Green
} else {
    Write-Host "⚠️  Type checking warnings found" -ForegroundColor Yellow
}

# Run tests with coverage
Write-Host "`n=== 4. Running Tests with Coverage ===" -ForegroundColor Cyan
pytest --cov=fhir_search_to_mql --cov-report=html --cov-report=term-missing -v
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ All tests passed" -ForegroundColor Green
} else {
    Write-Host "❌ Some tests failed" -ForegroundColor Red
    exit 1
}

# Summary
Write-Host "`n=== All Quality Checks Complete ===" -ForegroundColor Cyan
Write-Host "✅ Code formatted" -ForegroundColor Green
Write-Host "✅ Linting checked" -ForegroundColor Green
Write-Host "✅ Type checking done" -ForegroundColor Green
Write-Host "✅ Tests passed with coverage report" -ForegroundColor Green
Write-Host "`n📊 View coverage report: htmlcov\index.html" -ForegroundColor Yellow
Write-Host "`nTo open coverage report:" -ForegroundColor White
Write-Host "   start htmlcov\index.html" -ForegroundColor Gray
