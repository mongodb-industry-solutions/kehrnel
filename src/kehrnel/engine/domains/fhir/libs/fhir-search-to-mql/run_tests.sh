#!/bin/bash
# Quick Test Script for FHIR Search to MQL
# Run this script to execute all quality checks

echo "=== FHIR Search to MQL - Quality Checks ==="

# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo ""
    echo "⚠️  Virtual environment not activated!"
    echo "Activating virtual environment..."
    
    if [ -f ".venv/bin/activate" ]; then
        source .venv/bin/activate
    else
        echo "❌ Virtual environment not found. Please create it first:"
        echo "   python3 -m venv .venv"
        echo "   source .venv/bin/activate"
        exit 1
    fi
fi

echo "✅ Virtual environment: $VIRTUAL_ENV"

# Format code
echo ""
echo "=== 1. Formatting with Black ==="
black src/ tests/
if [ $? -eq 0 ]; then
    echo "✅ Code formatting complete"
else
    echo "❌ Code formatting failed"
    exit 1
fi

# Lint
echo ""
echo "=== 2. Linting with Flake8 ==="
flake8 src/ tests/ --max-line-length=100
if [ $? -eq 0 ]; then
    echo "✅ Linting passed"
else
    echo "⚠️  Linting warnings found"
fi

# Type check
echo ""
echo "=== 3. Type Checking with MyPy ==="
mypy src/
if [ $? -eq 0 ]; then
    echo "✅ Type checking passed"
else
    echo "⚠️  Type checking warnings found"
fi

# Run tests with coverage
echo ""
echo "=== 4. Running Tests with Coverage ==="
pytest --cov=fhir_search_to_mql --cov-report=html --cov-report=term-missing -v
if [ $? -eq 0 ]; then
    echo "✅ All tests passed"
else
    echo "❌ Some tests failed"
    exit 1
fi

# Summary
echo ""
echo "=== All Quality Checks Complete ==="
echo "✅ Code formatted"
echo "✅ Linting checked"
echo "✅ Type checking done"
echo "✅ Tests passed with coverage report"
echo ""
echo "📊 View coverage report: htmlcov/index.html"
echo ""
echo "To open coverage report:"
echo "   macOS:  open htmlcov/index.html"
echo "   Linux:  xdg-open htmlcov/index.html"
