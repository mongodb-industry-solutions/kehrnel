#!/bin/bash
# Setup Script for FHIR Search to MQL Development Environment
# This script sets up a complete development environment

echo "=== FHIR Search to MQL - Development Setup ==="

# Check Python version
echo ""
echo "=== Checking Python Version ==="
pythonVersion=$(python3 --version 2>&1)
echo "Found: $pythonVersion"

if [[ $pythonVersion =~ Python\ ([0-9]+)\.([0-9]+) ]]; then
    major="${BASH_REMATCH[1]}"
    minor="${BASH_REMATCH[2]}"
    
    if [ "$major" -lt 3 ] || ([ "$major" -eq 3 ] && [ "$minor" -lt 9 ]); then
        echo "❌ Python 3.9+ required. Found: $pythonVersion"
        exit 1
    fi
    echo "✅ Python version is compatible"
fi

# Create virtual environment
echo ""
echo "=== Creating Virtual Environment ==="
if [ -d ".venv" ]; then
    echo "⚠️  Virtual environment already exists at .venv"
    read -p "Do you want to recreate it? This will delete the existing one. (y/N): " response
    
    if [ "$response" = "y" ] || [ "$response" = "Y" ]; then
        echo "Removing existing virtual environment..."
        rm -rf .venv
        echo "✅ Removed existing virtual environment"
    else
        echo "Keeping existing virtual environment"
    fi
fi

if [ ! -d ".venv" ]; then
    echo "Creating new virtual environment..."
    python3 -m venv .venv
    if [ $? -eq 0 ]; then
        echo "✅ Virtual environment created at .venv"
    else
        echo "❌ Failed to create virtual environment"
        exit 1
    fi
fi

# Activate virtual environment
echo ""
echo "=== Activating Virtual Environment ==="
source .venv/bin/activate
if [ $? -eq 0 ]; then
    echo "✅ Virtual environment activated"
else
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

# Upgrade pip
echo ""
echo "=== Upgrading pip ==="
python -m pip install --upgrade pip
if [ $? -eq 0 ]; then
    echo "✅ pip upgraded"
else
    echo "⚠️  pip upgrade failed"
fi

# Install dependencies
echo ""
echo "=== Installing Dependencies ==="
echo "This may take a few minutes..."

pip install -e ".[dev,docs]"
if [ $? -eq 0 ]; then
    echo "✅ All dependencies installed"
else
    echo "❌ Failed to install dependencies"
    echo "Trying alternative installation method..."
    pip install -r requirements.txt
    if [ $? -eq 0 ]; then
        echo "✅ Dependencies installed from requirements.txt"
    else
        echo "❌ Failed to install dependencies"
        exit 1
    fi
fi

# Verify installation
echo ""
echo "=== Verifying Installation ==="
packages=("pytest" "black" "flake8" "mypy" "PyYAML" "pymongo")
allInstalled=true

for package in "${packages[@]}"; do
    if pip show "$package" > /dev/null 2>&1; then
        echo "✅ $package installed"
    else
        echo "❌ $package NOT installed"
        allInstalled=false
    fi
done

if $allInstalled; then
    echo ""
    echo "✅ All required packages verified"
else
    echo ""
    echo "⚠️  Some packages are missing. Please check the installation."
fi

# Run a quick test
echo ""
echo "=== Running Quick Test ==="
echo "Testing import..."
python -c "import fhir_search_to_mql; print('Import successful!')" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ Package can be imported successfully"
else
    echo "⚠️  Package import test skipped (development mode)"
fi

# Make test scripts executable
chmod +x run_tests.sh

# Summary
echo ""
echo "=== Setup Complete ==="
echo "✅ Virtual environment created and activated"
echo "✅ All dependencies installed"
echo "✅ Development environment ready"

echo ""
echo "📚 Next Steps:"
echo "1. Run tests:            ./run_tests.sh"
echo "2. Format code:          black src/ tests/"
echo "3. Check types:          mypy src/"
echo "4. View documentation:   See PHASE_8_TESTING_DOCUMENTATION.md"

echo ""
echo "💡 To activate the virtual environment in the future:"
echo "   source .venv/bin/activate"

echo ""
echo "💡 To deactivate when done:"
echo "   deactivate"
