#!/bin/bash
echo "====================================="
echo "  IoT Power Pipeline Setup Script"
echo "====================================="

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed"
    echo "Please install Python 3.8+ first"
    exit 1
fi

echo "Python found. Setting up environment..."

# Create virtual environment if not exists
if [ ! -d "iot_env" ]; then
    echo "Creating virtual environment..."
    python3 -m venv iot_env
fi

# Activate virtual environment
echo "Activating virtual environment..."
source iot_env/bin/activate

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

echo ""
echo "====================================="
echo "  Setup completed successfully!"
echo "====================================="
echo ""
echo "To run the pipeline:"
echo "1. Run example: python example.py"
echo "2. Full pipeline: python main.py -e local -i 'path/to/data.csv' -s full"
echo "3. Show results: python main.py -e local -s show"
echo ""