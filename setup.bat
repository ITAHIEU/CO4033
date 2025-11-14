@echo off
echo =====================================
echo   IoT Power Pipeline Setup Script
echo =====================================

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    echo Please install Python 3.8+ and add it to PATH
    pause
    exit /b 1
)

echo Python found. Setting up environment...

REM Create virtual environment if not exists
if not exist "iot_env" (
    echo Creating virtual environment...
    python -m venv iot_env
)

REM Activate virtual environment
echo Activating virtual environment...
call iot_env\Scripts\activate.bat

REM Install requirements
echo Installing requirements...
pip install -r requirements.txt

echo.
echo =====================================
echo   Setup completed successfully!
echo =====================================
echo.
echo To run the pipeline:
echo 1. Run example: python example.py
echo 2. Full pipeline: python main.py -e local -i "path\to\data.csv" -s full
echo 3. Show results: python main.py -e local -s show
echo.
pause