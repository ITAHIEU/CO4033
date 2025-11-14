"""
Constants and configuration for IoT Power Pipeline
"""

# Data schema constants
NUM_COLS = [
    "Global_active_power",
    "Global_reactive_power", 
    "Voltage",
    "Global_intensity",
    "Sub_metering_1",
    "Sub_metering_2",
    "Sub_metering_3"
]

# Data validation ranges
DATA_RANGES = {
    "Global_active_power": (0.0, 15.0),
    "Global_reactive_power": (-5.0, 5.0),
    "Voltage": (180.0, 270.0),
    "Global_intensity": (0.0, 80.0),
    "Sub_metering_1": (0.0, 20.0),
    "Sub_metering_2": (0.0, 20.0),
    "Sub_metering_3": (0.0, 20.0),
}

# Timestamp parsing patterns
TIMESTAMP_PATTERNS = [
    'dd/MM/yyyy HH:mm:ss',
    'd/M/yyyy HH:mm:ss', 
    'dd/MM/yyyy H:m:s',
    'MM/dd/yyyy HH:mm:ss',
    'M/d/yyyy H:m:s',
    'yyyy-MM-dd HH:mm:ss',
    'dd-MM-yyyy HH:mm:ss'
]

# Forecasting constants
FORECAST_HORIZON = 30
TEST_DAYS = 14
LAG_FEATURES = [1, 7, 14, 21, 28]
ROLLING_WINDOWS = [7, 14, 30]