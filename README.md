# IoT Power Analytics Pipeline

Refactored IoT power consumption data analytics pipeline với kiến trúc modular, hỗ trợ chạy cả local và Databricks.

## Cấu Trúc Dự Án

```
iot_pipeline/
├── main.py                 # Main runner script
├── requirements.txt        # Python dependencies
├── README.md              # This file
├── config/
│   ├── constants.py       # Constants và configurations  
│   └── settings.py        # Environment-specific settings
├── src/
│   ├── layers/           # Data processing layers
│   │   ├── bronze.py     # Raw data ingestion
│   │   ├── silver.py     # Data cleaning & transformation
│   │   ├── gold.py       # Business aggregations
│   │   └── forecasting.py # Time series forecasting
│   └── utils/
│       ├── transformations.py # Data transformation functions
│       └── helpers.py         # Utility functions
└── data/                 # Local data storage (for local runs)
    ├── bronze/
    ├── silver/
    └── gold/
```

## Luồng Hoạt Động (Data Flow)

### 1. Bronze Layer - Raw Data Ingestion
- **Input**: CSV files với dữ liệu IoT power consumption
- **Processing**: 
  - Đọc CSV với schema cố định
  - Làm sạch Date/Time columns (bỏ quotes, trim)
  - Thêm metadata (ingest_time, source_file)
  - Data quality validation
- **Output**: Raw data với metadata

### 2. Silver Layer - Data Cleaning & Transformation  
- **Input**: Bronze layer data
- **Processing**:
  - Parse timestamp từ Date/Time với multiple formats
  - Tạo time-based columns (year, month, dow, hour, etc.)
  - Tính toán electrical features (power factor, apparent power)
  - Data quality flags và null out-of-range values
  - Impute missing values bằng time-based median
- **Output**: Clean, transformed data

### 3. Gold Layer - Business Aggregations
- **Input**: Silver layer data  
- **Processing**:
  - Tính toán energy consumption (kWh) 
  - Hourly aggregations (energy, power factor, submetering)
  - Daily aggregations với peak hour detection
  - KPI aggregations (monthly, yearly)
- **Output**: Business-ready aggregated data

### 4. Forecasting Layer - Time Series Prediction
- **Input**: Gold daily aggregations
- **Processing**:
  - Feature engineering (lags, rolling stats, cyclical encoding)
  - Multiple forecasting models:
    - Naive Seasonal (baseline)
    - Prophet (Facebook's time series)
    - XGBoost (gradient boosting)
    - Ensemble (top models combination)
  - Model evaluation (RMSE, MAPE)
- **Output**: 30-day forecasts với model comparison

## Cài Đặt và Sử Dụng

### 1. Cài Đặt Dependencies

```bash
# Tạo virtual environment (khuyến nghị)
python -m venv iot_env
source iot_env/bin/activate  # Linux/Mac
# hoặc
iot_env\\Scripts\\activate     # Windows

# Cài đặt packages
pip install -r requirements.txt
```

### 2. Chạy Pipeline

#### Chạy Local (Full Pipeline)
```bash
python main.py --environment local --input-path "path/to/your/data.csv" --stage full
```

#### Chạy từng stage riêng lẻ
```bash
# Chỉ Bronze layer
python main.py -e local -i "data.csv" -s bronze

# Bronze + Silver
python main.py -e local -i "data.csv" -s silver  

# Chỉ Gold (cần Silver data có sẵn)
python main.py -e local -s gold

# Chỉ Forecasting
python main.py -e local -s forecast

# Hiển thị kết quả
python main.py -e local -s show
```

#### Databricks Environment
```bash
python main.py --environment databricks --input-path "/path/to/unity/catalog/data" --stage full
```

### 3. Cấu Hình

#### Local Environment
- Data được lưu dưới dạng Parquet files trong `data/` directory
- Sử dụng local Spark cluster
- Delta Lake support cho ACID transactions

#### Databricks Environment  
- Sử dụng Unity Catalog managed tables
- Delta format mặc định
- Tận dụng Databricks cluster optimization

## Tính Năng Chính

### ✅ Modular Architecture
- Tách biệt rõ ràng các layer (Bronze/Silver/Gold)
- Dễ maintain và extend
- Reusable components

### ✅ Environment Flexibility
- Chạy được cả local và Databricks
- Auto-detect environment configurations
- Consistent API across environments

### ✅ Data Quality & Validation
- Schema validation
- Data quality checks
- Out-of-range value handling
- Missing value imputation

### ✅ Advanced Analytics
- Multiple forecasting models
- Model comparison và selection
- Ensemble methods
- Feature engineering tự động

### ✅ Error Handling & Logging
- Comprehensive logging
- Graceful error handling
- Data quality reporting

## Dữ Liệu Đầu Vào

CSV file với các columns sau:
- `Date`: Date string (multiple formats supported)
- `Time`: Time string  
- `Global_active_power`: Active power (kW)
- `Global_reactive_power`: Reactive power (kW)
- `Voltage`: Voltage (V)
- `Global_intensity`: Current intensity (A)
- `Sub_metering_1`: Kitchen power (Wh)
- `Sub_metering_2`: Laundry power (Wh) 
- `Sub_metering_3`: Water heater & AC (Wh)

## Output Data

### Gold Tables
- **Hourly**: Energy consumption theo giờ với submetering breakdown
- **Daily**: Daily aggregations với peak hour detection
- **Forecasting**: 30-day energy consumption predictions

### Metrics
- Energy consumption (kWh)
- Power factor
- Submeter utilization ratios
- Peak usage patterns
- Forecast accuracy (RMSE, MAPE)

## Troubleshooting

### Common Issues

1. **PySpark Installation**
   ```bash
   # Nếu gặp lỗi Java/Spark
   export JAVA_HOME=/path/to/java
   export SPARK_HOME=/path/to/spark
   ```

2. **Delta Lake Support**
   ```bash
   # Đảm bảo có delta-spark package
   pip install delta-spark
   ```

3. **Forecasting Libraries**
   ```bash
   # Nếu prophet không install được
   pip install pystan prophet
   
   # Hoặc dùng conda
   conda install -c conda-forge prophet
   ```

## Development

### Thêm Model Mới
1. Implement trong `src/layers/forecasting.py`
2. Add vào `run_forecasting_pipeline()` method
3. Update model evaluation

### Thêm Transformations
1. Add functions vào `src/utils/transformations.py`
2. Import và sử dụng trong appropriate layer

### Custom Configurations
- Modify `config/constants.py` cho business rules
- Update `config/settings.py` cho environment settings

## Tác Giả

IoT Power Analytics Pipeline - Refactored for modularity and local execution support.