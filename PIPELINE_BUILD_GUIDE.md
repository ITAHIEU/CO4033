# 🔧 Hướng dẫn Build IoT Analytics Pipeline Từ Đầu

## 📋 Tổng quan

Pipeline IoT này xử lý dữ liệu năng lượng qua 3 layers:
- **Bronze**: Raw data ingestion
- **Silver**: Data cleaning & transformation  
- **Gold**: Business aggregations & analytics
- **Forecasting**: ML predictions với 6 models

## 🛠️ Yêu cầu hệ thống

### Software Requirements:
- Python 3.8+
- Java 8+ (cho PySpark)
- Power BI Desktop (cho visualization)

### Hardware Requirements:
- RAM: 8GB+ (16GB khuyến nghị)
- Storage: 5GB+ free space
- CPU: 4 cores+

## 📁 Cấu trúc Project

```
iot_pipeline/
├── src/                          # Source code
│   ├── layers/                   # Data processing layers
│   │   ├── bronze.py            # Raw data ingestion
│   │   ├── silver.py            # Data cleaning
│   │   ├── gold.py              # Business aggregations
│   │   └── forecasting.py       # ML forecasting
│   ├── utils/                   # Utility functions
│   │   ├── helpers.py           # Logging, validation
│   │   └── transformations.py   # Data transformations
│   └── config/                  # Configuration
│       ├── settings.py          # Environment settings
│       └── constants.py         # Pipeline constants
├── data/                        # Data storage
│   ├── bronze/                  # Raw data
│   ├── silver/                  # Clean data
│   └── gold/                    # Business data
├── main.py                      # Pipeline runner
├── requirements.txt             # Python dependencies
└── README.md                    # Project documentation
```

## 🚀 Setup Pipeline

### Bước 1: Tạo môi trường ảo

```bash
# Tạo virtual environment
python -m venv iot_env

# Kích hoạt (Windows)
.\iot_env\Scripts\Activate.ps1

# Kích hoạt (Linux/Mac)
source iot_env/bin/activate
```

### Bước 2: Cài đặt dependencies

```bash
# Cài đặt Python packages
pip install -r requirements.txt

# Nếu thiếu pyarrow cho Parquet support
pip install pyarrow
```

### Bước 3: Cấu hình environment

```python
# src/config/settings.py
class Config:
    def __init__(self, environment="local"):
        self.environment = environment
        self.base_path = "data"
        
        # Table paths
        self.bronze_table = f"{self.base_path}/bronze/raw_data.parquet"
        self.silver_table = f"{self.base_path}/silver/clean_data.parquet"
        self.gold_daily_table = f"{self.base_path}/gold/daily_aggregates.parquet"
        self.gold_hourly_table = f"{self.base_path}/gold/hourly_aggregates.parquet"
```

## 📊 Data Processing Layers

### Bronze Layer - Raw Data Ingestion

**File**: `src/layers/bronze.py`

**Chức năng**:
- Đọc raw CSV data
- Validate schema
- Add metadata (ingest_time, source_file)
- Basic data quality checks

**Key Methods**:
```python
def read_csv_data(self, input_path):
    """Đọc CSV với schema định sẵn"""
    
def process_bronze_data(self, df):
    """Xử lý raw data cho Bronze layer"""
    
def save_bronze_data(self, df, output_path):
    """Lưu data vào Bronze layer"""
```

### Silver Layer - Data Cleaning

**File**: `src/layers/silver.py`

**Chức năng**:
- Clean date/time columns
- Add derived time features (year, month, day, hour)
- Calculate electrical features (power factor, energy)
- Handle outliers và missing values
- Impute data theo time patterns

**Key Transformations**:
```python
# Transformation pipeline
df_transformed = (df
    .transform(with_timestamp)           # Tạo timestamp
    .transform(with_time_columns)        # Tạo time features
    .transform(with_electric_features)   # Tính electrical metrics
    .transform(null_out_of_range)        # Xử lý outliers
    .transform(impute_by_time_median))   # Impute missing values
```

### Gold Layer - Business Aggregations

**File**: `src/layers/gold.py`

**Chức năng**:
- Tạo hourly aggregations
- Tạo daily aggregations
- Calculate business KPIs
- Find peak consumption patterns

**Aggregations**:
```python
# Hourly aggregations
hourly_df = df.groupBy("date", "hour").agg(
    F.sum("energy_kWh").alias("energy_kWh"),
    F.avg("power_factor").alias("pf_avg"),
    F.sum("Sub_metering_1").alias("sm1_sum")
)

# Daily aggregations với peak hour detection
daily_df = hourly_df.groupBy("date").agg(
    F.sum("energy_kWh").alias("daily_energy"),
    F.max("energy_kWh").alias("peak_energy")
)
```

## 🤖 Machine Learning Forecasting

### Enhanced Forecasting Pipeline

**File**: `src/layers/forecasting.py`

**6 ML Models**:
1. **Naive Seasonal-7**: Baseline model (7-day pattern)
2. **SARIMAX**: Statistical time series model
3. **Prophet**: Facebook's forecasting tool
4. **XGBoost**: Gradient boosting (Best performer)
5. **LightGBM**: Microsoft's gradient boosting
6. **Ensemble**: Top-3 model combination

**Feature Engineering (19 features)**:
```python
def create_features(self, df):
    # Lag features
    for lag in [1, 2, 3, 7, 14]:
        df[f"lag_{lag}"] = df["energy_kWh"].shift(lag)
    
    # Rolling statistics
    for window in [3, 7, 14]:
        df[f"roll_mean_{window}"] = df["energy_kWh"].rolling(window).mean()
        df[f"roll_std_{window}"] = df["energy_kWh"].rolling(window).std()
    
    # Cyclical encoding
    df["dayofweek_sin"] = np.sin(2 * np.pi * df.index.dayofweek / 7)
    df["dayofweek_cos"] = np.cos(2 * np.pi * df.index.dayofweek / 7)
    df["month_sin"] = np.sin(2 * np.pi * df.index.month / 12)
    df["month_cos"] = np.cos(2 * np.pi * df.index.month / 12)
```

## 🏃‍♂️ Chạy Pipeline: Standalone Testing 
```bash

python test_enhanced_real.py

# Kết quả: 6 ML models, feature engineering, performance metrics
# Output: enhanced_forecast_data.csv ready for Power BI
```


## 📈 Performance Benchmarks

### Expected Results:
- **XGBoost**: RMSE=6.577, MAPE=20.61% (Best)
- **Naive Seasonal**: RMSE=6.583, MAPE=21.44%
- **Prophet**: RMSE=8.559, MAPE=29.23%

### Data Volume:
- **Bronze**: 2,075,259 rows (raw sensor data)
- **Silver**: 2,075,259 rows (cleaned data)
- **Gold Daily**: 1,442 rows (daily aggregates)
- **Gold Hourly**: 34,589 rows (hourly aggregates)
- **Forecasts**: 90 predictions (30 days × 3 models)

## 📊 Power BI Integration

### Export Data
```bash
# Export tất cả data cho Power BI
python export_for_powerbi.py
```

### Files được tạo:
- `bronze_raw_data.csv` - Raw sensor data
- `silver_clean_data.csv` - Clean data
- `gold_daily_aggregates.csv` - Daily KPIs
- `gold_hourly_aggregates.csv` - Hourly patterns
- `enhanced_forecast_data.csv` - ML predictions
- `DAX_Measures.txt` - Power BI formulas

### Import vào Power BI:
1. Mở Power BI Desktop
2. Get Data → Text/CSV
3. Navigate to `powerbi_data/` folder
4. Import các CSV files
5. Tạo relationships theo date columns
6. Use DAX measures từ `DAX_Measures.txt`

## 🐛 Troubleshooting

### Lỗi Spark trên Windows:
```bash
# COMMON ERROR: TypeError: 'JavaPackage' object is not callable
# Đây là lỗi phổ biến với PySpark trên Windows

# GIẢI PHÁP 1: Sử dụng standalone mode (KHUYẾN NGHỊ)
python test_enhanced_real.py

# GIẢI PHÁP 2: Downgrade PySpark
pip uninstall pyspark
pip install pyspark==3.4.0

# GIẢI PHÁP 3: Set JAVA_HOME (nếu có Java)
# set JAVA_HOME=C:\Program Files\Java\jdk-11.0.x
# set PATH=%JAVA_HOME%\bin;%PATH%

# GIẢI PHÁP 4: Sử dụng WSL (Windows Subsystem for Linux)
wsl
python main.py --stage full --enhanced
```

**⚠️ LƯU Ý**: Trên Windows, khuyến nghị sử dụng `test_enhanced_real.py` thay vì `main.py` để tránh Spark issues.

### Lỗi Missing Dependencies:
```bash
# Cài thêm packages
pip install pyarrow         # Cho Parquet support
pip install openpyxl        # Cho Excel support
pip install plotly          # Cho Prophet plots
```

### Lỗi Memory:
```bash
# Giảm data size trong config
FORECAST_HORIZON = 7        # Thay vì 30
TEST_DAYS = 7              # Thay vì 14
```

## 🔧 Customization

### Thêm Model mới:
```python
# Trong forecasting.py
def your_model_forecast(self, train_data, test_data):
    # Your model logic here
    return test_pred, future_pred

# Thêm vào pipeline
results.append({
    "model": "your_model",
    "rmse": self.rmse(y_test, test_pred),
    "mape": self.mape(y_test, test_pred)
})
```

### Thêm Features mới:
```python
# Trong create_features()
def create_features(self, df):
    # Existing features...
    
    # Your custom features
    df["weekend"] = (df.index.dayofweek >= 5).astype(int)
    df["season"] = df.index.month % 12 // 3 + 1
    return df
```

## 📚 Best Practices

### 1. Data Quality:
- Always validate input data schema
- Handle missing values appropriately
- Log data quality metrics
- Monitor outliers

### 2. Performance:
- Use Parquet format for large datasets
- Partition data by date for better performance
- Cache frequently used DataFrames
- Use appropriate Spark configurations

### 3. Model Management:
- Track model performance metrics
- Save model artifacts for reproducibility
- Use cross-validation for model selection
- Monitor model drift in production

### 4. Monitoring:
- Log all pipeline steps
- Set up alerts for failures
- Monitor data freshness
- Track business KPIs

## 🎯 Production Deployment

### Databricks:
1. Upload notebooks to Databricks workspace
2. Create cluster with appropriate configurations
3. Schedule jobs using Databricks Jobs
4. Set up monitoring and alerting

### Local Scheduler:
```bash
# Sử dụng cron (Linux/Mac) hoặc Task Scheduler (Windows)
# Chạy pipeline hàng ngày lúc 2AM
0 2 * * * /path/to/iot_env/bin/python /path/to/main.py --stage full
```

## 🔗 Tài liệu tham khảo

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Prophet Documentation](https://facebook.github.io/prophet/)
- [XGBoost Documentation](https://xgboost.readthedocs.io/)
- [Power BI Documentation](https://docs.microsoft.com/en-us/power-bi/)

---

**🎉 Chúc bạn build pipeline thành công!** 

Nếu gặp vấn đề, hãy check logs trong `pipeline.log` và follow troubleshooting guide trên.