#  IoT Power Analytics Pipeline

##  Enhanced ML Forecasting Pipeline

Advanced IoT data processing pipeline với machine learning forecasting và Power BI integration.

###  Features
- **6 ML Models**: Naive, SARIMAX, Prophet, XGBoost, LightGBM, Ensemble
- **19 Features**: Lag, rolling statistics, cyclical encoding
- **Power BI Ready**: CSV export với DAX measures
- **Production Ready**: Spark support, configuration management

###  Quick Start
\\\ash
pip install -r requirements.txt
python test_enhanced_real.py
\\\

###  Power BI Integration
1. Run \xport_for_powerbi.py\
2. Import CSV files to Power BI
3. Use DAX measures from \DAX_Measures.txt\
4. Follow \PowerBI_Guide.md\

###  Best Model Performance
- **XGBoost**: RMSE=6.577, MAPE=20.61%
- **Feature Engineering**: 19 engineered features
- **Ensemble Method**: Top-3 model combination
