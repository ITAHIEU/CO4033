"""
Forecasting Layer - Time series forecasting and ML models
"""

import logging
import pandas as pd
import numpy as np
import warnings
from typing import Dict, List, Tuple

# Suppress warnings
warnings.filterwarnings("ignore")

from utils.helpers import setup_logging
from config.constants import FORECAST_HORIZON, TEST_DAYS, LAG_FEATURES, ROLLING_WINDOWS

# ML imports
try:
    import statsmodels.api as sm
    from prophet import Prophet
    import xgboost as xgb
    import lightgbm as lgb
    from sklearn.metrics import mean_squared_error
    FORECASTING_AVAILABLE = True
except ImportError:
    FORECASTING_AVAILABLE = False

logger = setup_logging()


class ForecastingLayer:
    """Handles time series forecasting"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config  
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if not FORECASTING_AVAILABLE:
            self.logger.warning("Forecasting libraries not available. Install required packages.")
    
    def read_gold_data(self, gold_path=None):
        """Read daily data from Gold layer"""
        gold_path = gold_path or self.config.gold_daily_table
        self.logger.info(f"Reading Gold data from: {gold_path}")
        
        if self.config.environment == "local":
            df = self.spark.read.parquet(gold_path)
        else:
            df = self.spark.table(gold_path)
        
        # Select relevant columns for forecasting
        df = df.select("date", "energy_kWh").orderBy("date")
        self.logger.info(f"Loaded {df.count()} rows for forecasting")
        return df
    
    def prepare_data(self, df) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Prepare data for forecasting"""
        self.logger.info("Preparing data for forecasting...")
        
        # Convert to Pandas
        pdf = df.toPandas().dropna().sort_values("date")
        pdf["date"] = pd.to_datetime(pdf["date"])
        pdf = pdf.set_index("date").asfreq("D")
        
        # Interpolate missing values
        pdf["energy_kWh"] = pdf["energy_kWh"].interpolate(limit_direction="both")
        
        # Create features
        pdf_features = self.create_features(pdf)
        pdf_clean = pdf_features.dropna()
        
        # Split data
        train = pdf_clean.iloc[:-TEST_DAYS].copy()
        test = pdf_clean.iloc[-TEST_DAYS:].copy()
        
        self.logger.info(f"Train data: {len(train)} rows, Test data: {len(test)} rows")
        return train, test
    
    def create_features(self, df: pd.DataFrame, target_col="energy_kWh") -> pd.DataFrame:
        """Create features for ML models"""
        data = df.copy()
        
        # Lag features
        for lag in LAG_FEATURES:
            data[f"lag_{lag}"] = data[target_col].shift(lag)
        
        # Rolling statistics
        for window in ROLLING_WINDOWS:
            shifted_data = data[target_col].shift(1)
            data[f"roll_mean_{window}"] = shifted_data.rolling(window).mean()
            data[f"roll_std_{window}"] = shifted_data.rolling(window).std()
        
        # Time features
        data["dayofweek"] = data.index.dayofweek
        data["month"] = data.index.month
        data["quarter"] = data.index.quarter
        data["dayofyear"] = data.index.dayofyear
        
        # Cyclical encoding
        data["dayofweek_sin"] = np.sin(2 * np.pi * data["dayofweek"] / 7)
        data["dayofweek_cos"] = np.cos(2 * np.pi * data["dayofweek"] / 7)
        data["month_sin"] = np.sin(2 * np.pi * data["month"] / 12)
        data["month_cos"] = np.cos(2 * np.pi * data["month"] / 12)
        
        return data
    
    def rmse(self, y_true, y_pred):
        """Calculate RMSE"""
        return float(np.sqrt(mean_squared_error(y_true, y_pred)))
    
    def mape(self, y_true, y_pred, eps=1e-6):
        """Calculate MAPE"""
        return float(np.mean(np.abs((y_true - y_pred) / (np.abs(y_true) + eps))) * 100)
    
    def naive_seasonal_forecast(self, train_data, test_data, periods=7):
        """Simple seasonal naive forecasting"""
        self.logger.info("Running Naive Seasonal forecast...")
        
        # Use last week pattern
        last_week = train_data["energy_kWh"].iloc[-periods:].values
        
        # Test predictions
        test_pred = train_data["energy_kWh"].shift(periods).loc[test_data.index]
        
        # Future predictions
        future_pred = np.tile(last_week, int(np.ceil(FORECAST_HORIZON / periods)))[:FORECAST_HORIZON]
        
        return test_pred, future_pred
    
    def prophet_forecast(self, train_data, test_data):
        """Prophet forecasting"""
        if not FORECASTING_AVAILABLE:
            return None, None
        
        self.logger.info("Running Prophet forecast...")
        
        try:
            # Prepare data for Prophet
            train_prophet = train_data.reset_index().rename(columns={"date": "ds", "energy_kWh": "y"})
            
            # Fit model
            model = Prophet(weekly_seasonality=True, daily_seasonality=False, yearly_seasonality=True)
            model.fit(train_prophet)
            
            # Test predictions
            test_prophet = test_data.reset_index()[["date"]].rename(columns={"date": "ds"})
            test_pred = model.predict(test_prophet)["yhat"].values
            
            # Future predictions
            future = model.make_future_dataframe(periods=FORECAST_HORIZON, freq="D", include_history=False)
            future_pred = model.predict(future)["yhat"].values
            
            return test_pred, future_pred
            
        except Exception as e:
            self.logger.error(f"Prophet forecast failed: {e}")
            return None, None
    
    def xgboost_forecast(self, train_data, test_data):
        """XGBoost forecasting"""
        if not FORECASTING_AVAILABLE:
            return None, None
        
        self.logger.info("Running XGBoost forecast...")
        
        try:
            # Prepare features
            feature_cols = [col for col in train_data.columns if col != "energy_kWh"]
            X_train, y_train = train_data[feature_cols], train_data["energy_kWh"]
            X_test, y_test = test_data[feature_cols], test_data["energy_kWh"]
            
            # Train model
            model = xgb.XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.05, random_state=42)
            model.fit(X_train, y_train)
            
            # Test predictions
            test_pred = model.predict(X_test)
            
            # Future predictions (recursive)
            future_pred = self.recursive_forecast(model, train_data, feature_cols, FORECAST_HORIZON)
            
            return test_pred, future_pred
            
        except Exception as e:
            self.logger.error(f"XGBoost forecast failed: {e}")
            return None, None
    
    def recursive_forecast(self, model, history_data, feature_cols, horizon):
        """Recursive forecasting for ML models"""
        forecast = []
        history = history_data.copy()
        
        for i in range(horizon):
            # Get features for prediction
            last_row_features = history.iloc[-1:][feature_cols]
            pred = model.predict(last_row_features)[0]
            forecast.append(pred)
            
            # Add prediction to history
            next_date = history.index[-1] + pd.Timedelta(days=1)
            new_row = pd.DataFrame([[pred]], columns=["energy_kWh"], index=[next_date])
            history = pd.concat([history, new_row])
            
            # Recreate features
            history = self.create_features(history)
        
        return np.array(forecast)
    
    def run_forecasting_pipeline(self):
        """Run complete forecasting pipeline"""
        if not FORECASTING_AVAILABLE:
            self.logger.error("Forecasting libraries not available")
            return None
        
        self.logger.info("Starting forecasting pipeline...")
        
        # Read and prepare data
        df = self.read_gold_data()
        train_data, test_data = self.prepare_data(df)
        
        # Run different forecasting methods
        models = {}
        results = []
        
        # Naive Seasonal
        test_pred, future_pred = self.naive_seasonal_forecast(train_data, test_data)
        if test_pred is not None:
            rmse = self.rmse(test_data["energy_kWh"], test_pred)
            mape = self.mape(test_data["energy_kWh"], test_pred)
            results.append({"model": "naive_seasonal", "rmse": rmse, "mape": mape})
            models["naive_seasonal"] = {"test": test_pred, "future": future_pred}
        
        # Prophet
        test_pred, future_pred = self.prophet_forecast(train_data, test_data)
        if test_pred is not None:
            rmse = self.rmse(test_data["energy_kWh"], test_pred)
            mape = self.mape(test_data["energy_kWh"], test_pred)
            results.append({"model": "prophet", "rmse": rmse, "mape": mape})
            models["prophet"] = {"test": test_pred, "future": future_pred}
        
        # XGBoost
        test_pred, future_pred = self.xgboost_forecast(train_data, test_data)
        if test_pred is not None:
            rmse = self.rmse(test_data["energy_kWh"], test_pred)
            mape = self.mape(test_data["energy_kWh"], test_pred)
            results.append({"model": "xgboost", "rmse": rmse, "mape": mape})
            models["xgboost"] = {"test": test_pred, "future": future_pred}
        
        # Create ensemble of top models
        if len(results) >= 2:
            results_df = pd.DataFrame(results).sort_values("rmse")
            top_models = results_df.head(2)["model"].tolist()
            
            ensemble_test = np.mean([models[model]["test"] for model in top_models], axis=0)
            ensemble_future = np.mean([models[model]["future"] for model in top_models], axis=0)
            
            rmse = self.rmse(test_data["energy_kWh"], ensemble_test)
            mape = self.mape(test_data["energy_kWh"], ensemble_test)
            results.append({"model": "ensemble", "rmse": rmse, "mape": mape})
            models["ensemble"] = {"test": ensemble_test, "future": ensemble_future}
        
        # Save forecasting results
        forecast_df = self.create_forecast_dataframe(models, train_data.index[-1])
        self.save_forecast_results(forecast_df)
        
        self.logger.info("Forecasting pipeline completed successfully")
        return pd.DataFrame(results), forecast_df
    
    def create_forecast_dataframe(self, models, last_date):
        """Create forecast DataFrame for saving"""
        future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=FORECAST_HORIZON, freq="D")
        
        forecast_records = []
        for model_name, predictions in models.items():
            for i, pred in enumerate(predictions["future"]):
                forecast_records.append({
                    "date": future_dates[i],
                    "model": model_name,
                    "yhat": pred,
                    "created_at": pd.Timestamp.utcnow()
                })
        
        return pd.DataFrame(forecast_records)
    
    def save_forecast_results(self, forecast_df):
        """Save forecasting results"""
        output_path = self.config.gold_forecast_table
        self.logger.info(f"Saving forecast results to: {output_path}")
        
        spark_df = self.spark.createDataFrame(forecast_df)
        
        if self.config.environment == "local":
            (spark_df.write.mode("overwrite")
             .option("overwriteSchema", "true")
             .parquet(output_path))
        else:
            (spark_df.write.mode("overwrite")
             .format("delta")
             .option("overwriteSchema", "true")
             .saveAsTable(output_path))
        
        self.logger.info("Forecast results saved successfully")