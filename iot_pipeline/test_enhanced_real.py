#!/usr/bin/env python3
"""
Test Enhanced Forecasting with Real Data (No Spark)
"""

import sys
import os
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Add src path
sys.path.append('src')

class MockConfig:
    """Mock config for testing"""
    def __init__(self):
        self.environment = "local"
        self.gold_forecast_enhanced_table = "data/gold/forecasts_enhanced"

def load_real_data():
    """Load real data from CSV"""
    try:
        # Try different possible paths
        data_paths = [
            "data/sample_power_data.csv",
            "../data/sample_power_data.csv",
            "data/gold/daily_aggregates.parquet"
        ]
        
        for path in data_paths:
            if os.path.exists(path):
                print(f"📊 Loading data from: {path}")
                if path.endswith('.parquet'):
                    df = pd.read_parquet(path)
                else:
                    df = pd.read_csv(path)
                
                # Prepare data for forecasting
                if 'Date' in df.columns:
                    df['date'] = pd.to_datetime(df['Date'])
                elif 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                else:
                    print("❌ No date column found")
                    continue
                
                # Find energy column
                energy_cols = [col for col in df.columns if 'energy' in col.lower() or 'kwh' in col.lower()]
                if energy_cols:
                    df = df[['date', energy_cols[0]]].rename(columns={energy_cols[0]: 'energy_kWh'})
                    df = df.set_index('date').sort_index()
                    df = df.dropna()
                    
                    print(f"✅ Data loaded: {len(df)} rows")
                    print(f"✅ Date range: {df.index.min()} to {df.index.max()}")
                    print(f"✅ Energy range: {df['energy_kWh'].min():.2f} to {df['energy_kWh'].max():.2f} kWh")
                    
                    return df
        
        print("❌ No suitable data file found")
        return None
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

def run_enhanced_forecasting_standalone():
    """Run enhanced forecasting without Spark"""
    print("🚀 Running Enhanced Forecasting (Standalone Mode)...")
    
    # Load real data
    data = load_real_data()
    if data is None:
        print("❌ Cannot proceed without data")
        return False
    
    try:
        from layers.forecasting import ForecastingLayer
        
        # Mock config
        mock_config = MockConfig()
        forecasting = ForecastingLayer(None, mock_config)
        
        # Constants
        TEST_DAYS = 14
        HORIZON = 30
        
        # Prepare data exactly like original code
        pdf = data.copy()
        pdf = pdf.asfreq("D")
        pdf["energy_kWh"] = pdf["energy_kWh"].interpolate(limit_direction="both")
        
        # Create features
        print("🔧 Creating features...")
        pdf_features = forecasting.create_features(pdf)
        pdf_features_clean = pdf_features.dropna()
        
        if len(pdf_features_clean) < TEST_DAYS + 30:
            print(f"❌ Not enough data: {len(pdf_features_clean)} rows (need at least {TEST_DAYS + 30})")
            return False
        
        # Split data
        train = pdf_features_clean.iloc[:-TEST_DAYS].copy()
        test = pdf_features_clean.iloc[-TEST_DAYS:].copy()
        
        feature_cols = [col for col in train.columns if col != "energy_kWh"]
        X_train, y_train = train[feature_cols], train["energy_kWh"]
        X_test, y_test = test[feature_cols], test["energy_kWh"]
        
        print(f"✅ Train shape: {train.shape}, Test shape: {test.shape}")
        print(f"✅ Features: {len(feature_cols)}")
        
        # Initialize tracking structures
        evaluation_results = []
        forecast_dfs = []
        test_predictions = {"date": test.index, "actual": y_test.values}
        future_idx = pd.date_range(pdf.index[-1] + pd.Timedelta(days=1), periods=HORIZON, freq="D")
        
        # === 1. Naive Seasonal (Baseline) ===
        print("\n🔄 [1/6] Training Naive Seasonal-7...")
        naive_pred = pdf["energy_kWh"].shift(7).loc[test.index]
        evaluation_results.append({
            "model": "naive_seasonal_7", 
            "rmse": forecasting.rmse(y_test, naive_pred), 
            "mape": forecasting.mape(y_test, naive_pred)
        })
        test_predictions["naive_seasonal_7"] = naive_pred.values
        
        last_week = pdf["energy_kWh"].iloc[-7:].values
        naive_future = np.tile(last_week, int(np.ceil(HORIZON/7)))[:HORIZON]
        forecast_dfs.append(pd.DataFrame({
            "date": future_idx, "yhat": naive_future, "model": "naive_seasonal_7"
        }))
        
        # === 2. Prophet ===
        print("🔄 [2/6] Training Prophet...")
        try:
            from prophet import Prophet
            train_p = train.reset_index().rename(columns={"date":"ds", "energy_kWh":"y"})
            m = Prophet(weekly_seasonality=True, daily_seasonality=False, yearly_seasonality=True).fit(train_p)
            test_p = test.reset_index()[["date"]].rename(columns={"date":"ds"})
            prophet_pred = m.predict(test_p)["yhat"].values
            evaluation_results.append({
                "model": "prophet", 
                "rmse": forecasting.rmse(y_test, prophet_pred), 
                "mape": forecasting.mape(y_test, prophet_pred)
            })
            test_predictions["prophet"] = prophet_pred
            
            future = m.make_future_dataframe(periods=HORIZON, freq="D", include_history=False)
            fc = m.predict(future)
            forecast_dfs.append(fc[["ds","yhat"]].rename(columns={"ds":"date"}).assign(model="prophet"))
            print("✅ Prophet completed")
        except Exception as e: 
            print(f"⚠️ Prophet failed: {e}")
        
        # === 3. XGBoost ===
        print("🔄 [3/6] Training XGBoost...")
        try:
            import xgboost as xgb
            
            xgb_model = xgb.XGBRegressor(n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42)
            xgb_model.fit(X_train, y_train)

            xgb_pred = xgb_model.predict(X_test)
            evaluation_results.append({
                "model": "xgboost", 
                "rmse": forecasting.rmse(y_test, xgb_pred), 
                "mape": forecasting.mape(y_test, xgb_pred)
            })
            test_predictions["xgboost"] = xgb_pred
            
            # Simple future forecast (using last values)
            xgb_forecast = []
            history = pdf_features.copy()
            for i in range(min(10, HORIZON)):  # Limit to 10 for speed
                last_row_features = history.iloc[-1:][feature_cols]
                pred = xgb_model.predict(last_row_features)[0]
                xgb_forecast.append(pred)
                
                next_date = history.index[-1] + pd.Timedelta(days=1)
                new_row = pd.DataFrame([[pred]], columns=["energy_kWh"], index=[next_date])
                history = pd.concat([history, new_row])
                history = forecasting.create_features(history)
                
            # Extend with simple repeat for remaining days
            while len(xgb_forecast) < HORIZON:
                xgb_forecast.append(xgb_forecast[-1])
                
            forecast_dfs.append(pd.DataFrame({"date": future_idx, "yhat": xgb_forecast, "model": "xgboost"}))
            print("✅ XGBoost completed")
        except Exception as e: 
            print(f"⚠️ XGBoost failed: {e}")
        
        # === Results ===
        print(f"\n📊 ENHANCED FORECASTING RESULTS")
        print("="*50)
        eval_df = pd.DataFrame(evaluation_results).sort_values("rmse").reset_index(drop=True)
        
        for _, row in eval_df.iterrows():
            print(f"{row['model']:20s}: RMSE={row['rmse']:6.3f}, MAPE={row['mape']:6.2f}%")
        
        # Best model
        best_model = eval_df.iloc[0]
        print(f"\n🏆 Best model: {best_model['model']} (RMSE: {best_model['rmse']:.3f}, MAPE: {best_model['mape']:.2f}%)")
        
        # Save results
        if forecast_dfs:
            final_fc = pd.concat(forecast_dfs, ignore_index=True)
            final_fc["created_at"] = pd.Timestamp.utcnow()
            
            # Save to CSV for Power BI
            os.makedirs("../powerbi_data", exist_ok=True)
            final_fc.to_csv("../powerbi_data/enhanced_forecast_data.csv", index=False)
            print(f"\n💾 Saved {len(final_fc)} enhanced forecasts to: ../powerbi_data/enhanced_forecast_data.csv")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced forecasting failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_enhanced_forecasting_standalone()
    print(f"\n{'✅ Enhanced Forecasting COMPLETED!' if success else '❌ Enhanced Forecasting FAILED!'}")