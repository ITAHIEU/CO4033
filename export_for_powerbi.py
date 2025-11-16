"""
Export processed IoT data from Parquet to CSV for Power BI visualization
"""

import pandas as pd
from pathlib import Path

def export_to_csv():
    """Export all processed data to CSV files for Power BI"""
    
    data_path = Path("iot_pipeline/data")
    output_path = Path("powerbi_data")
    
    # Create output directory
    output_path.mkdir(exist_ok=True)
    
    print("Exporting IoT Pipeline data to CSV for Power BI...")
    
    try:
        # Export Bronze layer (raw data)
        bronze_file = data_path / "bronze" / "raw_data.parquet"
        if bronze_file.exists():
            df_bronze = pd.read_parquet(bronze_file)
            df_bronze.to_csv(output_path / "bronze_raw_data.csv", index=False)
            print(f"✅ Bronze data exported: {len(df_bronze)} rows")
        
        # Export Silver layer (clean data)
        silver_file = data_path / "silver" / "clean_data.parquet"
        if silver_file.exists():
            df_silver = pd.read_parquet(silver_file)
            df_silver.to_csv(output_path / "silver_clean_data.csv", index=False)
            print(f"✅ Silver data exported: {len(df_silver)} rows")
        
        # Export Gold layer - Daily aggregates
        daily_file = data_path / "gold" / "daily_aggregates.parquet"
        if daily_file.exists():
            df_daily = pd.read_parquet(daily_file)
            df_daily.to_csv(output_path / "gold_daily_aggregates.csv", index=False)
            print(f"✅ Daily aggregates exported: {len(df_daily)} rows")
        
        # Export Gold layer - Hourly aggregates
        hourly_file = data_path / "gold" / "hourly_aggregates.parquet"
        if hourly_file.exists():
            df_hourly = pd.read_parquet(hourly_file)
            df_hourly.to_csv(output_path / "gold_hourly_aggregates.csv", index=False)
            print(f"✅ Hourly aggregates exported: {len(df_hourly)} rows")
        
        # Export Forecast data
        forecast_file = data_path / "gold" / "forecasts.parquet"
        if forecast_file.exists():
            df_forecast = pd.read_parquet(forecast_file)
            df_forecast.to_csv(output_path / "forecast_data.csv", index=False)
            print(f"✅ Forecast data exported: {len(df_forecast)} rows")
        
        print(f"\n🎉 All data exported to '{output_path}' folder!")
        print("\nFiles ready for Power BI:")
        for csv_file in output_path.glob("*.csv"):
            print(f"  📊 {csv_file.name}")
        
        print("\n📋 Next steps for Power BI:")
        print("1. Open Power BI Desktop")
        print("2. Click 'Get Data' → 'Text/CSV'")
        print(f"3. Navigate to: {output_path.absolute()}")
        print("4. Import the CSV files you need")
        print("5. Create visualizations and dashboards")
        
        return True
        
    except Exception as e:
        print(f"❌ Error exporting data: {e}")
        return False

if __name__ == "__main__":
    export_to_csv()