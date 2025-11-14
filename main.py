"""
Main pipeline runner for IoT Power Analytics
Supports both local and Databricks execution
"""

import sys
import argparse
import logging
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from config.settings import Config
from layers.bronze import BronzeLayer
from layers.silver import SilverLayer
from layers.gold import GoldLayer
from layers.forecasting import ForecastingLayer
from utils.helpers import setup_logging


class IoTPowerPipeline:
    """Main IoT Power Analytics Pipeline"""
    
    def __init__(self, environment="local", log_level="INFO"):
        self.logger = setup_logging(log_level)
        self.logger.info(f"Initializing IoT Power Pipeline in {environment} environment")
        
        # Setup configuration
        self.config = Config(environment)
        self.spark = self.config.get_spark_session()
        
        # Initialize layers
        self.bronze_layer = BronzeLayer(self.spark, self.config)
        self.silver_layer = SilverLayer(self.spark, self.config)
        self.gold_layer = GoldLayer(self.spark, self.config)
        self.forecasting_layer = ForecastingLayer(self.spark, self.config)
        
        self.logger.info("Pipeline initialized successfully")
    
    def run_bronze_to_silver(self, input_path):
        """Run Bronze and Silver layers"""
        self.logger.info("Running Bronze to Silver pipeline...")
        
        # Bronze layer
        bronze_path, bronze_summary = self.bronze_layer.run_bronze_pipeline(input_path)
        self.logger.info(f"Bronze layer completed: {bronze_summary}")
        
        # Silver layer
        silver_path, quality_report = self.silver_layer.run_silver_pipeline(bronze_path)
        
        self.logger.info("Bronze to Silver pipeline completed successfully")
        return silver_path
    
    def run_silver_to_gold(self, silver_path=None):
        """Run Gold layer aggregations"""
        self.logger.info("Running Silver to Gold pipeline...")
        
        # Gold layer
        gold_results = self.gold_layer.run_gold_pipeline(silver_path)
        
        self.logger.info(f"Gold layer completed: {gold_results}")
        return gold_results
    
    def run_forecasting(self):
        """Run forecasting pipeline"""
        self.logger.info("Running forecasting pipeline...")
        
        try:
            results, forecast_df = self.forecasting_layer.run_forecasting_pipeline()
            self.logger.info("Forecasting completed successfully")
            return results, forecast_df
        except Exception as e:
            self.logger.error(f"Forecasting failed: {e}")
            return None, None
    
    def run_full_pipeline(self, input_path):
        """Run complete end-to-end pipeline"""
        self.logger.info("Starting full IoT Power Analytics Pipeline...")
        
        try:
            # Bronze to Silver
            silver_path = self.run_bronze_to_silver(input_path)
            
            # Silver to Gold
            gold_results = self.run_silver_to_gold(silver_path)
            
            # Forecasting (optional)
            forecast_results, forecast_df = self.run_forecasting()
            
            self.logger.info("Full pipeline completed successfully!")
            
            return {
                "silver_path": silver_path,
                "gold_results": gold_results,
                "forecast_results": forecast_results
            }
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise
        
        finally:
            # Cleanup
            if hasattr(self, 'spark'):
                self.spark.stop()
    
    def show_results(self):
        """Display pipeline results"""
        self.logger.info("Displaying pipeline results...")
        
        try:
            # Show Gold layer results
            if self.config.environment == "local":
                daily_df = self.spark.read.parquet(self.config.gold_daily_table)
                hourly_df = self.spark.read.parquet(self.config.gold_hourly_table)
            else:
                daily_df = self.spark.table(self.config.gold_daily_table)
                hourly_df = self.spark.table(self.config.gold_hourly_table)
            
            print("\n=== DAILY AGGREGATES (Top 10) ===")
            daily_df.orderBy("energy_kWh", ascending=False).show(10)
            
            print("\n=== HOURLY AGGREGATES (Sample) ===")
            hourly_df.orderBy("date", "hour").show(24)
            
            # Show forecast results if available
            try:
                if self.config.environment == "local":
                    forecast_df = self.spark.read.parquet(self.config.gold_forecast_table)
                else:
                    forecast_df = self.spark.table(self.config.gold_forecast_table)
                
                print("\n=== FORECAST RESULTS ===")
                forecast_df.orderBy("date", "model").show(20)
                
            except Exception:
                self.logger.info("No forecast results available")
            
        except Exception as e:
            self.logger.error(f"Failed to display results: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="IoT Power Analytics Pipeline")
    parser.add_argument("--environment", "-e", choices=["local", "databricks"], 
                       default="local", help="Execution environment")
    parser.add_argument("--input-path", "-i", required=True,
                       help="Path to input CSV file(s)")
    parser.add_argument("--stage", "-s", 
                       choices=["bronze", "silver", "gold", "forecast", "full", "show"],
                       default="full", help="Pipeline stage to run")
    parser.add_argument("--log-level", "-l", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = IoTPowerPipeline(args.environment, args.log_level)
    
    try:
        if args.stage == "bronze":
            pipeline.bronze_layer.run_bronze_pipeline(args.input_path)
        elif args.stage == "silver":
            pipeline.run_bronze_to_silver(args.input_path)
        elif args.stage == "gold":
            if args.input_path:
                pipeline.run_bronze_to_silver(args.input_path)
            pipeline.run_silver_to_gold()
        elif args.stage == "forecast":
            pipeline.run_forecasting()
        elif args.stage == "full":
            pipeline.run_full_pipeline(args.input_path)
        elif args.stage == "show":
            pipeline.show_results()
        
        print("\\n✅ Pipeline execution completed successfully!")
        
    except Exception as e:
        print(f"\\n❌ Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()