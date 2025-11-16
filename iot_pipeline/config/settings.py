"""
Configuration settings for IoT Power Pipeline
Supports both local and Databricks environments
"""

import os
from pathlib import Path

class Config:
    """Configuration class that adapts to local vs Databricks environment"""
    
    def __init__(self, environment="local"):
        self.environment = environment
        self.setup_paths()
        self.setup_spark_configs()
    
    def setup_paths(self):
        """Setup data paths based on environment"""
        if self.environment == "local":
            # Local file system paths
            self.base_path = Path(__file__).parent.parent / "data"
            self.bronze_path = self.base_path / "bronze"
            self.silver_path = self.base_path / "silver" 
            self.gold_path = self.base_path / "gold"
            self.checkpoint_path = self.base_path / "checkpoints"
            
            # Table names for local (use parquet files)
            self.bronze_table = str(self.bronze_path / "raw_data")
            self.silver_table = str(self.silver_path / "clean_data")
            self.gold_hourly_table = str(self.gold_path / "hourly_aggregates")
            self.gold_daily_table = str(self.gold_path / "daily_aggregates")
            self.gold_forecast_table = str(self.gold_path / "forecasts")
            
        else:
            # Databricks Unity Catalog paths
            self.catalog = "workspace"
            self.schema_name = "iot_power"
            self.volume_name = "store"
            
            self.base_path = f"/Volumes/{self.catalog}/{self.schema_name}/{self.volume_name}"
            self.bronze_path = f"{self.base_path}/bronze"
            self.silver_path = f"{self.base_path}/silver"
            self.gold_path = f"{self.base_path}/gold"
            self.checkpoint_path = f"{self.base_path}/checkpoints"
            
            # Unity Catalog table names
            self.bronze_table = f"{self.catalog}.{self.schema_name}.bronze_raw"
            self.silver_table = f"{self.catalog}.{self.schema_name}.silver_power_clean"
            self.gold_hourly_table = f"{self.catalog}.{self.schema_name}.gold_power_hourly"
            self.gold_daily_table = f"{self.catalog}.{self.schema_name}.gold_power_daily"
            self.gold_forecast_table = f"{self.catalog}.{self.schema_name}.gold_forecast_daily"
    
    def setup_spark_configs(self):
        """Setup Spark configuration based on environment"""
        if self.environment == "local":
            self.spark_configs = {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.shuffle.partitions": "4",  # Reduced for local
                "spark.sql.execution.arrow.pyspark.enabled": "true"
            }
        else:
            self.spark_configs = {
                "spark.sql.shuffle.partitions": "auto",
                "spark.databricks.delta.autoCompact.enabled": "true", 
                "spark.databricks.delta.optimizeWrite.enabled": "true"
            }
    
    def get_spark_session(self):
        """Get configured Spark session"""
        from pyspark.sql import SparkSession
        
        builder = SparkSession.builder.appName("IoT Power Pipeline")
        
        # Apply configurations
        for key, value in self.spark_configs.items():
            builder = builder.config(key, value)
        
        if self.environment == "local":
            # Local Spark configuration
            builder = builder.master("local[*]")
            
            # Simplified local configuration - use parquet instead of delta for now
            # Delta Lake has some compatibility issues in local mode
            pass
        
        return builder.getOrCreate()

# Default configuration instances
local_config = Config("local")
databricks_config = Config("databricks")