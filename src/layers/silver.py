"""
Silver Layer - Data cleaning, transformation, and quality enhancement
"""

import logging
from pyspark.sql import functions as F
from utils.transformations import (
    with_timestamp, with_time_columns, with_electric_features,
    null_out_of_range, impute_by_time_median, quality_report
)
from utils.helpers import setup_logging

logger = setup_logging()


class SilverLayer:
    """Handles data transformation and cleaning for Silver layer"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def read_bronze_data(self, bronze_path=None):
        """Read data from Bronze layer"""
        bronze_path = bronze_path or self.config.bronze_table
        self.logger.info(f"Reading Bronze data from: {bronze_path}")
        
        if self.config.environment == "local":
            df = self.spark.read.parquet(bronze_path)
        else:
            df = self.spark.table(bronze_path)
        
        self.logger.info(f"Loaded {df.count()} rows from Bronze")
        return df
    
    def silver_transform(self, df):
        """Apply all Silver layer transformations"""
        self.logger.info("Applying Silver transformations...")
        
        # Chain transformations
        df_transformed = (df
                         .transform(with_timestamp)
                         .transform(with_time_columns)  
                         .transform(with_electric_features)
                         .transform(null_out_of_range)
                         .transform(impute_by_time_median))
        
        self.logger.info(f"Silver transformations completed. Rows: {df_transformed.count()}")
        return df_transformed
    
    def generate_quality_report(self, df):
        """Generate data quality report"""
        self.logger.info("Generating quality report...")
        report = quality_report(df)
        return report
    
    def save_silver_data(self, df, output_path=None):
        """Save data to Silver layer"""
        output_path = output_path or self.config.silver_table
        self.logger.info(f"Saving Silver data to: {output_path}")
        
        if self.config.environment == "local":
            # Save as Parquet for local
            (df.write.mode("overwrite")
             .option("overwriteSchema", "true")
             .parquet(output_path))
        else:
            # Save as Delta table for Databricks
            (df.write.mode("overwrite")
             .format("delta")
             .option("overwriteSchema", "true")
             .saveAsTable(output_path))
        
        self.logger.info("Silver data saved successfully")
        return output_path
    
    def run_silver_pipeline(self, bronze_path=None):
        """Run complete Silver layer pipeline"""
        self.logger.info("Starting Silver layer pipeline...")
        
        # Read Bronze data
        bronze_df = self.read_bronze_data(bronze_path)
        
        # Apply transformations
        silver_df = self.silver_transform(bronze_df)
        
        # Generate quality report
        quality_report_df = self.generate_quality_report(silver_df)
        self.logger.info("Quality report generated")
        
        # Save Silver data
        output_path = self.save_silver_data(silver_df)
        
        self.logger.info("Silver layer pipeline completed successfully")
        return output_path, quality_report_df