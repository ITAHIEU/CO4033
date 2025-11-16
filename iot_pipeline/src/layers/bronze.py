"""
Bronze Layer - Raw data ingestion and basic cleaning
"""

import logging
from pyspark.sql import functions as F
from utils.transformations import get_raw_schema, clean_date_time_columns
from utils.helpers import setup_logging, validate_data_quality, get_data_summary

logger = setup_logging()


class BronzeLayer:
    """Handles raw data ingestion into Bronze layer"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def read_csv_data(self, input_path):
        """Read raw CSV data with proper schema"""
        self.logger.info(f"Reading CSV data from: {input_path}")
        
        df = (self.spark.read.format("csv")
              .option("header", True)
              .option("sep", ",")
              .option("nullValue", "?")
              .schema(get_raw_schema())
              .load(input_path))
        
        self.logger.info(f"Loaded {df.count()} rows from CSV")
        return df
    
    def process_bronze_data(self, df):
        """Process raw data for Bronze layer"""
        self.logger.info("Processing Bronze data...")
        
        # Clean date/time columns
        df_clean = clean_date_time_columns(df)
        
        # Add ingestion metadata
        df_bronze = (df_clean
                    .withColumn("_ingest_time", F.current_timestamp())
                    .withColumn("_source_file", F.input_file_name()))
        
        # Validate data quality
        is_valid, message = validate_data_quality(df_bronze)
        if not is_valid:
            self.logger.warning(f"Data quality warning: {message}")
        else:
            self.logger.info(f"Data quality check passed: {message}")
        
        return df_bronze
    
    def save_bronze_data(self, df, output_path=None):
        """Save data to Bronze layer"""
        output_path = output_path or self.config.bronze_table
        self.logger.info(f"Saving Bronze data to: {output_path}")
        
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
        
        self.logger.info("Bronze data saved successfully")
        return output_path
    
    def run_bronze_pipeline(self, input_path):
        """Run complete Bronze layer pipeline"""
        self.logger.info("Starting Bronze layer pipeline...")
        
        # Read raw data
        raw_df = self.read_csv_data(input_path)
        
        # Process data
        bronze_df = self.process_bronze_data(raw_df)
        
        # Get summary statistics
        summary = get_data_summary(bronze_df)
        self.logger.info(f"Bronze data summary: {summary}")
        
        # Save data
        output_path = self.save_bronze_data(bronze_df)
        
        self.logger.info("Bronze layer pipeline completed successfully")
        return output_path, summary