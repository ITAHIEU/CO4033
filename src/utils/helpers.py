"""
Helper utilities for IoT Power Pipeline
"""

import logging
from pathlib import Path
from pyspark.sql import functions as F


def setup_logging(log_level="INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline.log')
        ]
    )
    return logging.getLogger(__name__)


def ensure_directory_exists(path):
    """Ensure directory exists, create if not"""
    Path(path).mkdir(parents=True, exist_ok=True)


def validate_data_quality(df, threshold=0.95):
    """Validate data quality meets minimum threshold"""
    total_rows = df.count()
    if total_rows == 0:
        return False, "No data found"
    
    # Check for null timestamps
    valid_timestamps = df.filter(F.col("timestamp").isNotNull()).count()
    timestamp_quality = valid_timestamps / total_rows
    
    if timestamp_quality < threshold:
        return False, f"Timestamp quality {timestamp_quality:.2%} below threshold {threshold:.2%}"
    
    return True, f"Data quality acceptable: {timestamp_quality:.2%}"


def get_data_summary(df):
    """Get basic data summary statistics"""
    return {
        "total_rows": df.count(),
        "date_range": df.agg(
            F.min("timestamp").alias("min_date"),
            F.max("timestamp").alias("max_date")
        ).first().asDict(),
        "null_counts": {
            col: df.filter(F.col(col).isNull()).count()
            for col in df.columns
        }
    }