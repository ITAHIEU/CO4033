"""
Utility functions for data transformations in IoT Power Pipeline
"""

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from config.constants import DATA_RANGES, NUM_COLS, TIMESTAMP_PATTERNS


def get_raw_schema():
    """Define the raw CSV schema"""
    return StructType([
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Global_active_power", DoubleType(), True),
        StructField("Global_reactive_power", DoubleType(), True),
        StructField("Voltage", DoubleType(), True),
        StructField("Global_intensity", DoubleType(), True),
        StructField("Sub_metering_1", DoubleType(), True),
        StructField("Sub_metering_2", DoubleType(), True),
        StructField("Sub_metering_3", DoubleType(), True),
        StructField("datetime", StringType(), True),  # Extra column in file
    ])


def clean_date_time_columns(df):
    """Clean Date and Time columns by removing quotes and trimming"""
    for col in ["Date", "Time"]:
        df = df.withColumn(col, F.trim(F.regexp_replace(F.col(col), r'^"|"$', "")))
    return df


def with_timestamp(df):
    """Parse timestamp from Date and Time columns with multiple format support"""
    # Create temporary datetime string
    df_with_dt = df.withColumn("dt", F.concat_ws(" ", F.col("Date"), F.col("Time")))
    
    # Try multiple timestamp patterns
    ts_expr = F.coalesce(*[
        F.expr(f"try_to_timestamp(dt, '{pattern}')")
        for pattern in TIMESTAMP_PATTERNS
    ])
    
    return (df_with_dt
            .withColumn("timestamp", ts_expr)
            .drop("dt")
            .filter(F.col("timestamp").isNotNull()))


def with_time_columns(df):
    """Add time-based columns from timestamp"""
    df_with_time = (df
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("day", F.dayofmonth("timestamp"))
        .withColumn("dow_raw", F.dayofweek("timestamp"))  # 1=Sun..7=Sat
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("minute", F.minute("timestamp"))
    )
    
    # Convert to Monday=1 ... Sunday=7 format
    return df_with_time.withColumn(
        "dow", F.when(F.col("dow_raw") == 1, 7).otherwise(F.col("dow_raw") - 1)
    ).drop("dow_raw")


def with_electric_features(df):
    """Calculate electrical engineering features"""
    return (df
        .withColumn("S_pq", F.sqrt(
            F.pow("Global_active_power", 2) + F.pow("Global_reactive_power", 2)
        ))
        .withColumn("S_vi", (F.col("Voltage") * F.col("Global_intensity")) / 1000.0)
        .withColumn("pf_pq", F.when(
            F.col("S_pq") > 0, F.col("Global_active_power") / F.col("S_pq")
        ))
        .withColumn("pf_vi", F.when(
            F.col("S_vi") > 0, F.col("Global_active_power") / F.col("S_vi")
        ))
    )


def with_quality_flags(df, ranges=DATA_RANGES):
    """Add data quality flags based on valid ranges"""
    for col, (lo, hi) in ranges.items():
        df = df.withColumn(
            f"valid_{col}",
            (F.col(col).isNull()) | ((F.col(col) >= lo) & (F.col(col) <= hi))
        )
    return df


def null_out_of_range(df, ranges=DATA_RANGES):
    """Set out-of-range values to null"""
    for col, (lo, hi) in ranges.items():
        df = df.withColumn(col, F.when(
            (F.col(col) >= lo) & (F.col(col) <= hi), F.col(col)
        ).otherwise(F.lit(None)))
    return df


def impute_by_time_median(df, num_cols=NUM_COLS):
    """Impute missing values using time-based median (dow, hour)"""
    # Calculate median by time window
    for col in num_cols:
        med_time = F.expr(f"percentile_approx({col}, 0.5) over (partition by dow, hour)")
        df = df.withColumn(f"{col}_med_time", med_time)
    
    # Calculate global medians as fallback
    medians = df.select([
        F.expr(f"percentile_approx({col}, 0.5)").alias(col) 
        for col in num_cols
    ]).first().asDict()
    
    # Fill nulls: time median first, then global median
    for col in num_cols:
        df = df.withColumn(col, F.coalesce(
            F.col(col),
            F.col(f"{col}_med_time"),
            F.lit(medians[col])
        )).drop(f"{col}_med_time")
    
    return df


def quality_report(df, ranges=DATA_RANGES):
    """Generate data quality report"""
    exprs = [
        (1 - F.avg(F.col(f"valid_{col}").cast("double"))).alias(f"invalid_{col}")
        for col in ranges
    ]
    return df.select(exprs)


def calculate_energy_consumption(df):
    """Calculate energy consumption using time deltas"""
    w = Window.orderBy("timestamp")
    
    return (df
        .withColumn("ts_next", F.lead("timestamp").over(w))
        .withColumn("delta_min_raw", 
                   (F.col("ts_next").cast("long") - F.col("timestamp").cast("long")) / 60.0)
        .withColumn("delta_min",
                   F.when(F.col("delta_min_raw").isNull(), 1.0)
                    .otherwise(F.least(F.greatest(F.col("delta_min_raw"), F.lit(0.0)), F.lit(10.0))))
        .withColumn("energy_kWh", F.col("Global_active_power") * (F.col("delta_min") / 60.0))
        .drop("ts_next", "delta_min_raw")
    )