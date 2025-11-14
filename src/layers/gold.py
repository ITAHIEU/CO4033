"""
Gold Layer - Business aggregations and analytics
"""

import logging
from pyspark.sql import functions as F, Window
from utils.transformations import calculate_energy_consumption
from utils.helpers import setup_logging

logger = setup_logging()


class GoldLayer:
    """Handles business aggregations and analytics for Gold layer"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def read_silver_data(self, silver_path=None):
        """Read data from Silver layer"""
        silver_path = silver_path or self.config.silver_table
        self.logger.info(f"Reading Silver data from: {silver_path}")
        
        if self.config.environment == "local":
            df = self.spark.read.parquet(silver_path)
        else:
            df = self.spark.table(silver_path)
        
        self.logger.info(f"Loaded {df.count()} rows from Silver")
        return df
    
    def create_hourly_aggregates(self, df):
        """Create hourly energy consumption aggregates"""
        self.logger.info("Creating hourly aggregates...")
        
        # Calculate energy consumption
        df_energy = calculate_energy_consumption(df)
        
        # Hourly aggregations
        hourly_df = (df_energy
                    .groupBy("date", "year", "month", "day", "dow", "hour")
                    .agg(
                        F.sum("energy_kWh").alias("energy_kWh"),
                        F.avg("pf_pq").alias("pf_avg"),
                        F.sum("Sub_metering_1").alias("sm1_sum"),
                        F.sum("Sub_metering_2").alias("sm2_sum"),
                        F.sum("Sub_metering_3").alias("sm3_sum"),
                        F.avg("Voltage").alias("voltage_avg"),
                        F.avg("Global_intensity").alias("intensity_avg"),
                    )
                    .withColumn("submeters_sum", 
                               F.col("sm1_sum") + F.col("sm2_sum") + F.col("sm3_sum"))
                    .withColumn("submeter_share", 
                               F.when(F.col("energy_kWh") > 0, 
                                     F.col("submeters_sum") / F.col("energy_kWh"))))
        
        self.logger.info(f"Created {hourly_df.count()} hourly records")
        return hourly_df
    
    def create_daily_aggregates(self, hourly_df):
        """Create daily aggregates from hourly data"""
        self.logger.info("Creating daily aggregates...")
        
        # Find peak hour each day
        w_peak = Window.partitionBy("date").orderBy(
            F.col("energy_kWh").desc(), F.col("hour").asc()
        )
        peak_df = (hourly_df
                  .withColumn("rn", F.row_number().over(w_peak))
                  .where("rn=1")
                  .select("date", 
                         F.col("hour").alias("peak_hour"),
                         F.col("energy_kWh").alias("peak_hour_kWh")))
        
        # Daily aggregations
        daily_core = (hourly_df
                     .groupBy("date", "year", "month", "day", "dow")
                     .agg(
                         F.sum("energy_kWh").alias("energy_kWh"),
                         F.avg("pf_avg").alias("pf_avg"),
                         F.sum("sm1_sum").alias("sm1_sum"),
                         F.sum("sm2_sum").alias("sm2_sum"),
                         F.sum("sm3_sum").alias("sm3_sum"),
                         F.avg("voltage_avg").alias("voltage_avg"),
                         F.avg("intensity_avg").alias("intensity_avg"),
                     )
                     .withColumn("submeters_sum",
                                F.col("sm1_sum") + F.col("sm2_sum") + F.col("sm3_sum"))
                     .withColumn("submeter_share",
                                F.when(F.col("energy_kWh") > 0,
                                      F.col("submeters_sum") / F.col("energy_kWh"))))
        
        # Join with peak hour data
        daily_df = (daily_core
                   .join(peak_df, on="date", how="left")
                   .select("date", "year", "month", "day", "dow", "energy_kWh",
                          "pf_avg", "submeter_share", "sm1_sum", "sm2_sum", "sm3_sum",
                          "peak_hour", "peak_hour_kWh", "voltage_avg", "intensity_avg"))
        
        self.logger.info(f"Created {daily_df.count()} daily records")
        return daily_df
    
    def create_kpi_aggregates(self, daily_df):
        """Create KPI aggregates (monthly, yearly)"""
        self.logger.info("Creating KPI aggregates...")
        
        # Monthly KPIs
        monthly_kpis = (daily_df
                       .groupBy("year", "month")
                       .agg(
                           F.sum("energy_kWh").alias("energy_kWh"),
                           F.avg("pf_avg").alias("pf_avg"),
                           F.avg("submeter_share").alias("submeter_share_avg"),
                           F.avg("voltage_avg").alias("voltage_avg"),
                           F.avg("intensity_avg").alias("intensity_avg"),
                       )
                       .withColumn("_level", F.lit("monthly")))
        
        # Yearly KPIs
        yearly_kpis = (daily_df
                      .groupBy("year")
                      .agg(
                          F.sum("energy_kWh").alias("energy_kWh"),
                          F.avg("pf_avg").alias("pf_avg"),
                          F.avg("submeter_share").alias("submeter_share_avg"),
                          F.avg("voltage_avg").alias("voltage_avg"),
                          F.avg("intensity_avg").alias("intensity_avg"),
                      )
                      .withColumn("_level", F.lit("yearly"))
                      .withColumn("month", F.lit(None).cast("int")))
        
        # Combine KPIs
        kpi_df = monthly_kpis.union(yearly_kpis)
        
        self.logger.info(f"Created {kpi_df.count()} KPI records")
        return kpi_df
    
    def save_gold_data(self, df, table_name):
        """Save data to Gold layer"""
        self.logger.info(f"Saving Gold data to: {table_name}")
        
        if self.config.environment == "local":
            # Save as Parquet for local
            (df.write.mode("overwrite")
             .option("overwriteSchema", "true")
             .parquet(table_name))
        else:
            # Save as Delta table for Databricks
            (df.write.mode("overwrite")
             .format("delta")
             .option("overwriteSchema", "true")
             .saveAsTable(table_name))
        
        self.logger.info(f"Gold data saved successfully to {table_name}")
    
    def run_gold_pipeline(self, silver_path=None):
        """Run complete Gold layer pipeline"""
        self.logger.info("Starting Gold layer pipeline...")
        
        # Read Silver data
        silver_df = self.read_silver_data(silver_path)
        
        # Create hourly aggregates
        hourly_df = self.create_hourly_aggregates(silver_df)
        self.save_gold_data(hourly_df, self.config.gold_hourly_table)
        
        # Create daily aggregates
        daily_df = self.create_daily_aggregates(hourly_df)
        self.save_gold_data(daily_df, self.config.gold_daily_table)
        
        # Create KPI aggregates (optional - save to daily table with _level column)
        kpi_df = self.create_kpi_aggregates(daily_df)
        
        self.logger.info("Gold layer pipeline completed successfully")
        return {
            "hourly_path": self.config.gold_hourly_table,
            "daily_path": self.config.gold_daily_table,
            "hourly_count": hourly_df.count(),
            "daily_count": daily_df.count(),
            "kpi_count": kpi_df.count()
        }