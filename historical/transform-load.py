from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

# Configure Spark
hadoop_home = "C:/hadoop"
os.environ['HADOOP_HOME'] = hadoop_home
os.environ['PATH'] = os.path.join(hadoop_home, 'bin') + os.pathsep + os.environ['PATH']
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create spark session
spark = SparkSession.builder.appName("XTD_Labs_Historical_Data_Processing").master("local").getOrCreate()
# Ignore Warnings
spark.sparkContext.setLogLevel("ERROR")

BRONZE_DIR = "data/bronze"

# Read all the raw json file from the bronze folder
raw_df = spark.read.json(f'./{BRONZE_DIR}/*json', multiLine=True)
# Count the number of data point per count
data_count = raw_df.count()
print(f"Total Data Point for 30mins interval found: {data_count}")

# SILVER LAYER I: FLATTENING THE DATA POINTS
exploded_df = raw_df.select(
    F.col("from").alias("timestamp"),
    F.explode(F.col("regions")).alias("region")
)

# Unpack
silver_df = exploded_df.select(
    "timestamp",
    F.col("region.regionid").alias('regionid'),
    F.col("region.shortname").alias('shortname'),
    F.col("region.dnoregion").alias('dno'),
    F.col("region.intensity.forecast").alias("intensity"),
    F.col("region.intensity.index").alias("index"),
    F.explode(F.col("region.generationmix")).alias("mix")
)

# SILVER LAYER II: pIVOT
silver_df_pivoted = silver_df.groupBy("regionid", "shortname", "dno", "timestamp", "intensity", "index").pivot("mix.fuel").agg(F.first("mix.perc"))

# GOLD LAYER: Aggregate to Daily Averages
print('Starting Gold Layer Aggregation')
gold_df = silver_df_pivoted.withColumn("date_recorded", F.to_date("timestamp")) \
    .groupBy("regionid", "date_recorded") \
    .agg(
        F.first("shortname").alias("shortname"),
        F.first("dno").alias("dno"),
        F.round(F.mean("intensity"), 2).alias("intensity_avg"),
        F.mode("index").alias("index_mode"),
        F.round(F.mean("biomass"), 2).alias("fuel_biomass"),
        F.round(F.mean("coal"), 2).alias("fuel_coal"),
        F.round(F.mean("gas"), 2).alias("fuel_gas"),
        F.round(F.mean("hydro"), 2).alias("fuel_hydro"),
        F.round(F.mean("imports"), 2).alias("fuel_imports"),
        F.round(F.mean("nuclear"), 2).alias("fuel_nuclear"),
        F.round(F.mean("other"), 2).alias("fuel_other"),
        F.round(F.mean("solar"), 2).alias("fuel_solar"),
        F.round(F.mean("wind"), 2).alias("fuel_wind")
    ).orderBy("date_recorded", "regionid")

final_count = gold_df.count()
print(f"Total Data Point processed and saved: {final_count}")

# Save
gold_df.write.csv("data/gold_carbon_historical.csv", header=True, mode="overwrite")

# See what we processed
gold_df.show(10)